/*-------------------------------------------------------------------------
 *
 * handoff_receipt.c
 *    Wait for durable receipt of a WAL-logged pg_upgrade handoff.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * src/backend/access/pgupgrade/handoff_receipt.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/pgupgrade_handoff.h"
#include "access/pgupgrade_handoff_receipt.h"
#include "access/xlogrecovery.h"
#include "miscadmin.h"
#include "postmaster/startup.h"
#include "replication/slot.h"
#include "utils/builtins.h"

typedef struct PgUpgradeHandoffSlot
{
	ReplicationSlot *slot;
	NameData	name;
	bool		confirmed;
} PgUpgradeHandoffSlot;

static bool handoff_slots_prepared = false;
static PgUpgradeHandoffSlot *handoff_slots = NULL;
static int	nhandoff_slots = 0;

static bool
handoff_slot_identity_matches(ReplicationSlot *slot,
							  const NameData *expected_name)
{
	return SlotIsPhysical(slot) &&
		slot->data.persistency == RS_PERSISTENT &&
		namestrcmp(&slot->data.name, NameStr(*expected_name)) == 0;
}

static PgUpgradeHandoffSlot *
snapshot_handoff_slots(int *nslots)
{
	PgUpgradeHandoffSlot *slots;
	int			max_slots = max_replication_slots + max_repack_replication_slots;

	*nslots = 0;
	/* Cascaded standby acknowledgement is added by the next stack layer. */
	if (RecoveryInProgress())
		return NULL;
	if (max_slots == 0)
		return NULL;

	slots = palloc0_array(PgUpgradeHandoffSlot, max_slots);
	LWLockAcquire(ReplicationSlotAllocationLock, LW_EXCLUSIVE);
	LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);

	for (int i = 0; i < max_slots; i++)
	{
		ReplicationSlot *slot = &ReplicationSlotCtl->replication_slots[i];

		if (!slot->in_use)
			continue;

		SpinLockAcquire(&slot->mutex);
		if (SlotIsPhysical(slot) &&
			slot->data.persistency == RS_PERSISTENT &&
			namestrcmp(&slot->data.name, CONFLICT_DETECTION_SLOT) != 0)
		{
			if (slot->data.invalidated != RS_INVAL_NONE ||
				!XLogRecPtrIsValid(slot->data.restart_lsn))
			{
				SpinLockRelease(&slot->mutex);
				ereport(FATAL,
						(errmsg("physical replication slot \"%s\" is invalid during pg_upgrade handoff",
								NameStr(slot->data.name))));
			}
			slots[*nslots].slot = slot;
			memcpy(&slots[*nslots].name, &slot->data.name, sizeof(NameData));
			(*nslots)++;
		}
		SpinLockRelease(&slot->mutex);
	}

	LWLockRelease(ReplicationSlotControlLock);
	LWLockRelease(ReplicationSlotAllocationLock);
	return slots;
}

void
PgUpgradePrepareHandoffSlots(void)
{
	if (handoff_slots_prepared)
		elog(PANIC, "pg_upgrade handoff slots already prepared");

	handoff_slots = snapshot_handoff_slots(&nhandoff_slots);
	handoff_slots_prepared = true;
}

void
PgUpgradeCancelHandoffSlots(void)
{
	if (handoff_slots != NULL)
		pfree(handoff_slots);
	handoff_slots = NULL;
	nhandoff_slots = 0;
	handoff_slots_prepared = false;
}

static void
check_handoff_interrupts(void)
{
	if (AmStartupProcess())
	{
		ProcessStartupProcInterrupts();
		if (IsPromoteSignaled())
			ereport(FATAL,
					(errmsg("cannot promote while waiting for pg_upgrade handoff checkpoint receipt")));
	}
	else
		CHECK_FOR_INTERRUPTS();
}

static bool
wait_for_handoff_slots(PgUpgradeHandoffSlot *slots, int nslots,
					   XLogRecPtr checkpoint_end_lsn)
{
	int			remaining = nslots;

	if (remaining > 0)
		ereport(LOG,
				(errmsg("waiting for %d physical replication slots to confirm durable pg_upgrade handoff receipt through %X/%X",
						nslots, LSN_FORMAT_ARGS(checkpoint_end_lsn))));

	for (;;)
	{
		check_handoff_interrupts();
		if (PgUpgradeHandoffCancellationRequested())
			return false;
		if (AmStartupProcess())
		{
			TimeLineID	replay_tli;
			XLogRecPtr	replay_lsn = GetXLogReplayRecPtr(&replay_tli);

			EnsurePgUpgradeHandoffWalReceiver(replay_tli, replay_lsn);
		}
		if (remaining > 0)
		{
			LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
			for (int i = 0; i < nslots; i++)
			{
				ReplicationSlot *slot = slots[i].slot;
				bool		invalidated;
				bool		matches;
				bool		received;
				XLogRecPtr	restart_lsn;

				if (slots[i].confirmed)
					continue;
				if (!slot->in_use)
					ereport(FATAL,
							(errmsg("physical replication slot \"%s\" was removed during pg_upgrade handoff",
									NameStr(slots[i].name))));

				SpinLockAcquire(&slot->mutex);
				matches = handoff_slot_identity_matches(slot, &slots[i].name);
				invalidated = slot->data.invalidated != RS_INVAL_NONE;
				restart_lsn = slot->data.restart_lsn;
				received = matches && !invalidated &&
					XLogRecPtrIsValid(restart_lsn) &&
					restart_lsn >= checkpoint_end_lsn;
				if (received)
				{
					if (!XLogRecPtrIsValid(slot->handoff_restart_lsn_floor) ||
						slot->handoff_restart_lsn_floor < checkpoint_end_lsn)
						slot->handoff_restart_lsn_floor = checkpoint_end_lsn;
					slot->just_dirtied = true;
					slot->dirty = true;
				}
				SpinLockRelease(&slot->mutex);
				if (!matches)
					ereport(FATAL,
							(errmsg("physical replication slot \"%s\" changed during pg_upgrade handoff",
									NameStr(slots[i].name))));
				if (invalidated || !XLogRecPtrIsValid(restart_lsn))
					ereport(FATAL,
							(errmsg("physical replication slot \"%s\" became invalid during pg_upgrade handoff",
									NameStr(slots[i].name))));
				if (!received)
					continue;

				slots[i].confirmed = true;
				remaining--;
			}
			LWLockRelease(ReplicationSlotControlLock);
		}

		if (remaining == 0)
			break;
		pg_usleep(10000L);
	}

	if (nslots > 0)
	{
		CheckPointReplicationSlots(false);
		if (PgUpgradeHandoffCancellationRequested())
			return false;

		LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
		for (int i = 0; i < nslots; i++)
		{
			ReplicationSlot *slot = slots[i].slot;
			bool		invalidated;
			bool		matches;
			XLogRecPtr	saved_restart_lsn;

			if (!slot->in_use)
				ereport(FATAL,
						(errmsg("physical replication slot \"%s\" was removed while persisting pg_upgrade handoff receipt",
								NameStr(slots[i].name))));

			SpinLockAcquire(&slot->mutex);
			matches = handoff_slot_identity_matches(slot, &slots[i].name);
			invalidated = slot->data.invalidated != RS_INVAL_NONE;
			saved_restart_lsn = slot->last_saved_restart_lsn;
			SpinLockRelease(&slot->mutex);

			if (!matches || invalidated ||
				saved_restart_lsn < checkpoint_end_lsn)
				ereport(FATAL,
						(errmsg("could not persist pg_upgrade handoff receipt for physical replication slot \"%s\"",
								NameStr(slots[i].name))));
		}
		LWLockRelease(ReplicationSlotControlLock);

		ereport(LOG,
				(errmsg("all physical replication slots confirmed durable pg_upgrade handoff receipt")));
	}

	return true;
}

bool
PgUpgradeWaitForHandoffSlots(XLogRecPtr checkpoint_end_lsn)
{
	bool		completed;

	if (!handoff_slots_prepared)
		PgUpgradePrepareHandoffSlots();

	completed = wait_for_handoff_slots(handoff_slots,
									   nhandoff_slots,
									   checkpoint_end_lsn);
	PgUpgradeCancelHandoffSlots();
	return completed;
}
