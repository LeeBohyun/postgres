/*-------------------------------------------------------------------------
 *
 * control.c
 *    Completion-checkpoint state for WAL-logged pg_upgrade.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * src/backend/access/pgupgrade/control.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/pgupgrade_control.h"
#include "storage/shmem.h"
#include "storage/spin.h"

typedef struct PgUpgradeControlData
{
	XLogRecPtr	completion_lsn;
	slock_t		mutex;
} PgUpgradeControlData;

static PgUpgradeControlData *upgrade_control;

static void pgupgrade_shmem_request(void *arg);
static void pgupgrade_shmem_init(void *arg);

const ShmemCallbacks PgUpgradeShmemCallbacks = {
	.request_fn = pgupgrade_shmem_request,
	.init_fn = pgupgrade_shmem_init,
};

static void
pgupgrade_shmem_request(void *arg)
{
	ShmemRequestStruct(.name = "WAL Upgrade Control",
					   .size = sizeof(PgUpgradeControlData),
					   .ptr = (void **) &upgrade_control,
		);
}

static void
pgupgrade_shmem_init(void *arg)
{
	MemSet(upgrade_control, 0, sizeof(PgUpgradeControlData));
	upgrade_control->completion_lsn = InvalidXLogRecPtr;
	SpinLockInit(&upgrade_control->mutex);
}

/*
 * Save the committed COMPLETE LSN for the shutdown checkpoint that marks the
 * new cluster's pg_control as upgrade-finalized.
 */
void
ArmUpgradeCompletionCheckpoint(XLogRecPtr complete_lsn)
{
	bool		already_armed;

	Assert(!XLogRecPtrIsInvalid(complete_lsn));

	SpinLockAcquire(&upgrade_control->mutex);
	already_armed = !XLogRecPtrIsInvalid(upgrade_control->completion_lsn);
	if (!already_armed)
		upgrade_control->completion_lsn = complete_lsn;
	SpinLockRelease(&upgrade_control->mutex);

	if (already_armed)
		elog(PANIC, "pg_upgrade completion checkpoint is already armed");
}

/* Verify that the shutdown checkpoint follows COMPLETE. */
XLogRecPtr
PgUpgradeGetCompletionLSN(XLogRecPtr checkpoint_lsn)
{
	XLogRecPtr	completion_lsn;

	SpinLockAcquire(&upgrade_control->mutex);
	completion_lsn = upgrade_control->completion_lsn;
	SpinLockRelease(&upgrade_control->mutex);
	if (!XLogRecPtrIsInvalid(completion_lsn) &&
		checkpoint_lsn < completion_lsn)
		elog(PANIC, "pg_upgrade completion checkpoint precedes COMPLETE");

	return completion_lsn;
}

void
PgUpgradeClearCompletionLSN(void)
{
	SpinLockAcquire(&upgrade_control->mutex);
	upgrade_control->completion_lsn = InvalidXLogRecPtr;
	SpinLockRelease(&upgrade_control->mutex);
}
