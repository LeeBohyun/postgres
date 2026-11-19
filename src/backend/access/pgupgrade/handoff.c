/*-------------------------------------------------------------------------
 *
 * handoff.c
 *    Old-cluster handoff for WAL-logged pg_upgrade.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * src/backend/access/pgupgrade/handoff.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "access/pgupgrade_handoff.h"
#include "access/pgupgrade_handoff_receipt.h"
#include "access/pgupgrade_wal.h"
#include "access/timeline.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "access/xlogrecovery.h"
#include "catalog/pg_control.h"
#include "common/pg_upgrade_data.h"
#include "common/pg_upgrade_records.h"
#include "miscadmin.h"
#include "replication/slot.h"
#include "storage/fd.h"

/* HANDOFF state restored from the persisted shutdown checkpoint. */
static XLogRecPtr restorePgUpgradeHandoffLSN = InvalidXLogRecPtr;
static uint32 restorePgUpgradeNewMajor;

static XLogRecPtr
write_handoff_record(uint32 new_major_version)
{
	xl_pg_upgrade_handoff xlrec;
	XLogRecPtr	recptr;

	memset(&xlrec, 0, sizeof(xlrec));
	xlrec.old_major_version = PG_VERSION_NUM / 10000;
	xlrec.new_major_version = new_major_version;
	xlrec.handoff_time = (pg_time_t) time(NULL);

	recptr = PgUpgradeInsertRecord(XLOG_UPGRADE_HANDOFF, &xlrec,
								   SizeOfPgUpgradeHandoff);

	ereport(LOG,
			(errmsg("pg_upgrade handoff trigger recorded at %X/%X "
					"(old major version %u, new major version %u)",
					LSN_FORMAT_ARGS(recptr),
					xlrec.old_major_version, new_major_version)));

	return recptr;
}

bool
PgUpgradeHandoffIsArmed(void)
{
	struct stat st;

	if (stat(PG_UPGRADE_HANDOFF_SIGNAL_FILE, &st) == 0)
		return true;
	if (errno != ENOENT)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not access pg_upgrade handoff signal file \"%s\": %m",
						PG_UPGRADE_HANDOFF_SIGNAL_FILE)));
	return false;
}

bool
PgUpgradeHandoffSlotsAreFrozen(void)
{
	return XLogRecPtrIsValid(GetPgUpgradeHandoffRetention()) ||
		PgUpgradeHandoffIsArmed();
}

XLogRecPtr
PgUpgradeEmitHandoffIfArmed(void)
{
	const char *path = PG_UPGRADE_HANDOFF_SIGNAL_FILE;
	FILE	   *f;
	int			new_major = 0;

	f = AllocateFile(path, "r");
	if (f == NULL)
	{
		if (errno != ENOENT)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not open pg_upgrade handoff signal file \"%s\": %m",
							path)));
		return InvalidXLogRecPtr;
	}

	if (fscanf(f, "%d", &new_major) != 1 || new_major <= 0)
	{
		FreeFile(f);
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid data in pg_upgrade handoff signal file \"%s\"",
						path)));
	}
	FreeFile(f);

	PgUpgradePrepareHandoffSlots();
	(void) RequestXLogSwitch(false);

	durable_unlink(path, FATAL);

	return write_handoff_record((uint32) new_major);
}

typedef struct PgUpgradeHandoffProbe
{
	TimeLineID	tli;
	XLogRecPtr	endptr;
} PgUpgradeHandoffProbe;

static int
read_local_handoff_page(XLogReaderState *state,
						XLogRecPtr target_page_ptr, int req_len,
						XLogRecPtr target_rec_ptr, char *read_buf)
{
	PgUpgradeHandoffProbe *probe = state->private_data;
	XLogSegNo	segno;
	uint32		offset;
	char		path[MAXPGPATH];
	int			count = XLOG_BLCKSZ;
	int			fd;
	ssize_t		nread;

	(void) target_rec_ptr;
	if (XLogRecPtrIsValid(probe->endptr))
	{
		if (target_page_ptr + req_len > probe->endptr)
			return -1;
		if (target_page_ptr + count > probe->endptr)
			count = probe->endptr - target_page_ptr;
	}

	XLByteToSeg(target_page_ptr, segno, state->segcxt.ws_segsize);
	offset = XLogSegmentOffset(target_page_ptr, state->segcxt.ws_segsize);
	XLogFilePath(path, probe->tli, segno, state->segcxt.ws_segsize);

	fd = BasicOpenFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return -1;
	nread = pg_pread(fd, read_buf, count, (pgoff_t) offset);
	close(fd);
	if (nread != count)
		return -1;

	state->seg.ws_tli = probe->tli;
	return count;
}

static bool
read_handoff_record(XLogRecPtr handoff_lsn, TimeLineID tli,
					XLogRecPtr read_end_lsn, uint64 system_identifier,
					List *timeline_history, xl_pg_upgrade_handoff *handoff)
{
	PgUpgradeHandoffProbe probe;
	XLogReaderState *reader;
	XLogRecord *record;
	char	   *errormsg;
	bool		is_handoff = false;

	probe.tli = tli;
	probe.endptr = read_end_lsn;
	reader = XLogReaderAllocate(wal_segment_size, NULL,
								XL_ROUTINE(.page_read = read_local_handoff_page),
								&probe);
	if (reader == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating a WAL reading processor.")));

	reader->system_identifier = system_identifier;
	XLogBeginRead(reader, handoff_lsn);
	record = XLogReadRecord(reader, &errormsg);
	if (record != NULL &&
		reader->ReadRecPtr == handoff_lsn &&
		record->xl_rmid == RM_PG_UPGRADE_ID &&
		(record->xl_info & ~XLR_INFO_MASK) == XLOG_UPGRADE_HANDOFF)
	{
		if (XLogRecGetDataLen(reader) != SizeOfPgUpgradeHandoff)
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("invalid pg_upgrade handoff record length")));
		/* Accept an earlier-timeline page copied into a promoted segment. */
		if (!tliInHistory(reader->latestPageTLI, timeline_history) ||
			tliOfPointInHistory(handoff_lsn, timeline_history) != tli)
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("pg_upgrade handoff and shutdown checkpoint are on different timelines")));
		memcpy(handoff, XLogRecGetData(reader), sizeof(*handoff));
		is_handoff = true;
	}
	XLogReaderFree(reader);

	return is_handoff;
}

void
PgUpgradeFindPersistedHandoff(const ControlFileData *control_file,
							  XLogReaderState *checkpoint_reader,
							  const CheckPoint *checkpoint,
							  TimeLineID checkpoint_tli,
							  List *timeline_history,
							  bool standby_mode_requested)
{
	XLogRecPtr	checkpoint_lsn = checkpoint_reader->ReadRecPtr;
	XLogRecPtr	checkpoint_end_lsn;
	XLogRecPtr	preceding_lsn;
	xl_pg_upgrade_handoff handoff;

	if ((XLogRecGetInfo(checkpoint_reader) & ~XLR_INFO_MASK) !=
		XLOG_CHECKPOINT_SHUTDOWN ||
		checkpoint_lsn != control_file->checkPoint)
		return;

	checkpoint_end_lsn = checkpoint_reader->EndRecPtr;
	preceding_lsn = XLogRecGetPrev(checkpoint_reader);
	if (!XLogRecPtrIsValid(preceding_lsn))
		return;
	if (!read_handoff_record(preceding_lsn, checkpoint_tli,
							 checkpoint_lsn, control_file->system_identifier,
							 timeline_history, &handoff))
		return;
	if (handoff.old_major_version != PG_VERSION_NUM / 10000)
	{
		if (!standby_mode_requested)
			return;
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("pg_upgrade handoff record has old major version %u, expected %u",
						handoff.old_major_version, PG_VERSION_NUM / 10000)));
	}

	if (checkpoint->redo != checkpoint_lsn)
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("pg_upgrade handoff checkpoint has an invalid REDO location")));
	if (checkpoint->ThisTimeLineID != checkpoint_tli)
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("pg_upgrade handoff checkpoint has timeline %u, expected %u",
						checkpoint->ThisTimeLineID, checkpoint_tli)));

	if (control_file->minRecoveryPoint > checkpoint_end_lsn)
		return;

	if (!standby_mode_requested)
	{
		if (control_file->state == DB_SHUTDOWNED_IN_RECOVERY ||
			control_file->state == DB_IN_ARCHIVE_RECOVERY)
			ereport(FATAL,
					(errmsg("cannot promote a standby paused for pg_upgrade handoff"),
					 errhint("Restore standby mode and execute pg_wal_replay_resume() before promotion.")));
		return;
	}

	if (control_file->minRecoveryPoint != checkpoint_end_lsn)
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("pg_upgrade handoff checkpoint has an invalid minimum recovery point"),
				 errdetail("The minimum recovery point %X/%08X does not match the checkpoint end %X/%08X.",
						   LSN_FORMAT_ARGS(control_file->minRecoveryPoint),
						   LSN_FORMAT_ARGS(checkpoint_end_lsn))));

	if (control_file->minRecoveryPointTLI != checkpoint_tli)
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("pg_upgrade handoff checkpoint has an invalid minimum recovery timeline"),
				 errdetail("The minimum recovery timeline is %u, but the checkpoint timeline is %u.",
						   control_file->minRecoveryPointTLI, checkpoint_tli)));

	restorePgUpgradeHandoffLSN = preceding_lsn;
	restorePgUpgradeNewMajor = handoff.new_major_version;
}

void
PgUpgradeResetHandoffRecovery(void)
{
	restorePgUpgradeHandoffLSN = InvalidXLogRecPtr;
	restorePgUpgradeNewMajor = 0;
}

/* Initialize the shared HANDOFF fields while info_lck is held. */
void
PgUpgradeInitializeHandoffRecovery(void)
{
	XLogRecoveryCtl->pgUpgradeHandoffOwnsPause = false;
	XLogRecoveryCtl->pgUpgradeHandoffCancelRequested = false;
	if (XLogRecPtrIsValid(restorePgUpgradeHandoffLSN))
	{
		XLogRecoveryCtl->pgUpgradeHandoffPhase =
			PG_UPGRADE_HANDOFF_PENDING;
		XLogRecoveryCtl->pgUpgradeHandoffRetainLSN =
			restorePgUpgradeHandoffLSN;
	}
	else
	{
		XLogRecoveryCtl->pgUpgradeHandoffPhase = PG_UPGRADE_HANDOFF_NONE;
		XLogRecoveryCtl->pgUpgradeHandoffRetainLSN = InvalidXLogRecPtr;
	}
}

bool
PgUpgradeRestoreHandoff(XLogRecPtr min_recovery_point)
{
	uint32		new_major;

	if (!XLogRecPtrIsValid(restorePgUpgradeHandoffLSN))
		return false;

	new_major = restorePgUpgradeNewMajor;
	PgUpgradeResetHandoffRecovery();

	if (!HotStandbyActive())
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("restored the final pg_upgrade checkpoint before hot standby was active"),
				 errdetail("The primary is upgrading to major version %u.",
						   new_major)));

	if (!PgUpgradeWaitForHandoffSlots(min_recovery_point))
	{
		CancelPgUpgradeHandoff();
		ereport(LOG,
				(errmsg("cancelled the pg_upgrade handoff at operator request")));
		return false;
	}

	CompletePgUpgradeHandoff();
	ereport(LOG,
			(errmsg("restored pg_upgrade pause from the final old-major shutdown checkpoint"),
			 errdetail("The primary is upgrading to major version %u.",
					   new_major)));
	return true;
}

void
PgUpgradeCancelHandoffIfRequested(void)
{
	if (!PgUpgradeHandoffCancellationRequested())
		return;

	CancelPgUpgradeHandoff();
	ereport(LOG,
			(errmsg("cancelled the pg_upgrade handoff at operator request")));
}

/* Update HANDOFF ownership and cancellation while info_lck is held. */
void
PgUpgradeUpdateHandoffPause(bool recovery_pause)
{
	if (!recovery_pause &&
		XLogRecoveryCtl->pgUpgradeHandoffPhase != PG_UPGRADE_HANDOFF_NONE)
		XLogRecoveryCtl->pgUpgradeHandoffCancelRequested = true;
	else if (recovery_pause &&
			 XLogRecoveryCtl->pgUpgradeHandoffPhase ==
			 PG_UPGRADE_HANDOFF_DURABLE_PAUSE)
		XLogRecoveryCtl->pgUpgradeHandoffOwnsPause = false;
}

void
PgUpgradeCheckPromotion(void)
{
	if (XLogRecPtrIsValid(GetPgUpgradeHandoffRetention()))
		ereport(FATAL,
				(errmsg("cannot promote during pg_upgrade handoff")));
}

void
PgUpgradeDiscardStaleHandoff(DBState state)
{
	if (state == DB_SHUTDOWNED || state == DB_SHUTDOWNED_IN_RECOVERY ||
		!PgUpgradeHandoffIsArmed())
		return;

	durable_unlink(PG_UPGRADE_HANDOFF_SIGNAL_FILE, FATAL);
	ereport(LOG,
			(errmsg("removed stale pg_upgrade handoff request after server crash")));
}

void
BeginPgUpgradeHandoff(XLogRecPtr lsn)
{
	bool		invalid_phase;

	Assert(XLogRecPtrIsValid(lsn));
	if (PromoteIsTriggered())
		ereport(FATAL,
				(errmsg("cannot promote during pg_upgrade handoff")));

	SpinLockAcquire(&XLogRecoveryCtl->info_lck);
	invalid_phase = XLogRecoveryCtl->pgUpgradeHandoffPhase !=
		PG_UPGRADE_HANDOFF_NONE;
	if (!invalid_phase)
	{
		XLogRecoveryCtl->pgUpgradeHandoffPhase =
			PG_UPGRADE_HANDOFF_PENDING;
		XLogRecoveryCtl->pgUpgradeHandoffRetainLSN = lsn;
		XLogRecoveryCtl->pgUpgradeHandoffOwnsPause = false;
		XLogRecoveryCtl->pgUpgradeHandoffCancelRequested = false;
	}
	SpinLockRelease(&XLogRecoveryCtl->info_lck);

	if (invalid_phase)
		elog(PANIC, "duplicate pg_upgrade handoff before its checkpoint");

	PgUpgradePrepareHandoffSlots();
}

void
CompletePgUpgradeHandoff(void)
{
	bool		invalid_phase;

	SpinLockAcquire(&XLogRecoveryCtl->info_lck);
	invalid_phase = XLogRecoveryCtl->pgUpgradeHandoffPhase !=
		PG_UPGRADE_HANDOFF_PENDING;
	if (!invalid_phase)
	{
		XLogRecoveryCtl->pgUpgradeHandoffPhase =
			PG_UPGRADE_HANDOFF_DURABLE_PAUSE;
		if (XLogRecoveryCtl->recoveryPauseState == RECOVERY_NOT_PAUSED)
		{
			XLogRecoveryCtl->recoveryPauseState = RECOVERY_PAUSE_REQUESTED;
			XLogRecoveryCtl->pgUpgradeHandoffOwnsPause = true;
		}
		else
			XLogRecoveryCtl->pgUpgradeHandoffOwnsPause = false;
	}
	SpinLockRelease(&XLogRecoveryCtl->info_lck);

	if (invalid_phase)
		elog(PANIC, "pg_upgrade handoff checkpoint has no pending handoff");
}

void
CancelPgUpgradeHandoff(void)
{
	bool		wake = false;

	PgUpgradeCancelHandoffSlots();
	LWLockAcquire(ReplicationSlotAllocationLock, LW_EXCLUSIVE);
	ReplicationSlotsClearPgUpgradeHandoffFloors();

	SpinLockAcquire(&XLogRecoveryCtl->info_lck);
	if (XLogRecoveryCtl->pgUpgradeHandoffPhase != PG_UPGRADE_HANDOFF_NONE)
	{
		XLogRecoveryCtl->pgUpgradeHandoffPhase = PG_UPGRADE_HANDOFF_NONE;
		XLogRecoveryCtl->pgUpgradeHandoffRetainLSN = InvalidXLogRecPtr;
		if (XLogRecoveryCtl->pgUpgradeHandoffOwnsPause &&
			XLogRecoveryCtl->recoveryPauseState != RECOVERY_NOT_PAUSED)
		{
			XLogRecoveryCtl->recoveryPauseState = RECOVERY_NOT_PAUSED;
			wake = true;
		}
		XLogRecoveryCtl->pgUpgradeHandoffOwnsPause = false;
		XLogRecoveryCtl->pgUpgradeHandoffCancelRequested = false;
	}
	SpinLockRelease(&XLogRecoveryCtl->info_lck);
	LWLockRelease(ReplicationSlotAllocationLock);

	if (wake)
		ConditionVariableBroadcast(&XLogRecoveryCtl->recoveryNotPausedCV);
}

XLogRecPtr
GetPgUpgradeHandoffRetention(void)
{
	XLogRecPtr	lsn;

	SpinLockAcquire(&XLogRecoveryCtl->info_lck);
	lsn = XLogRecoveryCtl->pgUpgradeHandoffRetainLSN;
	SpinLockRelease(&XLogRecoveryCtl->info_lck);

	return lsn;
}

bool
PgUpgradeHandoffCancellationRequested(void)
{
	bool		requested;

	SpinLockAcquire(&XLogRecoveryCtl->info_lck);
	requested = XLogRecoveryCtl->pgUpgradeHandoffCancelRequested;
	SpinLockRelease(&XLogRecoveryCtl->info_lck);

	return requested;
}
