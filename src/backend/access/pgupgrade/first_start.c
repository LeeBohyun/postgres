/*-------------------------------------------------------------------------
 *
 * first_start.c
 *    Emit upgrade WAL before connections or other database writers can run.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * src/backend/access/pgupgrade/first_start.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>

#include "access/pgupgrade_emit.h"
#include "access/pgupgrade_first_start.h"
#include "access/pgupgrade_retention_slots.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "catalog/pg_control.h"
#include "common/pg_upgrade_records.h"
#include "miscadmin.h"
#include "postmaster/startup.h"
#include "storage/fd.h"

static bool in_progress;
static XLogRecPtr first_start_redo = InvalidXLogRecPtr;

static bool
input_exists(const char *path)
{
	struct stat st;

	if (lstat(path, &st) == 0)
		return true;
	if (errno != ENOENT)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not inspect upgrade input \"%s\": %m", path)));
	return false;
}

bool
PgUpgradeFirstStartPending(void)
{
	return input_exists(PG_UPGRADE_RELINK_FILE);
}

bool
PgUpgradeFirstStartInProgress(void)
{
	return in_progress;
}

XLogRecPtr
PgUpgradeFirstStartRedo(void)
{
	Assert(AmStartupProcess() && in_progress);
	Assert(XLogRecPtrIsValid(first_start_redo));
	return first_start_redo;
}

/* Reject interrupted emission before ordinary recovery can replay upgrade WAL. */
void
PgUpgradePrepareFirstStart(void)
{
	ControlFileData control;
	bool		pending = PgUpgradeFirstStartPending();

	GetControlFileCopy(&control);

	if (control.backupEndPoint == PG_INT64_MAX)
		ereport(FATAL,
				(errmsg("cannot restart an interrupted upgrade WAL emission"),
				 errhint("Use a fresh target cluster for another upgrade attempt.")));

	/*
	 * pg_upgrade creates pg_upgrade_relink.tmp before preparing the new
	 * cluster so ordinary startup rejects the target if preparation fails.
	 * Allow its binary-upgrade servers to restore catalogs and migrate slots
	 * while the file exists. Renaming it to pg_upgrade_relink marks
	 * preparation complete.
	 */
	if (!IsBinaryUpgrade && input_exists(PG_UPGRADE_RELINK_FILE ".tmp"))
		ereport(FATAL,
				(errmsg("upgrade preparation is incomplete")));
	if (!pending)
		return;
	if (IsBinaryUpgrade || !AmStartupProcess() || control.state != DB_SHUTDOWNED ||
		input_exists(STANDBY_SIGNAL_FILE) || input_exists(RECOVERY_SIGNAL_FILE) ||
		input_exists(BACKUP_LABEL_FILE))
		ereport(FATAL,
				(errmsg("pending upgrade WAL requires exclusive primary startup")));

	(void) PgUpgradePrepareEmission();

	/*
	 * Save the initial redo position so KeepLogSeg() retains its segment and
	 * all later startup and upgrade WAL during checkpoint cleanup.
	 */
	first_start_redo = control.checkPointCopy.redo;
	BeginControlFileUpgrade();
	in_progress = true;
}

void
PgUpgradeRunFirstStart(void)
{
	XLogRecPtr	complete_lsn;
	XLogRecPtr	checkpoint_end;

	if (!in_progress)
		return;

	ereport(LOG,
			(errmsg("emitting upgrade WAL during exclusive startup")));

	PgUpgradeResetPhysicalSlotsForEmission(PgUpgradeFirstStartRedo());

	complete_lsn = PgUpgradeEmitWalFile();
	checkpoint_end = PgUpgradeCompletionCheckPoint(complete_lsn);
	ProcessStartupProcInterrupts();

	/* Remove the commands durably before allowing ordinary restarts. */
	durable_unlink(PG_UPGRADE_RELINK_FILE, ERROR);
	FinishControlFileUpgrade();
	in_progress = false;
	first_start_redo = InvalidXLogRecPtr;
	ereport(LOG,
			(errmsg("upgrade WAL emission complete at %X/%X",
					LSN_FORMAT_ARGS(checkpoint_end))));
}
