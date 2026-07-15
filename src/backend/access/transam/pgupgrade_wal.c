/*
 * pgupgrade_wal.c
 *
 * WAL redo and emit functions for RM_PG_UPGRADE_ID records.
 *
 * This file implements the redo path and pg_waldump support for the five
 * WAL record types written by pg_upgrade --wal-log-upgrade:
 *
 *   XLOG_PG_UPGRADE_START    (0x00) — window open, write PG_VERSION
 *   XLOG_PG_UPGRADE_COMPLETE (0x10) — window close, informational
 *   XLOG_UPGRADE_SLRU_DATA   (0x20) — bulk SLRU segment image
 *   XLOG_UPGRADE_RELFILE_DATA(0x30) — bulk relation file segment image
 *   XLOG_UPGRADE_RAWFILE     (0x50) — verbatim non-relation file image
 *
 * The XID/OID/multixact counters are NOT a WAL record: they are transplanted
 * into pg_control (via pg_resetwal) before the end-of-upgrade checkpoint, which
 * therefore carries them, and recovery reproduces them from that checkpoint.
 *
 * src/backend/access/transam/pgupgrade_wal.c
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/clog.h"		/* CLOGUpgradeRestoreSegment */
#include "access/multixact.h"	/* MultiXact*UpgradeRestoreSegment */
#include "access/pgupgrade_wal.h"
#include "access/slru.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogrecovery.h"	/* pgUpgradeReplayInProgress */
#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"	/* wal_segment_close */
#include "catalog/pg_control.h"
#include "catalog/pg_tablespace_d.h"
#include "common/file_perm.h"	/* pg_dir_create_mode */
#include "common/relpath.h"		/* RelFileLocator, ForkNumber */
#include "miscadmin.h"
#include "storage/bufmgr.h"		/* buffer-manager RELFILE_DATA redo */
#include "storage/smgr.h"		/* smgr create for empty relfiles */
#include "storage/bufpage.h"	/* PageSetLSN */
#include "storage/fd.h"
#include "storage/copydir.h"	/* copydir() for WAL segment migration */
#include "storage/ipc.h"		/* proc_exit() for the quarantine hold */
#include "storage/lwlock.h"
#include "utils/elog.h"

/* -------------------------------------------------------------------------
 * pg_upgrade WAL-replay-based atomicity check
 * -------------------------------------------------------------------------
 */

/*
 * PerformWalUpgradeIfNeeded() — scan pg_wal/ for the pg_upgrade START/COMPLETE
 * markers and decide whether StartupXLOG() should crash-recover the upgrade.
 *
 * pg_upgrade --wal-log-upgrade uses the following protocol:
 *
 *   1. Transplant the XID/OID/multixact counters into pg_control, then restart
 *      and CHECKPOINT (this is CN, the recovery anchor; it carries the counters)
 *   2. Write START, the full physical image (DIRSKEL/RELFILE/RAWFILE/SLRU),
 *      COMPLETE, pg_switch_wal()
 *   3. stop_postmaster_immediate() — no checkpoint, WAL intact in pg_wal/
 *   4. wipe the on-disk data image (files only; the skeleton is in DIRSKEL)
 *   5. pg_resetwal --upgrade-recovery=CN,REDO sets pg_control:
 *        checkPoint = CN (end-of-upgrade checkpoint record LSN)
 *        state      = DB_IN_PRODUCTION   ← triggers crash recovery
 *
 * The upgrade WAL simply stays in pg_wal/ (there is no rename to a side
 * directory): the START/COMPLETE records in the WAL are themselves the signal.
 * This also lets a physical standby receive the same WAL stream.
 *
 * On startup:
 *   START + COMPLETE present -> arm the bootstrap and return true so
 *      StartupXLOG() crash-recovers from CN through COMPLETE.  Replay applies
 *      only the end-of-upgrade images and reconstructs the entire cluster from
 *      WAL — the data directory need only contain the folder skeleton (rebuilt
 *      by DIRSKEL redo), pg_control, and the top-level PG_VERSION.
 *   START but NO COMPLETE -> crash mid-upgrade: FATAL "re-run pg_upgrade"
 *      (the old cluster is intact).
 *   no START -> return false — normal startup, nothing to do.
 *
 * Called from StartupProcessMain() before StartupXLOG().
 * Returns true if an upgrade should be replayed.
 */
/*
 * Private state for the XLogReader used by upgrade_wal_has_complete().
 */
typedef struct UpgradeWalReadPrivate
{
	char		dir[MAXPGPATH]; /* WAL directory to read segments from */
	TimeLineID	tli;			/* timeline (always 1 for upgrade WAL) */
	XLogRecPtr	endptr;			/* one past the last byte of available WAL */
} UpgradeWalReadPrivate;

static void
UpgradeWalSegOpen(XLogReaderState *state, XLogSegNo nextSegNo, TimeLineID *tli_p)
{
	UpgradeWalReadPrivate *priv = (UpgradeWalReadPrivate *) state->private_data;
	char		fname[MAXFNAMELEN];
	char		path[MAXPGPATH];

	XLogFileName(fname, priv->tli, nextSegNo, state->segcxt.ws_segsize);
	snprintf(path, sizeof(path), "%s/%s", priv->dir, fname);
	state->seg.ws_file = BasicOpenFile(path, O_RDONLY | PG_BINARY);
	if (state->seg.ws_file < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open upgrade WAL segment \"%s\": %m", path)));
}

static void
UpgradeWalSegClose(XLogReaderState *state)
{
	if (state->seg.ws_file >= 0)
		close(state->seg.ws_file);
	state->seg.ws_file = -1;
}

static int
UpgradeWalPageRead(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
				   XLogRecPtr targetRecPtr, char *readBuf)
{
	UpgradeWalReadPrivate *priv = (UpgradeWalReadPrivate *) state->private_data;
	int			count = XLOG_BLCKSZ;
	WALReadError errinfo;

	/* Never read past the last available byte of WAL. */
	if (targetPagePtr + XLOG_BLCKSZ > priv->endptr)
	{
		if (targetPagePtr + reqLen > priv->endptr)
			return -1;
		count = (int) (priv->endptr - targetPagePtr);
	}

	if (!WALRead(state, readBuf, targetPagePtr, count, priv->tli, &errinfo))
		return -1;

	return count;
}

/*
 * Properly parse the WAL in "waldir" and locate the pg_upgrade markers plus the
 * end-of-upgrade checkpoint (CN) that recovery must anchor at.
 *
 * Out-params:
 *   found_start / found_complete — the START / COMPLETE markers were seen.
 *   cn        — the CheckPoint struct of the LAST online checkpoint that
 *               precedes START.  This is CN, the recovery anchor written by
 *               pg_upgrade right before the full-page-image burst; it carries
 *               the transplanted XID/OID/multixact counters.
 *   cn_lsn    — the record LSN of that checkpoint (goes to ControlFile.checkPoint).
 *   complete_lsn — the record LSN of XLOG_PG_UPGRADE_COMPLETE (InvalidXLogRecPtr
 *               if not found).  The CALLER decides "already applied?" by
 *               comparing the control file's current checkpoint LSN against
 *               this: once first startup has replayed through COMPLETE and
 *               finalized, pg_control's checkpoint advances PAST complete_lsn
 *               (on any timeline, since LSNs keep increasing across a timeline
 *               switch), so the window must not be re-armed.  Reading the answer
 *               from pg_control (authoritative, durable) rather than scanning
 *               for a post-COMPLETE checkpoint (transient, and unreachable once
 *               the scan's timeline diverges) is what keeps this correct after a
 *               standby's end-of-recovery timeline switch.
 *
 * Returns false if there is no readable WAL at all.
 *
 * This must NOT be a byte-pattern heuristic: the upgrade WAL is full of
 * arbitrary full-page-image bytes, so any fixed byte pair appears many times by
 * chance.  Detecting the markers and checkpoints with a real XLogReader is what
 * makes the atomicity guarantee sound — a crash mid-upgrade (START but no
 * COMPLETE) is reliably distinguished from a completed one.
 */
static bool
upgrade_wal_scan_markers(const char *waldir, bool *found_start,
						 bool *found_complete, CheckPoint *cn,
						 XLogRecPtr *cn_lsn, XLogRecPtr *complete_lsn,
						 uint64 *wal_sysid)
{
	DIR		   *dir;
	struct dirent *de;
	int			segsize = 0;
	XLogSegNo	lowseg = 0;
	XLogSegNo	highseg = 0;
	bool		any = false;
	char		lowseg_path[MAXPGPATH] = {0};
	UpgradeWalReadPrivate priv;
	XLogReaderState *reader;
	XLogRecPtr	startptr;
	XLogRecPtr	first;
	CheckPoint	last_ckpt;
	XLogRecPtr	last_ckpt_lsn = InvalidXLogRecPtr;

	*found_start = false;
	*found_complete = false;
	*cn_lsn = InvalidXLogRecPtr;
	*complete_lsn = InvalidXLogRecPtr;
	*wal_sysid = 0;
	MemSet(cn, 0, sizeof(CheckPoint));
	MemSet(&last_ckpt, 0, sizeof(CheckPoint));

	/*
	 * First pass over the directory: determine the segment size (all WAL
	 * segment files are exactly one segment long) and the lowest/highest
	 * segment numbers present.
	 */
	dir = AllocateDir(waldir);
	if (dir == NULL)
		return false;
	while ((de = ReadDir(dir, waldir)) != NULL)
	{
		TimeLineID	ftli;
		XLogSegNo	segno;
		char		path[MAXPGPATH];
		struct stat st;

		if (!IsXLogFileName(de->d_name))
			continue;

		if (segsize == 0)
		{
			snprintf(path, sizeof(path), "%s/%s", waldir, de->d_name);
			if (stat(path, &st) != 0 || st.st_size == 0)
				continue;
			segsize = (int) st.st_size;
		}

		XLogFromFileName(de->d_name, &ftli, &segno, segsize);
		/*
		 * The upgrade WAL (CN..COMPLETE) is always on timeline 1.  Only consider
		 * TLI-1 segments when bounding the scan: after a standby's end-of-recovery
		 * timeline switch, higher-TLI segments (e.g. 00000002...) also live in
		 * pg_wal/, and including them would push the scan's end past the last
		 * TLI-1 segment, making the TLI-1 reader try to open a nonexistent
		 * 00000001... segment and FATAL.
		 */
		if (ftli != 1)
			continue;
		if (!any || segno < lowseg)
		{
			lowseg = segno;
			snprintf(lowseg_path, sizeof(lowseg_path), "%s/%s", waldir, de->d_name);
		}
		if (!any || segno > highseg)
			highseg = segno;
		any = true;
	}
	FreeDir(dir);

	if (!any || segsize == 0)
		return false;

	/*
	 * LEE: capture the system identifier the upgrade WAL was emitted under, by
	 * reading xlp_sysid from the long page header at the start of the lowest
	 * segment.  Recovery validates every WAL page's xlp_sysid against
	 * pg_control->system_identifier, so the arming step (ArmControlFileForUpgrade
	 * Recovery) stamps pg_control with THIS value.  That lets a fresh skeleton
	 * replay the delivered burst without any offline sysid stamping -- the sysid
	 * is adopted in-process from the WAL, exactly as CN is.  (We do NOT force the
	 * old cluster's sysid; the burst carries whatever the new cluster had, and
	 * consistency between pg_control and the WAL is all recovery requires.)
	 */
	{
		int			fd = OpenTransientFile(lowseg_path, O_RDONLY | PG_BINARY);
		XLogLongPageHeaderData longhdr;

		if (fd >= 0)
		{
			if (pg_pread(fd, &longhdr, sizeof(longhdr), 0) == sizeof(longhdr) &&
				longhdr.std.xlp_magic == XLOG_PAGE_MAGIC &&
				(longhdr.std.xlp_info & XLP_LONG_HEADER))
				*wal_sysid = longhdr.xlp_sysid;
			CloseTransientFile(fd);
		}
	}

	priv.tli = 1;
	strlcpy(priv.dir, waldir, sizeof(priv.dir));
	XLogSegNoOffsetToRecPtr(lowseg, 0, segsize, startptr);
	XLogSegNoOffsetToRecPtr(highseg + 1, 0, segsize, priv.endptr);

	reader = XLogReaderAllocate(segsize, NULL,
							   XL_ROUTINE(.page_read = UpgradeWalPageRead,
										  .segment_open = UpgradeWalSegOpen,
										  .segment_close = UpgradeWalSegClose),
							   &priv);
	if (reader == NULL)
		return false;

	/* Find the first valid record at/after the start of the lowest segment. */
	{
		char	   *errormsg = NULL;

		first = XLogFindNextRecord(reader, startptr, &errormsg);
	}
	if (XLogRecPtrIsInvalid(first))
	{
		XLogReaderFree(reader);
		return false;
	}

	XLogBeginRead(reader, first);
	for (;;)
	{
		char	   *errormsg;
		XLogRecord *record = XLogReadRecord(reader, &errormsg);
		uint8		rmid;
		uint8		info;

		if (record == NULL)
			break;				/* end of WAL or unreadable — stop */

		rmid = XLogRecGetRmid(reader);
		info = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

		if (rmid == RM_XLOG_ID &&
			(info == XLOG_CHECKPOINT_ONLINE || info == XLOG_CHECKPOINT_SHUTDOWN))
		{
			/*
			 * Track the most recent checkpoint so that, when we hit START, we
			 * can capture CN (the last checkpoint preceding START).  We do NOT
			 * try to detect a post-COMPLETE checkpoint here to infer
			 * "already applied": after a standby's end-of-recovery timeline
			 * switch that checkpoint is on a later timeline this TLI-1 scan
			 * cannot read.  The caller decides "already applied?" authoritatively
			 * from the control file vs complete_lsn instead.
			 */
			memcpy(&last_ckpt, XLogRecGetData(reader), sizeof(CheckPoint));
			last_ckpt_lsn = reader->ReadRecPtr;
		}
		else if (rmid == RM_PG_UPGRADE_ID)
		{
			if (info == XLOG_PG_UPGRADE_START)
			{
				*found_start = true;
				/* CN is the checkpoint immediately preceding START */
				*cn = last_ckpt;
				*cn_lsn = last_ckpt_lsn;
			}
			else if (info == XLOG_PG_UPGRADE_COMPLETE)
			{
				*found_complete = true;
				*complete_lsn = reader->ReadRecPtr;
				break;			/* window is closed; nothing after COMPLETE matters */
			}
		}
	}

	XLogReaderFree(reader);
	return true;
}

/*
 * LEE: true once PerformWalUpgradeIfNeeded() has armed the sanctioned upgrade
 * bootstrap (pg_wal_upgrade/ present with a COMPLETE marker) for this startup.
 * The pg_upgrade redo handlers consult it to distinguish the bootstrap replay
 * (where applying the upgrade images is correct) from an ordinary/standby WAL
 * stream that merely happens to contain these records (where the server must
 * instead stop and require a restart).  Startup-process-local; never shared.
 */
static bool in_upgrade_bootstrap = false;

/*
 * LEE: revertable upgrade commit request sentinel.
 *
 * "pg_upgrade --commit" drops this file in the new cluster's data directory,
 * then starts the server.  Its presence tells PerformWalUpgradeIfNeeded() to
 * FINALIZE a quarantined cluster (re-replay CN..COMPLETE and go live) instead
 * of re-entering the hold.  The COMPLETE redo handler checks it too: with the
 * sentinel present, it does NOT quarantine — it lets recovery finalize.  The
 * file is removed once finalization begins so a later crash re-holds cleanly.
 */
#define UPGRADE_COMMIT_SENTINEL	"pg_upgrade_commit.signal"

static bool
upgrade_commit_requested(void)
{
	struct stat st;

	return stat(UPGRADE_COMMIT_SENTINEL, &st) == 0;
}

bool
PerformWalUpgradeIfNeeded(void)
{
	char		wal_dir[MAXPGPATH];
	bool		found_start = false;
	bool		found_complete = false;
	CheckPoint	cn;
	XLogRecPtr	cn_lsn = InvalidXLogRecPtr;
	XLogRecPtr	complete_lsn = InvalidXLogRecPtr;
	uint64		wal_sysid = 0;

	/* Skip during pg_upgrade internal server starts (-b binary upgrade mode) */
	if (IsBinaryUpgrade)
		return false;

	snprintf(wal_dir, sizeof(wal_dir), XLOGDIR);

	/*
	 * Parse pg_wal/ with a real XLogReader and look for the pg_upgrade
	 * START/COMPLETE markers plus the end-of-upgrade checkpoint (CN).  The
	 * upgrade WAL lives in pg_wal/ (no rename): a completed --wal-log-upgrade
	 * run leaves a START..COMPLETE window there.  Here we decide what to do:
	 *
	 *   pending (control file not yet past COMPLETE) -> DERIVE CN from the WAL,
	 *                       arm pg_control in-process (checkPoint = CN,
	 *                       state = DB_IN_PRODUCTION), and let StartupXLOG()
	 *                       recover the whole window.
	 *   already applied (control file checkpoint > COMPLETE's LSN) -> do nothing,
	 *                       normal startup.  The upgrade was replayed and
	 *                       finalized by a prior startup; its end-of-recovery
	 *                       checkpoint sits past COMPLETE (on this or a later
	 *                       timeline).
	 *   START, no COMPLETE -> crash mid-upgrade; refuse to start (old cluster
	 *                       is intact — re-run pg_upgrade).
	 *   no START         -> not an upgrade; normal startup.
	 *
	 * Deriving CN here (rather than requiring a prior "pg_resetwal
	 * --upgrade-recovery" to have stamped it) is what lets the SAME WAL stream
	 * drive recovery on the primary and, eventually, on a physical standby: the
	 * anchor is recovered from the CN checkpoint record itself.  It is also the
	 * default and only path — there is no flag to enable it.
	 *
	 * This must NOT be a byte-pattern heuristic: the upgrade WAL is full of
	 * arbitrary full-page-image bytes, so a real XLogReader is what makes the
	 * atomicity guarantee sound.
	 */
	if (!upgrade_wal_scan_markers(wal_dir, &found_start, &found_complete,
								  &cn, &cn_lsn, &complete_lsn, &wal_sysid))
		return false;			/* no readable upgrade WAL — normal startup */

	if (!found_start)
		return false;			/* not an upgrade */

	if (!found_complete)
		ereport(FATAL,
				(errmsg("pg_upgrade failed mid-upgrade: new cluster is unusable"),
				 errhint("Re-run pg_upgrade from the old cluster to start fresh.")));

	/*
	 * Already held in quarantine?  A prior startup replayed the window to
	 * COMPLETE and entered DB_UPGRADE_QUARANTINED (the revertable hold), then
	 * this cluster was restarted.  Its control-file checkpoint is still at CN
	 * (before COMPLETE), so the LSN test below would wrongly re-arm and
	 * re-replay the window.  Two sub-cases:
	 *
	 *   - commit requested ("pg_upgrade --commit" dropped the sentinel): remove
	 *     the sentinel and fall through to re-arm + replay.  This time the
	 *     COMPLETE handler sees the commit request and lets recovery FINALIZE
	 *     (end-of-recovery record, go live) instead of re-holding.
	 *   - otherwise: refuse to serve.  The operator must run "pg_upgrade
	 *     --commit" to finalize or "--rollback" to discard.
	 */
	if (ControlFileInUpgradeQuarantine())
	{
		if (!upgrade_commit_requested())
			ereport(FATAL,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("new cluster is held in pg_upgrade quarantine"),
					 errhint("Run \"pg_upgrade --commit\" to adopt this cluster, or \"pg_upgrade --rollback\" to discard it.")));

		/*
		 * Commit: fall through to re-arm at CN and replay.  We must NOT remove
		 * the sentinel here -- the COMPLETE redo handler checks it to decide to
		 * finalize instead of re-holding, and removes it once finalization
		 * begins.
		 */
		ereport(LOG,
				(errmsg("pg_upgrade --commit: finalizing quarantined cluster")));
	}

	/*
	 * Already applied?  Ask the control file, not the WAL: if its current
	 * checkpoint already sits at or past COMPLETE's LSN, a prior startup
	 * replayed the window and finalized (writing an end-of-recovery checkpoint
	 * past COMPLETE, possibly on a later timeline after a standby's timeline
	 * switch).  Re-arming at CN would re-replay the upgrade over live
	 * post-upgrade data, so treat it as an ordinary startup.  This is
	 * authoritative and timeline-independent -- unlike scanning for a
	 * post-COMPLETE checkpoint, which cannot see a checkpoint on a timeline the
	 * TLI-1 scan does not follow.
	 */
	if (GetControlFileCheckPointLSN() >= complete_lsn)
		return false;

	/*
	 * The upgrade is pending.  We must have found CN (the checkpoint preceding
	 * START); if not, the WAL is malformed and re-arming at an invalid LSN would
	 * corrupt recovery, so fail loudly instead.
	 */
	if (XLogRecPtrIsInvalid(cn_lsn))
		ereport(FATAL,
				(errmsg("pg_upgrade WAL is missing the end-of-upgrade checkpoint"),
				 errhint("Re-run pg_upgrade from the old cluster to start fresh.")));

	ereport(LOG,
			(errmsg("pg_upgrade WAL found in pg_wal/; arming recovery from end-of-upgrade checkpoint at %X/%08X",
					LSN_FORMAT_ARGS(cn_lsn))));

	/*
	 * Arm the control file in-process: point recovery at CN with state =
	 * DB_IN_PRODUCTION and wal_level = replica, and adopt the upgrade WAL's
	 * system identifier so recovery's per-page xlp_sysid check passes.
	 * StartupXLOG() (called right after us) reads ControlFile->checkPointCopy, so
	 * this takes effect for this recovery cycle.  This replaces the old offline
	 * pg_resetwal --upgrade-recovery step, and adopting wal_sysid here replaces
	 * the offline pg_resetwal --system-identifier stamping (the skeleton no longer
	 * needs its sysid set to match the delivered burst -- it is done in-process).
	 */
	ArmControlFileForUpgradeRecovery(&cn, cn_lsn, wal_sysid);

	/*
	 * Arm the sanctioned bootstrap: the pg_upgrade redo handlers may now apply
	 * the upgrade images.  Any pg_upgrade record reached WITHOUT this flag set
	 * came in through an ordinary/standby WAL stream and must not be applied
	 * live (see the XLOG_PG_UPGRADE_START handler in pg_upgrade_redo()).
	 */
	in_upgrade_bootstrap = true;

	return true;
}


/* -------------------------------------------------------------------------
 * Redo
 * -------------------------------------------------------------------------
 */

void
pg_upgrade_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == XLOG_PG_UPGRADE_START)
	{
		xl_pg_upgrade *xlrec = (xl_pg_upgrade *) XLogRecGetData(record);
		int			fd;
		int			len = strlen(xlrec->pg_version);

		/*
		 * LEE: standby / ordinary-stream guard.  The upgrade image records
		 * (DIRSKEL/RELFILE/SLRU/RAWFILE) carry the OLD cluster's page LSNs and
		 * are only safe to apply from the sanctioned bootstrap replay set up by
		 * PerformWalUpgradeIfNeeded() (which anchors at CN into a non-serving
		 * data directory).  A server that reaches START WITHOUT that bootstrap
		 * armed must NOT apply the window live -- doing so would violate replay
		 * LSN invariants.  We stop the recovery process (FATAL) at the boundary.
		 *
		 * This FATAL is the INTENTIONAL halt for a physical standby that streamed
		 * up to the pg_upgrade boundary: the standby stops here; the operator
		 * installs the new-version binary and relaunches; on relaunch, startup
		 * re-enters PerformWalUpgradeIfNeeded(), which anchors at CN and replays
		 * the self-contained upgrade window into the (skeleton) data directory.
		 * A FATAL is the right mechanism -- the old-version binary cannot proceed
		 * past the boundary anyway (it is about to be replaced), and a
		 * recovery-process FATAL cleanly brings the server down for that swap; we
		 * deliberately do NOT attempt a graceful in-loop shutdown from a redo
		 * callback.  StandbyMode distinguishes that expected case from an
		 * unsupported attempt to replay upgrade WAL in some other context, so the
		 * operator sees which happened.
		 */
		if (!in_upgrade_bootstrap)
		{
			if (StandbyMode)
				ereport(FATAL,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("reached pg_upgrade boundary on standby; halting to apply the upgrade"),
						 errdetail("A --wal-log-upgrade was performed on the primary; the standby cannot apply it while streaming."),
						 errhint("Install the new-version binaries and restart this standby; it will replay the upgrade from the end-of-upgrade checkpoint.")));
			else
				ereport(FATAL,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("pg_upgrade WAL encountered during replay"),
						 errhint("Restart this server to apply the pg_upgrade; "
								 "the upgrade cannot be replayed on a running standby.")));
		}

		/*
		 * LEE: the upgrade window is now open.  Suppress hot standby activation
		 * until XLOG_PG_UPGRADE_COMPLETE replays, so no read-only connection can
		 * observe the half-upgraded cluster (new catalogs partially applied).
		 */
		pgUpgradeReplayInProgress = true;

		/*
		 * Write $PGDATA/PG_VERSION from the embedded string.  The top-level
		 * PG_VERSION is created by initdb outside the server and is not
		 * otherwise WAL-logged.  Per-database PG_VERSION files are covered by
		 * XLOG_DBASE_CREATE_WAL_LOG emitted by pg_restore.
		 */

		fd = OpenTransientFile("PG_VERSION",
							   O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
		if (fd < 0)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("pg_upgrade_redo: could not open PG_VERSION: %m")));
		if (pg_pwrite(fd, xlrec->pg_version, len, 0) != len)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("pg_upgrade_redo: could not write PG_VERSION: %m")));
		if (pg_fsync(fd) != 0)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("pg_upgrade_redo: could not fsync PG_VERSION: %m")));
		CloseTransientFile(fd);
	}
	else if (info == XLOG_PG_UPGRADE_COMPLETE)
	{
		/*
		 * LEE: the upgrade window is now closed and the cluster is fully
		 * upgraded.  Clear the guard so hot standby may activate normally
		 * (CheckRecoveryConsistency will pick it up on the next call).
		 */
		pgUpgradeReplayInProgress = false;

		/*
		 * LEE: revertable upgrade quarantine hold.  When this COMPLETE is
		 * reached under the sanctioned bootstrap (the first startup that
		 * PerformWalUpgradeIfNeeded() armed for this new cluster), do NOT let
		 * StartupXLOG() finalize and bring the cluster live.  The whole upgrade
		 * has now been reconstructed on disk; hold it, dark, so the operator can
		 * verify and then explicitly "pg_upgrade --commit" (adopt) or
		 * "--rollback" (discard).  We mark pg_control DB_UPGRADE_QUARANTINED and
		 * proc_exit(3) -- the same "shut down the postmaster" exit code that
		 * recovery_target_action=shutdown uses -- BEFORE the end-of-recovery
		 * record is written or a timeline is forked (xlog.c), so new_dir is
		 * frozen exactly at COMPLETE and remains trivially discardable.
		 *
		 * A restart re-reads DB_UPGRADE_QUARANTINED and re-holds (the guard in
		 * PerformWalUpgradeIfNeeded()), so this is idempotent.
		 *
		 * Only under in_upgrade_bootstrap: a standby or ordinary stream that
		 * reaches COMPLETE without the sanctioned bootstrap must not be affected
		 * (those paths are handled by the START-side guard above).
		 */
		if (in_upgrade_bootstrap)
		{
			if (!upgrade_commit_requested())
			{
				ereport(LOG,
						(errmsg("pg_upgrade reached end-of-upgrade (COMPLETE); holding new cluster in quarantine"),
						 errhint("Run \"pg_upgrade --commit\" to adopt this cluster, or \"pg_upgrade --rollback\" to discard it.")));
				SetControlFileUpgradeQuarantined();
				proc_exit(3);
			}

			/*
			 * Commit requested: consume the sentinel now (so a crash before
			 * finalize re-holds cleanly) and let StartupXLOG() finalize normally
			 * -- write the end-of-recovery record and bring the cluster live.
			 */
			if (unlink(UPGRADE_COMMIT_SENTINEL) != 0)
				ereport(WARNING,
						(errcode_for_file_access(),
						 errmsg("could not remove pg_upgrade commit sentinel \"%s\": %m",
								UPGRADE_COMMIT_SENTINEL)));
			ereport(LOG,
					(errmsg("pg_upgrade --commit: end-of-upgrade reached; finalizing cluster")));
		}
	}
	else if (info == XLOG_PG_UPGRADE_HANDOFF)
	{
		/*
		 * LEE: the OLD-format streaming-handoff TRIGGER.  This record was emitted
		 * into the OLD primary's own WAL (old page format) just before pg_upgrade
		 * shut it down, and a physical standby still streaming the old primary
		 * has now replayed it.  It carries NO upgrade data -- it is purely a
		 * control signal.
		 *
		 * When a StandbyMode server reaches it, stop cleanly at this LSN: an
		 * upgrade is beginning on the primary, and everything past this point in
		 * the old stream is either nonexistent (the primary is shutting down) or,
		 * once the upgrade completes, in the NEW WAL page format that this OLD
		 * binary cannot read.  So there is nothing more to stream; the operator
		 * must swap to the new-version binary/host and re-provision this standby
		 * from the delivered new-version upgrade window (replayed from CN, out of
		 * band -- see PerformWalUpgradeIfNeeded()).
		 *
		 * A recovery-process FATAL is the right mechanism: the old-version binary
		 * cannot proceed past the handoff anyway (it is about to be replaced), and
		 * this cleanly brings the standby down for that swap.  We deliberately do
		 * NOT attempt a graceful in-loop shutdown from a redo callback.
		 *
		 * Outside StandbyMode (e.g. ordinary crash recovery of the old primary
		 * that itself wrote this record before shutdown), the trigger is a no-op:
		 * it means only "an upgrade was initiated from here", and normal recovery
		 * of the old cluster continues.
		 */
		if (StandbyMode)
		{
			xl_pg_upgrade_handoff *xlrec =
				(xl_pg_upgrade_handoff *) XLogRecGetData(record);

			ereport(FATAL,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("reached pg_upgrade handoff on standby; shutting down for pg_upgrade"),
					 errdetail("The primary initiated a --wal-log-upgrade to major version %u; "
							   "this standby cannot follow the upgrade in the old WAL format.",
							   xlrec->target_major_version),
					 errhint("Install the new-version binaries and re-provision this standby "
							 "from the delivered upgrade WAL; it will replay the upgrade from "
							 "the end-of-upgrade checkpoint.")));
		}
	}
	else if (info == XLOG_UPGRADE_DIRSKEL)
	{
		/*
		 * Rebuild the new cluster's directory skeleton (the logged after-image
		 * of initdb's directory tree) before any file image is replayed into
		 * it.  Paths are PGDATA-relative and were emitted parent-before-child,
		 * so a plain mkdir() per path suffices.  Idempotent: EEXIST is expected
		 * because the target already has some of these directories (the
		 * primary's wipe leaves a few behind; a re-provisioned standby starts
		 * from a fresh initdb skeleton), so replay only fills in the rest.
		 */
		xl_upgrade_dirskel *xlrec =
			(xl_upgrade_dirskel *) XLogRecGetData(record);
		char	   *p = (char *) xlrec + SizeOfXLUpgradeDirskel;
		char	   *dir_end = p + xlrec->dir_bytes;
		char	   *sym_end = dir_end + xlrec->sym_bytes;
		uint32		done = 0;

		while (p < dir_end && done < xlrec->ndirs)
		{
			Size		plen = strnlen(p, dir_end - p);

			if (plen == 0 || p + plen >= dir_end)	/* need the NUL terminator */
				break;

			if (mkdir(p, pg_dir_create_mode) != 0 && errno != EEXIST)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("pg_upgrade_redo: could not create directory \"%s\": %m",
								p)));

			p += plen + 1;
			done++;
		}

		if (done != xlrec->ndirs)
			ereport(PANIC,
					(errmsg("pg_upgrade_redo: dirskel record damaged: created %u of %u directories",
							done, xlrec->ndirs)));

		/*
		 * Recreate captured symlinks (pg_tblspc/<spcoid> -> external tablespace
		 * location).  Each entry is two NUL-terminated strings: linkpath, target.
		 * We create the target directory (idempotent) then the symlink, so the
		 * tablespace exists before its RELFILE images replay through smgr.  An
		 * existing correct symlink is fine; EEXIST is tolerated.
		 */
		p = dir_end;
		done = 0;
		while (p < sym_end && done < xlrec->nsymlinks)
		{
			char	   *linkpath = p;
			Size		llen = strnlen(linkpath, sym_end - p);
			char	   *target;
			Size		tlen;

			if (llen == 0 || p + llen >= sym_end)
				break;
			target = p + llen + 1;
			if (target >= sym_end)
				break;
			tlen = strnlen(target, sym_end - target);
			if (p + llen + 1 + tlen >= sym_end)
				break;

			/* ensure the external target directory exists */
			if (mkdir(target, pg_dir_create_mode) != 0 && errno != EEXIST)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("pg_upgrade_redo: could not create tablespace directory \"%s\": %m",
								target)));

			if (symlink(target, linkpath) != 0 && errno != EEXIST)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("pg_upgrade_redo: could not create symlink \"%s\" -> \"%s\": %m",
								linkpath, target)));

			p = target + tlen + 1;
			done++;
		}

		if (done != xlrec->nsymlinks)
			ereport(PANIC,
					(errmsg("pg_upgrade_redo: dirskel record damaged: created %u of %u symlinks",
							done, xlrec->nsymlinks)));
	}
	else if (info == XLOG_UPGRADE_SLRU_DATA)
	{
		/*
		 * Restore the captured SLRU segment image(s) authoritatively.
		 *
		 * These records are emitted at the very END of pg_upgrade, after all
		 * transactions have committed and a CHECKPOINT flushed the final,
		 * merged CLOG/multixact state to disk.  The image therefore contains
		 * both the old cluster's historical commit bits (which live ONLY here —
		 * they were never in any WAL) and the new cluster's restore-transaction
		 * statuses.  Because the record is emitted last, it dominates anything
		 * an earlier replayed commit record wrote for the same page.
		 *
		 * pg_upgrade truncated the on-disk SLRU segment files ("skip the disk
		 * writes"), so this redo is the sole source that reconstructs pg_xact
		 * and pg_multixact on first startup.  We install each captured page
		 * into the SimpleLru buffers and flush it, so the end-of-recovery
		 * checkpoint cannot clobber it with a stale buffer.
		 */
		xl_upgrade_slru_data *xlrec =
			(xl_upgrade_slru_data *) XLogRecGetData(record);
		char	   *data = (char *) xlrec + SizeOfXLUpgradeSlruData;
		Size		seg_size = SLRU_PAGES_PER_SEGMENT * BLCKSZ;
		int64		seg;
		Size		off = 0;

		for (seg = xlrec->first_seg; seg <= xlrec->last_seg; seg++)
		{
			if (off + seg_size > (Size) xlrec->total_bytes)
				break;

			switch (xlrec->slru_type)
			{
				case UPGRADE_SLRU_XACT:
					CLOGUpgradeRestoreSegment(seg, data + off, seg_size);
					break;
				case UPGRADE_SLRU_MXOFF:
					MultiXactOffsetUpgradeRestoreSegment(seg, data + off, seg_size);
					break;
				case UPGRADE_SLRU_MXMEM:
					MultiXactMemberUpgradeRestoreSegment(seg, data + off, seg_size);
					break;
				default:
					elog(PANIC, "pg_upgrade_redo: invalid slru_type %u",
						 xlrec->slru_type);
			}
			off += seg_size;
		}
	}
	else if (info == XLOG_UPGRADE_RELFILE_DATA)
	{
		/*
		 * The record batches many relation-file chunks:
		 *     [entry_0][data_0][entry_1][data_1] ...
		 * Walk the entries and restore each chunk page-by-page THROUGH the
		 * buffer manager.  Recovery is anchored at the end-of-upgrade checkpoint
		 * (CN), so these images are the sole writers of the relation's pages —
		 * the on-disk file was wiped by pg_upgrade and pg_restore's own WAL is
		 * not replayed.
		 *
		 * Going through the buffer manager (rather than a direct pwrite) lets
		 * XLogReadBufferExtended create the file and its database directory on
		 * demand (smgrcreate) and flush the page at the end-of-recovery
		 * checkpoint like any other recovered page.  RBM_ZERO_AND_LOCK gives us
		 * a zero-extended, locked buffer whose full contents we overwrite.
		 */
		char	   *ptr = XLogRecGetData(record);
		char	   *end = ptr + XLogRecGetDataLen(record);

		while (ptr < end)
		{
			xl_upgrade_relfile_entry ent;
			char	   *data;
			RelFileLocator rlocator;
			ForkNumber	forknum;
			uint32		npages;
			BlockNumber base_block;

			memcpy(&ent, ptr, SizeOfXLUpgradeRelfileEntry);
			ptr += SizeOfXLUpgradeRelfileEntry;
			data = ptr;
			ptr += ent.nbytes;

			rlocator.spcOid = ent.tablespace_oid;
			rlocator.dbOid = ent.database_oid;
			rlocator.relNumber = ent.relfilenumber;
			forknum = (ForkNumber) ent.forknum;

			/*
			 * LEE: a zero-page entry (nbytes==0) means "the relation file is
			 * empty; create it".  Empty system catalogs (pg_publication, pg_enum,
			 * ...) have 0-byte relfiles in a fresh cluster; without recreating
			 * them, the first write that touches such a catalog fails with
			 * "could not open file".  Create the (main-fork) file via smgr,
			 * idempotently, and move on -- there are no pages to install.
			 */
			if (ent.nbytes == 0)
			{
				SMgrRelation srel = smgropen(rlocator, INVALID_PROC_NUMBER);

				if (!smgrexists(srel, forknum))
					smgrcreate(srel, forknum, true);
				smgrclose(srel);
				continue;
			}

			/* segments are RELSEG_SIZE blocks; blockoff is this chunk's start */
			base_block = (BlockNumber) ent.segno * RELSEG_SIZE + ent.blockoff;
			npages = ent.nbytes / BLCKSZ;

			for (uint32 i = 0; i < npages; i++)
			{
				Buffer		buffer;
				Page		page;

				buffer = XLogReadBufferExtended(rlocator, forknum,
												base_block + i,
												RBM_ZERO_AND_LOCK,
												InvalidBuffer);
				if (!BufferIsValid(buffer))
					ereport(PANIC,
							(errmsg("pg_upgrade_redo: could not read block %u of relation %u/%u/%u fork %d",
									base_block + i,
									rlocator.spcOid, rlocator.dbOid,
									rlocator.relNumber, forknum)));

				page = BufferGetPage(buffer);
				memcpy(page, data + (Size) i * BLCKSZ, BLCKSZ);

				/*
				 * Keep the page's captured LSN verbatim -- do NOT restamp it.
				 * Recovery is anchored at CN and pg_restore's WAL is never
				 * replayed, so nothing else writes these blocks.  The captured
				 * LSN is the old cluster's (below CN, so the WAL-before-data
				 * rule holds at flush time), which makes the reconstructed page
				 * BYTE-IDENTICAL to what a normal pg_upgrade leaves on disk.
				 */
				MarkBufferDirty(buffer);
				UnlockReleaseBuffer(buffer);
			}
		}
	}
	else if (info == XLOG_UPGRADE_RAWFILE)
	{
		/*
		 * Write a verbatim non-relation file (pg_filenode.map, PG_VERSION),
		 * creating any missing parent directories.  These files are not
		 * reachable through the buffer manager, so they are the only way to
		 * rebuild the relation map and version stamps when recovering from an
		 * otherwise-empty data directory.
		 */
		xl_upgrade_rawfile *xlrec =
			(xl_upgrade_rawfile *) XLogRecGetData(record);
		char	   *payload = (char *) xlrec + SizeOfXLUpgradeRawfile;
		char		path[MAXPGPATH];
		char	   *data = payload + xlrec->path_len;
		int			fd;
		char	   *slash;

		if (xlrec->path_len >= MAXPGPATH)
			elog(PANIC, "pg_upgrade_redo: rawfile path too long");
		memcpy(path, payload, xlrec->path_len);
		path[xlrec->path_len] = '\0';

		/* create parent directory if it does not exist (e.g. base/<dboid>) */
		slash = strrchr(path, '/');
		if (slash != NULL)
		{
			*slash = '\0';
			if (mkdir(path, pg_dir_create_mode) != 0 && errno != EEXIST)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("pg_upgrade_redo: could not create directory \"%s\": %m",
								path)));
			*slash = '/';
		}

		fd = OpenTransientFile(path, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
		if (fd < 0)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("pg_upgrade_redo: could not open \"%s\": %m", path)));
		if (pg_pwrite(fd, data, xlrec->data_len, 0) != (ssize_t) xlrec->data_len)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("pg_upgrade_redo: could not write \"%s\": %m", path)));
		if (pg_fsync(fd) != 0)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("pg_upgrade_redo: could not fsync \"%s\": %m", path)));
		CloseTransientFile(fd);
	}
	else
		elog(PANIC, "pg_upgrade_redo: unknown op code %u", info);
}
