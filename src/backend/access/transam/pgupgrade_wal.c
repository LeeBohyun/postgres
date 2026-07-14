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
#include "storage/bufpage.h"	/* PageSetLSN */
#include "storage/fd.h"
#include "storage/copydir.h"	/* copydir() for WAL segment migration */
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
 *   applied   — true if a checkpoint appears AFTER COMPLETE.  Such a checkpoint
 *               can only have been written by normal operation once the upgrade
 *               was already replayed (the end-of-recovery checkpoint, or a later
 *               one), so its presence means "the upgrade is already applied —
 *               do NOT re-arm and re-replay it."  This is what makes startup
 *               idempotent without a side flag: the WAL itself records whether
 *               the window has been consumed.
 *
 * Returns false if there is no readable WAL at all.
 *
 * This must NOT be a byte-pattern heuristic: the upgrade WAL is full of
 * arbitrary full-page-image bytes, so any fixed byte pair appears many times by
 * chance.  Detecting the markers and checkpoints with a real XLogReader is what
 * makes the atomicity guarantee sound — a crash mid-upgrade (START but no
 * COMPLETE) is reliably distinguished from a completed one, and an
 * already-applied upgrade from a pending one.
 */
static bool
upgrade_wal_scan_markers(const char *waldir, bool *found_start,
						 bool *found_complete, CheckPoint *cn,
						 XLogRecPtr *cn_lsn, bool *applied)
{
	DIR		   *dir;
	struct dirent *de;
	int			segsize = 0;
	XLogSegNo	lowseg = 0;
	XLogSegNo	highseg = 0;
	bool		any = false;
	UpgradeWalReadPrivate priv;
	XLogReaderState *reader;
	XLogRecPtr	startptr;
	XLogRecPtr	first;
	CheckPoint	last_ckpt;
	XLogRecPtr	last_ckpt_lsn = InvalidXLogRecPtr;

	*found_start = false;
	*found_complete = false;
	*cn_lsn = InvalidXLogRecPtr;
	*applied = false;
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
		if (!any || segno < lowseg)
			lowseg = segno;
		if (!any || segno > highseg)
			highseg = segno;
		any = true;
	}
	FreeDir(dir);

	if (!any || segsize == 0)
		return false;

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
			 * Track the most recent checkpoint.  Before START this lets us
			 * capture CN (the last checkpoint preceding START).  After COMPLETE
			 * a checkpoint means the upgrade has already been applied by a prior
			 * startup — record that so we do not re-arm and re-replay it.
			 */
			memcpy(&last_ckpt, XLogRecGetData(reader), sizeof(CheckPoint));
			last_ckpt_lsn = reader->ReadRecPtr;
			if (*found_complete)
				*applied = true;
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
				*found_complete = true;
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

bool
PerformWalUpgradeIfNeeded(void)
{
	char		wal_dir[MAXPGPATH];
	bool		found_start = false;
	bool		found_complete = false;
	bool		applied = false;
	CheckPoint	cn;
	XLogRecPtr	cn_lsn = InvalidXLogRecPtr;

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
	 *   START + COMPLETE, no later checkpoint -> upgrade pending; DERIVE CN from
	 *                       the WAL, arm pg_control in-process (checkPoint = CN,
	 *                       state = DB_IN_PRODUCTION), and let StartupXLOG()
	 *                       crash-recover the whole window.
	 *   START + COMPLETE, checkpoint AFTER COMPLETE -> already applied by a prior
	 *                       startup; do nothing, normal startup.
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
								  &cn, &cn_lsn, &applied))
		return false;			/* no readable upgrade WAL — normal startup */

	if (!found_start)
		return false;			/* not an upgrade */

	if (!found_complete)
		ereport(FATAL,
				(errmsg("pg_upgrade failed mid-upgrade: new cluster is unusable"),
				 errhint("Re-run pg_upgrade from the old cluster to start fresh.")));

	/*
	 * A checkpoint after COMPLETE means a prior startup already replayed the
	 * whole window and resumed normal operation.  Re-arming at CN now would
	 * re-replay the upgrade over live post-upgrade data — so treat it as an
	 * ordinary startup.
	 */
	if (applied)
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
	 * DB_IN_PRODUCTION and wal_level = replica.  StartupXLOG() (called right
	 * after us) reads ControlFile->checkPointCopy, so this takes effect for this
	 * recovery cycle.  This replaces the old offline pg_resetwal
	 * --upgrade-recovery step.
	 */
	ArmControlFileForUpgradeRecovery(&cn, cn_lsn);

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
		 * data directory).  A physical standby streaming the primary's WAL will
		 * reach this START record during ordinary replay, WITHOUT that bootstrap
		 * armed; applying the images live would violate replay LSN invariants.
		 * So we stop here and require a restart, at which point startup re-enters
		 * PerformWalUpgradeIfNeeded() and replays the upgrade from CN safely.
		 */
		if (!in_upgrade_bootstrap)
			ereport(FATAL,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("pg_upgrade WAL encountered during replay"),
					 errhint("Restart this server to apply the pg_upgrade; "
							 "the upgrade cannot be replayed on a running standby.")));

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
	}
	else if (info == XLOG_UPGRADE_DIRSKEL)
	{
		/*
		 * Rebuild the new cluster's directory skeleton (the logged after-image
		 * of initdb's directory tree) before any file image is replayed into
		 * it.  Paths are PGDATA-relative and were emitted parent-before-child,
		 * so a plain mkdir() per path suffices.  Idempotent: EEXIST is expected
		 * (on the primary the wipe leaves some dirs; on a standby the tree
		 * largely already exists as a copy of the old cluster).
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
