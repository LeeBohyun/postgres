/*
 * pgupgrade_wal.c
 *
 * WAL redo and startup handling for RM_PG_UPGRADE_ID records written by
 * pg_upgrade --wal-upgrade:
 *
 *   XLOG_UPGRADE_START    (0x00) -- window open, write PG_VERSION
 *   XLOG_UPGRADE_COMPLETE (0x10) -- window close, informational
 *   XLOG_UPGRADE_DIRTREE     (0x40) -- initdb directory + symlink skeleton
 *   XLOG_UPGRADE_RAWFILE     (0x50) -- raw non-relation file image, written at
 *                                     a byte offset (PG_VERSION, SLRU segments)
 *   XLOG_UPGRADE_HANDOFF     (0x60) -- shut-down trigger for an old-cluster standby
 *   XLOG_UPGRADE_RELINK      (0x70) -- manifest of user relation files a streaming
 *                                     standby links from its retained old datadir
 *                                     (the window omits their data)
 *
 * The XID/OID/multixact counters are not WAL-logged: they are transplanted into
 * pg_control before the end-of-upgrade checkpoint, which carries them, and
 * recovery reproduces them from that checkpoint.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/pgupgrade_wal.c
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/pgupgrade_wal.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogrecovery.h"	/* pgUpgradeReplayInProgress */
#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"	/* wal_segment_close */
#include "catalog/pg_control.h"
#include "catalog/pg_tablespace_d.h"
#include "common/controldata_utils.h"	/* get_controlfile for local CN
										 * derivation */
#include "common/file_perm.h"	/* pg_dir_create_mode */
#include "common/file_utils.h"	/* pg_clone_file, pg_copy_file_range_all */
#include "common/relpath.h"		/* RelFileLocator, ForkNumber */
#include "miscadmin.h"
#include "storage/bufmgr.h"		/* buffer-manager RELFILE_DATA redo */
#include "storage/smgr.h"		/* smgr create for empty relfiles */
#include "storage/bufpage.h"	/* PageSetLSN */
#include "storage/fd.h"
#include "storage/copydir.h"	/* copydir() for WAL segment migration */
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "replication/walreceiver.h"	/* libpqwalreceiver client for
										 * auto-anchor */
#include "utils/elog.h"

/* -------------------------------------------------------------------------
 * pg_upgrade WAL-replay-based atomicity check
 * -------------------------------------------------------------------------
 */

/*
 * Private state for the XLogReader used by UpgradeWalScanMarkers().
 */
typedef struct UpgradeWalReadPrivate
{
	char		dir[MAXPGPATH]; /* WAL directory to read segments from */
	TimeLineID	tli;			/* timeline (always 1 for upgrade WAL) */
	XLogRecPtr	endptr;			/* one past the last byte of available WAL */
	int			openerr_elevel; /* ereport level if a segment cannot be opened */
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
		ereport(priv->openerr_elevel,
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
 * Parse the WAL in "waldir" and locate the pg_upgrade markers plus the
 * end-of-upgrade checkpoint (CN) that recovery must anchor at.
 *
 * Out-params:
 *   found_start / found_complete -- the START / COMPLETE markers were seen.
 *   cn        -- CheckPoint of the last online checkpoint preceding START.  This
 *               is CN, the recovery anchor; it carries the transplanted
 *               XID/OID/multixact counters.
 *   cn_lsn    -- record LSN of that checkpoint (-> ControlFile.checkPoint).
 *
 * Returns false if there is no readable WAL at all.  A present-but-unreadable or
 * corrupt window (a segment cannot be opened, a checkpoint record is malformed)
 * stops recovery with FATAL: this runs in the startup process, where a broken
 * local window must not be silently ignored.
 */
bool
UpgradeWalScanMarkers(const char *waldir, bool *found_start,
					  bool *found_complete, CheckPoint *cn,
					  XLogRecPtr *cn_lsn, uint64 *wal_sysid)
{
	DIR		   *dir;
	struct dirent *de;
	int			segsize = 0;
	XLogSegNo	lowseg = 0;
	XLogSegNo	highseg = 0;
	XLogSegNo	runstart = 0;
	bool		any = false;
	char		runstart_path[MAXPGPATH] = {0};
	UpgradeWalReadPrivate priv;
	XLogReaderState *reader;
	XLogRecPtr	startptr;
	XLogRecPtr	first;
	CheckPoint	last_ckpt;
	XLogRecPtr	last_ckpt_lsn = InvalidXLogRecPtr;

	*found_start = false;
	*found_complete = false;
	*cn_lsn = InvalidXLogRecPtr;
	*wal_sysid = 0;
	MemSet(cn, 0, sizeof(CheckPoint));
	MemSet(&last_ckpt, 0, sizeof(CheckPoint));

	/*
	 * Determine the segment size (all WAL segment files are exactly one
	 * segment long) and the lowest/highest segment numbers present.
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
		 * The upgrade WAL (CN..COMPLETE) is always on timeline 1.  Only bound
		 * the scan by TLI-1 segments: after a standby's end-of-recovery
		 * timeline switch, higher-TLI segments also live in pg_wal/, and
		 * including them would push the scan's end past the last TLI-1
		 * segment, so the reader would try to open a nonexistent 00000001...
		 * segment and FATAL.
		 */
		if (ftli != 1)
			continue;
		if (!any || segno < lowseg)
			lowseg = segno;
		if (!any || segno > highseg)
			highseg = segno;
		any = true;
	}
	FreeDir(dir);

	if (!any || segsize == 0)
		return false;

	/*
	 * Bound the scan to the contiguous run of TLI-1 segments ending at
	 * highseg, not from lowseg.  When delivered by archive-PITR staging,
	 * pg_wal/ can also hold unrelated pre-window segments from the restored
	 * base backup, with a gap before the window.  Starting at lowseg would
	 * make XLogFindNextRecord walk into that hole and FATAL, so walk down
	 * from highseg while each preceding segment is present.
	 */
	runstart = highseg;
	{
		char		segname[MAXFNAMELEN];
		char		segpath[MAXPGPATH];
		struct stat st;

		while (runstart > lowseg)
		{
			XLogFileName(segname, 1, runstart - 1, segsize);
			snprintf(segpath, sizeof(segpath), "%s/%s", waldir, segname);
			if (stat(segpath, &st) != 0)
				break;			/* gap: runstart-1 is missing */
			runstart--;
		}
		XLogFileName(segname, 1, runstart, segsize);
		snprintf(runstart_path, sizeof(runstart_path), "%s/%s", waldir, segname);
	}

	/*
	 * Capture the system identifier the upgrade WAL was emitted under, from
	 * xlp_sysid in the run-start segment's long page header.  Recovery
	 * validates every WAL page's xlp_sysid against
	 * pg_control->system_identifier, so the arming step stamps pg_control
	 * with this value, letting a fresh skeleton adopt the sysid from the WAL.
	 */
	{
		int			fd = OpenTransientFile(runstart_path, O_RDONLY | PG_BINARY);
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
	priv.openerr_elevel = FATAL;
	strlcpy(priv.dir, waldir, sizeof(priv.dir));
	XLogSegNoOffsetToRecPtr(runstart, 0, segsize, startptr);
	XLogSegNoOffsetToRecPtr(highseg + 1, 0, segsize, priv.endptr);

	reader = XLogReaderAllocate(segsize, NULL,
								XL_ROUTINE(.page_read = UpgradeWalPageRead,
										   .segment_open = UpgradeWalSegOpen,
										   .segment_close = UpgradeWalSegClose),
								&priv);
	if (reader == NULL)
		return false;

	/*
	 * Find the first valid record at/after the start of the run-start
	 * segment.
	 */
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
			break;				/* end of WAL or unreadable */

		rmid = XLogRecGetRmid(reader);
		info = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

		if (rmid == RM_XLOG_ID &&
			(info == XLOG_CHECKPOINT_ONLINE || info == XLOG_CHECKPOINT_SHUTDOWN))
		{
			/*
			 * Track the most recent checkpoint so that, on reaching START, we
			 * capture CN (the last checkpoint preceding START).  "Already
			 * applied?" is decided by the caller from the control file, not
			 * by detecting a post-COMPLETE checkpoint here (it may be on a
			 * later timeline this TLI-1 scan cannot read).
			 */
			if (XLogRecGetDataLen(reader) < sizeof(CheckPoint))
				ereport(FATAL,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("checkpoint record too short")));
			memcpy(&last_ckpt, XLogRecGetData(reader), sizeof(CheckPoint));
			last_ckpt_lsn = reader->ReadRecPtr;
		}
		else if (rmid == RM_PG_UPGRADE_ID)
		{
			if (info == XLOG_UPGRADE_START)
			{
				*found_start = true;
				/* CN is the checkpoint immediately preceding START */
				*cn = last_ckpt;
				*cn_lsn = last_ckpt_lsn;
			}
			else if (info == XLOG_UPGRADE_COMPLETE)
			{
				*found_complete = true;
				break;			/* window is closed; nothing after COMPLETE
								 * matters */
			}
		}
	}

	XLogReaderFree(reader);
	return true;
}


/*
 * True once PerformWalUpgradeIfNeeded() has armed the upgrade replay for this
 * startup.  The redo handlers use it to tell that armed replay (apply the
 * upgrade images) from an ordinary/standby stream.
 */
static bool in_upgrade_bootstrap = false;

/*
 * Set when first-startup armed the streaming-standby path (a fresh skeleton
 * auto-armed from the primary), rather than the primary's own crash recovery or
 * an archive/PITR restore.  On the streaming path the skeleton has no user data,
 * so an XLOG_UPGRADE_RELINK manifest with no old-datadir path to link from is a
 * fatal misconfiguration.  On the other two paths the files are already on disk,
 * so the same manifest is a legitimate no-op.  Startup-process-local.
 */
static bool armed_streaming_standby = false;

/*
 * The CN LSN the streaming-standby path derived from the retained old datadir
 * and stamped into pg_control as the recovery anchor (see
 * ArmFromLocalDerivationIfConfigured).  The XLOG_UPGRADE_START redo handler
 * checks that recovery's redo pointer equals it.  A mismatch means the
 * derivation picked the wrong CN, so it FATALs rather than mis-recovering
 * silently.
 */
static XLogRecPtr armed_cn_lsn = InvalidXLogRecPtr;

/*
 * Is the durable "window reached COMPLETE" flag set in pg_control?  Set by the
 * XLOG_UPGRADE_COMPLETE redo handler when the window completes, it distinguishes
 * a finished upgrade from a crashed partial one.
 */
static bool
UpgradeWindowFinalized(void)
{
	return GetControlFileUpgradeFinalized();
}


/*
 * Compute the CN segment number by replicating pg_resetwal's FindEndOfXLOG rule
 * against the retained old datadir, matching the segment the producer's
 * "pg_resetwal -l" targeted on the primary:
 *
 *   CN_seg = max( XLByteToSeg(old_tail, old_seg_size),
 *                 highest segment in <old_datadir>/pg_wal ) + 1
 *
 * The scan takes the max segno over all timelines and over partial (.partial)
 * segments, without a timeline filter, as pg_resetwal.c FindEndOfXLOG
 * does.  This must match pg_resetwal because the producer's -l target came from
 * the old cluster's own "pg_resetwal -n" result: if the old cluster was ever
 * promoted, a higher-timeline segment could hold the max segno, and a
 * TLI-1-only scan would compute a smaller CN_seg than the producer targeted.
 * (The window itself is on timeline 1, but this is a floor computation, not a
 * window scan.)  The +1 advances to the next unused segment, as pg_resetwal does.
 * Segment math uses the old cluster's segment size.
 */
static XLogSegNo
DeriveUpgradeCnSegment(const char *old_datadir, XLogRecPtr old_tail,
					   int old_seg_size)
{
	char		waldir[MAXPGPATH];
	DIR		   *dir;
	struct dirent *de;
	XLogSegNo	maxseg;

	/* Floor: the segment holding the old cluster's checkpoint redo. */
	XLByteToSeg(old_tail, maxseg, old_seg_size);

	snprintf(waldir, sizeof(waldir), "%s/%s", old_datadir, XLOGDIR);
	dir = AllocateDir(waldir);
	if (dir == NULL)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open old cluster WAL directory \"%s\": %m",
						waldir)));
	while ((de = ReadDir(dir, waldir)) != NULL)
	{
		TimeLineID	ftli;
		XLogSegNo	segno;

		/* complete and partial segments both count (see header) */
		if (!IsXLogFileName(de->d_name) && !IsPartialXLogFileName(de->d_name))
			continue;

		XLogFromFileName(de->d_name, &ftli, &segno, old_seg_size);
		(void) ftli;			/* parsed for the API; not a filter */

		if (segno > maxseg)
			maxseg = segno;
	}
	FreeDir(dir);

	/* Advance by one to the next unused segment (matches pg_resetwal). */
	return maxseg + 1;
}

/*
 * Is this data directory staged for --wal-upgrade recovery?  True iff the
 * pg_upgrade.signal sentinel is present.  The sentinel is the single gate all
 * three detection sites share (checkDataDir's control-file synthesis, and the
 * streaming/archive arms in this file), so they classify a directory
 * identically; the surrounding config (primary_conninfo vs recovery.signal)
 * then selects streaming vs archive-PITR.
 */
bool
UpgradeSignalStaged(void)
{
	char		path[MAXPGPATH];
	struct stat st;

	snprintf(path, sizeof(path), "%s/%s", DataDir, PG_UPGRADE_SIGNAL_FILE);
	return stat(path, &st) == 0;
}

/*
 * Automatic streaming-standby arming, deriving CN locally.
 *
 * A fresh vN+1 skeleton with primary_conninfo set arms its control file at CN
 * without asking the primary for the anchor: it derives CN from its own
 * retained old data directory, reproducing where the producer's pg_resetwal
 * placed it.  From the primary it needs only the system identifier (via the
 * standard IDENTIFY_SYSTEM).  Runs in the startup process before StartupXLOG.
 *
 * Derivation:
 *   1. sysid from IDENTIFY_SYSTEM on the primary.
 *   2. old_tail = old cluster's checkPointCopy.redo.
 *   3. CN_seg = FindEndOfXLOG rule over the old datadir (DeriveUpgradeCnSegment).
 *   4. CN is the DB_SHUTDOWNED checkpoint at the start of CN_seg on timeline 1
 *      (redo == cn_lsn == the segment-boundary LSN past the long page header).
 *
 * Gated on the pg_upgrade.signal sentinel, so an ordinary standby without it is
 * untouched.  Returns false (caller falls back to the local-window path) when
 * the sentinel is absent or no primary is configured.  Once armed, a missing
 * old-datadir GUC, a connection failure, or an unreadable old control file is a
 * hard FATAL.
 */
static bool
ArmFromLocalDerivationIfConfigured(void)
{
	WalReceiverConn *conn;
	char	   *err = NULL;
	TimeLineID	ignored_tli = 0;
	char	   *sysid_str;
	uint64		sysid = 0;
	const char *old_datadir;
	ControlFileData *old_control;
	bool		crc_ok = false;
	XLogRecPtr	old_tail;
	int			old_seg_size;
	XLogSegNo	cn_seg;
	XLogRecPtr	cn_lsn;
	CheckPoint	cn;
	struct stat st;

	/*
	 * Only act on a skeleton explicitly staged for --wal-upgrade recovery
	 * (the pg_upgrade.signal sentinel).  Without it an ordinary streaming
	 * standby would try to derive an upgrade anchor on every start.
	 */
	if (!UpgradeSignalStaged())
		return false;

	/*
	 * Once the window has been replayed to COMPLETE this node is a finalized
	 * new-version cluster and the durable pg_control upgrade_finalized flag
	 * is set (the COMPLETE redo fsync'd it).  Re-deriving CN and re-arming at
	 * the old CN would rewind recovery behind this standby's actual replay
	 * position, and fail once the primary drops the retention slot, so a
	 * restart with the sentinel still staged falls through here and starts as
	 * an ordinary hot standby.  This mirrors the local-window path's
	 * UpgradeWindowFinalized() guard.
	 */
	if (UpgradeWindowFinalized())
		return false;

	/*
	 * primary_conninfo set -> this is the streaming path (as opposed to the
	 * archive/PITR path, which has recovery.signal and no primary).
	 */
	if (PrimaryConnInfo == NULL || PrimaryConnInfo[0] == '\0')
		return false;

	/*
	 * A streamed upgrade standby must also be in standby mode: arming the
	 * control file at CN commits this node to following the primary's forward
	 * WAL (which exists only on the primary).  Without standby.signal the
	 * node would leave recovery and come up read-write at CN, split-brain
	 * against the real primary.  standby.signal is the operator's
	 * responsibility (like any standby); if the upgrade sentinel is staged
	 * with primary_conninfo but without it, refuse to arm rather than risk
	 * that outcome.
	 */
	{
		char		standbysig[MAXPGPATH];

		snprintf(standbysig, sizeof(standbysig), "%s/standby.signal", DataDir);
		if (stat(standbysig, &st) != 0)
			ereport(FATAL,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("pg_upgrade streaming skeleton has \"%s\" and primary_conninfo but no \"standby.signal\"",
							PG_UPGRADE_SIGNAL_FILE),
					 errhint("Create a standby.signal file so this node follows the primary; a streamed upgrade target must run in standby mode.")));
	}

	/*
	 * The old datadir path is standby-local (the primary cannot know it); the
	 * operator supplies it in the skeleton's postgresql.conf.  Without it CN
	 * cannot be derived, so this is a hard misconfiguration.
	 */
	old_datadir = pg_upgrade_standby_old_datadir;
	if (old_datadir == NULL || old_datadir[0] == '\0')
		ereport(FATAL,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("streaming --wal-upgrade skeleton requires \"pg_upgrade_standby_old_datadir\" to derive the upgrade anchor"),
				 errhint("Set \"pg_upgrade_standby_old_datadir\" in postgresql.conf to this standby's retained pre-upgrade data directory.")));

	/*
	 * Obtain the sysid from IDENTIFY_SYSTEM on the primary.  This is the
	 * primary's == the new cluster's sysid == what the window WAL pages are
	 * stamped with, so recovery's per-page xlp_sysid check passes once the
	 * control file adopts it.  The returned TLI is ignored: CN lives on
	 * timeline 1 (hardcoded below).  A connection failure while the sentinel
	 * is staged is a hard FATAL.
	 */
	load_file("libpqwalreceiver", false);
	if (WalReceiverFunctions == NULL)
		elog(FATAL, "libpqwalreceiver didn't initialize correctly");

	conn = walrcv_connect(PrimaryConnInfo, true, false, false,
						  "pg_upgrade_anchor", &err);
	if (conn == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("could not connect to the primary to identify the pg_upgrade system: %s",
						err ? err : "unknown error"),
				 errhint("Set primary_conninfo to a live --wal-upgrade primary.")));

	sysid_str = walrcv_identify_system(conn, &ignored_tli);
	walrcv_disconnect(conn);
	if (sysid_str == NULL ||
		sscanf(sysid_str, "%" SCNu64, &sysid) != 1 ||
		sysid == 0)
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("malformed system identifier from primary: \"%s\"",
						sysid_str ? sysid_str : "(null)")));

	/*
	 * Read the retained old cluster's control file to get old_tail (its clean
	 * shutdown checkpoint redo == checkPoint) and its WAL segment size. Using
	 * the backend get_controlfile(): it OpenTransientFile()s the file,
	 * palloc's a copy, and CRC-checks it, with no shared state, so it is safe
	 * from the startup process before StartupXLOG.
	 *
	 * Pre-check that the control file is present and readable:
	 * get_controlfile() ereport(ERROR)s with a generic "could not open file"
	 * if it is missing or unreadable, which the startup process turns into a
	 * bare postmaster restart loop with no hint at the real cause.  Fail with
	 * a clear, actionable FATAL instead (a misconfigured
	 * pg_upgrade_standby_old_datadir is the usual cause).
	 */
	{
		char		ctlpath[MAXPGPATH];
		struct stat st;

		snprintf(ctlpath, sizeof(ctlpath), "%s/global/pg_control", old_datadir);
		if (stat(ctlpath, &st) != 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not access old cluster control file \"%s\": %m",
							ctlpath),
					 errhint("\"pg_upgrade_standby_old_datadir\" must point at this standby's retained pre-upgrade data directory.")));
	}

	old_control = get_controlfile(old_datadir, &crc_ok);
	if (!crc_ok)
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("pg_control CRC check failed for old data directory \"%s\"",
						old_datadir),
				 errhint("The retained pre-upgrade data directory named by \"pg_upgrade_standby_old_datadir\" is unreadable or corrupt.")));

	old_tail = old_control->checkPointCopy.redo;
	old_seg_size = (int) old_control->xlog_seg_size;
	pfree(old_control);

	/*
	 * Byte-contiguous CN derivation assumes the new cluster's WAL segment
	 * size equals the old cluster's: pg_upgrade --wal-upgrade-exact positions
	 * the new WAL at the old cluster's next SEGMENT, and we recompute that
	 * segment here from the old datadir using old_seg_size.  If the two
	 * differ (the upgrade changed wal_segment_size), the segment-to-LSN
	 * mapping on each side no longer lines up and the derived CN would be
	 * wrong.  Recovery's START-time guard would eventually catch the mismatch
	 * and FATAL, but far downstream and opaquely; reject it here with a clear
	 * message instead.  (wal_segment_size is this node's, already loaded from
	 * its control file by LocalProcessControlFile() before StartupXLOG.)
	 */
	if (old_seg_size != wal_segment_size)
		ereport(FATAL,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("pg_upgrade streaming standby requires matching WAL segment sizes"),
				 errdetail("The retained old data directory uses %d-byte WAL segments but this cluster uses %d-byte segments.",
						   old_seg_size, wal_segment_size),
				 errhint("Re-initialize the new cluster with the same --wal-segsize as the old cluster before streaming the upgrade window.")));

	/* Replicate pg_resetwal's FindEndOfXLOG rule over the old datadir. */
	cn_seg = DeriveUpgradeCnSegment(old_datadir, old_tail, old_seg_size);

	/*
	 * CN is the DB_SHUTDOWNED checkpoint the producer wrote at the very start
	 * of CN_seg on timeline 1: the record begins just past the segment's long
	 * page header.  Because it is a shutdown checkpoint, redo == checkPoint
	 * == that segment-boundary LSN.
	 */
	XLogSegNoOffsetToRecPtr(cn_seg, SizeOfXLogLongPHD, old_seg_size, cn_lsn);

	MemSet(&cn, 0, sizeof(cn));
	cn.redo = cn_lsn;
	cn.ThisTimeLineID = 1;
	cn.PrevTimeLineID = 1;

	/*
	 * The remaining CheckPoint counters (nextXid/nextOid/... ) are left zero:
	 * ArmControlFileForUpgradeRecovery() copies *cn into checkPointCopy only
	 * to seed recovery's start point, and StartupXLOG re-reads them from the
	 * real CN checkpoint record the moment it is streamed in and replayed. So
	 * the zero counters here are transient placeholders, never consulted for
	 * anything durable.
	 */
	armed_cn_lsn = cn_lsn;

	ereport(LOG,
			(errmsg("auto-armed streaming standby from locally derived anchor "
					"(sysid " UINT64_FORMAT ", CN %X/%08X, redo %X/%08X, TLI 1, CN segment %llu)",
					sysid, LSN_FORMAT_ARGS(cn_lsn),
					LSN_FORMAT_ARGS(cn.redo), (unsigned long long) cn_seg)));

	ArmControlFileForUpgradeRecovery(&cn, cn_lsn, sysid, true);
	return true;
}

/*
 * Detect a pending pg_upgrade --wal-upgrade and, if found, arm recovery to
 * replay it.  Called from the startup process before StartupXLOG().  Dispatches
 * to one of three paths:
 *
 *   - streaming standby: a fresh skeleton with primary_conninfo derives CN
 *     locally and arms to stream the window from the primary;
 *   - local window: START/COMPLETE markers already in pg_wal/ are scanned,
 *     CN derived, and the control file armed to replay them;
 *   - archive PITR: no local window, but the sentinel is staged, so the
 *     bootstrap is armed for the window arriving via restore_command.
 *
 * Returns true when it armed the bootstrap (caller proceeds into StartupXLOG),
 * false for an ordinary (non-upgrade or already-finalized) start.
 */
bool
PerformWalUpgradeIfNeeded(void)
{
	char		wal_dir[MAXPGPATH];
	bool		found_start = false;
	bool		found_complete = false;
	CheckPoint	cn;
	XLogRecPtr	cn_lsn = InvalidXLogRecPtr;
	uint64		wal_sysid = 0;

	/* Skip during pg_upgrade internal server starts (-b binary upgrade mode) */
	if (IsBinaryUpgrade)
		return false;

	/*
	 * Streaming-standby path.  A fresh skeleton with primary_conninfo set
	 * derives CN locally from its retained old data directory and arms the
	 * control file (sysid + CN + TLI=1), then lets StartupXLOG() enter
	 * standby mode and stream the window forward.  Returns false when no
	 * primary is configured (fall through to the local-window path).
	 */
	if (ArmFromLocalDerivationIfConfigured())
	{
		in_upgrade_bootstrap = true;
		armed_streaming_standby = true;

		/*
		 * Suppress hot standby from the very start of recovery, before any
		 * record replays.  A streaming standby has no shared catalogs (those
		 * under global/) on disk until the window streams in; without this,
		 * recovery could reach consistency and admit a read-only connection
		 * in the gap before XLOG_UPGRADE_START, which would FATAL opening a
		 * not-yet-materialized catalog.  Held until XLOG_UPGRADE_COMPLETE
		 * clears it.
		 */
		pgUpgradeReplayInProgress = true;

		return true;
	}

	snprintf(wal_dir, sizeof(wal_dir), XLOGDIR);

	/*
	 * LOCAL-WINDOW PATH.  Scan pg_wal/ for the START/COMPLETE markers and CN.
	 * A completed --wal-upgrade run leaves a START..COMPLETE window in
	 * pg_wal/ (no rename).  Cases:
	 *
	 * pending (not finalized) -> derive CN from the WAL, arm pg_control
	 * in-process, and let StartupXLOG() recover the window. already applied
	 * (COMPLETE marker present, or control checkpoint > CN) -> normal
	 * startup; a prior startup finalized the upgrade. START, no COMPLETE and
	 * not finalized -> crash mid-upgrade; FATAL (see below). no START -> not
	 * an upgrade; normal startup.
	 *
	 * Deriving CN here (rather than a prior offline pg_resetwal stamp) lets
	 * the same WAL stream drive recovery on the primary and on a physical
	 * standby.
	 */
	if (!UpgradeWalScanMarkers(wal_dir, &found_start, &found_complete,
							   &cn, &cn_lsn, &wal_sysid))
	{
		/*
		 * ARCHIVE-PITR PATH.  No local window, but a cross-version
		 * upgrade-PITR restore stages the pg_upgrade.signal sentinel (the
		 * same marker checkDataDir() keys the control-file synthesis on): the
		 * window arrives later via restore_command as recovery replays
		 * forward from a pre-upgrade base backup across the upgrade boundary.
		 * Recovery starts at the base backup's checkpoint and flows through
		 * CN organically, so no re-anchoring is needed here; arm
		 * in_upgrade_bootstrap so the XLOG_UPGRADE_START redo does not FATAL
		 * when the window is reached. (This is the archive path because there
		 * is no local window and, on this branch, recovery.signal drives the
		 * restore rather than primary_conninfo.)
		 *
		 * Gate on the sentinel (not raw recovery.signal/standby.signal): an
		 * ordinary archive PITR or a plain streaming standby must not arm the
		 * bootstrap, or the standby-safety FATAL-halt guard is defeated. This
		 * keeps all three detection sites (here, checkDataDir, and the
		 * streaming path) on the one pg_upgrade.signal sentinel.
		 */
		if (UpgradeSignalStaged())
		{
			ereport(LOG,
					(errmsg("archive recovery active with no local pg_upgrade window; "
							"arming upgrade replay in case the window arrives from the archive")));
			in_upgrade_bootstrap = true;

			/*
			 * Suppress hot standby from the very start of recovery, as the
			 * local-window and streaming paths do.  On this path recovery
			 * replays a pre-upgrade base backup forward and can reach
			 * consistency well before the window's XLOG_UPGRADE_START, so a
			 * read-only backend could otherwise be admitted against the still
			 * old-version, half-upgraded catalog.  Hot-standby activation is
			 * a one-way latch, so the flag must be set here at arm time, not
			 * later in the START redo handler (by then a backend may already
			 * be in). Cleared at XLOG_UPGRADE_COMPLETE.
			 */
			pgUpgradeReplayInProgress = true;
		}
		return false;			/* let StartupXLOG drive recovery from
								 * backup_label */
	}

	/*
	 * Crash-atomicity guard, WAL-scan-independent.  The burst server durably
	 * set upgrade_started in pg_control just before emitting
	 * XLOG_UPGRADE_START; the COMPLETE path sets upgrade_finalized.  So
	 * upgrade_started && !finalized is a partial (crashed) upgrade that must
	 * never auto-serve its half-built catalog, even if the START-bearing
	 * WAL did not survive to first boot (byte-contiguous CN generation can
	 * recycle those segments, so the shutdown checkpoint then hides START
	 * from the scan above and found_start is false). Refuse before the
	 * found_start early-return that would otherwise treat this as an ordinary
	 * cluster.  The old cluster was never written, so re-running pg_upgrade
	 * is the safe recovery.
	 */
	if (GetControlFileUpgradeStarted() && !UpgradeWindowFinalized())
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_upgrade WAL is incomplete: found START without COMPLETE"),
				 errhint("The upgrade did not finish; re-run pg_upgrade from the old cluster (which is intact).")));

	if (!found_start)
		return false;			/* not an upgrade */

	/*
	 * Already finalized?  Requires both durable signals: the pg_control
	 * upgrade_finalized flag (COMPLETE was reached) and the control
	 * checkpoint strictly past CN (a checkpoint was written after the
	 * window).  The flag alone is insufficient because the COMPLETE handler
	 * sets it one checkpoint before the control checkpoint advances, so a
	 * crash in that gap must be re-armed and re-replayed (idempotent), not
	 * skipped.  A checkpoint past CN alone is insufficient because a
	 * smart-shut-down partial upgrade also has one but never set the flag,
	 * and must be refused.  Checking this before the partial-window diagnosis
	 * also means a finalized cluster whose final WAL page is torn (COMPLETE
	 * missed by the scan) is still recognized as done.
	 */
	if (UpgradeWindowFinalized() &&
		!XLogRecPtrIsInvalid(cn_lsn) && GetControlFileCheckPointLSN() > cn_lsn)
		return false;			/* finalized; ordinary startup */

	/*
	 * A complete window whose checkpoint has not yet advanced past CN is a
	 * pending or mid-finalization upgrade (first start, or a crash after the
	 * COMPLETE marker but before the end-of-recovery checkpoint).  Fall
	 * through to arm and (re-)replay it.  The window images are idempotent.
	 * Only a window that never reached COMPLETE is a genuine partial upgrade:
	 * the catalog is half-built and, since the new cluster auto-serves
	 * read-write at end of recovery, arming it would serve a corrupt catalog.
	 * Refuse; the old cluster was never written and is intact, so re-running
	 * pg_upgrade is the safe recovery.
	 */
	if (!found_complete)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_upgrade WAL is incomplete: found START without COMPLETE"),
				 errhint("The upgrade did not finish; re-run pg_upgrade from the old cluster (which is intact).")));

	/*
	 * Pending.  CN must have been found; otherwise the WAL is malformed and
	 * re-arming at an invalid LSN would corrupt recovery.
	 */
	if (XLogRecPtrIsInvalid(cn_lsn))
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_upgrade WAL is missing the end-of-upgrade checkpoint"),
				 errhint("Re-run pg_upgrade from the old cluster to start fresh.")));

	ereport(LOG,
			(errmsg("pg_upgrade WAL found in pg_wal/; arming recovery from end-of-upgrade checkpoint at %X/%08X",
					LSN_FORMAT_ARGS(cn_lsn))));

	/*
	 * Arm the control file in-process: point recovery at CN (state =
	 * DB_IN_PRODUCTION, wal_level = replica) and adopt the upgrade WAL's
	 * system identifier so recovery's per-page xlp_sysid check passes.
	 * StartupXLOG() (called right after) reads ControlFile->checkPointCopy.
	 * Deriving CN and the sysid from the WAL here lets the same WAL stream
	 * drive recovery on the primary and on a physical standby, with no
	 * offline stamping step.
	 */
	ArmControlFileForUpgradeRecovery(&cn, cn_lsn, wal_sysid, false);

	/*
	 * Arm the bootstrap so the redo handlers may apply the upgrade
	 * images.  A pg_upgrade record reached without this flag came in through
	 * an ordinary/standby stream and must not be applied live (see
	 * pg_upgrade_redo).
	 */
	in_upgrade_bootstrap = true;

	/*
	 * Suppress hot standby before any record replays, as the streaming path
	 * does: the window's full-page images rebuild the catalogs as they
	 * replay, so a read-only connection admitted between consistency and
	 * XLOG_UPGRADE_START could observe a half-built catalog.  Held until
	 * XLOG_UPGRADE_COMPLETE clears it.  Matters for archive-PITR (consistency
	 * can be reached well before CN); harmless for the primary's own restart.
	 */
	pgUpgradeReplayInProgress = true;

	return true;
}

/*
 * Place one user relation file from the standby's retained old datadir into the
 * new skeleton during XLOG_UPGRADE_RELINK redo, reproducing the result
 * pg_upgrade's transfer step produced on the primary for the given mode:
 *
 *   COPY            - independent full byte copy (copy_file()).
 *   CLONE           - reflink/COW clone: copyfile(COPYFILE_CLONE_FORCE) on macOS,
 *                     ioctl(FICLONE) on Linux (same primitives as cloneFile()).
 *   COPY_FILE_RANGE - copy_file_range().
 *   LINK            - per-file hardlink, sharing the old inode.
 *   SWAP            - rename(), moving the file out of the old datadir.
 *
 * Like the reflink modes in pg_upgrade itself, CLONE/COPY_FILE_RANGE FATAL if the
 * filesystem cannot reflink rather than fall back to a full copy.  The standby
 * either reproduces the operator's chosen space profile or refuses, never silently
 * costing 2x.  The caller has already unlink()ed any pre-existing dst.
 *
 * The placed file is fsync'd before returning (the caller fsyncs the parent dir).
 * These files bypass smgr, so the checkpointer has no sync request for them and the
 * end-of-recovery checkpoint would otherwise advance pg_control past CN with the
 * data only in the OS cache.  A crash there would leave a finalized upgrade with
 * missing user relations, never re-replayed.
 */
static void
RelinkPlaceFile(const char *oldfile, const char *newfile, uint8 mode)
{
	switch (mode)
	{
		case UPGRADE_RELINK_MODE_LINK:
			/* mirror --link: a per-file hardlink, sharing the old inode */
			if (link(oldfile, newfile) != 0)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not link \"%s\" to \"%s\": %m",
								oldfile, newfile)));
			break;

		case UPGRADE_RELINK_MODE_SWAP:

			/*
			 * Mirror --swap: move the file out of the old datadir (upstream
			 * swap renames whole DB dirs, consuming the old cluster).  Unlike
			 * --link this leaves no entry behind.  rename() over an existing
			 * dst is atomic and idempotent on replay (a re-run finds the
			 * source gone via the caller's stat() and skips).
			 */
			if (rename(oldfile, newfile) != 0)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not move \"%s\" to \"%s\": %m",
								oldfile, newfile)));
			break;

		case UPGRADE_RELINK_MODE_CLONE:
			{
				int			save_errno;

				/*
				 * Reproduce pg_upgrade's cloneFile() via the shared helper,
				 * so the primary's transfer and this standby-side placement
				 * use one implementation (copyfile(COPYFILE_CLONE_FORCE) on
				 * macOS, else ioctl(FICLONE) on Linux).
				 */
				switch (pg_clone_file(oldfile, newfile, &save_errno))
				{
					case PG_REFLINK_OK:
						break;
					case PG_REFLINK_ERROR:
						errno = save_errno;
						ereport(PANIC,
								(errcode_for_file_access(),
								 errmsg("could not clone \"%s\" to \"%s\": %m",
										oldfile, newfile),
								 errhint("The old data directory and the new skeleton must be on the same reflink-capable filesystem.")));
						break;
					case PG_REFLINK_UNSUPPORTED:
						ereport(PANIC,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("file cloning not supported on this platform"),
								 errhint("The primary used --clone; a standby on this platform cannot reproduce it.")));
						break;
				}
				break;
			}

		case UPGRADE_RELINK_MODE_COPY_FILE_RANGE:
			{
				int			save_errno;

				/*
				 * reproduce pg_upgrade's copyFileByRange() via the shared
				 * helper
				 */
				switch (pg_copy_file_range_all(oldfile, newfile, &save_errno))
				{
					case PG_REFLINK_OK:
						break;
					case PG_REFLINK_ERROR:
						errno = save_errno;
						ereport(PANIC,
								(errcode_for_file_access(),
								 errmsg("could not copy_file_range \"%s\" to \"%s\": %m",
										oldfile, newfile)));
						break;
					case PG_REFLINK_UNSUPPORTED:
						ereport(PANIC,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("copy_file_range not supported on this platform"),
								 errhint("The primary used --copy-file-range; a standby on this platform cannot reproduce it.")));
						break;
				}
				break;
			}

		case UPGRADE_RELINK_MODE_COPY:
		default:
			/* plain full copy: matches --copy (independent file, 2x space) */
			copy_file(oldfile, newfile);
			break;
	}

	/*
	 * Make the placed file's contents durable.  copy_file() flushes but does
	 * not fsync (its bin/ callers fsync separately); the reflink/hardlink
	 * paths fsync nothing.  Without this the end-of-recovery checkpoint could
	 * advance the control file past CN with the data still in the OS cache.
	 * See the function header.  The caller fsyncs the parent directory so the
	 * new dentry itself is durable.
	 */
	fsync_fname(newfile, false);
}

/*
 * Build the source path of a user relation in the retained old datadir for
 * XLOG_UPGRADE_RELINK redo.
 *
 * For base/ and global/ relations the relpath is version-independent, so
 * prefixing the old datadir is correct.  For a relation in a user-created tablespace the
 * relpath GetRelationPath() produced embeds this (new) binary's
 * TABLESPACE_VERSION_DIRECTORY ("PG_<newmajor>_<newcat>"), but the retained
 * old datadir's tablespace area only holds the old version's directory
 * ("PG_<oldmajor>_<oldcat>").  Blindly prefixing would name a nonexistent path
 * and the entry would be silently skipped, losing every tablespace relation on
 * the standby.  So for the tablespace case we substitute the actual PG_*
 * directory found under old_datadir/pg_tblspc/<spcoid>/.  Returns false if that
 * directory cannot be resolved (no PG_* entry), so the caller can error rather
 * than skip.
 */
static bool
RelinkBuildOldFile(const char *old_datadir, Oid spcOid,
				   const char *relpath, const char *segsuffix,
				   char *oldfile, size_t oldfile_size)
{
	const char *tblspc_prefix = PG_TBLSPC_DIR_SLASH;
	size_t		tblspc_prefix_len = strlen(tblspc_prefix);
	const char *verseg;
	const char *after_ver;
	DIR		   *dir;
	struct dirent *de;
	char		spcdir[MAXPGPATH];
	char		oldverdir[MAXFNAMELEN];
	bool		found = false;

	/* base/ and global/ are unversioned: a plain prefix is correct. */
	if (strncmp(relpath, tblspc_prefix, tblspc_prefix_len) != 0)
	{
		snprintf(oldfile, oldfile_size, "%s/%s%s",
				 old_datadir, relpath, segsuffix);
		return true;
	}

	/*
	 * Tablespace relpath: "pg_tblspc/<spc>/PG_<ver>/<db>/<rel>".  Locate the
	 * "PG_<ver>" segment (the component after "pg_tblspc/<spc>/") and the
	 * tail that follows it ("/<db>/<rel>").
	 */
	verseg = strchr(relpath + tblspc_prefix_len, '/');
	if (verseg == NULL)
		return false;			/* malformed; no version segment */
	verseg++;					/* now at "PG_<ver>/..." */
	after_ver = strchr(verseg, '/');
	if (after_ver == NULL)
		return false;			/* malformed; no db/rel tail */

	/* Find the single PG_* version directory under the old tablespace. */
	snprintf(spcdir, sizeof(spcdir), "%s/%s%u",
			 old_datadir, tblspc_prefix, spcOid);
	dir = AllocateDir(spcdir);
	if (dir == NULL)
		return false;
	while ((de = ReadDir(dir, spcdir)) != NULL)
	{
		if (strncmp(de->d_name, "PG_", 3) == 0)
		{
			strlcpy(oldverdir, de->d_name, sizeof(oldverdir));
			found = true;
			break;
		}
	}
	FreeDir(dir);
	if (!found)
		return false;

	/* Reassemble with the old version directory in place of the new one. */
	snprintf(oldfile, oldfile_size, "%s/%s%u/%s%s%s",
			 old_datadir, tblspc_prefix, spcOid, oldverdir, after_ver,
			 segsuffix);
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

	if (info == XLOG_UPGRADE_START)
	{
		xl_pg_upgrade *xlrec = (xl_pg_upgrade *) XLogRecGetData(record);
		int			fd;

		/*
		 * pg_version is a fixed char[8]; a corrupt record may not
		 * NUL-terminate
		 */
		int			len = strnlen(xlrec->pg_version, sizeof(xlrec->pg_version));

		/*
		 * Standby / ordinary-stream guard.  The upgrade image records carry
		 * the old cluster's page LSNs and are only safe to apply from the
		 * upgrade bootstrap (anchored at CN into a non-serving data
		 * directory).  Reaching START without in_upgrade_bootstrap means an
		 * ordinary/standby stream, so FATAL at the boundary rather than apply
		 * the window live.
		 *
		 * For a physical standby this FATAL is the intentional halt: the
		 * operator installs the new-version binary and relaunches, and
		 * startup then anchors at CN and replays the window.  StandbyMode
		 * selects the message so the operator sees which case fired.
		 */
		if (!in_upgrade_bootstrap)
		{
			if (StandbyMode)
				ereport(FATAL,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("reached pg_upgrade boundary on standby; halting to apply the upgrade"),
						 errdetail("A --wal-upgrade was performed on the primary; the standby cannot apply it while streaming."),
						 errhint("Install the new-version binaries and restart this standby; it will replay the upgrade from the end-of-upgrade checkpoint.")));
			else
				ereport(FATAL,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("pg_upgrade WAL encountered during replay"),
						 errhint("Restart this server to apply the pg_upgrade; "
								 "the upgrade cannot be replayed on a running standby.")));
		}

		/*
		 * Streaming-standby derivation guard.  On the streaming path CN is
		 * derived locally (ArmFromLocalDerivationIfConfigured) and recovery
		 * armed at armed_cn_lsn.  If that derivation put CN at the wrong
		 * segment, recovery would anchor at the wrong LSN and silently
		 * mis-recover.  CN is the shutdown checkpoint immediately preceding
		 * this START, so recovery's redo pointer here must still equal the
		 * LSN we armed at.  A mismatch means the derived CN did not match the
		 * streamed window: FATAL clearly rather than continue.  (On the
		 * local-window and archive paths armed_cn_lsn is Invalid and this
		 * check is skipped; there CN comes from the WAL scan itself, not a
		 * derivation that could disagree.)
		 */
		if (!XLogRecPtrIsInvalid(armed_cn_lsn))
		{
			XLogRecPtr	redo_now = GetRedoRecPtr();

			if (redo_now != armed_cn_lsn)
				ereport(FATAL,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("derived pg_upgrade CN (%X/%08X) did not match the streamed window (recovery redo at %X/%08X)",
								LSN_FORMAT_ARGS(armed_cn_lsn),
								LSN_FORMAT_ARGS(redo_now)),
						 errhint("The retained old data directory named by \"pg_upgrade_standby_old_datadir\" must be this standby's own pre-upgrade directory, so CN is derived at the segment the primary placed it.")));
		}

		/* Window open: hold off hot standby until XLOG_UPGRADE_COMPLETE. */
		pgUpgradeReplayInProgress = true;

		/*
		 * Informational only: record DB_IN_UPGRADE so a crash mid-window (or
		 * pg_controldata) shows "in pg_upgrade" rather than the "in
		 * production" the arm set as its crash-recovery trigger.  Does not
		 * drive recovery; COMPLETE restores DB_IN_PRODUCTION.
		 */
		if (in_upgrade_bootstrap)
			SetControlFileInUpgrade();

		/*
		 * Write $PGDATA/PG_VERSION from the embedded string; the top-level
		 * PG_VERSION is created by initdb and is not otherwise WAL-logged.
		 * (Per-database PG_VERSION is covered by XLOG_DBASE_CREATE_WAL_LOG.)
		 */

		fd = OpenTransientFile("PG_VERSION",
							   O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
		if (fd < 0)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not open PG_VERSION: %m")));
		if (pg_pwrite(fd, xlrec->pg_version, len, 0) != len)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not write PG_VERSION: %m")));
		if (pg_fsync(fd) != 0)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not fsync PG_VERSION: %m")));
		CloseTransientFile(fd);
	}
	else if (info == XLOG_UPGRADE_COMPLETE)
	{
		/*
		 * Window closed and the cluster is fully reconstructed on disk. Clear
		 * the guard so hot standby may activate.  The redo loop finishes and
		 * StartupXLOG() writes its end-of-recovery checkpoint (advancing the
		 * control checkpoint past COMPLETE), then the cluster comes up
		 * read-write; a streaming standby continues as an ordinary hot
		 * standby.
		 */
		pgUpgradeReplayInProgress = false;

		/* Restore the DB_IN_PRODUCTION trigger borrowed at START. */
		if (in_upgrade_bootstrap)
			ClearControlFileInUpgrade();

		/*
		 * Record the durable "window reached COMPLETE" flag in pg_control,
		 * which (paired with a control checkpoint past CN) lets the next
		 * startup tell a fully-upgraded cluster from a crashed partial one.
		 * SetControlFileUpgradeFinalized() fsyncs pg_control, so it survives
		 * a crash immediately after redo with no clean shutdown in between.
		 * It is set here, one checkpoint before StartupXLOG's end-of-recovery
		 * checkpoint advances the control checkpoint past CN, so a crash in
		 * that gap is correctly re-armed and re-replayed (see
		 * PerformWalUpgradeIfNeeded's finalized guard).
		 */
		SetControlFileUpgradeFinalized();
	}
	else if (info == XLOG_UPGRADE_HANDOFF)
	{
		/*
		 * Old-format streaming-handoff trigger (see xl_pg_upgrade_handoff /
		 * XLogWritePgUpgradeHandoff() for why it exists and when it is
		 * emitted). A standby cannot follow the upgrade in the old WAL
		 * format, so rather than shut down it pauses recovery at the boundary
		 * and keeps serving read-only queries while the operator provisions a
		 * new-version skeleton to stream the window (this node's data
		 * directory is what that skeleton links user relations from).  The
		 * pause is reversible, so an aborted upgrade is recoverable: resume
		 * to follow the restarted old primary, or drain to the end of the old
		 * WAL on the commit path.  Outside StandbyMode (the old primary's own
		 * crash recovery) it is a no-op.
		 */
		if (StandbyMode)
		{
			xl_pg_upgrade_handoff *xlrec =
				(xl_pg_upgrade_handoff *) XLogRecGetData(record);

			if (!HotStandbyActive())
			{
				/*
				 * recoveryPausesHere() refuses to pause unless hot standby is
				 * active (there would be no session able to resume it).  A
				 * caught-up streaming standby reached consistency long ago,
				 * so this is the unusual case; rather than silently continue
				 * past the boundary into WAL this old binary cannot read,
				 * stop cleanly here.  The operator re-provisions from the
				 * window.
				 */
				ereport(FATAL,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("reached pg_upgrade handoff on standby before hot standby was active; shutting down for pg_upgrade"),
						 errdetail("The primary initiated a --wal-upgrade to major version %u; "
								   "this standby cannot follow the upgrade in the old WAL format.",
								   xlrec->target_major_version),
						 errhint("Install the new-version binaries and start a fresh skeleton "
								 "standby with a pg_upgrade.signal file naming this data directory; "
								 "it will stream the upgrade window and link user relations in.")));
			}
			else
			{
				/*
				 * Request a recovery pause.  The main redo loop honors it
				 * before applying the next record (xlogrecovery.c), so the
				 * startup process blocks at the handoff boundary while the
				 * postmaster and hot-standby backends keep serving read-only
				 * queries.  pg_wal_replay_resume() (or promotion) releases
				 * it.
				 */
				ereport(LOG,
						(errmsg("reached pg_upgrade handoff on standby; pausing recovery at the upgrade boundary"),
						 errdetail("The primary initiated a --wal-upgrade to major version %u; "
								   "this standby cannot follow the upgrade in the old WAL format.",
								   xlrec->target_major_version),
						 errhint("Provision a fresh new-version skeleton standby from this data directory, "
								 "then resume with pg_wal_replay_resume(); or, if the upgrade was aborted, "
								 "restart the old primary and resume to keep following it.")));
				SetRecoveryPause(true);
			}
		}
	}
	else if (info == XLOG_UPGRADE_DIRTREE)
	{
		/*
		 * Rebuild the directory skeleton before any file image replays into
		 * it. Paths are PGDATA-relative and emitted parent-before-child, so
		 * one mkdir() per path suffices.  Idempotent: EEXIST is expected
		 * (some directories already exist on disk).
		 */
		xl_upgrade_dirtree *xlrec =
			(xl_upgrade_dirtree *) XLogRecGetData(record);
		char	   *p = (char *) xlrec + SizeOfXLUpgradeDirtree;
		char	   *dir_end;
		char	   *sym_end;
		uint32		done = 0;

		/*
		 * dir_bytes/sym_bytes are untrusted; make sure the two regions fit
		 * within the record before deriving end pointers from them.
		 */
		if ((Size) xlrec->dir_bytes + xlrec->sym_bytes >
			XLogRecGetDataLen(record) - SizeOfXLUpgradeDirtree)
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("dirtree record overruns the record")));
		dir_end = p + xlrec->dir_bytes;
		sym_end = dir_end + xlrec->sym_bytes;

		while (p < dir_end && done < xlrec->ndirs)
		{
			Size		plen = strnlen(p, dir_end - p);

			if (plen == 0 || p + plen >= dir_end)	/* need the NUL terminator */
				break;

			if (mkdir(p, pg_dir_create_mode) != 0 && errno != EEXIST)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not create directory \"%s\": %m",
								p)));

			p += plen + 1;
			done++;
		}

		if (done != xlrec->ndirs)
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("dirtree record damaged: created %u of %u directories",
							done, xlrec->ndirs)));

		/*
		 * Recreate captured symlinks (pg_tblspc/<spcoid> -> external
		 * tablespace location).  Each entry is two NUL-terminated strings:
		 * linkpath, target. Create the target directory then the symlink, so
		 * the tablespace exists before its RELFILE images replay.  EEXIST
		 * tolerated.
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

			/*
			 * linkpath is a PGDATA-relative link (pg_tblspc/<oid>); reject an
			 * absolute or ".." path so a corrupt record cannot plant a
			 * symlink outside the data directory.  (target is legitimately an
			 * absolute external location, so it is not constrained here.)
			 */
			if (linkpath[0] == '/' || strstr(linkpath, "..") != NULL)
				ereport(PANIC,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("unsafe symlink path \"%s\"",
								linkpath)));

			/* ensure the external target directory exists */
			if (mkdir(target, pg_dir_create_mode) != 0 && errno != EEXIST)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not create tablespace directory \"%s\": %m",
								target)));

			if (symlink(target, linkpath) != 0 && errno != EEXIST)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not create symlink \"%s\" -> \"%s\": %m",
								linkpath, target)));

			p = target + tlen + 1;
			done++;
		}

		if (done != xlrec->nsymlinks)
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("dirtree record damaged: created %u of %u symlinks",
							done, xlrec->nsymlinks)));
	}
	else if (info == XLOG_UPGRADE_RELINK)
	{
		/*
		 * Manifest of user relation files the window omits.  On a streaming
		 * standby, link each from this node's retained old datadir into the
		 * new skeleton at the identical relative path.  pg_upgrade preserves
		 * relfilenumbers and db/tablespace OIDs, so old and new paths match.
		 * This is the standby's equivalent of the primary's
		 * transfer_relfile() step, driven by replay.
		 *
		 * The old datadir path is standby-local, supplied via the
		 * pg_upgrade_standby_old_datadir GUC (see
		 * ArmFromLocalDerivationIfConfigured); read it here.  When it is unset
		 * there is nothing to do: on the primary the files are already present,
		 * and on archive/PITR recovery the old files arrived with the base
		 * backup.  A streaming standby that reaches this record with no path
		 * cannot materialize user data, so it FATALs (see below).
		 */
		char	   *ptr = XLogRecGetData(record);
		char	   *end = ptr + XLogRecGetDataLen(record);
		const char *old_datadir = pg_upgrade_standby_old_datadir;
		bool		have_old_datadir = (old_datadir != NULL &&
										old_datadir[0] != '\0');

		if (!have_old_datadir)
		{
			/*
			 * A streaming-standby skeleton (armed_streaming_standby) is empty
			 * of user data, so reaching the manifest with no old-datadir path
			 * to link from means every user relation would silently be absent,
			 * a misconfiguration rather than a no-op.  Refuse to continue rather
			 * than bring up a hot standby that is missing all its user data.
			 * (On the primary's own crash recovery and on archive/PITR the
			 * files are already on disk, so the same manifest is a legitimate
			 * no-op.)
			 */
			if (armed_streaming_standby)
				ereport(FATAL,
						(errcode(ERRCODE_CONFIG_FILE_ERROR),
						 errmsg("reached the relink manifest but \"pg_upgrade_standby_old_datadir\" is not set"),
						 errhint("Set \"pg_upgrade_standby_old_datadir\" in postgresql.conf to this standby's retained pre-upgrade data directory so user relations can be linked in.")));
		}
		else
		{
			/*
			 * The standby may place user relations with a different transfer
			 * mode than the primary used: pg_upgrade_standby_transfer_mode
			 * overrides the per-file mode recorded in the manifest.
			 * PG_UPGRADE_XFER_MIRROR (the default) keeps the primary's
			 * per-file mode; anything else forces that one mode for every
			 * file (e.g. the primary linked to save space, but this standby
			 * wants independent copies so its old datadir stays a bootable
			 * rollback target).  Resolved once per record; whether it is
			 * applied is decided per entry below.
			 */
			bool		override_mode =
				(pg_upgrade_standby_transfer_mode != PG_UPGRADE_XFER_MIRROR);
			uint8		forced_mode = (uint8) pg_upgrade_standby_transfer_mode;

			if (override_mode)
				ereport(DEBUG1,
						(errmsg("overriding relink transfer mode with pg_upgrade_standby_transfer_mode=%d",
								pg_upgrade_standby_transfer_mode)));

			while (ptr < end)
			{
				xl_upgrade_relink_entry ent;
				RelFileLocator rlocator;
				RelPathStr	relpath;
				char		oldfile[MAXPGPATH];
				char		newfile[MAXPGPATH];
				char		segsuffix[32];
				char	   *slash;
				struct stat oldstat;
				uint8		place_mode;

				if (end - ptr < (ptrdiff_t) SizeOfXLUpgradeRelinkEntry)
					ereport(PANIC,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("truncated relink entry")));
				memcpy(&ent, ptr, SizeOfXLUpgradeRelinkEntry);
				ptr += SizeOfXLUpgradeRelinkEntry;

				/*
				 * The record is untrusted (a corrupt-but-CRC-valid record
				 * could carry any byte values).  Bound the fields we index or
				 * branch on before use: forknum indexes forkNames[]
				 * (0..MAX_FORKNUM).
				 */
				if (ent.forknum > MAX_FORKNUM)
					ereport(PANIC,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("relink entry fork number %u out of range",
									ent.forknum)));

				/*
				 * Effective placement mode.  Only "mirror" consults the
				 * manifest tuple's transfer_mode, so only then do we read
				 * and bounds-check it (it selects a placement primitive).
				 * With an operator override the manifest's mode is irrelevant
				 * and left untouched; forced_mode comes from a validated GUC
				 * enum, so it is always in range.
				 */
				if (override_mode)
					place_mode = forced_mode;
				else
				{
					if (ent.transfer_mode > UPGRADE_RELINK_MODE_SWAP)
						ereport(PANIC,
								(errcode(ERRCODE_DATA_CORRUPTED),
								 errmsg("relink entry transfer mode %u out of range",
										ent.transfer_mode)));
					place_mode = ent.transfer_mode;
				}

				rlocator.spcOid = ent.tablespace_oid;
				rlocator.dbOid = ent.database_oid;
				rlocator.relNumber = ent.relfilenumber;
				relpath = GetRelationPath(rlocator.dbOid, rlocator.spcOid,
										  rlocator.relNumber,
										  INVALID_PROC_NUMBER,
										  (ForkNumber) ent.forknum);

				/* segment 0 has no suffix; higher segments are ".N" */
				if (ent.segno == 0)
					segsuffix[0] = '\0';
				else
					snprintf(segsuffix, sizeof(segsuffix), ".%u", ent.segno);

				/*
				 * The destination path is version-stable (this binary built
				 * both relpath and DataDir), but the source path in the
				 * retained old datadir uses the old version's tablespace
				 * directory for a user-tablespace relation, so resolve it (see
				 * RelinkBuildOldFile).
				 */
				if (!RelinkBuildOldFile(old_datadir, rlocator.spcOid,
										relpath.str, segsuffix,
										oldfile, sizeof(oldfile)))
					ereport(PANIC,
							(errcode_for_file_access(),
							 errmsg("could not resolve the old-cluster source path for tablespace %u relation \"%s\"",
									rlocator.spcOid, relpath.str),
							 errhint("The retained old data directory named by \"pg_upgrade_standby_old_datadir\" must contain this standby's pre-upgrade tablespace directory.")));
				snprintf(newfile, sizeof(newfile), "%s/%s%s",
						 DataDir, relpath.str, segsuffix);

				/*
				 * Create the destination's parent dir (base/<dboid>/ etc.);
				 * the skeleton's initdb only made the built-in databases, and
				 * the window's DIRTREE record may not have arrived yet.
				 */
				slash = strrchr(newfile, '/');
				if (slash != NULL)
				{
					*slash = '\0';
					if (MakePGDirectory(newfile) != 0 && errno != EEXIST)
						ereport(PANIC,
								(errcode_for_file_access(),
								 errmsg("could not create directory \"%s\": %m",
										newfile)));
					*slash = '/';
				}

				/*
				 * Place the file, reproducing pg_upgrade's transfer mode (see
				 * RelinkPlaceFile for the per-mode taxonomy).
				 *
				 * The manifest is authoritative for user data, so a
				 * destination that already exists (an empty placeholder a
				 * RELFILE image created for a same-numbered relation, or a
				 * prior replay of this record) is removed and replaced.  A
				 * source that has since vanished (ENOENT) is skipped.
				 */
				if (stat(oldfile, &oldstat) != 0)
				{
					if (errno == ENOENT)
						continue;	/* source gone; nothing to place */
					ereport(PANIC,
							(errcode_for_file_access(),
							 errmsg("could not stat \"%s\": %m",
									oldfile)));
				}
				unlink(newfile);
				RelinkPlaceFile(oldfile, newfile, place_mode);

				/*
				 * Make the new directory entry durable.  RelinkPlaceFile()
				 * fsync'd the file contents; fsyncing the parent directory
				 * here persists the dentry (the new hardlink for --link, the
				 * moved entry for --swap) before COMPLETE advances the
				 * control file past CN.
				 */
				slash = strrchr(newfile, '/');
				if (slash != NULL)
				{
					*slash = '\0';
					fsync_fname(newfile, true);
					*slash = '/';
				}
			}
		}
	}
	else if (info == XLOG_UPGRADE_RAWFILE)
	{
		/*
		 * Write one chunk of a raw non-relation file (PG_VERSION, or an
		 * SLRU segment under pg_xact/ or pg_multixact/), creating any missing
		 * parent directory.  These files are not reachable through the buffer
		 * manager, so this is the only way to rebuild them from an
		 * otherwise-empty data directory.
		 */
		xl_upgrade_rawfile *xlrec =
			(xl_upgrade_rawfile *) XLogRecGetData(record);
		char	   *payload = (char *) xlrec + SizeOfXLUpgradeRawfile;
		char		path[MAXPGPATH];
		char	   *data = payload + xlrec->path_len;
		int			fd;
		char	   *slash;

		/*
		 * The record is untrusted.  Validate path_len + data_len against the
		 * bytes actually present before reading either, and require the path
		 * to be a safe PGDATA-relative path (no absolute path, no "..") so a
		 * corrupt or hostile record cannot write outside the data directory.
		 */
		if (xlrec->path_len >= MAXPGPATH)
			elog(PANIC, "rawfile path too long");
		if ((Size) xlrec->path_len + xlrec->data_len >
			XLogRecGetDataLen(record) - SizeOfXLUpgradeRawfile)
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("rawfile record overruns the record")));
		memcpy(path, payload, xlrec->path_len);
		path[xlrec->path_len] = '\0';
		if (path[0] == '/' || strstr(path, "..") != NULL)
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("unsafe rawfile path \"%s\"", path)));

		/* create parent directory if it does not exist (e.g. base/<dboid>) */
		slash = strrchr(path, '/');
		if (slash != NULL)
		{
			*slash = '\0';
			if (mkdir(path, pg_dir_create_mode) != 0 && errno != EEXIST)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not create directory \"%s\": %m",
								path)));
			*slash = '/';
		}

		/*
		 * Write this chunk at its own offset.  A file larger than one WAL
		 * record arrives as several records with increasing offsets, so the
		 * file must not be truncated here: only the first chunk (offset 0)
		 * truncates, and later chunks extend what the earlier ones wrote.
		 * Replay of the same record twice is idempotent either way.
		 */
		fd = OpenTransientFile(path, O_WRONLY | O_CREAT | PG_BINARY |
							   (xlrec->offset == 0 ? O_TRUNC : 0));
		if (fd < 0)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not open \"%s\": %m", path)));
		if (pg_pwrite(fd, data, xlrec->data_len, (off_t) xlrec->offset) !=
			(ssize_t) xlrec->data_len)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not write \"%s\": %m", path)));
		if (pg_fsync(fd) != 0)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not fsync \"%s\": %m", path)));
		CloseTransientFile(fd);
	}
	else
		elog(PANIC, "unknown op code %u", info);
}
