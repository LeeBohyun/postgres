/*
 * pgupgrade_wal.c
 *
 * WAL redo and startup handling for RM_PG_UPGRADE_ID records written by
 * pg_upgrade --wal-upgrade:
 *
 *   XLOG_UPGRADE_START    (0x00) -- window open, write PG_VERSION
 *   XLOG_UPGRADE_COMPLETE (0x10) -- window close, informational
 *   XLOG_UPGRADE_SLRU_DATA   (0x20) -- bulk SLRU segment image
 *   XLOG_UPGRADE_RELFILE_DATA(0x30) -- bulk relation file segment image
 *   XLOG_UPGRADE_RAWFILE     (0x50) -- verbatim non-relation file image
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
 * Parse the WAL in "waldir" and locate the pg_upgrade markers plus the
 * end-of-upgrade checkpoint (CN) that recovery must anchor at.  A real
 * XLogReader is used, not a byte-pattern match: the upgrade WAL is full of
 * arbitrary full-page-image bytes, so any fixed byte pair recurs by chance.
 *
 * Out-params:
 *   found_start / found_complete -- the START / COMPLETE markers were seen.
 *   cn        -- CheckPoint of the last online checkpoint preceding START.  This
 *               is CN, the recovery anchor; it carries the transplanted
 *               XID/OID/multixact counters.
 *   cn_lsn    -- record LSN of that checkpoint (-> ControlFile.checkPoint).
 *   complete_lsn -- record LSN of XLOG_UPGRADE_COMPLETE (Invalid if not found).
 *               The caller decides "already applied?" from pg_control (durable)
 *               rather than this, which stays correct after a standby's
 *               end-of-recovery timeline switch.
 *
 * Returns false if there is no readable WAL at all.
 */
bool
UpgradeWalScanMarkers(const char *waldir, bool *found_start,
					  bool *found_complete, CheckPoint *cn,
					  XLogRecPtr *cn_lsn, XLogRecPtr *complete_lsn,
					  uint64 *wal_sysid)
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
	 * highseg, not from lowseg.  The upgrade window is always the topmost
	 * contiguous run; when delivered by archive-PITR staging, pg_wal/ can
	 * also hold unrelated pre-window segments from the restored base backup,
	 * with a gap between them. Starting at lowseg would make
	 * XLogFindNextRecord walk into that hole and FATAL.  Walk down from
	 * highseg while each preceding segment is present.  (On the primary's own
	 * first start the window is the only content, so runstart == lowseg.)
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
	 * with this value -- letting a fresh skeleton adopt the sysid in-process
	 * from the WAL, exactly as it does CN, with no offline sysid stamping.
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
			break;				/* end of WAL or unreadable -- stop */

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
				ereport(PANIC,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("pg_upgrade: checkpoint record too short")));
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
				*complete_lsn = reader->ReadRecPtr;
				break;			/* window is closed; nothing after COMPLETE
								 * matters */
			}
		}
	}

	XLogReaderFree(reader);
	return true;
}


/*
 * True once PerformWalUpgradeIfNeeded() has armed the sanctioned upgrade
 * bootstrap for this startup.  The redo handlers consult it to distinguish the
 * bootstrap replay (apply the upgrade images) from an ordinary/standby stream
 * that merely contains these records (stop and require a restart).
 * Startup-process-local.
 */
static bool in_upgrade_bootstrap = false;

/*
 * Durable "upgrade window fully replayed to COMPLETE" marker, written+fsync'd
 * by the XLOG_UPGRADE_COMPLETE redo handler the instant redo reaches COMPLETE.
 * Paired with a control checkpoint past CN, it lets startup tell a finalized
 * cluster from a crashed partial one (see PerformWalUpgradeIfNeeded).  Relative
 * path: redo runs with cwd == DataDir.
 */
#define UPGRADE_COMPLETE_MARKER	"pg_upgrade_complete.done"

/*
 * Is the durable pg_upgrade_complete.done marker present?  It is fsync'd by the
 * XLOG_UPGRADE_COMPLETE redo handler (and written by pg_upgrade on the primary)
 * the instant the window reaches COMPLETE, so it distinguishes a completed
 * upgrade from a crashed partial one even when a torn final WAL page hides the
 * COMPLETE record from the scan.  The caller pairs this with a control
 * checkpoint past CN to decide "finalized" (see PerformWalUpgradeIfNeeded).
 * Runs before ChangeToDataDir(), so build the path from DataDir.
 */
static bool
UpgradeWindowFinalized(void)
{
	char		marker[MAXPGPATH];
	struct stat st;

	snprintf(marker, sizeof(marker), "%s/%s", DataDir, UPGRADE_COMPLETE_MARKER);
	return stat(marker, &st) == 0;
}


/*
 * Automatic streaming-standby arming from the primary.
 *
 * A fresh vN+1 skeleton with primary_conninfo set fetches the upgrade window
 * anchor over the same replication connection it is about to stream on: it loads
 * libpqwalreceiver, connects, runs PG_UPGRADE_WINDOW_ANCHOR (sysid + CN lsn +
 * redo + TLI), and arms the control file at CN.  Runs in the startup process
 * before StartupXLOG, so no SQL backend is needed.
 *
 * Gated on the pg_upgrade_stream.signal sentinel: only a bare skeleton staged
 * for --wal-upgrade streaming carries it, so an ordinary streaming standby
 * (primary_conninfo set, but no sentinel) is left entirely untouched and starts
 * normally.  Returns false (caller falls back to the local-window path) when the
 * sentinel is absent, no primary is configured, or the primary retains no
 * window.  Connection failure while armed as a skeleton is a hard FATAL.
 */
static bool
ArmFromPrimaryAnchorIfConfigured(void)
{
	WalReceiverConn *conn;
	char	   *err = NULL;
	TimeLineID	primary_tli = 0;
	char	   *anchor_str;
	uint64		sysid = 0;
	uint32		cn_hi = 0,
				cn_lo = 0,
				redo_hi = 0,
				redo_lo = 0;
	XLogRecPtr	cn_lsn;
	CheckPoint	cn;
	char		streamsig[MAXPGPATH];
	struct stat st;

	/*
	 * Only act on a skeleton explicitly staged for --wal-upgrade streaming.
	 * Without this sentinel an ordinary streaming standby would try to fetch
	 * an upgrade anchor from its primary on every start.
	 */
	snprintf(streamsig, sizeof(streamsig), "%s/pg_upgrade_stream.signal", DataDir);
	if (stat(streamsig, &st) != 0)
		return false;

	/* No primary configured -> not an auto-fetch standby. */
	if (PrimaryConnInfo == NULL || PrimaryConnInfo[0] == '\0')
		return false;

	/* Load libpqwalreceiver, exactly as the walreceiver process does. */
	load_file("libpqwalreceiver", false);
	if (WalReceiverFunctions == NULL)
		elog(FATAL, "libpqwalreceiver didn't initialize correctly");

	conn = walrcv_connect(PrimaryConnInfo, true, false, false,
						  "pg_upgrade_anchor", &err);
	if (conn == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("could not connect to the primary to fetch the pg_upgrade window anchor: %s",
						err ? err : "unknown error"),
				 errhint("Set primary_conninfo to a live --wal-upgrade primary.")));

	/*
	 * Everything the standby needs comes from the single
	 * PG_UPGRADE_WINDOW_ANCHOR command: sysid + CN lsn + redo + primary TLI.
	 * A NULL result means the primary retains no window (not upgrading, or
	 * already released) -> fall back to normal startup.
	 */
	anchor_str = walrcv_upgrade_window_anchor(conn);
	walrcv_disconnect(conn);
	if (anchor_str == NULL)
	{
		ereport(LOG,
				(errmsg("primary is not retaining a pg_upgrade window; "
						"this node will start normally (no upgrade to stream)")));
		return false;
	}

	/* "<sysid>/<cn_hi>/<cn_lo>/<redo_hi>/<redo_lo>/<tli>" */
	if (sscanf(anchor_str, UINT64_FORMAT "/%X/%X/%X/%X/%u",
			   &sysid, &cn_hi, &cn_lo, &redo_hi, &redo_lo, &primary_tli) != 6 ||
		sysid == 0 || primary_tli == 0)
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("malformed pg_upgrade window anchor from primary: \"%s\"",
						anchor_str)));

	cn_lsn = ((uint64) cn_hi << 32) | cn_lo;

	MemSet(&cn, 0, sizeof(cn));
	cn.redo = ((uint64) redo_hi << 32) | redo_lo;
	cn.ThisTimeLineID = primary_tli;
	cn.PrevTimeLineID = primary_tli;

	ereport(LOG,
			(errmsg("pg_upgrade: auto-armed streaming standby from primary "
					"(sysid " UINT64_FORMAT ", CN %X/%08X, redo %X/%08X, TLI %u)",
					sysid, LSN_FORMAT_ARGS(cn_lsn),
					LSN_FORMAT_ARGS(cn.redo), primary_tli)));

	ArmControlFileForUpgradeRecovery(&cn, cn_lsn, sysid, true);
	return true;
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

	/*
	 * STREAMING STANDBY PATH.  A fresh skeleton with primary_conninfo set
	 * fetches the anchor from the live primary and arms the control file
	 * (sysid + CN + TLI), then lets StartupXLOG() enter standby mode and
	 * stream the window forward.  Returns false when no primary is configured
	 * or it retains no window (fall through to the local-window path).
	 */
	if (ArmFromPrimaryAnchorIfConfigured())
	{
		in_upgrade_bootstrap = true;

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
							   &cn, &cn_lsn, &complete_lsn, &wal_sysid))
	{
		/*
		 * ARCHIVE-PITR PATH.  No local window, but a cross-version
		 * upgrade-PITR restore stages the pg_upgrade_recovery.signal sentinel
		 * (the same marker checkDataDir() keys the control-file synthesis
		 * on): the window arrives later via restore_command as recovery
		 * replays forward from a pre-upgrade base backup across the upgrade
		 * boundary.  Recovery starts at the base backup's checkpoint and
		 * flows through CN organically, so no re-anchoring is needed here;
		 * just arm in_upgrade_bootstrap so the XLOG_UPGRADE_START redo does
		 * not FATAL when the window is reached.
		 *
		 * Gate on the sentinel (not raw recovery.signal/standby.signal): an
		 * ordinary archive PITR or a plain streaming standby must NOT arm the
		 * bootstrap, or the standby-safety FATAL-halt guard is defeated.
		 * This keeps all three detection sites (here, checkDataDir, and the
		 * streaming path) on their explicit sentinels.
		 */
		char		upgradesig[MAXPGPATH];
		struct stat st;

		snprintf(upgradesig, sizeof(upgradesig), "%s/pg_upgrade_recovery.signal", DataDir);
		if (stat(upgradesig, &st) == 0)
		{
			ereport(LOG,
					(errmsg("archive recovery active with no local pg_upgrade window; "
							"arming upgrade replay in case the window arrives from the archive")));
			in_upgrade_bootstrap = true;

			/*
			 * Suppress hot standby from the very start of recovery, as the
			 * local-window and streaming paths do.  On this path recovery
			 * replays a pre-upgrade base backup forward and can reach
			 * consistency well BEFORE the window's XLOG_UPGRADE_START, so a
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
	 * complete_lsn is unused on this path (the already-applied test uses
	 * cn_lsn).
	 */
	(void) complete_lsn;

	if (!found_start)
		return false;			/* not an upgrade */

	/*
	 * Already finalized?  Requires BOTH durable signals: - the
	 * pg_upgrade_complete.done marker (COMPLETE was reached), and - the
	 * control checkpoint strictly past CN (a checkpoint was written after the
	 * window, so StartupXLOG recovers from there and never re-enters the
	 * window).
	 *
	 * Both are necessary, and neither alone is sufficient:
	 *
	 * - marker alone is not enough: on the redo paths the COMPLETE handler
	 * fsyncs the marker one checkpoint BEFORE the control checkpoint
	 * advances, so a crash in that window leaves the marker present with the
	 * checkpoint still at CN.  The window there must be re-armed and
	 * re-replayed (idempotent); treating it as finalized would skip replay
	 * yet leave the checkpoint at CN, so StartupXLOG re-enters
	 * XLOG_UPGRADE_START without the bootstrap armed and FATALs every start.
	 *
	 * - checkpoint-past-CN alone is not enough: a crashed *partial* upgrade
	 * that was nonetheless smart-shut-down also has a checkpoint past CN but
	 * never wrote the marker, and must be refused, not skipped.
	 *
	 * Checking finalization before the partial-window diagnosis also means a
	 * finalized cluster whose final WAL page is torn (the scan misses
	 * COMPLETE) is still recognized as done: it has both the marker and a
	 * checkpoint past CN.
	 */
	if (UpgradeWindowFinalized() &&
		!XLogRecPtrIsInvalid(cn_lsn) && GetControlFileCheckPointLSN() > cn_lsn)
		return false;			/* finalized; ordinary startup */

	/*
	 * A complete window whose checkpoint has not yet advanced past CN is a
	 * pending or mid-finalization upgrade (first start, or a crash after the
	 * COMPLETE marker but before the end-of-recovery checkpoint).  Fall
	 * through to arm and (re-)replay it -- the window images are idempotent.
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
	 * Replaces the old offline pg_resetwal --upgrade-recovery /
	 * --system-identifier stamping.
	 */
	ArmControlFileForUpgradeRecovery(&cn, cn_lsn, wal_sysid, false);

	/*
	 * Arm the sanctioned bootstrap so the redo handlers may apply the upgrade
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
		 * sanctioned bootstrap (anchored at CN into a non-serving data
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
		 * Drop the durable COMPLETE marker, which (paired with a control
		 * checkpoint past CN) lets the next startup tell a fully-upgraded
		 * cluster from a crashed partial one.  fsync it so it survives a
		 * crash immediately after redo, with no clean shutdown in between.
		 */
		{
			int			mfd;

			mfd = OpenTransientFile(UPGRADE_COMPLETE_MARKER,
									O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
			if (mfd < 0)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("pg_upgrade_redo: could not create \"%s\": %m",
								UPGRADE_COMPLETE_MARKER)));
			if (pg_fsync(mfd) != 0)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("pg_upgrade_redo: could not fsync \"%s\": %m",
								UPGRADE_COMPLETE_MARKER)));
			CloseTransientFile(mfd);
		}
	}
	else if (info == XLOG_UPGRADE_HANDOFF)
	{
		/*
		 * Old-format streaming-handoff trigger, emitted into the old
		 * primary's own WAL just before pg_upgrade shut it down.  Carries no
		 * data -- purely a control signal.
		 *
		 * A StandbyMode server stops cleanly here (FATAL): everything past
		 * this point is either nonexistent or in the new WAL page format this
		 * old binary cannot read.  The operator swaps to the new-version
		 * binary and re-provisions this standby from the delivered upgrade
		 * window.
		 *
		 * Outside StandbyMode (crash recovery of the old primary that wrote
		 * this record) the trigger is a no-op and recovery continues.
		 */
		if (StandbyMode)
		{
			xl_pg_upgrade_handoff *xlrec =
				(xl_pg_upgrade_handoff *) XLogRecGetData(record);

			ereport(FATAL,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("reached pg_upgrade handoff on standby; shutting down for pg_upgrade"),
					 errdetail("The primary initiated a --wal-upgrade to major version %u; "
							   "this standby cannot follow the upgrade in the old WAL format.",
							   xlrec->target_major_version),
					 errhint("Install the new-version binaries and re-provision this standby "
							 "from the delivered upgrade WAL; it will replay the upgrade from "
							 "the end-of-upgrade checkpoint.")));
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
					 errmsg("pg_upgrade_redo: dirtree record overruns the record")));
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
						 errmsg("pg_upgrade_redo: could not create directory \"%s\": %m",
								p)));

			p += plen + 1;
			done++;
		}

		if (done != xlrec->ndirs)
			ereport(PANIC,
					(errmsg("pg_upgrade_redo: dirtree record damaged: created %u of %u directories",
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
						 errmsg("pg_upgrade_redo: unsafe symlink path \"%s\"",
								linkpath)));

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
					(errmsg("pg_upgrade_redo: dirtree record damaged: created %u of %u symlinks",
							done, xlrec->nsymlinks)));
	}
	else if (info == XLOG_UPGRADE_SLRU_DATA)
	{
		/*
		 * Restore the captured SLRU segment image(s).  Emitted last in
		 * pg_upgrade, after all transactions committed and a CHECKPOINT
		 * flushed the merged CLOG/multixact state, so the image carries both
		 * the old cluster's historical commit bits (which live only here,
		 * never in WAL) and the new cluster's restore statuses, and dominates
		 * any earlier replayed commit record for the same page.  This redo is
		 * the sole source reconstructing pg_xact and pg_multixact.  Install
		 * each page into the SimpleLru buffers and flush it so the
		 * end-of-recovery checkpoint cannot clobber it.
		 */
		xl_upgrade_slru_data *xlrec =
			(xl_upgrade_slru_data *) XLogRecGetData(record);
		char	   *data = (char *) xlrec + SizeOfXLUpgradeSlruData;
		Size		seg_size = SLRU_PAGES_PER_SEGMENT * BLCKSZ;
		int64		seg;
		Size		off = 0;

		/*
		 * total_bytes is an untrusted field; validate it against the bytes
		 * the record actually carries before using it to bound the segment
		 * reads below.  Otherwise a record claiming more than it holds would
		 * over-read the WAL buffer.
		 */
		if ((Size) xlrec->total_bytes >
			XLogRecGetDataLen(record) - SizeOfXLUpgradeSlruData)
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("pg_upgrade_redo: SLRU record claims %u bytes but the "
							"record is shorter", xlrec->total_bytes)));

		for (seg = xlrec->first_seg; seg <= xlrec->last_seg; seg++)
		{
			/*
			 * Every segment in first_seg..last_seg must be fully present.  A
			 * short record means the WAL is damaged; silently restoring fewer
			 * segments would leave pg_xact/pg_multixact incomplete, so PANIC
			 * as the sibling handlers do rather than continue with a partial
			 * SLRU.
			 */
			if (off + seg_size > (Size) xlrec->total_bytes)
				ereport(PANIC,
						(errmsg("pg_upgrade_redo: SLRU record truncated: "
								"restored %d of segments %lld..%lld (slru_type %u)",
								(int) (seg - xlrec->first_seg),
								(long long) xlrec->first_seg,
								(long long) xlrec->last_seg,
								xlrec->slru_type)));

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
		 * [entry_0][data_0][entry_1][data_1] ... Restore each chunk
		 * page-by-page through the buffer manager.  Recovery is anchored at
		 * CN, so these images are the sole writers of these pages (the
		 * on-disk file was wiped and pg_restore's WAL is not replayed). Going
		 * through the buffer manager lets XLogReadBufferExtended create the
		 * file and its directory on demand and flush the page at the
		 * end-of-recovery checkpoint; RBM_ZERO_AND_LOCK gives a zero-extended
		 * buffer we overwrite.
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

			/*
			 * The record is untrusted (it only had to pass a CRC check).
			 * Validate every offset against the record end before reading: a
			 * torn or corrupt record must PANIC, not over-read the WAL
			 * buffer.
			 */
			if (end - ptr < (ptrdiff_t) SizeOfXLUpgradeRelfileEntry)
				ereport(PANIC,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("pg_upgrade_redo: truncated relfile entry header")));
			memcpy(&ent, ptr, SizeOfXLUpgradeRelfileEntry);
			ptr += SizeOfXLUpgradeRelfileEntry;
			data = ptr;
			if (ent.nbytes % BLCKSZ != 0 ||
				(Size) ent.nbytes > (Size) (end - ptr))
				ereport(PANIC,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("pg_upgrade_redo: relfile entry payload (%u bytes) "
								"is misaligned or overruns the record", ent.nbytes)));
			ptr += ent.nbytes;

			rlocator.spcOid = ent.tablespace_oid;
			rlocator.dbOid = ent.database_oid;
			rlocator.relNumber = ent.relfilenumber;
			forknum = (ForkNumber) ent.forknum;

			/*
			 * nbytes==0 means an empty relation file: create it and move on.
			 * Empty system catalogs (pg_publication, pg_enum, ...) have
			 * 0-byte relfiles; without recreating them the first write fails
			 * with "could not open file".
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
				 * Keep the captured LSN verbatim -- do not restamp it.  It is
				 * the old cluster's LSN (below CN, so the WAL-before-data
				 * rule holds at flush time), which makes the reconstructed
				 * page byte-identical to what a normal pg_upgrade leaves on
				 * disk.
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
		 * creating any missing parent directory.  These files are not
		 * reachable through the buffer manager, so this is the only way to
		 * rebuild the relation map and version stamps from an otherwise-empty
		 * data directory.
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
			elog(PANIC, "pg_upgrade_redo: rawfile path too long");
		if ((Size) xlrec->path_len + xlrec->data_len >
			XLogRecGetDataLen(record) - SizeOfXLUpgradeRawfile)
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("pg_upgrade_redo: rawfile record overruns the record")));
		memcpy(path, payload, xlrec->path_len);
		path[xlrec->path_len] = '\0';
		if (path[0] == '/' || strstr(path, "..") != NULL)
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("pg_upgrade_redo: unsafe rawfile path \"%s\"", path)));

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
