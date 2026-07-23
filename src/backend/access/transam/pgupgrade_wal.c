/*
 * pgupgrade_wal.c
 *
 * WAL redo and emit functions for RM_PG_UPGRADE_ID records.
 *
 * This file implements the redo path and pg_waldump support for the five
 * WAL record types written by pg_upgrade --wal-upgrade:
 *
 *   XLOG_UPGRADE_START    (0x00) — window open, write PG_VERSION
 *   XLOG_UPGRADE_COMPLETE (0x10) — window close, informational
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
#include "fmgr.h"				/* pg_upgrade_wal_window_anchor SQL function */
#include "miscadmin.h"
#include "storage/bufmgr.h"		/* buffer-manager RELFILE_DATA redo */
#include "storage/smgr.h"		/* smgr create for empty relfiles */
#include "storage/bufpage.h"	/* PageSetLSN */
#include "storage/fd.h"
#include "storage/copydir.h"	/* copydir() for WAL segment migration */
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "replication/walreceiver.h"	/* libpqwalreceiver client for auto-anchor */
#include "utils/builtins.h"		/* cstring_to_text, psprintf */
#include "utils/elog.h"

/* -------------------------------------------------------------------------
 * pg_upgrade WAL-replay-based atomicity check
 * -------------------------------------------------------------------------
 */

/*
 * PerformWalUpgradeIfNeeded() — scan pg_wal/ for the pg_upgrade START/COMPLETE
 * markers and decide whether StartupXLOG() should crash-recover the upgrade.
 *
 * pg_upgrade --wal-upgrade uses the following protocol:
 *
 *   1. Transplant the XID/OID/multixact counters into pg_control, then restart
 *      and CHECKPOINT (this is CN, the recovery anchor; it carries the counters)
 *   2. Write START, the full physical image (DIRTREE/RELFILE/RAWFILE/SLRU),
 *      COMPLETE, pg_switch_wal()
 *   3. stop_postmaster_immediate() — no checkpoint, WAL intact in pg_wal/
 *   4. wipe the on-disk data image (files only; the skeleton is in DIRTREE)
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
 *      by DIRTREE redo), pg_control, and the top-level PG_VERSION.
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
 *   complete_lsn — the record LSN of XLOG_UPGRADE_COMPLETE (InvalidXLogRecPtr
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
bool
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
			lowseg = segno;
		if (!any || segno > highseg)
			highseg = segno;
		any = true;
	}
	FreeDir(dir);

	if (!any || segsize == 0)
		return false;

	/*
	 * LEE: bound the scan to the CONTIGUOUS run of TLI-1 segments ending at
	 * highseg, not from lowseg.  The upgrade window (CN..COMPLETE) is always the
	 * topmost contiguous run of segments; when it is delivered by archive PITR
	 * staging, pg_wal/ can ALSO contain unrelated pre-window segments from the
	 * restored base backup, with a gap (the upgrade repositions the new cluster's
	 * WAL past the old cluster's end, so intermediate segment numbers never
	 * exist).  Starting the reader at lowseg would make XLogFindNextRecord walk
	 * forward into that hole and FATAL on a missing segment.  Walk down from
	 * highseg while each preceding segment is present to find the run start; the
	 * window's CN checkpoint lives at or after it.  (On the primary's own first
	 * start the window is the only content, so runstart == lowseg and behavior is
	 * unchanged.)
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
	 * LEE: capture the system identifier the upgrade WAL was emitted under, by
	 * reading xlp_sysid from the long page header at the start of the run-start
	 * segment (runstart).  Recovery validates every WAL page's xlp_sysid against
	 * pg_control->system_identifier, so the arming step (ArmControlFileForUpgrade
	 * Recovery) stamps pg_control with THIS value.  That lets a fresh skeleton
	 * replay the delivered burst without any offline sysid stamping -- the sysid
	 * is adopted in-process from the WAL, exactly as CN is.  (We do NOT force the
	 * old cluster's sysid; the burst carries whatever the new cluster had, and
	 * consistency between pg_control and the WAL is all recovery requires.)
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

	/* Find the first valid record at/after the start of the run-start segment. */
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
				break;			/* window is closed; nothing after COMPLETE matters */
			}
		}
	}

	XLogReaderFree(reader);
	return true;
}

/*
 * LEE: pg_upgrade_wal_window_anchor() -> text
 *
 * Run ON THE LIVE (committed) PRIMARY that retains the upgrade window (pinned by
 * the UPGRADE_WINDOW_SLOT replication slot).  Scans the primary's own pg_wal for
 * the CN checkpoint that precedes XLOG_UPGRADE_START and returns the anchor a
 * streaming standby needs to stamp into its control file BEFORE it connects:
 *
 *     "<cn_hi>/<cn_lo>/<redo_hi>/<redo_lo>"   (cn_lsn and redo_lsn, each an
 *                                              %X/%08X pair, slash-joined)
 *
 * The standby also learns the primary's sysid + TLI from IDENTIFY_SYSTEM (a
 * standard replication command), so those are not returned here.  Recovery on the
 * standby re-reads the full CN CheckPoint record from the streamed WAL, so only
 * the LSNs are needed to point recovery at CN.  Returns NULL if no window is
 * present (not an upgrade primary, or the window was already released).
 */
Datum		pg_upgrade_wal_window_anchor(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_upgrade_wal_window_anchor);

Datum
pg_upgrade_wal_window_anchor(PG_FUNCTION_ARGS)
{
	char		wal_dir[MAXPGPATH];
	bool		found_start = false;
	bool		found_complete = false;
	CheckPoint	cn;
	XLogRecPtr	cn_lsn = InvalidXLogRecPtr;
	XLogRecPtr	complete_lsn = InvalidXLogRecPtr;
	uint64		wal_sysid = 0;
	char	   *result;

	snprintf(wal_dir, sizeof(wal_dir), XLOGDIR);

	if (!upgrade_wal_scan_markers(wal_dir, &found_start, &found_complete,
								  &cn, &cn_lsn, &complete_lsn, &wal_sysid) ||
		!found_start ||
		XLogRecPtrIsInvalid(cn_lsn))
		PG_RETURN_NULL();		/* no retained upgrade window here */

	result = psprintf("%X/%08X/%X/%08X",
					  LSN_FORMAT_ARGS(cn_lsn),
					  LSN_FORMAT_ARGS(cn.redo));

	PG_RETURN_TEXT_P(cstring_to_text(result));
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
 * LEE: durable "the upgrade window fully replayed to COMPLETE" marker.
 *
 * It is written+fsync'd by the XLOG_UPGRADE_COMPLETE redo handler (below) the
 * instant redo actually reaches COMPLETE, so its presence proves the window
 * replayed in full.  A frontend "pg_upgrade --wal-upgrade-rollback" consults it, and
 * --status reports it.  Written with a relative path: redo runs with cwd ==
 * DataDir.
 */
#define UPGRADE_COMPLETE_MARKER	"pg_upgrade_complete.done"

/*
 * LEE: true once PerformWalUpgradeIfNeeded() has armed the sanctioned upgrade
 * replay for this startup.  StartupXLOG() consults it at end-of-recovery.
 */
bool
IsUpgradeBootstrap(void)
{
	return in_upgrade_bootstrap;
}

/*
 * LEE: AUTOMATIC streaming-standby arming from the primary.
 *
 * A fresh vN+1 skeleton with primary_conninfo set fetches the upgrade window
 * anchor itself over the SAME replication connection it is about to stream on --
 * no operator "prepare" step and no pre-staged anchor file.  It:
 *   - loads libpqwalreceiver and connects to the primary (replication conn),
 *   - runs IDENTIFY_SYSTEM (sysid + primary TLI),
 *   - runs the PG_UPGRADE_WINDOW_ANCHOR replication command (CN lsn + redo),
 *   - arms the control file at CN via ArmControlFileForUpgradeRecovery(for_streaming),
 * then lets StartupXLOG enter standby mode and stream the window forward.
 *
 * This runs in the startup process BEFORE StartupXLOG (see PerformWalUpgradeIfNeeded),
 * which is why it works: no SQL backend/libpq in the server binary is needed --
 * libpqwalreceiver is a loadable module, exactly as the walreceiver uses it.
 *
 * Fallbacks (all return false so the caller tries the local-window path):
 *   - primary_conninfo not set                     -> not an auto-fetch standby
 *   - primary has no retained window (NULL anchor)  -> not upgrading, or released
 * Connection failure (conninfo IS set and points at a live primary that should
 * have the window) is a hard FATAL: the operator asked for an auto-fetch standby.
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

	/* No primary configured -> not an auto-fetch standby. */
	if (PrimaryConnInfo == NULL || PrimaryConnInfo[0] == '\0')
		return false;

	/* Load libpqwalreceiver, exactly as the walreceiver process does. */
	load_file("libpqwalreceiver", false);
	if (WalReceiverFunctions == NULL)
		ereport(FATAL,
				(errmsg("libpqwalreceiver didn't initialize correctly")));

	conn = walrcv_connect(PrimaryConnInfo, true, false, false,
						  "pg_upgrade_anchor", &err);
	if (conn == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("could not connect to the primary to fetch the pg_upgrade window anchor: %s",
						err ? err : "unknown error"),
				 errhint("Set primary_conninfo to a live --wal-upgrade primary.")));

	/*
	 * Everything the standby needs to arm comes from the single
	 * PG_UPGRADE_WINDOW_ANCHOR command: sysid + CN lsn + redo + primary TLI (no
	 * separate IDENTIFY_SYSTEM round trip).  A NULL result means the primary
	 * retains no upgrade window (not upgrading, or already released) -> fall back
	 * to normal startup.  The standby always streams from an already-upgraded
	 * vN+1 primary, so the primary necessarily understands this command.
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

/*
 * LEE: is a complete pg_upgrade window (START..COMPLETE) present in this
 * cluster's pg_wal/?  Used by checkDataDir() during ARCHIVE-PITR cross-version
 * recovery to decide whether it is safe to synthesize a new-version pg_control
 * over the old one (see miscinit.c).  Runs BEFORE ChangeToDataDir(), so build
 * the pg_wal path from DataDir rather than using the relative XLOGDIR.
 */
bool
UpgradeWindowPresentInWal(void)
{
	char		waldir[MAXPGPATH];
	bool		found_start = false;
	bool		found_complete = false;
	CheckPoint	cn;
	XLogRecPtr	cn_lsn = InvalidXLogRecPtr;
	XLogRecPtr	complete_lsn = InvalidXLogRecPtr;
	uint64		wal_sysid = 0;

	snprintf(waldir, sizeof(waldir), "%s/%s", DataDir, XLOGDIR);
	if (!upgrade_wal_scan_markers(waldir, &found_start, &found_complete,
								  &cn, &cn_lsn, &complete_lsn, &wal_sysid))
		return false;
	return found_start && found_complete;
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
	 * STREAMING STANDBY PATH.  A fresh skeleton with primary_conninfo set fetches
	 * the upgrade window anchor directly from the live primary over the
	 * replication connection (PG_UPGRADE_WINDOW_ANCHOR) and arms from it -- stamps
	 * the control file with the primary's sysid + CN + TLI so the walreceiver's
	 * sysid check passes and recovery starts at CN -- then lets StartupXLOG()
	 * enter standby mode and stream the window forward.  Returns false (fall
	 * through to the local-window path / normal startup) when there is no primary
	 * configured or the primary retains no window.
	 */
	if (ArmFromPrimaryAnchorIfConfigured())
	{
		in_upgrade_bootstrap = true;

		/*
		 * Suppress hot standby from the very start of recovery, BEFORE any record
		 * is replayed -- not just from the XLOG_UPGRADE_START redo.  A streaming
		 * standby staged without initdb has no shared catalogs (global/*) on disk;
		 * they only arrive when the upgrade window streams in.  Recovery would
		 * otherwise reach a consistent point and admit read-only connections in the
		 * gap before XLOG_UPGRADE_START replays, and such a connection would FATAL
		 * trying to open a not-yet-materialized catalog (e.g. global/1260,
		 * pg_authid).  Setting the guard here holds hot standby off until
		 * XLOG_UPGRADE_COMPLETE clears it, by which point the whole cluster --
		 * including the shared catalogs -- has been reconstructed.
		 */
		pgUpgradeReplayInProgress = true;

		return true;
	}

	snprintf(wal_dir, sizeof(wal_dir), XLOGDIR);

	/*
	 * Parse pg_wal/ with a real XLogReader and look for the pg_upgrade
	 * START/COMPLETE markers plus the end-of-upgrade checkpoint (CN).  The
	 * upgrade WAL lives in pg_wal/ (no rename): a completed --wal-upgrade
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
	 *   START, no COMPLETE -> crash mid-upgrade.  Refuse to start (FATAL): since
	 *                       the new cluster now auto-serves, replaying a partial
	 *                       window would serve a corrupt half-built catalog.  The
	 *                       partial new_dir is a dead end: it does NOT resume or
	 *                       repair in place.  Recovery is --wal-upgrade-rollback
	 *                       (discard new_dir) then re-run pg_upgrade -- which is
	 *                       safe because the OLD cluster was never written during
	 *                       the upgrade and is intact.
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
	{
		/*
		 * LEE: ARCHIVE-PITR PATH.  No upgrade window is present in local pg_wal/,
		 * but if this is an archive-recovery restore (recovery.signal present),
		 * the window may arrive later via restore_command as recovery replays
		 * forward from a pre-upgrade base backup, across the upgrade boundary,
		 * into the post-upgrade tail.  Recovery starts at the base backup's
		 * checkpoint (from backup_label) and flows through CN organically, so we
		 * need not re-anchor the control file here (unlike the primary-first-start
		 * and streaming-standby paths); we only need to permit the upgrade redo
		 * handlers to apply when the window is reached.  Arm in_upgrade_bootstrap
		 * so the XLOG_UPGRADE_START redo does not FATAL as "encountered during
		 * replay".  When no window is ever reached (an ordinary PITR restore),
		 * the flag is simply never consulted.  See PITR_UPGRADE_DESIGN.md.
		 */
		struct stat st;

		if (stat(RECOVERY_SIGNAL_FILE, &st) == 0 ||
			stat(STANDBY_SIGNAL_FILE, &st) == 0)
		{
			ereport(LOG,
					(errmsg("archive recovery active with no local pg_upgrade window; "
							"arming upgrade replay in case the window arrives from the archive")));
			in_upgrade_bootstrap = true;
		}
		return false;			/* let StartupXLOG drive recovery from backup_label */
	}

	/*
	 * complete_lsn is populated by the scan but not needed on this path (the
	 * already-applied test below uses cn_lsn); reference it so the compiler does
	 * not flag it unused.  found_complete IS now consulted (auto-serve gate).
	 */
	(void) complete_lsn;

	if (!found_start)
		return false;			/* not an upgrade */

	/*
	 * LEE (2026-07-20, auto-serve): the LOCAL-window path now requires a
	 * COMPLETE marker up front.  The window is entirely present on local disk
	 * here (the incremental STREAMING case already returned above via the
	 * streaming anchor), so START-without-COMPLETE means the upgrade crashed
	 * mid-window and the reconstructed catalog is half-built.  Since the new
	 * cluster now AUTO-SERVES read-write at end of recovery (the quarantine hold
	 * is gone), we must NOT arm and replay a partial window -- that would serve a
	 * corrupt half-upgraded catalog.  Refuse loudly instead; the old cluster was
	 * never written during the upgrade and is intact, so re-running pg_upgrade is
	 * the safe recovery (mirrors upstream's refusal to start a half-done upgrade).
	 */
	if (!found_complete)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_upgrade WAL is incomplete: found START without COMPLETE"),
				 errhint("The upgrade did not finish; re-run pg_upgrade from the old cluster (which is intact).")));

	/*
	 * Already applied?  Decide from the control-file checkpoint vs CN, NOT vs
	 * COMPLETE (complete_lsn may be Invalid when the window is still streaming
	 * in).  A pending/held cluster's control checkpoint sits AT or before CN (it
	 * was armed at CN, or never armed); a finalized/committed cluster wrote its
	 * end-of-recovery checkpoint PAST CN.  So "checkpoint strictly past CN" means
	 * the upgrade already finalized -- re-arming at CN would re-replay over live
	 * data, so treat it as an ordinary startup.  This is COMPLETE-independent and
	 * timeline-independent.
	 */
	if (!XLogRecPtrIsInvalid(cn_lsn) && GetControlFileCheckPointLSN() > cn_lsn)
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
	ArmControlFileForUpgradeRecovery(&cn, cn_lsn, wal_sysid, false);

	/*
	 * Arm the sanctioned bootstrap: the pg_upgrade redo handlers may now apply
	 * the upgrade images.  Any pg_upgrade record reached WITHOUT this flag set
	 * came in through an ordinary/standby WAL stream and must not be applied
	 * live (see the XLOG_UPGRADE_START handler in pg_upgrade_redo()).
	 */
	in_upgrade_bootstrap = true;

	/*
	 * Suppress hot standby from the very start of recovery, BEFORE any record is
	 * replayed -- not just from the XLOG_UPGRADE_START redo -- exactly as the
	 * streaming-standby path above does.  Recovery is anchored at CN and the
	 * window's full-page images rebuild the cluster (catalogs included) as they
	 * replay; between reaching a consistent point and replaying XLOG_UPGRADE_START
	 * the cluster is only partially reconstructed, so a read-only connection
	 * admitted in that gap could observe (or FATAL on) a half-built catalog.
	 * Holding the guard here keeps hot standby off until XLOG_UPGRADE_COMPLETE
	 * clears it, by which point the whole cluster has been reconstructed.  This
	 * matters for the archive-PITR path (a restored base backup can reach
	 * consistency well before CN) and is harmless for the primary's own restart.
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
		int			len = strlen(xlrec->pg_version);

		/*
		 * LEE: standby / ordinary-stream guard.  The upgrade image records
		 * (DIRTREE/RELFILE/SLRU/RAWFILE) carry the OLD cluster's page LSNs and
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
		 * LEE: the upgrade window is now open.  Suppress hot standby activation
		 * until XLOG_UPGRADE_COMPLETE replays, so no read-only connection can
		 * observe the half-upgraded cluster (new catalogs partially applied).
		 */
		pgUpgradeReplayInProgress = true;

		/*
		 * INFORMATIONAL only: record DB_IN_UPGRADE in the control file so a crash
		 * mid-window (or an operator peeking with pg_controldata) shows "in
		 * pg_upgrade" instead of the misleading "in production" that the arm set as
		 * its crash-recovery trigger.  This does NOT drive recovery -- COMPLETE
		 * restores DB_IN_PRODUCTION, and the end-of-recovery seam then goes live.
		 * Guarded by in_upgrade_bootstrap so it only marks a sanctioned
		 * reconstruction, never an ordinary replay.
		 */
		if (in_upgrade_bootstrap)
			SetControlFileInUpgrade();

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
	else if (info == XLOG_UPGRADE_COMPLETE)
	{
		/*
		 * LEE: the upgrade window is now closed and the cluster is fully
		 * reconstructed on disk.  Clear the guard so hot standby may activate
		 * normally (CheckRecoveryConsistency will pick it up on the next call).
		 *
		 * The cluster now AUTO-SERVES: we let the redo loop finish and StartupXLOG()
		 * write its normal end-of-recovery checkpoint (flushing the reconstructed
		 * data durably and advancing the control checkpoint past COMPLETE), then it
		 * comes up read-write.  A STREAMING standby (fresh skeleton streaming the
		 * window from the already-committed primary) likewise simply continues as an
		 * ordinary hot standby following the primary once the window is replayed.
		 */
		pgUpgradeReplayInProgress = false;

		/*
		 * Restore the DB_IN_PRODUCTION crash-recovery trigger we borrowed at START
		 * (informational DB_IN_UPGRADE).  The window is fully applied; the
		 * end-of-recovery seam will set the terminal state (DB_IN_PRODUCTION on
		 * go-live).  Only meaningful for the sanctioned bootstrap that set it.
		 */
		if (in_upgrade_bootstrap)
			ClearControlFileInUpgrade();

		/*
		 * Drop the durable COMPLETE marker.  This file -- written only when redo
		 * actually reaches COMPLETE -- is what lets a frontend "pg_upgrade
		 * --wal-upgrade-rollback" tell a fully-upgraded cluster from a partial
		 * (crash-truncated) one.  fsync it: a frontend command may run right after
		 * this without a clean shutdown in between.
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
		 * Rebuild the new cluster's directory skeleton (the logged after-image
		 * of initdb's directory tree) before any file image is replayed into
		 * it.  Paths are PGDATA-relative and were emitted parent-before-child,
		 * so a plain mkdir() per path suffices.  Idempotent: EEXIST is expected
		 * because the target already has some of these directories (the
		 * primary's wipe leaves a few behind; a re-provisioned standby starts
		 * from a fresh initdb skeleton), so replay only fills in the rest.
		 */
		xl_upgrade_dirtree *xlrec =
			(xl_upgrade_dirtree *) XLogRecGetData(record);
		char	   *p = (char *) xlrec + SizeOfXLUpgradeDirtree;
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
					(errmsg("pg_upgrade_redo: dirtree record damaged: created %u of %u directories",
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
					(errmsg("pg_upgrade_redo: dirtree record damaged: created %u of %u symlinks",
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
