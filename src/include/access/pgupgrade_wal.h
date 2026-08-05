/*
 * pgupgrade_wal.h
 *
 * declarations for RM_PG_UPGRADE_ID WAL redo and emit functions.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/pgupgrade_wal.h
 */
#ifndef PGUPGRADE_WAL_H
#define PGUPGRADE_WAL_H

#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "lib/stringinfo.h"

/* WAL upgrade check -- called from StartupProcessMain() before StartupXLOG() */
extern bool PerformWalUpgradeIfNeeded(void);

/*
 * True iff the pg_upgrade.signal sentinel is present, i.e. this data directory
 * is staged for --wal-upgrade recovery.  Shared gate for the detection sites in
 * PerformWalUpgradeIfNeeded() and checkDataDir().
 */
extern bool UpgradeSignalStaged(void);

/*
 * Scan a pg_wal directory for the --wal-upgrade markers (START/COMPLETE)
 * and the end-of-upgrade checkpoint (CN) that precedes START.  Used by the
 * local/primary and archive-PITR startup paths (PerformWalUpgradeIfNeeded).
 * Returns false if there is no readable WAL at all.
 */
extern bool UpgradeWalScanMarkers(const char *waldir, bool *found_start,
								  bool *found_complete, CheckPoint *cn,
								  XLogRecPtr *cn_lsn, uint64 *wal_sysid);

/* RM_PG_UPGRADE_ID rmgr callbacks (registered in rmgrlist.h) */
extern void pg_upgrade_redo(XLogReaderState *record);
extern void pg_upgrade_desc(StringInfo buf, XLogReaderState *record);
extern const char *pg_upgrade_identify(uint8 info);

/* Emit functions (internal to the WAL-window capture, defined in xlog.c) */
extern XLogRecPtr XLogWritePgUpgrade(bool is_start, uint32 old_major_version,
									 uint32 new_major_version);
extern XLogRecPtr XLogWritePgUpgradeHandoff(uint32 old_major_version,
											uint32 target_major_version);
extern XLogRecPtr XLogWriteUpgradeSlruData(uint8 slru_type);
extern XLogRecPtr XLogWriteUpgradeRawFile(const char *path);
extern XLogRecPtr XLogWriteUpgradeDirSkel(void);

/*
 * Emit the entire --wal-upgrade window in one C routine.  Reachable only via
 * the binary-upgrade-gated SQL function binary_upgrade_emit_wal_window(); the
 * individual steps are not exposed as SQL.
 */
extern void EmitUpgradeWalWindow(uint32 old_major_version,
								 uint32 new_major_version,
								 uint8 transfer_mode, bool skip_complete);

/*
 * Accumulation state for the batched XLOG_UPGRADE_RELINK manifest.
 */
typedef struct UpgradeRelfileBatch
{
	char	   *buf;			/* accumulation buffer (freed by BatchEnd) */
	Size		cap;			/* capacity == payload cap */
	Size		used;			/* bytes accumulated for the current record */
	int			nentries;		/* entries in the current record */
	int			nrecords;		/* records flushed so far */
	int			nfiles;			/* files added so far */
	uint8		xfer_mode;		/* relink batch only: pg_upgrade transfer mode
								 * stamped into each entry */
} UpgradeRelfileBatch;

/*
 * Capture one relation-file segment as ordinary XLOG_FPI records (via
 * log_newpages), or log_smgrcreate() for an empty one.  No upgrade-specific
 * record type is involved; stock xlog_redo() restores the pages.
 */
extern void XLogUpgradeCaptureRelfile(const char *path, Oid tsoid, Oid dboid,
									  RelFileNumber rfnum, uint8 forknum,
									  uint32 segno);

/*
 * Batched emission of the XLOG_UPGRADE_RELINK manifest: one fixed-size entry
 * per user relation file that pg_upgrade transferred verbatim (and the window
 * therefore omits).  No file data, just identities; redo links them from the
 * standby's old datadir.  Reuses UpgradeRelfileBatch for the accumulation state.
 */
extern void XLogUpgradeRelinkBatchBegin(UpgradeRelfileBatch *b, uint8 xfer_mode);
extern void XLogUpgradeRelinkBatchAdd(UpgradeRelfileBatch *b,
									  Oid tsoid, Oid dboid, RelFileNumber rfnum,
									  uint8 forknum, uint32 segno);
extern void XLogUpgradeRelinkBatchEnd(UpgradeRelfileBatch *b);

/* force-flush SLRU dirty pages bypassing enableFsync -- for pg_upgrade with fsync=off */
extern void XLogFlushUpgradeSLRU(void);

#endif							/* PGUPGRADE_WAL_H */
