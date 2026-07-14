/*
 * pgupgrade_wal.h
 *
 * declarations for RM_PG_UPGRADE_ID WAL redo and emit functions.
 *
 * src/include/access/pgupgrade_wal.h
 */
#ifndef PGUPGRADE_WAL_H
#define PGUPGRADE_WAL_H

#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "lib/stringinfo.h"

/* WAL upgrade check — called from StartupProcessMain() before StartupXLOG() */
extern bool PerformWalUpgradeIfNeeded(void);

/* RM_PG_UPGRADE_ID rmgr callbacks (registered in rmgrlist.h) */
extern void pg_upgrade_redo(XLogReaderState *record);
extern void pg_upgrade_desc(StringInfo buf, XLogReaderState *record);
extern const char *pg_upgrade_identify(uint8 info);

/* Emit functions called via SQL wrappers in xlogfuncs.c */
extern XLogRecPtr XLogWritePgUpgrade(bool is_start, uint32 old_major_version,
									 uint32 new_major_version);
extern XLogRecPtr XLogWritePgUpgradeHandoff(uint32 old_major_version,
											uint32 target_major_version);
extern XLogRecPtr XLogWriteUpgradeSlruData(uint8 slru_type);
extern XLogRecPtr XLogWriteUpgradeRawFile(const char *path);
extern XLogRecPtr XLogWriteUpgradeDirSkel(void);

/*
 * Batched emission of relation-file images.  Many file chunks are packed into
 * each XLOG_UPGRADE_RELFILE_DATA record, up to the max WAL payload.
 */
typedef struct UpgradeRelfileBatch
{
	char	   *buf;			/* accumulation buffer (freed by BatchEnd) */
	Size		cap;			/* capacity == payload cap */
	Size		used;			/* bytes accumulated for the current record */
	int			nentries;		/* entries in the current record */
	int			nrecords;		/* records flushed so far */
	int			nfiles;			/* files added so far */
} UpgradeRelfileBatch;

extern void XLogUpgradeRelfileBatchBegin(UpgradeRelfileBatch *b);
extern void XLogUpgradeRelfileBatchAddFile(UpgradeRelfileBatch *b, const char *path,
										   Oid tsoid, Oid dboid, RelFileNumber rfnum,
										   uint8 forknum, uint32 segno);
extern void XLogUpgradeRelfileBatchEnd(UpgradeRelfileBatch *b);
/* force-flush SLRU dirty pages bypassing enableFsync — for pg_upgrade with fsync=off */
extern void XLogFlushUpgradeSLRU(void);

#endif							/* PGUPGRADE_WAL_H */
