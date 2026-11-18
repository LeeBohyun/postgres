/*-------------------------------------------------------------------------
 *
 * pgupgrade_wal.h
 *	  WAL upgrade emission and replay interfaces.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/include/access/pgupgrade_wal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGUPGRADE_WAL_H
#define PGUPGRADE_WAL_H

#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "common/pg_upgrade_records.h"
#include "lib/stringinfo.h"

/*
 * START contains xl_pg_upgrade_start. COMPLETE contains only the shared
 * xl_pg_upgrade_marker. Validate the record length before copying the marker.
 */
static inline bool
PgUpgradeReadMarker(uint8 opcode, const char *data, Size length,
					xl_pg_upgrade_marker *marker, const char **error)
{
	if (length != (opcode == XLOG_UPGRADE_START ?
				   SizeOfPgUpgradeStart : SizeOfPgUpgradeMarker))
	{
		*error = "invalid upgrade marker length";
		return false;
	}
	memcpy(marker, data, SizeOfPgUpgradeMarker);
	return true;
}

extern XLogRecPtr PgUpgradeInsertRecord(uint8 opcode, const void *data,
										Size length);
extern XLogRecPtr PgUpgradeInsertRawFileRecord(const char *path,
											   const void *data,
											   uint32 data_length,
											   uint64 offset);
extern bool PgUpgradeStartMatchesServer(const xl_pg_upgrade_start *start);

extern void PerformWalUpgradeIfNeeded(void);

typedef enum UpgradeRecoveryMode
{
	UPGRADE_RECOVERY_NONE,
	UPGRADE_RECOVERY_ARCHIVE,
	UPGRADE_RECOVERY_STANDBY
} UpgradeRecoveryMode;

extern UpgradeRecoveryMode GetUpgradeRecoveryMode(void);
extern int	GetUpgradeArchiveWalSegmentSize(void);

/* Old-cluster fields read with that installation's pg_controldata. */
typedef struct OldUpgradeControlData
{
	uint64		system_identifier;
	uint32		major_version;	/* server_version_num units */
	uint32		control_version;
	uint32		catalog_version;
	uint32		block_size;
	uint32		blocks_per_segment;
	uint32		wal_block_size;
	XLogRecPtr	checkpoint_lsn; /* checkPoint */
	XLogRecPtr	checkpoint_redo;	/* checkPointCopy.redo */
	XLogRecPtr	checkpoint_end_lsn; /* minRecoveryPoint */
	TimeLineID	checkpoint_tli; /* checkPointCopy.ThisTimeLineID */
	TimeLineID	checkpoint_end_tli; /* minRecoveryPointTLI */
	int			wal_segment_size;	/* xlog_seg_size in bytes */
} OldUpgradeControlData;

extern void ReadOldUpgradeControlData(const char *old_datadir,
									  OldUpgradeControlData *result);
extern void ReadArchiveUpgradeControlData(const char *old_datadir,
										  OldUpgradeControlData *result);
extern void pg_upgrade_redo(XLogReaderState *record);
extern void PgUpgradeReplayCommit(XLogReaderState *record);
extern void PgUpgradeCheckpointReplayed(const CheckPoint *checkpoint,
										XLogReaderState *record);
extern void PgUpgradeCheckpointApplied(void);
extern bool PgUpgradeReplayIsInProgress(void);
extern void PgUpgradeSelectReplay(void);
extern void PgUpgradeCheckRecoveryFinish(void);
extern void PgUpgradeCheckRecoveryTarget(void);
extern void pg_upgrade_desc(StringInfo buf, XLogReaderState *record);
extern const char *pg_upgrade_identify(uint8 info);

#endif
