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
#include "common/pg_upgrade_records.h"
#include "lib/stringinfo.h"

/*
 * START and COMPLETE contain xl_pg_upgrade_start and xl_pg_upgrade_complete,
 * respectively. Both begin with the same marker. Validate the record length
 * before copying it.
 */
static inline bool
PgUpgradeReadMarker(uint8 opcode, const char *data, Size length,
					xl_pg_upgrade_marker *marker, const char **error)
{
	if (length != (opcode == XLOG_UPGRADE_START ?
				   SizeOfPgUpgradeStart : SizeOfPgUpgradeComplete))
	{
		*error = "invalid upgrade marker length";
		return false;
	}
	memcpy(marker, data, SizeOfPgUpgradeMarker);
	return true;
}

extern XLogRecPtr PgUpgradeInsertRawFileRecord(const char *path,
											   const void *data,
											   uint32 data_length,
											   uint64 offset);
extern bool PgUpgradeStartMatchesServer(const xl_pg_upgrade_start *start);

extern void pg_upgrade_redo(XLogReaderState *record);
extern void pg_upgrade_desc(StringInfo buf, XLogReaderState *record);
extern const char *pg_upgrade_identify(uint8 info);

#endif
