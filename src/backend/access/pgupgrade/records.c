/*-------------------------------------------------------------------------
 *
 * records.c
 *    Insert WAL records for WAL-logged pg_upgrade.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * src/backend/access/pgupgrade/records.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/pgupgrade_wal.h"
#include "access/xlog.h"
#include "access/xloginsert.h"

/*
 * Check that START matches the new server's major version and block and
 * segment sizes, and contains a valid transfer-mode value.
 */
bool
PgUpgradeStartMatchesServer(const xl_pg_upgrade_start *start)
{
	return start->marker.new_major == PG_MAJORVERSION_NUM * 10000 &&
		strncmp(start->marker.pg_version, PG_MAJORVERSION "\n",
				sizeof(start->marker.pg_version)) == 0 &&
		start->block_size == BLCKSZ &&
		start->relseg_blocks == RELSEG_SIZE &&
		start->wal_block_size == XLOG_BLCKSZ &&
		start->wal_segment_size == wal_segment_size &&
		start->slru_pages_per_segment == SLRU_PAGES_PER_SEGMENT &&
		start->transfer_mode <= UPGRADE_RELINK_MODE_SWAP;
}

XLogRecPtr
PgUpgradeInsertRecord(uint8 opcode, const void *data, Size length)
{
	XLogBeginInsert();
	XLogRegisterData((char *) unconstify(void *, data), length);
	return XLogInsert(RM_PG_UPGRADE_ID, opcode);
}

XLogRecPtr
PgUpgradeInsertRawFileRecord(const char *path, const void *data,
							 uint32 data_length, uint64 offset)
{
	xl_pg_upgrade_rawfile record = {0};
	uint32		path_length = strlen(path);

	record.path_len = path_length;
	record.data_len = data_length;
	record.offset = offset;

	XLogBeginInsert();
	XLogRegisterData(&record, SizeOfPgUpgradeRawFile);
	XLogRegisterData(unconstify(char *, path), path_length);
	if (data_length != 0)
		XLogRegisterData((char *) unconstify(void *, data), data_length);
	return XLogInsert(RM_PG_UPGRADE_ID, XLOG_UPGRADE_RAWFILE);
}
