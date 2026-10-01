/*-------------------------------------------------------------------------
 *
 * pg_upgrade_records.h
 *    WAL payloads for the pg_upgrade resource manager.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_UPGRADE_RECORDS_H
#define PG_UPGRADE_RECORDS_H

/*
 * START and COMPLETE repeat this marker to identify one upgrade window.
 * window_time is its emission timestamp. pg_version contains the target
 * PG_VERSION contents.
 */
typedef struct xl_pg_upgrade_marker
{
	uint32		old_major;
	uint32		new_major;
	int64		window_time;
	char		pg_version[8];
} xl_pg_upgrade_marker;

/*
 * START extends the window marker with the old cluster identity, its
 * shutdown-checkpoint end LSN, and the physical layout required by replay.
 * transfer_mode records the window-wide primary mode used for FILE INHERIT
 * when the standby transfer mode is mirror.
 */
typedef struct xl_pg_upgrade_start
{
	xl_pg_upgrade_marker marker;
	uint64		old_system_identifier;
	uint64		boundary_lsn;
	uint32		old_catalog_version;
	uint32		old_control_version;
	uint32		block_size;
	uint32		relseg_blocks;
	uint32		wal_block_size;
	uint32		wal_segment_size;
	uint32		slru_pages_per_segment;
	uint32		old_tli;
	uint32		transfer_mode;
} xl_pg_upgrade_start;

#define SizeOfPgUpgradeMarker 24
#define SizeOfPgUpgradeStart (offsetof(xl_pg_upgrade_start, transfer_mode) + sizeof(uint32))

typedef struct xl_pg_upgrade_rawfile
{
	uint32		path_len;
	uint32		data_len;
	uint64		offset;

	/*
	 * Followed by path_len PGDATA-relative path bytes without a NUL, then
	 * data_len file bytes.
	 */
} xl_pg_upgrade_rawfile;

#define SizeOfPgUpgradeRawFile \
	(offsetof(xl_pg_upgrade_rawfile, offset) + sizeof(uint64))

typedef struct xl_pg_upgrade_key
{
	uint32		tablespace_oid;
	uint32		database_oid;
	uint32		filenumber;
} xl_pg_upgrade_key;

static inline int
PgUpgradeCompareKeys(const void *a, const void *b)
{
	const xl_pg_upgrade_key *left = a;
	const xl_pg_upgrade_key *right = b;

	if (left->tablespace_oid != right->tablespace_oid)
		return left->tablespace_oid < right->tablespace_oid ? -1 : 1;
	if (left->database_oid != right->database_oid)
		return left->database_oid < right->database_oid ? -1 : 1;
	return (left->filenumber > right->filenumber) -
		(left->filenumber < right->filenumber);
}

#define UPGRADE_RELINK_BEGIN 0x01
#define UPGRADE_RELINK_END 0x02
#define UPGRADE_RELINK_MAX_ENTRIES 4096
#define UPGRADE_RELINK_INPLACE 0x08

typedef enum PgUpgradeRelinkEntryType
{
	UPGRADE_RELINK_ENTRY_NONE = 0,
	UPGRADE_RELINK_DIRECTORY = 1,
	UPGRADE_RELINK_RELATION = 2,
	UPGRADE_RELINK_FILE = 3
} PgUpgradeRelinkEntryType;

typedef enum PgUpgradeRelinkOperation
{
	UPGRADE_RELINK_NONE = 0,
	UPGRADE_RELINK_INHERIT = 1,
	UPGRADE_RELINK_RECREATE = 2,
	UPGRADE_RELINK_CREATE = 3,
	UPGRADE_RELINK_DELETE = 4
} PgUpgradeRelinkOperation;

/*
 * DIRECTORY and RELATION state their filesystem operation. The key is the
 * common key for INHERIT and RECREATE, the target key for CREATE, and the old
 * key for DELETE. FILE CREATE and RECREATE clear one fork before SMGR CREATE
 * and page records rebuild it. FILE INHERIT entries list retained segments in
 * fork order. Their order within each relation and fork gives the zero-based
 * segment number. Each entry gives its block count. START supplies the
 * window-wide primary mode used when the standby transfer mode is mirror.
 * INPLACE creates a local tablespace directory.
 */
typedef struct xl_pg_upgrade_relink_entry
{
	xl_pg_upgrade_key key;
	uint8		entry_type;
	uint8		operation;
	uint8		fork;
	uint8		flags;
	uint32		blocks;
} xl_pg_upgrade_relink_entry;

typedef struct xl_pg_upgrade_relink
{
	/*
	 * BEGIN and END delimit one storage scope. FILE entries belong to the
	 * preceding RELATION, including across records.
	 */
	uint32		flags;
	xl_pg_upgrade_relink_entry entries[FLEXIBLE_ARRAY_MEMBER];
} xl_pg_upgrade_relink;

#define SizeOfPgUpgradeRelink offsetof(xl_pg_upgrade_relink, entries)
#define SizeOfPgUpgradeRelinkEntry 20

StaticAssertDecl(sizeof(xl_pg_upgrade_marker) == SizeOfPgUpgradeMarker,
				 "unexpected upgrade marker layout");
StaticAssertDecl(SizeOfPgUpgradeStart == 76, "unexpected upgrade START layout");
StaticAssertDecl(sizeof(xl_pg_upgrade_rawfile) == SizeOfPgUpgradeRawFile,
				 "unexpected upgrade RAWFILE layout");
StaticAssertDecl(sizeof(xl_pg_upgrade_relink_entry) == SizeOfPgUpgradeRelinkEntry,
				 "unexpected RELINK entry layout");

#endif							/* PG_UPGRADE_RECORDS_H */
