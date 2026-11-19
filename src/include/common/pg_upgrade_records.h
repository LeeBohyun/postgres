/*-------------------------------------------------------------------------
 *
 * pg_upgrade_records.h
 *    WAL payloads for the pg_upgrade resource manager.
 *
 * Upgrade records from START through COMPLETE carry the upgrade transaction's
 * XID. CREATE and RECREATE relations are rebuilt by ordinary SMGR create
 * records and full-page images. COMPLETE is the last upgrade record before
 * the matching upgrade transaction COMMIT.
 *
 * RELINK batches for one database or shared relations must end before an
 * upgrade record of another type. HANDOFF belongs to the old primary's final
 * shutdown sequence, outside the new cluster's upgrade transaction.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_UPGRADE_RECORDS_H
#define PG_UPGRADE_RECORDS_H

#include "pgtime.h"

/* XLogRecord xl_info values for RM_PG_UPGRADE_ID. */
/* First upgrade record in the upgrade transaction. */
#define XLOG_UPGRADE_START			0x00
/* Upgrade finalization marker emitted before COMMIT. */
#define XLOG_UPGRADE_COMPLETE		0x10
/* A chunk of a non-relation file's after-image. */
#define XLOG_UPGRADE_RAWFILE			0x50
/* Pause old-cluster standby replay after the next shutdown checkpoint. */
#define XLOG_UPGRADE_HANDOFF			0x60
/* Directory and relation operations to apply during upgrade replay. */
#define XLOG_UPGRADE_RELINK				0x70

/* Transfer-mode values stored in START. */
#define UPGRADE_RELINK_MODE_CLONE			0
#define UPGRADE_RELINK_MODE_COPY			1
#define UPGRADE_RELINK_MODE_COPY_FILE_RANGE	2
#define UPGRADE_RELINK_MODE_LINK			3
#define UPGRADE_RELINK_MODE_SWAP			4


/*
 * START and COMPLETE carry the same marker. Replay accepts COMPLETE only when
 * its marker and upgrade transaction ID match START. transaction_time is set
 * when pg_upgrade finalizes the relink input. pg_version contains the new
 * PG_VERSION file contents.
 */
typedef struct xl_pg_upgrade_marker
{
	uint32		old_major;
	uint32		new_major;
	int64		transaction_time;
	char		pg_version[8];
} xl_pg_upgrade_marker;

/*
 * START identifies the old cluster and the physical layout required for
 * replay. boundary_lsn is the end of the old primary's final shutdown
 * checkpoint on old_tli. transfer_mode records the primary's transfer mode.
 * Standby replay uses it when pg_upgrade_standby_transfer_mode is mirror.
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

#define SizeOfPgUpgradeMarker sizeof(xl_pg_upgrade_marker)
#define SizeOfPgUpgradeStart (offsetof(xl_pg_upgrade_start, transfer_mode) + sizeof(uint32))

typedef struct xl_pg_upgrade_rawfile
{
	uint32		path_len;
	uint32		data_len;
	uint64		offset;

	/*
	 * Followed by path_len PGDATA-relative path bytes without a terminating NUL,
	 * then data_len file bytes. For files other than pg_control, offset zero
	 * creates or truncates the file and later chunks write at their offsets.
	 * Replay adopts the pg_control settings without replacing local recovery
	 * progress.
	 */
} xl_pg_upgrade_rawfile;

#define SizeOfPgUpgradeRawFile \
	(offsetof(xl_pg_upgrade_rawfile, offset) + sizeof(uint64))

/*
 * Announce the old and new major versions to standbys before the old
 * primary's final shutdown checkpoint.
 */
typedef struct xl_pg_upgrade_handoff
{
	uint32		old_major_version;
	uint32		new_major_version;
	pg_time_t	handoff_time;
} xl_pg_upgrade_handoff;

#define SizeOfPgUpgradeHandoff sizeof(xl_pg_upgrade_handoff)

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

/*
 * BEGIN and END mark the first and last RELINK records for one database,
 * or for shared relations. Entries are batched into records of at most
 * UPGRADE_RELINK_MAX_ENTRIES entries, with directory entries before relation
 * entries across all batches.
 *
 * If all entries fit in one record, it sets both flags. Otherwise, the first
 * record sets BEGIN, the last sets END, and intermediate records set neither.
 */
#define UPGRADE_RELINK_BEGIN 0x01
#define UPGRADE_RELINK_END 0x02
#define UPGRADE_RELINK_MAX_ENTRIES 4096
#define UPGRADE_RELINK_INPLACE 0x08
#define UPGRADE_RELINK_DIRECTORY_FILENUMBER 0

/*
 * INHERIT retains old storage. RECREATE replaces storage present in both
 * inventories. CREATE adds new-only storage. DELETE removes old-only
 * storage. Ordinary WAL rebuilds CREATE and RECREATE relations.
 */
typedef enum PgUpgradeRelinkOperation
{
	UPGRADE_RELINK_INHERIT = 1,
	UPGRADE_RELINK_RECREATE = 2,
	UPGRADE_RELINK_CREATE = 3,
	UPGRADE_RELINK_DELETE = 4
} PgUpgradeRelinkOperation;

/*
 * Each 16-byte entry identifies storage by tablespace OID, database OID,
 * and relfilenumber. A zero filenumber indicates a directory operation.
 * A nonzero filenumber indicates a relation operation. INHERIT and RECREATE
 * use the key present in both inventories, CREATE uses the new key, and
 * DELETE uses the old key.
 *
 * The reserved field and relation flags must be zero. For directory entries,
 * the in-place flag allows replay to create the tablespace directly under
 * pg_tblspc, without a symbolic link.
 */
typedef struct xl_pg_upgrade_relink_entry
{
	xl_pg_upgrade_key key;
	uint8		operation;
	uint8		flags;
	uint16		reserved;
} xl_pg_upgrade_relink_entry;

static inline bool
PgUpgradeRelinkEntryIsDirectory(const xl_pg_upgrade_relink_entry *entry)
{
	return entry->key.filenumber == UPGRADE_RELINK_DIRECTORY_FILENUMBER;
}

typedef struct xl_pg_upgrade_relink
{
	uint32		flags;
	xl_pg_upgrade_relink_entry entries[FLEXIBLE_ARRAY_MEMBER];
} xl_pg_upgrade_relink;

#define SizeOfPgUpgradeRelink offsetof(xl_pg_upgrade_relink, entries)
#define SizeOfPgUpgradeRelinkEntry sizeof(xl_pg_upgrade_relink_entry)

#endif							/* PG_UPGRADE_RECORDS_H */
