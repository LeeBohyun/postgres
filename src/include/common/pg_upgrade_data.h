/*-------------------------------------------------------------------------
 *
 * pg_upgrade_data.h
 *    Private arguments for preparing and emitting upgrade WAL.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * src/include/common/pg_upgrade_data.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_UPGRADE_DATA_H
#define PG_UPGRADE_DATA_H

#include "common/pg_upgrade_records.h"

/* The visibility map gained its frozen bit at this catalog version. */
#define VISIBILITY_MAP_FROZEN_BIT_CAT_VER 201603011

/* Maximum catalog observations in one relink-file frame. */
#define PG_UPGRADE_CATALOG_MAX_ENTRIES 4096

#define PG_UPGRADE_RELINK_FILE "pg_upgrade_relink"
#define PG_UPGRADE_RELINK_TMP_FILE "pg_upgrade_relink.tmp"
#define PG_UPGRADE_RELINK_FILE_MAGIC UINT64CONST(0x50475552454C4E4B)
#define PG_UPGRADE_RELINK_FILE_VERSION 1
#define PG_UPGRADE_RELINK_NONCE_LENGTH 32

typedef enum PgUpgradeRelinkFileFrameKind
{
	PG_UPGRADE_RELINK_START = 1,
	PG_UPGRADE_RELINK_OLD = 2,
	PG_UPGRADE_RELINK_TARGET = 3,
	PG_UPGRADE_RELINK_COMPLETE = 4
} PgUpgradeRelinkFileFrameKind;

#define PG_UPGRADE_DATABASE_RELATIONS_AVAILABLE 0x01
#define PG_UPGRADE_DATABASE_TEMPLATE0 0x02

typedef struct PgUpgradeCatalogDatabase
{
	uint32		database_oid;
	uint32		default_tablespace;
	uint32		directory_count;
	uint32		relation_count;
	uint8		flags;
	uint8		reserved[3];
}			PgUpgradeCatalogDatabase;

#define PG_UPGRADE_CATALOG_TRANSFERRED 0x01
#define PG_UPGRADE_CATALOG_INPLACE 0x02

/* Directory entries have filenumber zero and precede relation entries. */
typedef struct PgUpgradeCatalogEntry
{
	uint32		tablespace_oid;
	uint32		filenumber;
	uint8		relkind;
	uint8		persistence;
	uint8		flags;
	uint8		reserved;
}			PgUpgradeCatalogEntry;

typedef struct PgUpgradeCatalogBatch
{
	PgUpgradeCatalogDatabase database;
	uint32		flags;
	PgUpgradeCatalogEntry entries[FLEXIBLE_ARRAY_MEMBER];
}			PgUpgradeCatalogBatch;

#define SizeOfPgUpgradeCatalogBatch offsetof(PgUpgradeCatalogBatch, entries)

/*
 * The first scope describes global storage and has no database OIDs. An
 * InvalidOid old or new database OID in later scopes means that side is
 * absent.
 */
typedef struct PgUpgradeDatabase
{
	uint32		old_database_oid;
	uint32		new_database_oid;
	uint32		new_default_tablespace;
}			PgUpgradeDatabase;

/* fork_mask uses 1 << ForkNumber. */
typedef struct PgUpgradeRelation
{
	xl_pg_upgrade_key old_key;
	xl_pg_upgrade_key new_key;
	uint8		operation;
	uint8		persistence;
	uint8		fork_mask;
}			PgUpgradeRelation;

/* The backend retains derived directories before derived relations. */
typedef struct PgUpgradeEmitDatabase
{
	PgUpgradeDatabase header;
	uint64		relation_count;
	uint32		directory_count;
}			PgUpgradeEmitDatabase;

/* A zero filenumber describes a database or global directory operation. */
typedef struct PgUpgradeEmitOperation
{
	PgUpgradeRelation relation;
	uint8		new_directory_inplace;
}			PgUpgradeEmitOperation;

typedef struct PgUpgradeRelinkFileHeader
{
	uint64		magic;
	uint32		version;
	uint32		producer_major;
	uint32		control_version;
	uint32		catalog_version;
	uint32		start_size;
	uint32		batch_header_size;
	uint32		entry_size;
	uint32		marker_size;
	uint64		target_system_identifier;
	uint32		block_size;
	uint32		relseg_blocks;
	uint32		wal_block_size;
	uint32		wal_segment_size;
	char		mock_authentication_nonce[PG_UPGRADE_RELINK_NONCE_LENGTH];
}			PgUpgradeRelinkFileHeader;

typedef struct PgUpgradeRelinkFileFrame
{
	uint32		opcode;
	uint32		sequence;
	uint32		payload_length;
	uint32		item_count;
	uint32		payload_crc;
}			PgUpgradeRelinkFileFrame;

typedef struct PgUpgradeRelinkFileEnd
{
	uint64		total_length;
	uint64		total_items;
	uint32		frame_count;
	uint32		file_crc;
}			PgUpgradeRelinkFileEnd;

StaticAssertDecl(offsetof(PgUpgradeRelinkFileEnd, file_crc) + sizeof(uint32) ==
				 sizeof(PgUpgradeRelinkFileEnd),
				 "relink file checksum must be the last field");

#endif							/* PG_UPGRADE_DATA_H */
