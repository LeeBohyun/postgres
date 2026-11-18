/*-------------------------------------------------------------------------
 *
 * pg_upgrade_data.h
 *    Shared file names and data for WAL-logged pg_upgrade.
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

/* Maximum relink input entries read together. */
#define PG_UPGRADE_RELINK_INPUT_BUFFER_ENTRIES 4096

#define PG_UPGRADE_ENDPOINT_FILE "pg_upgrade_endpoint"
#define PG_UPGRADE_HANDOFF_SIGNAL_FILE "pg_upgrade_handoff.pending"

/*
 * Preparation creates this file. pg_upgrade attempts to remove it after the
 * upgrade transaction commits.
 */
#define PG_UPGRADE_RELINK_FILE "pg_upgrade_relink"
#define PG_UPGRADE_RELINK_FILE_MAGIC UINT64CONST(0x50475552454C4E4B)
#define PG_UPGRADE_RELINK_FILE_VERSION 2

#define PG_UPGRADE_RELINK_RECREATE_SCOPE 0x01
#define PG_UPGRADE_RELINK_DEFAULT_INPLACE 0x02

typedef struct PgUpgradeRelinkDatabase
{
	uint32		database_oid;
	uint32		default_tablespace;
	uint32		relation_count;
	uint8		flags;
	uint8		reserved[3];
} PgUpgradeRelinkDatabase;

#define PG_UPGRADE_RELINK_TRANSFERRED 0x01

typedef struct PgUpgradeRelinkEntry
{
	uint32		tablespace_oid;
	uint32		filenumber;
	uint8		persistence;
	uint8		flags;
	uint16		reserved;
} PgUpgradeRelinkEntry;

typedef struct PgUpgradeRelinkFileHeader
{
	uint64		magic;
	uint32		version;
	uint32		checksum;
	uint64		new_system_identifier;
	uint64		new_section_offset;
	xl_pg_upgrade_start start;
} PgUpgradeRelinkFileHeader;

#endif							/* PG_UPGRADE_DATA_H */
