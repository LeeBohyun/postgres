/*
 * relink_file.h
 *
 * Build the relink file and invoke upgrade WAL emission.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 * src/bin/pg_upgrade/relink_file.h
 */
#ifndef PG_UPGRADE_RELINK_FILE_H
#define PG_UPGRADE_RELINK_FILE_H

/*
 * Include pg_upgrade.h first.
 */

typedef struct UpgradeRelinkFile UpgradeRelinkFile;

extern UpgradeRelinkFile *create_upgrade_relink_file(
													 const char *pgdata);

extern void begin_upgrade_relink_new(UpgradeRelinkFile *file);
extern void begin_upgrade_relink_scope(
									   UpgradeRelinkFile *file, bool new_section,
									   const PgUpgradeRelinkDatabase *database);
extern void append_upgrade_relink_entry(
										UpgradeRelinkFile *file, const PgUpgradeRelinkEntry *entry);
extern void finish_upgrade_relink_file(UpgradeRelinkFile *file,
									   uint64 new_system_identifier,
									   xl_pg_upgrade_start *start);
extern void invoke_upgrade_wal_emission(UpgradeRelinkFile *file, PGconn *conn);
extern void free_upgrade_relink_file(UpgradeRelinkFile *file);

#endif							/* PG_UPGRADE_RELINK_FILE_H */
