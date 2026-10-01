/*
 * emit_upgrade_wal.h
 *
 * Store prepared upgrade operations for the window-emission server.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 * src/bin/pg_upgrade/emit_upgrade_wal.h
 */
#ifndef PG_UPGRADE_EMIT_WAL_H
#define PG_UPGRADE_EMIT_WAL_H

/*
 * Include pg_upgrade.h first.
 */

typedef struct UpgradeRelinkFile UpgradeRelinkFile;

extern UpgradeRelinkFile * create_upgrade_relink_file(
													  const char *pgdata);

extern void begin_upgrade_relink_scope(
									   UpgradeRelinkFile * file, bool target,
									   const PgUpgradeCatalogDatabase * database);
extern void append_upgrade_relink_catalog_entry(
												UpgradeRelinkFile * file, const PgUpgradeCatalogEntry * entry);
extern void end_upgrade_relink_scope(UpgradeRelinkFile * file);
extern void set_upgrade_relink_start(UpgradeRelinkFile * file,
									 const xl_pg_upgrade_start *window);
extern void finish_upgrade_relink_file(UpgradeRelinkFile * file,
									   const xl_pg_upgrade_marker *marker);
extern void bind_upgrade_relink_file(UpgradeRelinkFile * file);
extern void emit_upgrade_relink_file(UpgradeRelinkFile * file, PGconn *conn);
extern void free_upgrade_relink_file(UpgradeRelinkFile * file);

#endif							/* PG_UPGRADE_EMIT_WAL_H */
