/*
 * relink_file.h
 *
 * Write and publish the first-start command file.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 * src/bin/pg_upgrade/relink_file.h
 */
#ifndef PG_UPGRADE_RELINK_FILE_H
#define PG_UPGRADE_RELINK_FILE_H

/* Include pg_upgrade.h first. */
typedef struct UpgradeRelinkFile
{
	char		path[MAXPGPATH];
	FILE	   *stream;
	uint32		flags;
	uint32		count;
	xl_pg_upgrade_relink_entry entries[UPGRADE_RELINK_MAX_ENTRIES];
} UpgradeRelinkFile;

extern UpgradeRelinkFile *create_upgrade_relink_file(const char *pgdata);
extern void append_upgrade_relink_entry(UpgradeRelinkFile *file,
										const xl_pg_upgrade_relink_entry *entry);
extern void finish_upgrade_relink_scope(UpgradeRelinkFile *file);
extern void start_upgrade_relink_file(UpgradeRelinkFile *file,
									  uint64 new_system_identifier,
									  xl_pg_upgrade_start *start);
extern void publish_upgrade_relink_file(UpgradeRelinkFile *file);

#endif							/* PG_UPGRADE_RELINK_FILE_H */
