/*
 * relink_file.c
 *
 * Write pg_upgrade_relink.tmp and rename it for WAL emission at first startup.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 * src/bin/pg_upgrade/relink_file.c
 */

#include "postgres_fe.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <time.h>

#include "common/file_perm.h"
#include "common/file_utils.h"
#include "pg_upgrade.h"
#include "relink_file.h"

static void
write_relink_batch(UpgradeRelinkFile *file)
{
	static const char *const operations[] = {
		NULL, "INHERIT", "RECREATE"
	};

	Assert(file->count > 0);
	fprintf(file->stream, "RELINK %u %u\n", file->flags, file->count);
	for (uint32 i = 0; i < file->count; i++)
	{
		const xl_pg_upgrade_relink_entry *entry = &file->entries[i];

		fprintf(file->stream, "%s %u %u %u %u\n",
				operations[entry->operation], entry->key.tablespace_oid,
				entry->key.database_oid, entry->key.filenumber, entry->flags);
	}
	file->count = 0;
	file->flags = 0;
}

UpgradeRelinkFile *
create_upgrade_relink_file(const char *pgdata)
{
	UpgradeRelinkFile *file = pg_malloc0_object(UpgradeRelinkFile);
	int			fd;
	struct stat st;

	if (snprintf(file->path, sizeof(file->path), "%s/%s", pgdata,
				 PG_UPGRADE_RELINK_FILE) >= sizeof(file->path))
		pg_fatal("upgrade relink file path is too long");
	if (lstat(file->path, &st) == 0)
		pg_fatal("upgrade relink file already exists: \"%s\". Use a fresh new cluster", file->path);
	if (errno != ENOENT)
		pg_fatal("could not inspect upgrade relink file \"%s\": %m", file->path);
	if (strlcat(file->path, ".tmp", sizeof(file->path)) >= sizeof(file->path))
		pg_fatal("upgrade relink file path is too long");
	fd = open(file->path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | PG_BINARY,
			  PG_FILE_MODE_OWNER);
	if (fd < 0 && errno == EEXIST)
		pg_fatal("upgrade relink file already exists: \"%s\". "
				 "Use a fresh new cluster", file->path);
	if (fd < 0)
		pg_fatal("could not create upgrade relink file \"%s\": %m", file->path);
#ifndef WIN32
	if (fchmod(fd, S_IRUSR | S_IWUSR) != 0)
		pg_fatal("could not set permissions on upgrade relink file \"%s\": %m",
				 file->path);
#endif
	file->stream = fdopen(fd, "wb");
	if (file->stream == NULL)
		pg_fatal("could not open upgrade relink file \"%s\": %m", file->path);

	/*
	 * Persist the .tmp marker so ordinary startup rejects incomplete
	 * preparation.
	 */
	if (user_opts.do_sync &&
		(fsync_fname(file->path, false) != 0 || fsync_parent_path(file->path) != 0))
		pg_fatal("could not sync upgrade input \"%s\": %m", file->path);
	file->flags = UPGRADE_RELINK_BEGIN;
	return file;
}

void
append_upgrade_relink_entry(UpgradeRelinkFile *file,
							const xl_pg_upgrade_relink_entry *entry)
{
	/* Keep a full last batch pending so it can carry END. */
	if (file->count == UPGRADE_RELINK_MAX_ENTRIES)
		write_relink_batch(file);
	file->entries[file->count++] = *entry;
}

void
finish_upgrade_relink_scope(UpgradeRelinkFile *file)
{
	file->flags |= UPGRADE_RELINK_END;
	write_relink_batch(file);
	file->flags = UPGRADE_RELINK_BEGIN;
}

void
start_upgrade_relink_file(UpgradeRelinkFile *file,
						  uint64 new_system_identifier,
						  xl_pg_upgrade_start *start)
{
	time_t		now = time(NULL);

	if (now == (time_t) -1)
		pg_fatal("could not obtain the upgrade timestamp: %m");
	start->marker.upgrade_time = (int64) now;
	fprintf(file->stream,
			"START " UINT64_FORMAT " %u %u " INT64_FORMAT " " UINT64_FORMAT
			" " UINT64_FORMAT " %u %u %u %u %u %u %u %u %u\n",
			new_system_identifier, start->marker.old_major, start->marker.new_major,
			start->marker.upgrade_time, start->old_system_identifier,
			start->boundary_lsn, start->old_catalog_version, start->old_control_version,
			start->block_size, start->relseg_blocks, start->wal_block_size,
			start->wal_segment_size, start->slru_pages_per_segment, start->old_tli,
			start->transfer_mode);
}

/*
 * Rename pg_upgrade_relink.tmp to pg_upgrade_relink so startup can consume it.
 * Unless --no-sync was requested, durable_rename() syncs the file and its
 * directory as well.
 */
void
publish_upgrade_relink_file(UpgradeRelinkFile *file)
{
	char		final_path[MAXPGPATH];

	Assert(file->stream == NULL);
	strlcpy(final_path, file->path, sizeof(final_path));
	final_path[strlen(final_path) - strlen(".tmp")] = '\0';
	if ((user_opts.do_sync ? durable_rename(file->path, final_path) :
		 rename(file->path, final_path)) != 0)
		pg_fatal("could not publish upgrade input \"%s\": %m", final_path);
}
