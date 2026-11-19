/*
 * relink_file.c
 *
 * Build the relink file for storage validation and standby replay.
 *
 * pg_upgrade records the old relation inventory before upgrade and the new
 * relation inventory after schema restore. The backend uses these inventories
 * to validate upgrade storage and derive RELINK WAL operations for standbys
 * to apply to their relation files and database directories.
 *
 * pg_upgrade invokes WAL emission in the new cluster's upgrade transaction
 * and removes the relink file after COMMIT.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 * src/bin/pg_upgrade/relink_file.c
 */

#include "postgres_fe.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <time.h>

#include "common/file_perm.h"
#include "pg_upgrade.h"
#include "relink_file.h"
#include "port/pg_crc32c.h"

#define RELINK_CHECKSUM_BUFFER_SIZE (64 * 1024)

struct UpgradeRelinkFile
{
	char		path[MAXPGPATH];
	FILE	   *stream;
	off_t		length;
	off_t		new_section_offset;
	uint64		entries_remaining;
	bool		new_section_started;
};

static void
write_relink_file(UpgradeRelinkFile *file, const void *data, size_t size)
{
	if (file->stream == NULL)
		pg_fatal("upgrade relink file was written after finalization");
	if ((uint64) file->length + size > PG_INT64_MAX)
		pg_fatal("upgrade relink file is too large");
	if (fwrite(data, size, 1, file->stream) != 1)
		pg_fatal("could not write upgrade relink file \"%s\": %m", file->path);
	file->length += size;
}

UpgradeRelinkFile *
create_upgrade_relink_file(const char *pgdata)
{
	UpgradeRelinkFile *file = pg_malloc0_object(UpgradeRelinkFile);
	PgUpgradeRelinkFileHeader header = {0};
	int			fd;

	if (snprintf(file->path, sizeof(file->path), "%s/%s",
				 pgdata, PG_UPGRADE_RELINK_FILE) >= sizeof(file->path))
		pg_fatal("upgrade relink file path is too long");
	fd = open(file->path, O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | PG_BINARY,
			  PG_FILE_MODE_OWNER);
	if (fd < 0 && errno == EEXIST)
		pg_fatal("upgrade relink file already exists: \"%s\". "
				 "Use a fresh new cluster",
				 file->path);
	if (fd < 0)
		pg_fatal("could not create upgrade relink file \"%s\": %m",
				 file->path);
#ifndef WIN32
	if (fchmod(fd, S_IRUSR | S_IWUSR) != 0)
		pg_fatal("could not set permissions on upgrade relink file \"%s\": %m",
				 file->path);
#endif
	file->stream = fdopen(fd, "w+b");
	if (file->stream == NULL)
		pg_fatal("could not open upgrade relink file \"%s\": %m", file->path);
	write_relink_file(file, &header, sizeof(header));
	return file;
}

void
begin_upgrade_relink_new(UpgradeRelinkFile *file)
{
	if (file->new_section_started || file->entries_remaining != 0 ||
		file->stream == NULL)
		pg_fatal("new-cluster relink input was started out of order");
	file->new_section_offset = file->length;
	file->new_section_started = true;
}

void
begin_upgrade_relink_scope(UpgradeRelinkFile *file, bool new_section,
						   const PgUpgradeRelinkDatabase *database)
{
	if (file->entries_remaining != 0 ||
		new_section != file->new_section_started ||
		file->stream == NULL)
		pg_fatal("upgrade relink scope was started out of order");
	file->entries_remaining = database->relation_count;
	write_relink_file(file, database, sizeof(*database));
}

void
append_upgrade_relink_entry(UpgradeRelinkFile *file,
							const PgUpgradeRelinkEntry *entry)
{
	if (file->entries_remaining == 0)
		pg_fatal("upgrade relink entry was appended outside a scope");
	write_relink_file(file, entry, sizeof(*entry));
	file->entries_remaining--;
}

static uint32
relink_file_checksum(UpgradeRelinkFile *file,
					 const PgUpgradeRelinkFileHeader *header)
{
	char	   *buffer = pg_malloc(RELINK_CHECKSUM_BUFFER_SIZE);
	off_t		offset = sizeof(*header);
	pg_crc32c	crc;
	int			fd = fileno(file->stream);

	if (fflush(file->stream) != 0)
		pg_fatal("could not flush upgrade relink file \"%s\": %m", file->path);
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, header, sizeof(*header));
	while (offset < file->length)
	{
		size_t		wanted = Min((uint64) RELINK_CHECKSUM_BUFFER_SIZE,
								 (uint64) (file->length - offset));
		size_t		done = 0;

		while (done < wanted)
		{
			ssize_t		amount = pg_pread(fd, buffer + done,
										  wanted - done, offset + done);

			if (amount < 0 && errno == EINTR)
				continue;
			if (amount < 0)
				pg_fatal("could not read upgrade relink file \"%s\": %m",
						 file->path);
			if (amount == 0)
				pg_fatal("unexpected end of upgrade relink file \"%s\"",
						 file->path);
			done += amount;
		}
		COMP_CRC32C(crc, buffer, wanted);
		offset += wanted;
	}
	FIN_CRC32C(crc);
	pg_free(buffer);
	return (uint32) crc;
}

void
finish_upgrade_relink_file(UpgradeRelinkFile *file,
						   uint64 new_system_identifier,
						   xl_pg_upgrade_start *start)
{
	PgUpgradeRelinkFileHeader header = {0};
	time_t		transaction_time;

	if (!file->new_section_started || file->entries_remaining != 0 ||
		file->stream == NULL)
		pg_fatal("upgrade relink file was finalized out of order");
	transaction_time = time(NULL);
	if (transaction_time == (time_t) -1)
		pg_fatal("could not obtain the upgrade transaction timestamp: %m");
	start->marker.transaction_time = (int64) transaction_time;
	header.magic = PG_UPGRADE_RELINK_FILE_MAGIC;
	header.version = PG_UPGRADE_RELINK_FILE_VERSION;
	header.new_system_identifier = new_system_identifier;
	header.new_section_offset = file->new_section_offset;
	header.start = *start;
	header.checksum = relink_file_checksum(file, &header);
	if (fseek(file->stream, 0, SEEK_SET) != 0)
		pg_fatal("could not rewind upgrade relink file \"%s\": %m", file->path);
	if (fwrite(&header, sizeof(header), 1, file->stream) != 1)
		pg_fatal("could not write upgrade relink file \"%s\": %m", file->path);
	if (fclose(file->stream) != 0)
		pg_fatal("could not close upgrade relink file \"%s\": %m",
				 file->path);
	file->stream = NULL;
}

void
invoke_upgrade_wal_emission(UpgradeRelinkFile *file, PGconn *conn)
{
	if (conn == NULL || PQstatus(conn) != CONNECTION_OK ||
		PQtransactionStatus(conn) != PQTRANS_IDLE || file->stream != NULL)
		pg_fatal("upgrade WAL requires an idle connection and a complete relink file");

#ifdef USE_ASSERT_CHECKING
	pg_upgrade_test_wait("PG_UPGRADE_TEST_PAUSE_AFTER_RELINK_FINALIZE",
						 "test pause after upgrade relink finalization");
#endif

	/*
	 * COMPLETE remains provisional until the upgrade transaction commits.
	 * The backend rejects commit unless emission reached COMPLETE without an
	 * error.
	 */
	PQclear(executeQueryOrDie(conn, "BEGIN"));
	PQclear(executeQueryOrDie(conn, "SET LOCAL statement_timeout = 0"));
	PQclear(executeQueryOrDie(conn, "SELECT binary_upgrade_emit_wal_file()"));

#ifdef USE_ASSERT_CHECKING
	if (getenv("PG_UPGRADE_TEST_CHECKPOINT_BEFORE_COMMIT") != NULL)
		PQclear(executeQueryOrDie(conn, "CHECKPOINT"));
	if (getenv("PG_UPGRADE_TEST_DISCONNECT_BEFORE_COMMIT") != NULL)
	{
		PQfinish(conn);
		pg_fatal("test disconnect after COMPLETE before COMMIT");
	}
#endif
	PQclear(executeQueryOrDie(conn, "COMMIT"));
	/*
	 * Remove the relink file only after COMMIT. A pre-COMMIT failure leaves it
	 * behind so pg_upgrade rejects reuse of the partial new cluster.
	 */
	if (unlink(file->path) != 0)
		pg_log(PG_WARNING, "could not remove upgrade relink file \"%s\": %m",
			   file->path);
}

void
free_upgrade_relink_file(UpgradeRelinkFile *file)
{
	if (file == NULL)
		return;
	if (file->stream != NULL)
		(void) fclose(file->stream);
	pg_free(file);
}
