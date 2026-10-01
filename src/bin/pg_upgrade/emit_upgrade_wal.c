/*
 * emit_upgrade_wal.c
 *
 * Store upgrade operations and ask the new server to emit their WAL.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 * src/bin/pg_upgrade/emit_upgrade_wal.c
 */

#include "postgres_fe.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <time.h>

#include "catalog/pg_control.h"
#include "common/controldata_utils.h"
#include "common/file_perm.h"
#include "common/file_utils.h"
#include "common/int.h"
#include "pg_upgrade.h"
#include "emit_upgrade_wal.h"
#include "port/pg_crc32c.h"

struct UpgradeRelinkFile
{
	char		temporary_path[MAXPGPATH];
	char		final_path[MAXPGPATH];
	int			fd;
	off_t		length;
	uint32		sequence;
	uint64		total_items;
	PgUpgradeCatalogBatch *batch;
	uint32		batch_count;
	uint32		batch_kind;
	bool		bound;
};

StaticAssertDecl(PG_UPGRADE_RELINK_NONCE_LENGTH == MOCK_AUTH_NONCE_LEN,
				 "relink file nonce length differs from pg_control");

static void
read_at(int fd, void *data, size_t length, off_t offset, const char *path)
{
	size_t		done = 0;

	while (done < length)
	{
		ssize_t		amount = pg_pread(fd, (char *) data + done,
									  length - done, offset + done);

		if (amount < 0 && errno == EINTR)
			continue;
		if (amount < 0)
			pg_fatal("could not read upgrade relink file \"%s\": %m", path);
		if (amount == 0)
			pg_fatal("unexpected end of upgrade relink file \"%s\"", path);
		done += amount;
	}
}

static void
write_at(int fd, const void *data, size_t length, off_t offset,
		 const char *path)
{
	size_t		done = 0;

	while (done < length)
	{
		ssize_t		amount = pg_pwrite(fd, (const char *) data + done,
									   length - done, offset + done);

		if (amount < 0 && errno == EINTR)
			continue;
		if (amount < 0)
			pg_fatal("could not write upgrade relink file \"%s\": %m", path);
		if (amount == 0)
			pg_fatal("could not write upgrade relink file \"%s\"", path);
		done += amount;
	}
}

static void
append_bytes(UpgradeRelinkFile * file, const void *data, size_t length)
{
	write_at(file->fd, data, length, file->length, file->temporary_path);
	file->length += length;
}

static uint32
payload_crc(const void *data, size_t length)
{
	pg_crc32c	crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, data, length);
	FIN_CRC32C(crc);
	return (uint32) crc;
}

static void
write_frame(UpgradeRelinkFile * file, uint32 opcode,
			const void *payload, size_t payload_length, uint32 item_count)
{
	PgUpgradeRelinkFileFrame frame = {0};

	if (payload_length > PG_UINT32_MAX)
		pg_fatal("upgrade relink frame is too large");
	if (file->sequence == PG_UINT32_MAX)
		pg_fatal("too many upgrade relink frames");
	frame.opcode = opcode;
	frame.sequence = file->sequence++;
	frame.payload_length = (uint32) payload_length;
	frame.item_count = item_count;
	frame.payload_crc = payload_crc(payload, payload_length);
	append_bytes(file, &frame, sizeof(frame));
	append_bytes(file, payload, payload_length);
	if (pg_add_u64_overflow(file->total_items, item_count,
							&file->total_items))
		pg_fatal("too many upgrade relink operations");
}

static void
reject_existing_file(const char *path)
{
	struct stat st;

	if (lstat(path, &st) == 0)
		pg_fatal("upgrade relink file already exists: \"%s\"", path);
	if (errno != ENOENT)
		pg_fatal("could not inspect upgrade relink file \"%s\": %m", path);
}

UpgradeRelinkFile *
create_upgrade_relink_file(const char *pgdata)
{
	UpgradeRelinkFile *file = pg_malloc0_object(UpgradeRelinkFile);
	PgUpgradeRelinkFileHeader header = {0};
	xl_pg_upgrade_start window = {0};

	if (snprintf(file->temporary_path, sizeof(file->temporary_path), "%s/%s",
				 pgdata, PG_UPGRADE_RELINK_TMP_FILE) >= sizeof(file->temporary_path) ||
		snprintf(file->final_path, sizeof(file->final_path), "%s/%s",
				 pgdata, PG_UPGRADE_RELINK_FILE) >= sizeof(file->final_path))
		pg_fatal("upgrade relink file path is too long");
	reject_existing_file(file->temporary_path);
	reject_existing_file(file->final_path);
	file->fd = open(file->temporary_path,
					O_RDWR | O_CREAT | O_EXCL | PG_BINARY,
					pg_file_create_mode);
	if (file->fd < 0)
		pg_fatal("could not create upgrade relink file \"%s\": %m",
				 file->temporary_path);
#ifndef WIN32
	if (fchmod(file->fd, S_IRUSR | S_IWUSR) != 0)
		pg_fatal("could not set permissions on upgrade relink file \"%s\": %m",
				 file->temporary_path);
#endif
	append_bytes(file, &header, sizeof(header));
	write_frame(file, PG_UPGRADE_RELINK_START, &window, sizeof(window), 0);
	file->batch = pg_malloc0(SizeOfPgUpgradeCatalogBatch +
							 PG_UPGRADE_CATALOG_MAX_ENTRIES * sizeof(PgUpgradeCatalogEntry));
	return file;
}

void
begin_upgrade_relink_scope(UpgradeRelinkFile * file, bool target,
						   const PgUpgradeCatalogDatabase * database)
{
	if (file->batch_count != 0 || file->batch->flags != 0)
		pg_fatal("upgrade relink scope overlaps its predecessor");
	memset(file->batch, 0, SizeOfPgUpgradeCatalogBatch);
	file->batch->database = *database;
	file->batch->flags = UPGRADE_RELINK_BEGIN;
	file->batch_kind = target ? PG_UPGRADE_RELINK_TARGET : PG_UPGRADE_RELINK_OLD;
}

static void
flush_catalog_entries(UpgradeRelinkFile * file)
{
	write_frame(file, file->batch_kind, file->batch,
				SizeOfPgUpgradeCatalogBatch +
				file->batch_count * sizeof(PgUpgradeCatalogEntry),
				file->batch_count);
	file->batch->flags = 0;
	file->batch_count = 0;
}

void
append_upgrade_relink_catalog_entry(UpgradeRelinkFile * file,
									const PgUpgradeCatalogEntry * entry)
{
	if (file->batch_count == PG_UPGRADE_CATALOG_MAX_ENTRIES)
		flush_catalog_entries(file);
	file->batch->entries[file->batch_count++] = *entry;
}

void
end_upgrade_relink_scope(UpgradeRelinkFile * file)
{
	file->batch->flags |= UPGRADE_RELINK_END;
	flush_catalog_entries(file);
}

void
set_upgrade_relink_start(UpgradeRelinkFile * file,
						 const xl_pg_upgrade_start *window)
{
	PgUpgradeRelinkFileFrame frame;
	off_t		frame_offset = sizeof(PgUpgradeRelinkFileHeader);
	off_t		payload_offset = frame_offset + sizeof(frame);

	read_at(file->fd, &frame, sizeof(frame), frame_offset,
			file->temporary_path);
	if (frame.opcode != PG_UPGRADE_RELINK_START || frame.sequence != 0 ||
		frame.payload_length != sizeof(*window) || frame.item_count != 0)
		pg_fatal("upgrade relink file \"%s\" has an invalid START frame",
				 file->temporary_path);
	frame.payload_crc = payload_crc(window, sizeof(*window));
	write_at(file->fd, &frame, sizeof(frame), frame_offset,
			 file->temporary_path);
	write_at(file->fd, window, sizeof(*window), payload_offset,
			 file->temporary_path);
}

void
finish_upgrade_relink_file(UpgradeRelinkFile * file,
						   const xl_pg_upgrade_marker *marker)
{
	PgUpgradeRelinkFileEnd end = {0};

	if (file->batch_count != 0 || file->batch->flags != 0)
		pg_fatal("upgrade relink scope is incomplete");
	write_frame(file, PG_UPGRADE_RELINK_COMPLETE, marker, sizeof(*marker), 0);
	end.total_length = file->length + sizeof(end);
	end.total_items = file->total_items;
	end.frame_count = file->sequence;
	append_bytes(file, &end, sizeof(end));
	if (close(file->fd) != 0)
		pg_fatal("could not close upgrade relink file \"%s\": %m",
				 file->temporary_path);
	file->fd = -1;
	pg_free(file->batch);
	file->batch = NULL;
}

void
bind_upgrade_relink_file(UpgradeRelinkFile * file)
{
	ControlFileData *control;
	PgUpgradeRelinkFileHeader header = {0};
	bool		crc_ok;
	int			fd;

	control = get_controlfile(new_cluster.pgdata, &crc_ok);
	if (!crc_ok)
		pg_fatal("target control file has an invalid checksum");
	if (control->upgrade_started || control->upgrade_finalized)
		pg_fatal("target control file already records an upgrade attempt");
	header.magic = PG_UPGRADE_RELINK_FILE_MAGIC;
	header.version = PG_UPGRADE_RELINK_FILE_VERSION;
	header.producer_major = PG_VERSION_NUM;
	header.control_version = control->pg_control_version;
	header.catalog_version = control->catalog_version_no;
	header.start_size = sizeof(xl_pg_upgrade_start);
	header.batch_header_size = SizeOfPgUpgradeCatalogBatch;
	header.entry_size = sizeof(PgUpgradeCatalogEntry);
	header.marker_size = sizeof(xl_pg_upgrade_marker);
	header.target_system_identifier = control->system_identifier;
	header.block_size = control->blcksz;
	header.relseg_blocks = control->relseg_size;
	header.wal_block_size = control->xlog_blcksz;
	header.wal_segment_size = control->xlog_seg_size;
	memcpy(header.mock_authentication_nonce,
		   control->mock_authentication_nonce, MOCK_AUTH_NONCE_LEN);
	pg_free(control);
	fd = open(file->temporary_path, O_RDWR | PG_BINARY, 0);
	if (fd < 0)
		pg_fatal("could not open upgrade relink file \"%s\": %m",
				 file->temporary_path);
	write_at(fd, &header, sizeof(header), 0, file->temporary_path);
	if (close(fd) != 0)
		pg_fatal("could not close upgrade relink file \"%s\": %m",
				 file->temporary_path);
	file->bound = true;
}

static bool
valid_frame(const PgUpgradeRelinkFileFrame * frame, off_t remaining)
{
	if ((off_t) sizeof(*frame) > remaining ||
		frame->payload_length > (uint64) remaining - sizeof(*frame))
		return false;
	if (frame->opcode == PG_UPGRADE_RELINK_START)
		return frame->payload_length == sizeof(xl_pg_upgrade_start) &&
			frame->item_count == 0;
	if (frame->opcode == PG_UPGRADE_RELINK_COMPLETE)
		return frame->payload_length == sizeof(xl_pg_upgrade_marker) &&
			frame->item_count == 0;
	if ((frame->opcode != PG_UPGRADE_RELINK_OLD &&
		 frame->opcode != PG_UPGRADE_RELINK_TARGET) ||
		frame->payload_length < SizeOfPgUpgradeCatalogBatch ||
		(frame->payload_length - SizeOfPgUpgradeCatalogBatch) %
		sizeof(PgUpgradeCatalogEntry) != 0)
		return false;
	return frame->item_count ==
		(frame->payload_length - SizeOfPgUpgradeCatalogBatch) /
		sizeof(PgUpgradeCatalogEntry) &&
		frame->item_count <= PG_UPGRADE_CATALOG_MAX_ENTRIES;
}

static void
finish_file_contents(UpgradeRelinkFile * file, int64 window_time)
{
	struct stat st;
	PgUpgradeRelinkFileHeader header;
	PgUpgradeRelinkFileEnd end;
	off_t		offset = sizeof(header);
	off_t		end_offset;
	uint32		sequence = 0;
	uint64		total_items = 0;
	size_t		buffer_size = SizeOfPgUpgradeCatalogBatch +
		PG_UPGRADE_CATALOG_MAX_ENTRIES * sizeof(PgUpgradeCatalogEntry);
	char	   *buffer = pg_malloc(buffer_size);
	bool		saw_complete = false;
	pg_crc32c	crc;

	file->fd = open(file->temporary_path, O_RDWR | PG_BINARY, 0);
	if (file->fd < 0 || fstat(file->fd, &st) != 0)
		pg_fatal("could not open or stat upgrade relink file \"%s\": %m",
				 file->temporary_path);
	if (!S_ISREG(st.st_mode) || st.st_size != file->length ||
		st.st_size < (off_t) (sizeof(header) + sizeof(end)))
		pg_fatal("upgrade relink file \"%s\" has an invalid size",
				 file->temporary_path);
	read_at(file->fd, &header, sizeof(header), 0, file->temporary_path);
	if (header.magic != PG_UPGRADE_RELINK_FILE_MAGIC ||
		header.version != PG_UPGRADE_RELINK_FILE_VERSION)
		pg_fatal("upgrade relink file \"%s\" is not bound to the target cluster",
				 file->temporary_path);
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, &header, sizeof(header));
	end_offset = st.st_size - sizeof(end);
	while (offset < end_offset)
	{
		PgUpgradeRelinkFileFrame frame;
		off_t		payload_offset = offset + sizeof(frame);

		read_at(file->fd, &frame, sizeof(frame), offset, file->temporary_path);
		if (frame.sequence != sequence ||
			!valid_frame(&frame, end_offset - offset) ||
			frame.payload_length > buffer_size || saw_complete)
			pg_fatal("upgrade relink file \"%s\" has an invalid frame",
					 file->temporary_path);
		read_at(file->fd, buffer, frame.payload_length,
				payload_offset, file->temporary_path);
		if (frame.payload_crc != payload_crc(buffer, frame.payload_length))
			pg_fatal("upgrade relink file \"%s\" has a damaged frame",
					 file->temporary_path);
		if (frame.opcode == PG_UPGRADE_RELINK_START)
		{
			if (sequence != 0)
				pg_fatal("upgrade relink START is out of order");
			((xl_pg_upgrade_start *) buffer)->marker.window_time = window_time;
		}
		else if (frame.opcode == PG_UPGRADE_RELINK_COMPLETE)
		{
			((xl_pg_upgrade_marker *) buffer)->window_time = window_time;
			saw_complete = true;
		}
		if (frame.opcode == PG_UPGRADE_RELINK_START ||
			frame.opcode == PG_UPGRADE_RELINK_COMPLETE)
		{
			frame.payload_crc = payload_crc(buffer, frame.payload_length);
			write_at(file->fd, &frame, sizeof(frame), offset,
					 file->temporary_path);
			write_at(file->fd, buffer, frame.payload_length, payload_offset,
					 file->temporary_path);
		}
		COMP_CRC32C(crc, &frame, sizeof(frame));
		COMP_CRC32C(crc, buffer, frame.payload_length);
		if (pg_add_u64_overflow(total_items, frame.item_count, &total_items) ||
			sequence == PG_UINT32_MAX)
			pg_fatal("upgrade relink file \"%s\" has excessive totals",
					 file->temporary_path);
		sequence++;
		offset = payload_offset + frame.payload_length;
	}
	if (offset != end_offset || !saw_complete)
		pg_fatal("upgrade relink file \"%s\" has an incomplete frame sequence",
				 file->temporary_path);
	read_at(file->fd, &end, sizeof(end), end_offset, file->temporary_path);
	if (end.total_length != (uint64) st.st_size ||
		end.total_items != total_items || end.frame_count != sequence)
		pg_fatal("upgrade relink file \"%s\" has inconsistent totals",
				 file->temporary_path);
	COMP_CRC32C(crc, &end, offsetof(PgUpgradeRelinkFileEnd, file_crc));
	FIN_CRC32C(crc);
	write_at(file->fd, &crc, sizeof(crc),
			 st.st_size - sizeof(end.file_crc), file->temporary_path);
	if (user_opts.do_sync && fsync(file->fd) != 0)
		pg_fatal("could not synchronize upgrade relink file \"%s\": %m",
				 file->temporary_path);
	if (close(file->fd) != 0)
		pg_fatal("could not close upgrade relink file \"%s\": %m",
				 file->temporary_path);
	file->fd = -1;
	pg_free(buffer);
}

void
emit_upgrade_relink_file(UpgradeRelinkFile * file, PGconn *conn)
{
	time_t		window_time;

	if (conn == NULL || PQstatus(conn) != CONNECTION_OK ||
		PQtransactionStatus(conn) != PQTRANS_IDLE || !file->bound)
		pg_fatal("upgrade WAL requires an idle connection and a bound relink file");
	window_time = time(NULL);
	if (window_time == (time_t) -1)
		pg_fatal("could not obtain the WAL-upgrade window timestamp: %m");
	finish_file_contents(file, (int64) window_time);
	reject_existing_file(file->final_path);
	if (rename(file->temporary_path, file->final_path) != 0)
		pg_fatal("could not publish upgrade relink file \"%s\": %m",
				 file->final_path);
	if (user_opts.do_sync && fsync_parent_path(file->final_path) != 0)
		pg_fatal("could not synchronize target data directory after publishing \"%s\"",
				 file->final_path);
#ifdef USE_ASSERT_CHECKING
	{
		const char *gate = getenv("PG_UPGRADE_TEST_PAUSE_AFTER_RELINK_PUBLISH");

		if (gate != NULL)
		{
			struct stat st;

			for (;;)
			{
				if (stat(gate, &st) == 0)
				{
					pg_usleep(10000);
					continue;
				}
				if (errno == EINTR)
					continue;
				if (errno != ENOENT)
					pg_fatal("could not inspect test gate \"%s\": %m", gate);
				break;
			}
		}
	}
#endif

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
	if (unlink(file->final_path) != 0)
		pg_log(PG_WARNING, "could not remove upgrade relink file \"%s\": %m",
			   file->final_path);
	else if (user_opts.do_sync && fsync_parent_path(file->final_path) != 0)
		pg_log(PG_WARNING,
			   "could not synchronize target data directory after removing \"%s\": %m",
			   file->final_path);
}

void
free_upgrade_relink_file(UpgradeRelinkFile * file)
{
	if (file == NULL)
		return;
	if (file->fd >= 0)
		(void) close(file->fd);
	pg_free(file->batch);
	pg_free(file);
}
