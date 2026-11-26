/*-------------------------------------------------------------------------
 *
 * emit.c
 *    Execute pg_upgrade commands and emit upgrade WAL records ending with
 *    COMPLETE.
 *
 * pg_upgrade chooses the storage operations and files to capture. The startup
 * process reads those commands before any other database writer can run.
 *
 * For each database, RELINK operations precede the SMGR create records and
 * full-page images for RECREATE relations. INHERIT relations do
 * not require page images.
 *
 * COMPLETE is flushed before the controlled completion checkpoint.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * src/backend/access/pgupgrade/emit.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>

#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/multixact.h"
#include "access/pgupgrade_emit.h"
#include "access/pgupgrade_first_start.h"
#include "access/pgupgrade_wal.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "catalog/pg_control.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/storage_xlog.h"
#include "common/relpath.h"
#include "miscadmin.h"
#include "postmaster/startup.h"
#include "replication/slot.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/fd.h"
#include "storage/procnumber.h"
#include "utils/builtins.h"

pg_noreturn static void
emit_error(const char *message)
{
	ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED),
			 errmsg("could not emit upgrade WAL: %s", message)));
}

static void
emit_relation_error_callback(void *arg)
{
	const xl_pg_upgrade_key *key = arg;

	errcontext("while emitting upgrade WAL for relation: database OID %u, tablespace OID %u, relfilenumber %u",
			   key->database_oid, key->tablespace_oid, key->filenumber);
}

static void
emit_rawfile_error_callback(void *arg)
{
	errcontext("while emitting upgrade WAL for RAWFILE \"%s\"", (const char *) arg);
}

static void
reject_reparse_point(const char *path)
{
#ifdef WIN32
	DWORD		attributes = GetFileAttributes(path);

	if (attributes == INVALID_FILE_ATTRIBUTES)
	{
		_dosmaperr(GetLastError());
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not read attributes of \"%s\": %m", path)));
	}
	if (attributes & FILE_ATTRIBUTE_REPARSE_POINT)
		emit_error("relation storage contains an unexpected reparse point");
#endif
}

static int
open_regular(const char *path, uint64 *bytes, bool missing_ok)
{
	struct stat st;
	int			flags = O_RDONLY | PG_BINARY;
	int			fd;

	if (lstat(path, &st) != 0)
	{
		if (missing_ok && errno == ENOENT)
			return -1;
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat \"%s\": %m", path)));
	}
	reject_reparse_point(path);
	if (!S_ISREG(st.st_mode) || st.st_size < 0)
		emit_error("relation storage contains a nonregular file");
#ifdef O_NOFOLLOW
	flags |= O_NOFOLLOW;
#endif
	fd = OpenTransientFile(path, flags);
	if (fd < 0 || fstat(fd, &st) != 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not open or stat \"%s\": %m", path)));
	if (!S_ISREG(st.st_mode) || st.st_size < 0)
		emit_error("relation storage contains a nonregular file");
	*bytes = st.st_size;
	return fd;
}

static void
close_regular(int fd, const char *path)
{
	if (CloseTransientFile(fd) != 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not close \"%s\": %m", path)));
}

static DIR *
open_directory(const char *path)
{
	struct stat st;
	DIR		   *dir;

	reject_reparse_point(path);
	if (lstat(path, &st) != 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat directory \"%s\": %m", path)));
	if (!S_ISDIR(st.st_mode))
		emit_error("declared storage path is not a directory");
	dir = AllocateDir(path);

	if (dir == NULL)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not open directory \"%s\": %m", path)));
	return dir;
}

static void
close_directory(DIR *dir, const char *path)
{
	if (FreeDir(dir) != 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not close directory \"%s\": %m", path)));
}

/*
 * Write every page in one relation segment to WAL as a full-page image.
 * Set each initialized page's LSN to the record that will restore it.
 */
static void
capture_relation_segment(const char *path, Oid tsoid, Oid dboid,
						 RelFileNumber rfnum, ForkNumber fork, uint32 segno,
						 uint32 expected_blocks)
{
	struct stat stbuf;
	RelFileLocator rlocator;
	BlockNumber segfirst;
	BlockNumber nblocks;
	BlockNumber blkno;

	if (lstat(path, &stbuf) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not stat \"%s\": %m", path)));

	rlocator.spcOid = tsoid;
	rlocator.dbOid = dboid;
	rlocator.relNumber = rfnum;
	if (!S_ISREG(stbuf.st_mode) ||
		(uint64) segno * RELSEG_SIZE + expected_blocks > InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid declared relation segment \"%s\"", path)));
	segfirst = (BlockNumber) segno * RELSEG_SIZE;
	if (stbuf.st_size < 0 || stbuf.st_size % BLCKSZ != 0 ||
		stbuf.st_size / BLCKSZ > RELSEG_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid relation segment size for \"%s\"", path)));
	nblocks = stbuf.st_size / BLCKSZ;
	if (nblocks != expected_blocks)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("relation segment \"%s\" changed during upgrade WAL capture", path),
				 errdetail("Expected %u blocks, found %u.", expected_blocks, nblocks)));

	if (nblocks == 0)
		return;

	XLogEnsureRecordSpace(XLR_MAX_BLOCK_ID - 1, 0);

	for (blkno = 0; blkno < nblocks;)
	{
		Buffer		buffers[XLR_MAX_BLOCK_ID];
		uint32		batch_limit = Min((uint32) XLR_MAX_BLOCK_ID,
									  nblocks - blkno);
		XLogRecPtr	recptr;
		uint32		nbatch = 0;

		ProcessStartupProcInterrupts();
		LimitAdditionalPins(&batch_limit);
		while (nbatch < batch_limit && blkno < nblocks)
		{
			Buffer		buffer;

			buffer = ReadBufferWithoutRelcache(rlocator, fork, segfirst + blkno,
											   RBM_NORMAL, NULL, true);
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
			buffers[nbatch++] = buffer;
			blkno++;
		}

		XLogBeginInsert();
		START_CRIT_SECTION();
		for (uint32 i = 0; i < nbatch; i++)
		{
			MarkBufferDirty(buffers[i]);

			/*
			 * Keep the full page image. REGBUF_STANDARD would omit the
			 * apparent hole between pd_lower and pd_upper, which can contain
			 * live metadata in copied indexes such as binary-upgraded GIN
			 * metapages.
			 */
			XLogRegisterBuffer(i, buffers[i], REGBUF_FORCE_IMAGE);
		}
		recptr = XLogInsert(RM_XLOG_ID, XLOG_FPI);
		for (uint32 i = 0; i < nbatch; i++)
		{
			Page		page = BufferGetPage(buffers[i]);

			if (!PageIsNew(page))
				PageSetLSN(page, recptr);
		}
		END_CRIT_SECTION();

		for (uint32 i = 0; i < nbatch; i++)
			UnlockReleaseBuffer(buffers[i]);
	}
}

static void
write_control_file_image(void)
{
	const char *path = XLOG_CONTROL_FILE;
	uint32		path_len = (uint32) strlen(path);
	char	   *buffer;

	if ((Size) PG_CONTROL_FILE_SIZE + SizeOfPgUpgradeRawFile + path_len +
		SizeOfXLogRecord > XLogRecordMaxSize)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("pg_control is too large to capture in a pg_upgrade WAL record")));

	buffer = palloc0(PG_CONTROL_FILE_SIZE);
	GetControlFileCopy((ControlFileData *) buffer);

	(void) PgUpgradeInsertRawFileRecord(path, buffer, PG_CONTROL_FILE_SIZE, 0);

	/*
	 * Record the current parameter and checksum state after the pg_control
	 * image.
	 */
	XLogReportCurrentControlState();

	pfree(buffer);
}

/*
 * Emit an SMGR create record and full-page images for a RECREATE
 * relation.
 */
static void
capture_relation_storage(const xl_pg_upgrade_key *key, ForkNumber fork)
{
	RelPathStr	base = GetRelationPath(key->database_oid, key->tablespace_oid,
									   key->filenumber, INVALID_PROC_NUMBER, fork);
	bool		partial = false;
	RelFileLocator locator =
	{key->tablespace_oid, key->database_oid, key->filenumber};
	ErrorContextCallback errcallback;

	errcallback.callback = emit_relation_error_callback;
	errcallback.arg = (void *) key;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	log_smgrcreate(&locator, fork);

	for (uint32 segment = 0;; segment++)
	{
		char	   *path = segment == 0 ? pstrdup(base.str) : psprintf("%s.%u", base.str, segment);
		uint64		bytes;
		int			fd = open_regular(path, &bytes, segment != 0);

		ProcessStartupProcInterrupts();
		if (fd < 0)
		{
			pfree(path);
			break;
		}
		close_regular(fd, path);
		if (bytes % BLCKSZ != 0 ||
			bytes > (uint64) RELSEG_SIZE * BLCKSZ ||
			(uint64) segment * RELSEG_SIZE + bytes / BLCKSZ > PG_UINT32_MAX)
			emit_error(psprintf("invalid relation segment size for \"%s\": " UINT64_FORMAT " bytes",
								path, bytes));
		/* Zero-length inactive segments may follow the first partial segment. */
		if (partial && bytes != 0)
			emit_error(psprintf("nonempty segment \"%s\" follows the relation end", path));
		partial |= bytes < (uint64) RELSEG_SIZE * BLCKSZ;
		capture_relation_segment(path, key->tablespace_oid, key->database_oid,
								 key->filenumber, fork, segment, bytes / BLCKSZ);
		pfree(path);
	}
	error_context_stack = errcallback.previous;
}

static void
read_exact_file(int fd, void *data, size_t length, off_t offset,
				const char *path)
{
	size_t		done = 0;

	while (done < length)
	{
		ssize_t		amount = pg_pread(fd, (char *) data + done,
									  length - done, offset + done);

		if (amount < 0 && errno == EINTR)
		{
			ProcessStartupProcInterrupts();
			continue;
		}
		if (amount < 0)
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not read \"%s\": %m", path)));
		if (amount == 0)
			emit_error("auxiliary file became shorter during capture");
		done += amount;
	}
}

static void
capture_raw_file(const char *path, bool version, bool slru)
{
	const Size	chunk_capacity = 1024 * 1024;
	uint64		bytes;
	int			fd;
	char	   *buffer;
	uint64		offset = 0;
	ErrorContextCallback errcallback;

	errcallback.callback = emit_rawfile_error_callback;
	errcallback.arg = (void *) path;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	fd = open_regular(path, &bytes, false);
	if (slru && (bytes % BLCKSZ != 0 ||
				 bytes > (uint64) SLRU_PAGES_PER_SEGMENT * BLCKSZ))
		emit_error("SLRU segment has an invalid length");
	if ((!slru && bytes == 0) ||
		(version && bytes != sizeof(PG_MAJORVERSION "\n") - 1))
		emit_error("required auxiliary file has an invalid length");
	Assert(path_is_safe_for_extraction(path));
	Assert(chunk_capacity + strlen(path) + SizeOfPgUpgradeRawFile +
		   SizeOfXLogRecord < XLogRecordMaxSize);
	buffer = palloc(chunk_capacity);

	/*
	 * Emit one RAWFILE record with offset = 0 and data_len = 0 for an empty
	 * SLRU segment, so replay creates or truncates the file to zero length.
	 */
	do
	{
		Size		chunk = Min((uint64) chunk_capacity, bytes - offset);

		ProcessStartupProcInterrupts();
		read_exact_file(fd, buffer, chunk, offset, path);
		if (version && memcmp(buffer, PG_MAJORVERSION "\n", chunk) != 0)
			emit_error("PG_VERSION contents do not match the new server");
		(void) PgUpgradeInsertRawFileRecord(path, buffer, chunk, offset);
		offset += chunk;
	} while (offset < bytes);

	close_regular(fd, path);
	pfree(buffer);
	error_context_stack = errcallback.previous;
}

static void
capture_slru(const char *path, bool long_names)
{
	DIR		   *dir = open_directory(path);
	struct dirent *entry;

	while ((entry = ReadDir(dir, path)) != NULL)
	{
		size_t		length = strlen(entry->d_name);
		char	   *file;

		if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
			continue;
		if (strspn(entry->d_name, "0123456789ABCDEF") != length ||
			(long_names ? length != 15 :
			 (length < 4 || length > 6 || (length > 4 && entry->d_name[0] == '0'))))
			emit_error("SLRU directory contains an unrecognized segment name");
		file = psprintf("%s/%s", path, entry->d_name);
		capture_raw_file(file, false, true);
		pfree(file);
	}
	close_directory(dir, path);
}

/* The command reader retains only one line and one bounded RELINK batch. */
typedef struct UpgradeCommandReader
{
	xl_pg_upgrade_start start;
	FILE	   *stream;
	const char *path;
	uint64		line_number;
	char		line[MAXPGPATH + 512];
	char	   *fields[16];
	int			nfields;
} UpgradeCommandReader;

static UpgradeCommandReader command_reader;

static void
command_error_context(void *arg)
{
	UpgradeCommandReader *reader = arg;

	errcontext("upgrade command file \"%s\", line " UINT64_FORMAT,
			   reader->path, reader->line_number);
}

static void
read_command(UpgradeCommandReader *reader)
{
	char	   *save;
	char	   *token;
	size_t		len;

	ProcessStartupProcInterrupts();
	reader->line_number++;
	if (fgets(reader->line, sizeof(reader->line), reader->stream) == NULL)
	{
		if (ferror(reader->stream))
			ereport(ERROR, (errcode_for_file_access(), errmsg("could not read upgrade commands: %m")));
		emit_error("upgrade commands end before COMPLETE");
	}
	len = strlen(reader->line);
	if (len == 0 || reader->line[len - 1] != '\n')
		emit_error("incomplete or overlong upgrade command");
	reader->nfields = 0;
	for (token = strtok_r(reader->line, " \t\r\n", &save);
		 token != NULL; token = strtok_r(NULL, " \t\r\n", &save))
	{
		if (reader->nfields == lengthof(reader->fields))
			emit_error("too many upgrade command arguments");
		reader->fields[reader->nfields++] = token;
	}
	if (reader->nfields == 0)
		emit_error("empty upgrade command");
}

static void
command_fields(UpgradeCommandReader *reader, int expected)
{
	if (reader->nfields != expected)
		emit_error(psprintf("%s expects %d arguments, found %d",
							reader->fields[0], expected - 1, reader->nfields - 1));
}

static uint64
command_uint(UpgradeCommandReader *reader, int field, uint64 maximum)
{
	const char *token = reader->fields[field];
	char	   *end;
	uint64		value;

	errno = 0;
	value = strtou64(token, &end, 10);
	if (token[0] < '0' || token[0] > '9' || *end != '\0' || errno != 0 || value > maximum)
		emit_error(psprintf("invalid unsigned integer \"%s\" in %s", token, reader->fields[0]));
	return value;
}

static xl_pg_upgrade_key
command_key(UpgradeCommandReader *reader)
{
	xl_pg_upgrade_key key = {
		command_uint(reader, 1, PG_UINT32_MAX),
		command_uint(reader, 2, PG_UINT32_MAX),
		command_uint(reader, 3, PG_UINT32_MAX)
	};

	if (key.tablespace_oid == InvalidOid ||
		((key.database_oid == InvalidOid) != (key.tablespace_oid == GLOBALTABLESPACE_OID)))
		emit_error("invalid shared or database storage key");
	return key;
}

static void
read_start(UpgradeCommandReader *reader)
{
	xl_pg_upgrade_start start = {0};
	uint64		target;

	read_command(reader);
	if (strcmp(reader->fields[0], "START") != 0)
		emit_error("upgrade commands must begin with START");
	command_fields(reader, 16);
	target = command_uint(reader, 1, PG_UINT64_MAX);
	start.marker.old_major = command_uint(reader, 2, PG_UINT32_MAX);
	start.marker.new_major = command_uint(reader, 3, PG_UINT32_MAX);
	start.marker.upgrade_time = pg_strtoint64(reader->fields[4]);
	start.old_system_identifier = command_uint(reader, 5, PG_UINT64_MAX);
	start.boundary_lsn = command_uint(reader, 6, PG_UINT64_MAX);
	start.old_catalog_version = command_uint(reader, 7, PG_UINT32_MAX);
	start.old_control_version = command_uint(reader, 8, PG_UINT32_MAX);
	start.block_size = command_uint(reader, 9, PG_UINT32_MAX);
	start.relseg_blocks = command_uint(reader, 10, PG_UINT32_MAX);
	start.wal_block_size = command_uint(reader, 11, PG_UINT32_MAX);
	start.wal_segment_size = command_uint(reader, 12, PG_UINT32_MAX);
	start.slru_pages_per_segment = command_uint(reader, 13, PG_UINT32_MAX);
	start.old_tli = command_uint(reader, 14, PG_UINT32_MAX);
	start.transfer_mode = command_uint(reader, 15, PG_UINT32_MAX);
	snprintf(start.marker.pg_version, sizeof(start.marker.pg_version), "%u\n",
			 start.marker.new_major / 10000);
	if (target != GetSystemIdentifier() || !PgUpgradeStartMatchesServer(&start))
		emit_error("START does not match this server");
	reader->start = start;
}

static void
execute_commands(UpgradeCommandReader *reader)
{
	xl_pg_upgrade_relink *batch = palloc0(SizeOfPgUpgradeRelink +
										  UPGRADE_RELINK_MAX_ENTRIES * SizeOfPgUpgradeRelinkEntry);
	bool		scope_open = false;
	bool		have_previous = false;
	xl_pg_upgrade_key previous = {0};

	for (;;)
	{
		const char *command;

		read_command(reader);
		command = reader->fields[0];
		if (strcmp(command, "RELINK") == 0)
		{
			uint32		count;

			command_fields(reader, 3);
			batch->flags = command_uint(reader, 1, UPGRADE_RELINK_BEGIN | UPGRADE_RELINK_END);
			count = command_uint(reader, 2, UPGRADE_RELINK_MAX_ENTRIES);
			if (count == 0 || scope_open == ((batch->flags & UPGRADE_RELINK_BEGIN) != 0))
				emit_error("invalid RELINK count or BEGIN/END sequence");
			if (!scope_open)
				have_previous = false;
			for (uint32 i = 0; i < count; i++)
			{
				xl_pg_upgrade_relink_entry *entry = &batch->entries[i];
				bool		directory;
				bool		previous_directory = previous.filenumber == 0;
				static const char *const operations[] = {
					NULL, "INHERIT", "RECREATE"
				};

				read_command(reader);
				command_fields(reader, 5);
				entry->key = command_key(reader);
				entry->operation = 0;
				for (int op = UPGRADE_RELINK_INHERIT; op <= UPGRADE_RELINK_RECREATE; op++)
					if (strcmp(reader->fields[0], operations[op]) == 0)
						entry->operation = op;
				entry->flags = command_uint(reader, 4, UPGRADE_RELINK_INPLACE);
				directory = PgUpgradeRelinkEntryIsDirectory(entry);
				if (entry->operation == 0 || (entry->flags != 0 &&
											  (!directory || entry->flags != UPGRADE_RELINK_INPLACE)))
					emit_error("invalid RELINK operation or flags");
				if ((!have_previous && !directory) ||
					(entry->flags != 0 &&
					 (entry->key.tablespace_oid == DEFAULTTABLESPACE_OID ||
					  entry->key.tablespace_oid == GLOBALTABLESPACE_OID)))
					emit_error("RELINK requires a directory first and INPLACE only on user tablespaces");
				if (have_previous &&
					(entry->key.database_oid != previous.database_oid ||
					 (directory && !previous_directory) ||
					 (directory == previous_directory && PgUpgradeCompareKeys(&previous, &entry->key) >= 0)))
					emit_error("RELINK entries must have one database, with ordered directories before ordered relations");
				previous = entry->key;
				have_previous = true;
			}
			scope_open = (batch->flags & UPGRADE_RELINK_END) == 0;
			XLogBeginInsert();
			XLogRegisterData(batch,
							 SizeOfPgUpgradeRelink + count * SizeOfPgUpgradeRelinkEntry);
			XLogInsert(RM_PG_UPGRADE_ID, XLOG_UPGRADE_RELINK);
			continue;
		}
		if (scope_open)
			emit_error("unfinished RELINK scope before next command");
		if (strcmp(command, "COPY_RELFILE") == 0)
		{
			xl_pg_upgrade_key key;
			ForkNumber	fork;

			command_fields(reader, 5);
			key = command_key(reader);
			fork = command_uint(reader, 4, INIT_FORKNUM);
			if (key.filenumber == 0)
				emit_error("COPY_RELFILE requires a relation");
			capture_relation_storage(&key, fork);
		}
		else if (strcmp(command, "COPY_FILE") == 0)
		{
			const char *path;
			const char *name;

			command_fields(reader, 2);
			path = reader->fields[1];
			name = last_dir_separator(path);
			name = name ? name + 1 : path;
			if (strlen(path) >= MAXPGPATH || !path_is_safe_for_extraction(path) ||
				(strcmp(name, "PG_VERSION") != 0 && strcmp(name, "pg_filenode.map") != 0))
				emit_error("COPY_FILE requires a relative PG_VERSION or pg_filenode.map path");
			capture_raw_file(path, strcmp(name, "PG_VERSION") == 0, false);
		}
		else if (strcmp(command, "COPY_SLRU") == 0)
		{
			const char *path;

			command_fields(reader, 2);
			path = reader->fields[1];
			if (strcmp(path, "pg_xact") != 0 && strcmp(path, "pg_multixact/offsets") != 0 &&
				strcmp(path, "pg_multixact/members") != 0)
				emit_error("COPY_SLRU requires a supported SLRU directory");
			capture_slru(path, strcmp(path, "pg_multixact/members") == 0);
		}
		else if (strcmp(command, "COMPLETE") == 0)
		{
			command_fields(reader, 1);
			if (fgetc(reader->stream) != EOF)
				emit_error("trailing data after COMPLETE");
			if (ferror(reader->stream) || FreeFile(reader->stream) != 0)
				ereport(ERROR, (errcode_for_file_access(), errmsg("could not close upgrade commands: %m")));
			reader->stream = NULL;
			ReplicationSlotsSaveUpgrade();
			break;
		}
		else
			emit_error(psprintf("unknown upgrade command \"%s\"", command));
	}
	pfree(batch);
}

const xl_pg_upgrade_start *
PgUpgradePrepareEmission(void)
{
	char	   *path = psprintf("%s/%s", DataDir, PG_UPGRADE_RELINK_FILE);
	struct stat path_stat;
	struct stat fd_stat;

	if (strlen(path) >= MAXPGPATH)
		emit_error("upgrade command file path is too long");
	reject_reparse_point(path);
	if (lstat(path, &path_stat) != 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat upgrade commands \"%s\": %m", path)));
	if (!S_ISREG(path_stat.st_mode))
		emit_error("upgrade command path is not a regular file");
	command_reader.stream = AllocateFile(path, "rb");
	if (command_reader.stream == NULL || fstat(fileno(command_reader.stream), &fd_stat) != 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not open or stat upgrade commands \"%s\": %m", path)));
	if (!S_ISREG(fd_stat.st_mode) || path_stat.st_size != fd_stat.st_size ||
		path_stat.st_dev != fd_stat.st_dev || path_stat.st_ino != fd_stat.st_ino)
		emit_error("upgrade command path changed while opening");
#ifndef WIN32
	if (fd_stat.st_uid != geteuid() || fd_stat.st_nlink != 1 ||
		(fd_stat.st_mode & (S_IRWXU | S_IRWXG | S_IRWXO)) != (S_IRUSR | S_IWUSR))
		emit_error("unsafe upgrade command file ownership or permissions");
#endif

	command_reader.path = path;
	read_start(&command_reader);
	return &command_reader.start;
}

XLogRecPtr
PgUpgradeEmitWalFile(void)
{
	ErrorContextCallback errcallback;
	xl_pg_upgrade_complete complete = {command_reader.start.marker};
	XLogRecPtr	complete_lsn;

	Assert(PgUpgradeFirstStartInProgress());
	errcallback.callback = command_error_context;
	errcallback.arg = &command_reader;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	CheckPointCLOG();
	CheckPointCommitTs();
	CheckPointMultiXact();
	XLogBeginInsert();
	XLogRegisterData(&command_reader.start, SizeOfPgUpgradeStart);
	XLogInsert(RM_PG_UPGRADE_ID, XLOG_UPGRADE_START);
	write_control_file_image();
	ReplicationSlotsPrepareUpgrade();
	execute_commands(&command_reader);
	XLogBeginInsert();
	XLogRegisterData(&complete, SizeOfPgUpgradeComplete);
	complete_lsn = XLogInsert(RM_PG_UPGRADE_ID, XLOG_UPGRADE_COMPLETE);
	XLogFlush(complete_lsn);
#ifdef USE_ASSERT_CHECKING
	if (getenv("PG_UPGRADE_TEST_CRASH_AFTER_COMPLETE") != NULL)
		elog(PANIC, "test crash after pg_upgrade COMPLETE");
#endif
	error_context_stack = errcallback.previous;
	return complete_lsn;
}
