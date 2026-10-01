/*-------------------------------------------------------------------------
 *
 * pgupgrade_emit.c
 *    Emit storage operations and capture files one storage scope at a time.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * src/backend/access/transam/pgupgrade_emit.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>

#include "access/parallel.h"
#include "access/pgupgrade_emit.h"
#include "access/pgupgrade_wal.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/storage_xlog.h"
#include "common/file_utils.h"
#include "common/int.h"
#include "common/pg_upgrade_data.h"
#include "common/relpath.h"
#include "miscadmin.h"
#include "postmaster/bgwriter.h"
#include "port/pg_crc32c.h"
#include "storage/fd.h"
#include "storage/procnumber.h"
#include "utils/memutils.h"

typedef struct EmitOperation
{
	PgUpgradeEmitOperation data;
	uint64		blocks[4];
}			EmitOperation;

typedef struct PgUpgradeEmitter
{
	MemoryContext window_context;
	MemoryContext database_context;
	MemoryContextCallback window_reset;
	xl_pg_upgrade_start input;
	PgUpgradeEmitDatabase database;
	EmitOperation *operations;
	size_t		noperations;
	size_t		operation_capacity;
	bool		poisoned;
	bool		complete;
	XLogRecPtr	complete_lsn;
}			PgUpgradeEmitter;

static PgUpgradeEmitter * emitter;
static bool callbacks_registered;

static bool
fork_rebuilt_from_wal(const PgUpgradeRelation * relation, ForkNumber fork)
{
	if (relation->operation != UPGRADE_RELINK_INHERIT)
		return true;
	if (relation->persistence == 'u')
		return emitter->input.transfer_mode != UPGRADE_RELINK_MODE_SWAP;
	return fork == VISIBILITYMAP_FORKNUM &&
		emitter->input.old_catalog_version < VISIBILITY_MAP_FROZEN_BIT_CAT_VER;
}

pg_noreturn static void
emit_error(const char *message)
{
	ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED),
			 errmsg("could not emit upgrade WAL: %s", message)));
}

static void
window_reset(void *arg)
{
	if (emitter == arg)
		emitter = NULL;
}

static void
emit_xact_callback(XactEvent event, void *arg)
{
	if (emitter == NULL)
		return;
	if (event == XACT_EVENT_PRE_PREPARE)
		emit_error("an emission transaction cannot be prepared");
	if ((event == XACT_EVENT_PRE_COMMIT || event == XACT_EVENT_PARALLEL_PRE_COMMIT) &&
		(!emitter->complete || emitter->poisoned))
		emit_error("emission transaction has not completed its window");
	if (event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT)
		emitter->poisoned = true;
	if (event == XACT_EVENT_COMMIT)
	{
		Assert(emitter->complete && !emitter->poisoned);
		Assert(!XLogRecPtrIsInvalid(emitter->complete_lsn));
		ArmUpgradeCompletionCheckpoint(emitter->complete_lsn);
	}
}

static void
emit_subxact_callback(SubXactEvent event, SubTransactionId my_subid,
					  SubTransactionId parent_subid, void *arg)
{
	if (emitter != NULL && event == SUBXACT_EVENT_START_SUB)
	{
		emitter->poisoned = true;
		emit_error("subtransactions are not allowed during upgrade WAL emission");
	}
}

static void
create_emitter(void)
{
	MemoryContext context;
	MemoryContext previous;

	if (!callbacks_registered)
	{
		RegisterXactCallback(emit_xact_callback, NULL);
		RegisterSubXactCallback(emit_subxact_callback, NULL);
		callbacks_registered = true;
	}
	context = AllocSetContextCreate(TopTransactionContext,
									"WAL upgrade emission window", ALLOCSET_DEFAULT_SIZES);
	previous = MemoryContextSwitchTo(context);
	emitter = palloc0_object(PgUpgradeEmitter);
	emitter->window_context = context;
	emitter->window_reset.func = window_reset;
	emitter->window_reset.arg = emitter;
	MemoryContextRegisterResetCallback(context, &emitter->window_reset);
	MemoryContextSwitchTo(previous);
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

static void
inspect_directory(const char *path)
{
	struct stat st;

	reject_reparse_point(path);
	if (lstat(path, &st) != 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat directory \"%s\": %m", path)));
	if (!S_ISDIR(st.st_mode))
		emit_error("declared storage path is not a directory");
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
	DIR		   *dir = AllocateDir(path);

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

static XLogRecPtr
write_record(uint8 opcode, const uint8 *data, size_t length)
{
	XLogBeginInsert();
	XLogRegisterData((char *) unconstify(uint8 *, data), length);
	return XLogInsert(RM_PG_UPGRADE_ID, opcode);
}

static void
append_relink_entry(xl_pg_upgrade_relink *batch, uint32 *count,
					const xl_pg_upgrade_relink_entry *entry)
{
	if (*count == UPGRADE_RELINK_MAX_ENTRIES)
	{
		write_record(XLOG_UPGRADE_RELINK, (const uint8 *) batch,
					 SizeOfPgUpgradeRelink + *count * SizeOfPgUpgradeRelinkEntry);
		batch->flags = 0;
		*count = 0;
	}
	batch->entries[(*count)++] = *entry;
}

static bool
key_is_present(const xl_pg_upgrade_key *key)
{
	return key->tablespace_oid != InvalidOid;
}

static void
emit_directory_operation(xl_pg_upgrade_relink *batch, uint32 *count,
						 const PgUpgradeEmitOperation * operation)
{
	const		PgUpgradeRelation *directory = &operation->relation;
	bool		have_old = key_is_present(&directory->old_key);
	bool		have_new = key_is_present(&directory->new_key);
	xl_pg_upgrade_relink_entry entry = {0};

	entry.entry_type = UPGRADE_RELINK_DIRECTORY;
	if (have_old && have_new &&
		PgUpgradeCompareKeys(&directory->old_key, &directory->new_key) != 0)
	{
		entry.key = directory->old_key;
		entry.operation = UPGRADE_RELINK_DELETE;
		append_relink_entry(batch, count, &entry);
		MemSet(&entry, 0, sizeof(entry));
		entry.entry_type = UPGRADE_RELINK_DIRECTORY;
		entry.key = directory->new_key;
		entry.operation = UPGRADE_RELINK_CREATE;
	}
	else if (have_old && have_new)
	{
		entry.key = directory->new_key;
		entry.operation = directory->operation == UPGRADE_RELINK_RECREATE ?
			UPGRADE_RELINK_RECREATE : UPGRADE_RELINK_INHERIT;
	}
	else if (have_old)
	{
		entry.key = directory->old_key;
		entry.operation = UPGRADE_RELINK_DELETE;
	}
	else
	{
		Assert(have_new);
		entry.key = directory->new_key;
		entry.operation = UPGRADE_RELINK_CREATE;
	}
	if (have_new && operation->new_directory_inplace)
		entry.flags = UPGRADE_RELINK_INPLACE;
	append_relink_entry(batch, count, &entry);
}

static void
emit_relation_files(xl_pg_upgrade_relink *batch, uint32 *count,
					const EmitOperation * operation)
{
	const		PgUpgradeRelation *relation = &operation->data.relation;

	/*
	 * Emit FILE CREATE/RECREATE first, then FILE INHERIT in fork and segment
	 * order.
	 */
	for (unsigned f = 0; f < 4; f++)
	{
		xl_pg_upgrade_relink_entry entry = {0};

		if (!(relation->fork_mask & (1 << f)) ||
			!fork_rebuilt_from_wal(relation, f))
			continue;
		entry.key = relation->new_key;
		entry.entry_type = UPGRADE_RELINK_FILE;
		entry.operation = relation->operation == UPGRADE_RELINK_CREATE ?
			UPGRADE_RELINK_CREATE : UPGRADE_RELINK_RECREATE;
		entry.fork = f;
		append_relink_entry(batch, count, &entry);
	}
	for (unsigned f = 0; f < 4; f++)
	{
		xl_pg_upgrade_relink_entry entry = {0};
		uint64		blocks = operation->blocks[f];

		if (!(relation->fork_mask & (1 << f)) ||
			fork_rebuilt_from_wal(relation, f))
			continue;
		entry.key = relation->new_key;
		entry.entry_type = UPGRADE_RELINK_FILE;
		entry.operation = UPGRADE_RELINK_INHERIT;
		entry.fork = f;
		do
		{
			entry.blocks = Min(blocks, (uint64) RELSEG_SIZE);
			append_relink_entry(batch, count, &entry);
			blocks -= entry.blocks;
		} while (blocks != 0);
	}
}

/* Emit each directory or relation operation before its file operations. */
static void
emit_database_operations(void)
{
	xl_pg_upgrade_relink *batch = palloc0(SizeOfPgUpgradeRelink +
										  UPGRADE_RELINK_MAX_ENTRIES * SizeOfPgUpgradeRelinkEntry);
	uint32		count = 0;

	batch->flags = UPGRADE_RELINK_BEGIN;
	for (size_t i = 0; i < emitter->database.directory_count; i++)
		emit_directory_operation(batch, &count, &emitter->operations[i].data);
	for (size_t i = emitter->database.directory_count; i < emitter->noperations; i++)
	{
		const		EmitOperation *operation = &emitter->operations[i];
		const		PgUpgradeRelation *relation = &operation->data.relation;
		xl_pg_upgrade_relink_entry entry = {0};

		entry.key = relation->operation == UPGRADE_RELINK_DELETE ?
			relation->old_key : relation->new_key;
		entry.entry_type = UPGRADE_RELINK_RELATION;
		entry.operation = relation->operation;
		append_relink_entry(batch, &count, &entry);
		emit_relation_files(batch, &count, operation);
	}
	batch->flags |= UPGRADE_RELINK_END;
	write_record(XLOG_UPGRADE_RELINK, (const uint8 *) batch,
				 SizeOfPgUpgradeRelink + count * SizeOfPgUpgradeRelinkEntry);
	pfree(batch);
}

/* Validate segments, optionally emit their pages, and count blocks. */
static uint64
read_fork(const xl_pg_upgrade_key *key, ForkNumber fork, bool required, bool capture)
{
	RelPathStr	base = GetRelationPath(key->database_oid, key->tablespace_oid,
									   key->filenumber, INVALID_PROC_NUMBER, fork);
	uint64		blocks = 0;
	bool		partial = false;

	for (uint32 segment = 0;; segment++)
	{
		char	   *path = segment == 0 ? pstrdup(base.str) : psprintf("%s.%u", base.str, segment);
		uint64		bytes;
		int			fd = open_regular(path, &bytes, segment != 0 || !required);

		CHECK_FOR_INTERRUPTS();
		if (fd < 0)
		{
			pfree(path);
			return segment == 0 ? PG_UINT64_MAX : blocks;
		}
		close_regular(fd, path);
		if (bytes % BLCKSZ != 0 ||
			bytes > (uint64) RELSEG_SIZE * BLCKSZ ||
			(uint64) segment * RELSEG_SIZE + bytes / BLCKSZ > PG_UINT32_MAX)
			emit_error("invalid relation segment size");
		/* Zero-length inactive segments may follow the first partial segment. */
		if (partial && bytes != 0)
			emit_error("nonempty segment follows the relation end");
		partial |= bytes < (uint64) RELSEG_SIZE * BLCKSZ;
		blocks += bytes / BLCKSZ;
		if (capture)
			XLogUpgradeCaptureImage(path, key->tablespace_oid, key->database_oid,
									key->filenumber, fork, segment, bytes / BLCKSZ);
		pfree(path);
	}
}

static void
prepare_forks(void)
{
	/* Omit missing optional forks and record transferred segment sizes. */
	for (size_t i = emitter->database.directory_count; i < emitter->noperations; i++)
	{
		EmitOperation *operation = &emitter->operations[i];
		PgUpgradeRelation *relation = &operation->data.relation;

		if (relation->new_key.filenumber == 0)
			continue;
		for (unsigned f = 0; f < 4; f++)
		{
			uint8		bit = 1 << f;
			bool		required = f == (relation->persistence == 'u' ? INIT_FORKNUM : MAIN_FORKNUM);

			if (!(relation->fork_mask & bit))
				continue;
			if (fork_rebuilt_from_wal(relation, f))
			{
				RelPathStr	path = GetRelationPath(relation->new_key.database_oid,
												   relation->new_key.tablespace_oid,
												   relation->new_key.filenumber, INVALID_PROC_NUMBER, f);
				struct stat st;

				if (!required && lstat(path.str, &st) != 0)
				{
					if (errno != ENOENT)
						ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat \"%s\": %m", path.str)));
					relation->fork_mask &= ~bit;
				}
				continue;
			}
			operation->blocks[f] = read_fork(&relation->new_key, f, required, false);
			if (operation->blocks[f] == PG_UINT64_MAX)
				relation->fork_mask &= ~bit;
		}
	}
}

static void
capture_raw_file(const char *path, bool version, bool slru)
{
	const Size	chunk_capacity = 1024 * 1024;
	uint64		bytes;
	int			fd = open_regular(path, &bytes, false);
	char	   *buffer;
	uint64		offset = 0;
	uint32		path_length = strlen(path);

	if (slru && (bytes % BLCKSZ != 0 ||
				 bytes > (uint64) SLRU_PAGES_PER_SEGMENT * BLCKSZ))
		emit_error("SLRU segment has an invalid length");
	if ((!slru && bytes == 0) ||
		(version && bytes != sizeof(PG_MAJORVERSION "\n") - 1))
		emit_error("required auxiliary file has an invalid length");
	Assert(PgUpgradeDirectoryPathIsSafe(path));
	Assert(chunk_capacity + path_length + SizeOfPgUpgradeRawFile + SizeOfXLogRecord < XLogRecordMaxSize);
	buffer = palloc(chunk_capacity);

	/* Offset zero creates an empty SLRU segment during replay. */
	do
	{
		Size		chunk = Min((uint64) chunk_capacity, bytes - offset);
		Size		done = 0;
		xl_pg_upgrade_rawfile record = {0};

		CHECK_FOR_INTERRUPTS();
		while (done < chunk)
		{
			ssize_t		amount = pg_pread(fd, buffer + done, chunk - done, offset + done);

			if (amount < 0)
			{
				if (errno == EINTR)
				{
					CHECK_FOR_INTERRUPTS();
					continue;
				}
				ereport(ERROR, (errcode_for_file_access(), errmsg("could not read \"%s\": %m", path)));
			}
			if (amount == 0)
				emit_error("auxiliary file became shorter during capture");
			done += amount;
		}
		if (version && memcmp(buffer, PG_MAJORVERSION "\n", chunk) != 0)
			emit_error("PG_VERSION contents do not match the target server");
		record.path_len = path_length;
		record.data_len = chunk;
		record.offset = offset;
		XLogBeginInsert();
		XLogRegisterData(&record, SizeOfPgUpgradeRawFile);
		XLogRegisterData(unconstify(char *, path), path_length);
		if (chunk != 0)
			XLogRegisterData(buffer, chunk);
		(void) XLogInsert(RM_PG_UPGRADE_ID, XLOG_UPGRADE_RAWFILE);
		offset += chunk;
	} while (offset < bytes);

	close_regular(fd, path);
	pfree(buffer);
}

static void
capture_database(void)
{
	const		PgUpgradeDatabase *header = &emitter->database.header;
	bool		shared = header->old_database_oid == InvalidOid &&
		header->new_database_oid == InvalidOid;

	/*
	 * Emit RELINK operations and auxiliary files before the SMGR CREATE and
	 * full-page WAL that rebuild created and recreated files.
	 */
	prepare_forks();
	emit_database_operations();
	if (shared || header->new_database_oid != InvalidOid)
	{
		char	   *directory = GetDatabasePath(header->new_database_oid,
												shared ? GLOBALTABLESPACE_OID : header->new_default_tablespace);
		char	   *path = psprintf("%s/pg_filenode.map", directory);

		capture_raw_file(path, false, false);
		pfree(path);
		if (!shared)
		{
			path = psprintf("%s/PG_VERSION", directory);
			capture_raw_file(path, true, false);
			pfree(path);
		}
		pfree(directory);
	}
	for (size_t i = emitter->database.directory_count; i < emitter->noperations; i++)
	{
		const		EmitOperation *operation = &emitter->operations[i];
		const		PgUpgradeRelation *relation = &operation->data.relation;
		const xl_pg_upgrade_key *key = &relation->new_key;
		RelFileLocator locator = {key->tablespace_oid, key->database_oid, key->filenumber};

		for (unsigned f = 0; f < 4; f++)
		{
			if (!(relation->fork_mask & (1 << f)) ||
				!fork_rebuilt_from_wal(relation, f))
				continue;
			log_smgrcreate(&locator, f);
			(void) read_fork(key, f, true, true);
		}
	}
}

static void
capture_slru(const char *path, bool long_names)
{
	DIR		   *dir = open_directory(path);
	struct dirent *entry;

	(void) inspect_directory(path);
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

static void
complete_window(const uint8 *data, size_t length)
{
	/*
	 * Accept COMPLETE only with the START marker after every declared scope
	 * ends. Capture PG_VERSION, pg_xact, and both pg_multixact SLRUs, then
	 * write and flush COMPLETE.
	 */
	if (length != sizeof(emitter->input.marker) ||
		memcmp(data, &emitter->input.marker, length) != 0 ||
		emitter->database_context != NULL)
		emit_error("COMPLETE precedes checked completion of every database");
	capture_raw_file("PG_VERSION", true, false);
	XLogFlushUpgradeSLRU();
	capture_slru("pg_xact", false);
	capture_slru("pg_multixact/offsets", false);
	capture_slru("pg_multixact/members", true);
	emitter->complete_lsn = write_record(XLOG_UPGRADE_COMPLETE,
										 (const uint8 *) &emitter->input.marker, SizeOfPgUpgradeMarker);
	XLogFlush(emitter->complete_lsn);
#ifdef USE_ASSERT_CHECKING
	if (getenv("PG_UPGRADE_TEST_CRASH_AFTER_COMPLETE") != NULL)
		elog(PANIC, "test crash after pg_upgrade COMPLETE");
#endif
	emitter->complete = true;
}

static void
start_window(const uint8 *data, size_t length)
{
	const xl_pg_upgrade_start *window = &emitter->input;

	if (length != sizeof(emitter->input))
		emit_error("invalid upgrade START arguments");
	memcpy(&emitter->input, data, sizeof(emitter->input));
	if (emitter->input.transfer_mode > UPGRADE_RELINK_MODE_SWAP)
		emit_error("invalid upgrade transfer mode");
	BeginControlFileUpgrade();

	/* Bind START, COMPLETE, and COMMIT to one transaction ID. */
	(void) GetCurrentTransactionId();
	XLogFlushUpgradeSLRU();
#ifdef USE_ASSERT_CHECKING
	if (getenv("PG_UPGRADE_TEST_CHECKPOINT_BEFORE_START") != NULL)
		RequestCheckpoint(CHECKPOINT_FORCE | CHECKPOINT_FAST | CHECKPOINT_WAIT);
#endif
	write_record(XLOG_UPGRADE_START, (const uint8 *) window, SizeOfPgUpgradeStart);
#ifdef USE_ASSERT_CHECKING
	if (getenv("PG_UPGRADE_TEST_CHECKPOINT_BEFORE_CONTROL") != NULL)
		RequestCheckpoint(CHECKPOINT_FORCE | CHECKPOINT_FAST | CHECKPOINT_WAIT);
#endif
	XLogWriteUpgradeControlFile();
}

static void
read_relink_file(int fd, void *data, size_t length, off_t offset,
				 const char *path)
{
	size_t		done = 0;

	while (done < length)
	{
		ssize_t		amount = pg_pread(fd, (char *) data + done,
									  length - done, offset + done);

		if (amount < 0 && errno == EINTR)
		{
			CHECK_FOR_INTERRUPTS();
			continue;
		}
		if (amount < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read upgrade relink file \"%s\": %m", path)));
		if (amount == 0)
			emit_error("upgrade relink file ends before its declared length");
		done += amount;
	}
}

static uint32
relink_payload_crc(const void *data, size_t length)
{
	pg_crc32c	crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, data, length);
	FIN_CRC32C(crc);
	return (uint32) crc;
}

static bool
valid_relink_file_frame(const PgUpgradeRelinkFileFrame * frame,
						off_t remaining)
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
check_relink_file_header(const PgUpgradeRelinkFileHeader * header)
{
	if (header->magic != PG_UPGRADE_RELINK_FILE_MAGIC ||
		header->version != PG_UPGRADE_RELINK_FILE_VERSION ||
		header->producer_major != PG_VERSION_NUM ||
		header->control_version != PG_CONTROL_VERSION ||
		header->catalog_version != CATALOG_VERSION_NO ||
		header->start_size != sizeof(xl_pg_upgrade_start) ||
		header->batch_header_size != SizeOfPgUpgradeCatalogBatch ||
		header->entry_size != sizeof(PgUpgradeCatalogEntry) ||
		header->marker_size != sizeof(xl_pg_upgrade_marker))
		emit_error("upgrade relink file has an incompatible format");
	if (header->target_system_identifier != GetSystemIdentifier() ||
		header->block_size != BLCKSZ ||
		header->relseg_blocks != RELSEG_SIZE ||
		header->wal_block_size != XLOG_BLCKSZ ||
		header->wal_segment_size != wal_segment_size ||
		memcmp(header->mock_authentication_nonce,
			   GetMockAuthenticationNonce(),
			   MOCK_AUTH_NONCE_LEN) != 0 ||
		GetControlFileUpgradeStarted() || GetControlFileUpgradeFinalized())
		emit_error("upgrade relink file does not match the target cluster");
}

typedef struct CatalogValidation
{
	uint32		kind;
	bool		in_database;
	bool		saw_shared;
	bool		saw_template0;
	PgUpgradeCatalogDatabase database;
	uint64		seen;
	Oid			previous_database;
	Oid			previous_directory;
	xl_pg_upgrade_key previous_relation;
	Oid		   *directories;
	bool	   *referenced;
	uint32		relation_directory;
	bool		saw_default;
}			CatalogValidation;

typedef struct RelinkFileLayout
{
	xl_pg_upgrade_start start;
	xl_pg_upgrade_marker complete;
	off_t		old_start;
	off_t		target_start;
	off_t		complete_offset;
}			RelinkFileLayout;

typedef enum RelinkFileReadState
{
	RELINK_FILE_EXPECT_START,
	RELINK_FILE_READING_OLD_CATALOG,
	RELINK_FILE_READING_TARGET_CATALOG,
	RELINK_FILE_READ_COMPLETE
}			RelinkFileReadState;

typedef struct CatalogCursor
{
	int			fd;
	const char *path;
	uint32		kind;
	off_t		offset;
	off_t		end;
	char	   *buffer;
	PgUpgradeCatalogBatch *batch;
	uint32		count;
	uint32		index;
	bool		database_open;
	PgUpgradeCatalogDatabase database;
}			CatalogCursor;

static void
reset_catalog_validation(CatalogValidation * validation)
{
	pfree(validation->directories);
	pfree(validation->referenced);
	validation->directories = NULL;
	validation->referenced = NULL;
	validation->relation_directory = 0;
	validation->saw_default = false;
}

static void
finish_validated_database(CatalogValidation * validation)
{
	if (validation->seen != (uint64) validation->database.directory_count +
		validation->database.relation_count || !validation->saw_default)
		emit_error("upgrade catalog database section is incomplete");
	for (uint32 i = 0; i < validation->database.directory_count; i++)
		if (validation->directories[i] != validation->database.default_tablespace &&
			!validation->referenced[i])
			emit_error("upgrade catalog contains an unused database directory");
	validation->in_database = false;
	reset_catalog_validation(validation);
}

static void
validate_catalog_frame(CatalogValidation * validation,
					   const PgUpgradeRelinkFileFrame * frame,
					   const char *payload)
{
	const		PgUpgradeCatalogBatch *batch = (const PgUpgradeCatalogBatch *) payload;
	const		PgUpgradeCatalogDatabase *database = &batch->database;
	uint32		flags = batch->flags;
	bool		target = validation->kind == PG_UPGRADE_RELINK_TARGET;

	if ((flags & ~(UPGRADE_RELINK_BEGIN | UPGRADE_RELINK_END)) != 0 ||
		((flags & UPGRADE_RELINK_BEGIN) ? validation->in_database :
		 !validation->in_database))
		emit_error("upgrade catalog has an invalid database boundary");
	if (flags & UPGRADE_RELINK_BEGIN)
	{
		bool		shared = database->database_oid == InvalidOid;

		if (database->directory_count == 0 ||
			database->relation_count > PG_UINT32_MAX - database->directory_count ||
			(database->flags & ~(PG_UPGRADE_DATABASE_RELATIONS_AVAILABLE |
								 PG_UPGRADE_DATABASE_TEMPLATE0)) != 0 ||
			memcmp(database->reserved, "\0\0\0", sizeof(database->reserved)) != 0 ||
			(shared != !validation->saw_shared) ||
			(!shared && validation->saw_shared &&
			 database->database_oid <= validation->previous_database) ||
			(shared && (database->default_tablespace != GLOBALTABLESPACE_OID ||
						(database->flags & PG_UPGRADE_DATABASE_TEMPLATE0) != 0)) ||
			(!shared && database->default_tablespace == InvalidOid) ||
			(target &&
			 (database->flags & PG_UPGRADE_DATABASE_RELATIONS_AVAILABLE) == 0) ||
			(!target &&
			 (database->flags & PG_UPGRADE_DATABASE_RELATIONS_AVAILABLE) == 0 &&
			 ((database->flags & PG_UPGRADE_DATABASE_TEMPLATE0) == 0 ||
			  database->relation_count != 0)))
			emit_error("upgrade catalog has an invalid database description");
		if (database->flags & PG_UPGRADE_DATABASE_TEMPLATE0)
		{
			if (validation->saw_template0)
				emit_error("upgrade catalog contains duplicate template0 state");
			validation->saw_template0 = true;
		}
		validation->saw_shared = true;
		validation->previous_database = database->database_oid;
		validation->database = *database;
		validation->seen = 0;
		validation->previous_directory = InvalidOid;
		MemSet(&validation->previous_relation, 0,
			   sizeof(validation->previous_relation));
		validation->directories = palloc_array(Oid, database->directory_count);
		validation->referenced = palloc0_array(bool, database->directory_count);
		validation->in_database = true;
	}
	else if (memcmp(database, &validation->database, sizeof(*database)) != 0)
		emit_error("upgrade catalog database description changed within a section");
	if (frame->item_count > (uint64) validation->database.directory_count +
		validation->database.relation_count - validation->seen)
		emit_error("upgrade catalog database contains excess observations");

	for (uint32 i = 0; i < frame->item_count; i++, validation->seen++)
	{
		const		PgUpgradeCatalogEntry *entry = &batch->entries[i];
		bool		directory = validation->seen < validation->database.directory_count;

		if (entry->reserved != 0 || entry->tablespace_oid == InvalidOid)
			emit_error("upgrade catalog contains an invalid observation");
		if (directory)
		{
			uint32		index = (uint32) validation->seen;

			if (entry->filenumber != InvalidRelFileNumber ||
				entry->relkind != 0 || entry->persistence != 0 ||
				entry->tablespace_oid <= validation->previous_directory ||
				(target ? (entry->flags & ~PG_UPGRADE_CATALOG_INPLACE) != 0 :
				 entry->flags != 0))
				emit_error("upgrade catalog contains an invalid directory observation");
			validation->directories[index] = entry->tablespace_oid;
			validation->previous_directory = entry->tablespace_oid;
			validation->saw_default |= entry->tablespace_oid ==
				validation->database.default_tablespace;
		}
		else
		{
			xl_pg_upgrade_key key = {entry->tablespace_oid, 0, entry->filenumber};

			if (entry->filenumber == InvalidRelFileNumber ||
				PgUpgradeCompareKeys(&validation->previous_relation, &key) >= 0 ||
				(target ?
				 (entry->flags & ~PG_UPGRADE_CATALOG_TRANSFERRED) != 0 ||
				 (entry->persistence != 'p' && entry->persistence != 'u') ||
				 entry->relkind == 0 || strchr("riStm", entry->relkind) == NULL :
				 entry->flags != 0 || entry->relkind != 0 || entry->persistence != 0))
				emit_error("upgrade catalog contains an invalid relation observation");
			while (validation->relation_directory <
				   validation->database.directory_count &&
				   validation->directories[validation->relation_directory] <
				   entry->tablespace_oid)
				validation->relation_directory++;
			if (validation->relation_directory ==
				validation->database.directory_count ||
				validation->directories[validation->relation_directory] !=
				entry->tablespace_oid)
				emit_error("upgrade relation has no declared database directory");
			validation->referenced[validation->relation_directory] = true;
			validation->previous_relation = key;
		}
	}
	if (flags & UPGRADE_RELINK_END)
		finish_validated_database(validation);
}

static void
validate_catalog_stream_complete(CatalogValidation * validation)
{
	if (validation->in_database || !validation->saw_shared ||
		!validation->saw_template0)
		emit_error(validation->kind == PG_UPGRADE_RELINK_OLD ?
				   "old upgrade catalog stream is incomplete" :
				   "target upgrade catalog stream is incomplete");
}

static void
load_catalog_cursor_frame(CatalogCursor * cursor, bool begin)
{
	PgUpgradeRelinkFileFrame frame;
	off_t		payload_offset;

	if (cursor->offset >= cursor->end)
		emit_error("upgrade catalog database section ends unexpectedly");
	read_relink_file(cursor->fd, &frame, sizeof(frame), cursor->offset,
					 cursor->path);
	payload_offset = cursor->offset + sizeof(frame);
	if (frame.opcode != cursor->kind ||
		!valid_relink_file_frame(&frame, cursor->end - cursor->offset))
		emit_error("upgrade catalog cursor encountered an invalid frame");
	read_relink_file(cursor->fd, cursor->buffer, frame.payload_length,
					 payload_offset, cursor->path);
	if (frame.payload_crc !=
		relink_payload_crc(cursor->buffer, frame.payload_length))
		emit_error("upgrade relink frame changed after validation");
	cursor->offset = payload_offset + frame.payload_length;
	cursor->batch = (PgUpgradeCatalogBatch *) cursor->buffer;
	cursor->count = frame.item_count;
	cursor->index = 0;
	if (begin)
	{
		if ((cursor->batch->flags & UPGRADE_RELINK_BEGIN) == 0)
			emit_error("upgrade catalog cursor did not begin a database");
		cursor->database = cursor->batch->database;
		cursor->database_open = true;
	}
	else if ((cursor->batch->flags & UPGRADE_RELINK_BEGIN) != 0 ||
			 memcmp(&cursor->batch->database, &cursor->database,
					sizeof(cursor->database)) != 0)
		emit_error("upgrade catalog cursor changed database unexpectedly");
}

static bool
catalog_cursor_next_database(CatalogCursor * cursor)
{
	if (cursor->database_open)
		emit_error("upgrade catalog cursor advanced before its database ended");
	if (cursor->offset == cursor->end)
		return false;
	load_catalog_cursor_frame(cursor, true);
	return true;
}

static bool
catalog_cursor_next_entry(CatalogCursor * cursor,
						  PgUpgradeCatalogEntry * entry)
{
	for (;;)
	{
		if (!cursor->database_open)
			emit_error("upgrade catalog cursor has no active database");
		if (cursor->index < cursor->count)
		{
			*entry = cursor->batch->entries[cursor->index++];
			return true;
		}
		if (cursor->batch->flags & UPGRADE_RELINK_END)
			return false;
		load_catalog_cursor_frame(cursor, false);
	}
}

static void
finish_catalog_cursor_database(CatalogCursor * cursor)
{
	PgUpgradeCatalogEntry entry;

	if (catalog_cursor_next_entry(cursor, &entry))
		emit_error("upgrade catalog database has excess observations");
	cursor->database_open = false;
}

static void
append_derived_operation(const PgUpgradeEmitOperation * operation)
{
	if (emitter->noperations == PG_UINT32_MAX)
		emit_error("too many storage operations in one database");
	if (emitter->noperations == emitter->operation_capacity)
	{
		size_t		capacity = emitter->operation_capacity == 0 ? 128 :
			add_size(emitter->operation_capacity, emitter->operation_capacity);

		if (emitter->operations == NULL)
			emitter->operations = MemoryContextAllocExtended(
															 emitter->database_context,
															 mul_size(capacity, sizeof(EmitOperation)), MCXT_ALLOC_HUGE);
		else
			emitter->operations = repalloc_array_extended(emitter->operations,
														  EmitOperation, capacity, MCXT_ALLOC_HUGE);
		emitter->operation_capacity = capacity;
	}
	emitter->operations[emitter->noperations++].data = *operation;
}

static int
compare_catalog_entries(const PgUpgradeCatalogEntry * left,
						const PgUpgradeCatalogEntry * right)
{
	if (left->tablespace_oid != right->tablespace_oid)
		return left->tablespace_oid < right->tablespace_oid ? -1 : 1;
	return (left->filenumber > right->filenumber) -
		(left->filenumber < right->filenumber);
}

static bool
reference_transfer_mode(void)
{
	return emitter->input.transfer_mode == UPGRADE_RELINK_MODE_CLONE ||
		emitter->input.transfer_mode == UPGRADE_RELINK_MODE_LINK ||
		emitter->input.transfer_mode == UPGRADE_RELINK_MODE_SWAP;
}

static void
derive_directories(CatalogCursor * old, CatalogCursor * target,
				   uint32 old_count, uint32 target_count, bool recreate)
{
	PgUpgradeCatalogEntry old_entry;
	PgUpgradeCatalogEntry target_entry;
	bool		have_old = false;
	bool		have_target = false;

	while (old_count != 0 || target_count != 0 || have_old || have_target)
	{
		PgUpgradeEmitOperation operation = {0};
		int			comparison;

		if (!have_old && old_count != 0)
		{
			if (!catalog_cursor_next_entry(old, &old_entry))
				emit_error("old catalog directory list ends early");
			old_count--;
			have_old = true;
		}
		if (!have_target && target_count != 0)
		{
			if (!catalog_cursor_next_entry(target, &target_entry))
				emit_error("target catalog directory list ends early");
			target_count--;
			have_target = true;
		}
		comparison = !have_old ? 1 : !have_target ? -1 :
			compare_catalog_entries(&old_entry, &target_entry);
		if (comparison <= 0)
		{
			operation.relation.old_key = (xl_pg_upgrade_key)
			{
				old_entry.tablespace_oid,
					emitter->database.header.old_database_oid, 0
			};
			have_old = false;
		}
		if (comparison >= 0)
		{
			operation.relation.new_key = (xl_pg_upgrade_key)
			{
				target_entry.tablespace_oid,
					emitter->database.header.new_database_oid, 0
			};
			operation.new_directory_inplace =
				(target_entry.flags & PG_UPGRADE_CATALOG_INPLACE) != 0;
			have_target = false;
		}
		operation.relation.operation = comparison < 0 ? UPGRADE_RELINK_DELETE :
			comparison > 0 ? UPGRADE_RELINK_CREATE :
			recreate ? UPGRADE_RELINK_RECREATE : UPGRADE_RELINK_INHERIT;
		append_derived_operation(&operation);
		emitter->database.directory_count++;
	}
}

static uint8
relation_fork_mask(const PgUpgradeCatalogEntry * entry)
{
	if (entry->persistence == 'u')
		return 1 << INIT_FORKNUM;
	if (entry->relkind == 'S')
		return 1 << MAIN_FORKNUM;
	if (entry->relkind == 'i')
		return (1 << MAIN_FORKNUM) | (1 << FSM_FORKNUM);
	return (1 << MAIN_FORKNUM) | (1 << FSM_FORKNUM) |
		(1 << VISIBILITYMAP_FORKNUM);
}

static void
derive_relations(CatalogCursor * old, CatalogCursor * target,
				 uint32 old_count, uint32 target_count)
{
	PgUpgradeCatalogEntry old_entry;
	PgUpgradeCatalogEntry target_entry;
	bool		have_old = false;
	bool		have_target = false;

	while (old_count != 0 || target_count != 0 || have_old || have_target)
	{
		PgUpgradeEmitOperation operation = {0};
		int			comparison;

		if (!have_old && old_count != 0)
		{
			if (!catalog_cursor_next_entry(old, &old_entry))
				emit_error("old catalog relation list ends early");
			old_count--;
			have_old = true;
		}
		if (!have_target && target_count != 0)
		{
			if (!catalog_cursor_next_entry(target, &target_entry))
				emit_error("target catalog relation list ends early");
			target_count--;
			have_target = true;
		}
		comparison = !have_old ? 1 : !have_target ? -1 :
			compare_catalog_entries(&old_entry, &target_entry);
		if (comparison <= 0)
		{
			operation.relation.old_key = (xl_pg_upgrade_key)
			{
				old_entry.tablespace_oid,
					emitter->database.header.old_database_oid,
					old_entry.filenumber
			};
			have_old = false;
		}
		if (comparison >= 0)
		{
			operation.relation.new_key = (xl_pg_upgrade_key)
			{
				target_entry.tablespace_oid,
					emitter->database.header.new_database_oid,
					target_entry.filenumber
			};
			operation.relation.persistence = target_entry.persistence;
			operation.relation.fork_mask = relation_fork_mask(&target_entry);
			have_target = false;
		}
		else
			operation.relation.persistence = 'p';
		operation.relation.operation = comparison < 0 ? UPGRADE_RELINK_DELETE :
			comparison > 0 ? UPGRADE_RELINK_CREATE :
			(target_entry.flags & PG_UPGRADE_CATALOG_TRANSFERRED) != 0 &&
			reference_transfer_mode() ? UPGRADE_RELINK_INHERIT :
			UPGRADE_RELINK_RECREATE;
		append_derived_operation(&operation);
		emitter->database.relation_count++;
	}
}

static void
derive_database(CatalogCursor * old, CatalogCursor * target)
{
	const		PgUpgradeCatalogDatabase *old_database = old ? &old->database : NULL;
	const		PgUpgradeCatalogDatabase *target_database = target ? &target->database : NULL;
	bool		recreate = old_database != NULL && target_database != NULL &&
		(old_database->flags & PG_UPGRADE_DATABASE_RELATIONS_AVAILABLE) == 0;
	MemoryContext previous;

	if ((old_database != NULL && target_database != NULL &&
		 old_database->database_oid != target_database->database_oid) ||
		(old_database != NULL && target_database == NULL &&
		 ((old_database->flags & PG_UPGRADE_DATABASE_TEMPLATE0) == 0 ||
		  (old_database->flags & PG_UPGRADE_DATABASE_RELATIONS_AVAILABLE) != 0)) ||
		(old_database != NULL && target_database != NULL &&
		 ((old_database->flags ^ target_database->flags) &
		  PG_UPGRADE_DATABASE_TEMPLATE0) != 0))
		emit_error("old and target catalog database scopes do not match");

	emitter->database_context = AllocSetContextCreate(emitter->window_context,
													  "WAL upgrade database",
													  ALLOCSET_DEFAULT_SIZES);
	previous = MemoryContextSwitchTo(emitter->database_context);
	MemSet(&emitter->database, 0, sizeof(emitter->database));
	emitter->noperations = 0;
	emitter->operation_capacity = 0;
	emitter->operations = NULL;
	if (old_database != NULL)
		emitter->database.header.old_database_oid = old_database->database_oid;
	if (target_database != NULL)
	{
		emitter->database.header.new_database_oid = target_database->database_oid;
		emitter->database.header.new_default_tablespace =
			target_database->default_tablespace;
	}
	derive_directories(old, target,
					   old_database ? old_database->directory_count : 0,
					   target_database ? target_database->directory_count : 0, recreate);
	derive_relations(old, target,
					 old_database ? old_database->relation_count : 0,
					 target_database ? target_database->relation_count : 0);
	if (old != NULL)
		finish_catalog_cursor_database(old);
	if (target != NULL)
		finish_catalog_cursor_database(target);
	capture_database();
	MemoryContextSwitchTo(previous);
	MemoryContextDelete(emitter->database_context);
	emitter->database_context = NULL;
	emitter->operations = NULL;
	emitter->noperations = emitter->operation_capacity = 0;
}

static void
merge_catalogs(CatalogCursor * old, CatalogCursor * target)
{
	bool		have_old = catalog_cursor_next_database(old);
	bool		have_target = catalog_cursor_next_database(target);

	while (have_old || have_target)
	{
		int			comparison = !have_old ? 1 : !have_target ? -1 :
			(old->database.database_oid > target->database.database_oid) -
			(old->database.database_oid < target->database.database_oid);

		derive_database(comparison <= 0 ? old : NULL,
						comparison >= 0 ? target : NULL);
		if (comparison <= 0)
			have_old = catalog_cursor_next_database(old);
		if (comparison >= 0)
			have_target = catalog_cursor_next_database(target);
	}
}

void
PgUpgradeEmitWalFile(void)
{
	char		path[MAXPGPATH];
	struct stat path_stat;
	struct stat fd_stat;
	PgUpgradeRelinkFileHeader header;
	PgUpgradeRelinkFileEnd end;
	RelinkFileLayout layout = {0};
	CatalogValidation old_catalog = {.kind = PG_UPGRADE_RELINK_OLD};
	CatalogValidation target_catalog = {.kind = PG_UPGRADE_RELINK_TARGET};
	size_t		buffer_size = SizeOfPgUpgradeCatalogBatch +
		PG_UPGRADE_CATALOG_MAX_ENTRIES * sizeof(PgUpgradeCatalogEntry);
	char	   *buffer = palloc(buffer_size);
	off_t		end_offset;
	off_t		offset;
	uint32		sequence = 0;
	uint64		total_items = 0;
	RelinkFileReadState state = RELINK_FILE_EXPECT_START;
	pg_crc32c	crc;
	int			fd;
	int			flags = O_RDONLY | PG_BINARY;

	if (snprintf(path, sizeof(path), "%s/%s", DataDir,
				 PG_UPGRADE_RELINK_FILE) >= sizeof(path))
		emit_error("upgrade relink file path is too long");
	reject_reparse_point(path);
	if (lstat(path, &path_stat) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not stat upgrade relink file \"%s\": %m", path)));
#ifdef O_NOFOLLOW
	flags |= O_NOFOLLOW;
#endif
	fd = OpenTransientFile(path, flags);
	if (fd < 0 || fstat(fd, &fd_stat) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open or stat upgrade relink file \"%s\": %m", path)));
	if (!S_ISREG(path_stat.st_mode) || !S_ISREG(fd_stat.st_mode) ||
		path_stat.st_size != fd_stat.st_size ||
		path_stat.st_dev != fd_stat.st_dev || path_stat.st_ino != fd_stat.st_ino)
		emit_error("upgrade relink path is not one regular file");
#ifndef WIN32
	if (fd_stat.st_uid != geteuid() || fd_stat.st_nlink != 1 ||
		(fd_stat.st_mode & (S_IRWXU | S_IRWXG | S_IRWXO)) !=
		(S_IRUSR | S_IWUSR))
		emit_error("upgrade relink file has unsafe ownership or permissions");
#endif
	if (fd_stat.st_size < (off_t) (sizeof(header) + sizeof(end)))
		emit_error("upgrade relink file is too short");
	end_offset = fd_stat.st_size - sizeof(end);
	read_relink_file(fd, &header, sizeof(header), 0, path);
	check_relink_file_header(&header);
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, &header, sizeof(header));
	offset = sizeof(header);
	while (offset < end_offset)
	{
		PgUpgradeRelinkFileFrame frame;
		off_t		payload_offset = offset + sizeof(frame);

		CHECK_FOR_INTERRUPTS();
		read_relink_file(fd, &frame, sizeof(frame), offset, path);
		if (frame.sequence != sequence ||
			!valid_relink_file_frame(&frame, end_offset - offset) ||
			frame.payload_length > buffer_size)
			emit_error("upgrade relink file has an invalid frame sequence");
		read_relink_file(fd, buffer, frame.payload_length, payload_offset, path);
		if (frame.payload_crc != relink_payload_crc(buffer, frame.payload_length))
			emit_error("upgrade relink frame checksum does not match its payload");
		COMP_CRC32C(crc, &frame, sizeof(frame));
		COMP_CRC32C(crc, buffer, frame.payload_length);
		if (frame.opcode == PG_UPGRADE_RELINK_START)
		{
			if (state != RELINK_FILE_EXPECT_START || sequence != 0)
				emit_error("upgrade relink START is out of order");
			memcpy(&layout.start, buffer, sizeof(layout.start));
			state = RELINK_FILE_READING_OLD_CATALOG;
		}
		else if (frame.opcode == PG_UPGRADE_RELINK_OLD)
		{
			if (state != RELINK_FILE_READING_OLD_CATALOG)
				emit_error("old catalog observations are out of order");
			if (layout.old_start == 0)
				layout.old_start = offset;
			validate_catalog_frame(&old_catalog, &frame, buffer);
		}
		else if (frame.opcode == PG_UPGRADE_RELINK_TARGET)
		{
			if (state == RELINK_FILE_READING_OLD_CATALOG)
			{
				validate_catalog_stream_complete(&old_catalog);
				layout.target_start = offset;
				state = RELINK_FILE_READING_TARGET_CATALOG;
			}
			if (state != RELINK_FILE_READING_TARGET_CATALOG)
				emit_error("target catalog observations are out of order");
			validate_catalog_frame(&target_catalog, &frame, buffer);
		}
		else if (frame.opcode == PG_UPGRADE_RELINK_COMPLETE)
		{
			if (state != RELINK_FILE_READING_TARGET_CATALOG)
				emit_error("upgrade relink COMPLETE is out of order");
			validate_catalog_stream_complete(&target_catalog);
			layout.complete_offset = offset;
			memcpy(&layout.complete, buffer, sizeof(layout.complete));
			state = RELINK_FILE_READ_COMPLETE;
		}
		else
			emit_error("upgrade relink file contains an unknown frame");
		if (pg_add_u64_overflow(total_items, frame.item_count, &total_items) ||
			sequence == PG_UINT32_MAX)
			emit_error("upgrade relink file has excessive frame totals");
		sequence++;
		offset = payload_offset + frame.payload_length;
	}
	read_relink_file(fd, &end, sizeof(end), end_offset, path);
	if (offset != end_offset || state != RELINK_FILE_READ_COMPLETE ||
		layout.old_start == 0 ||
		layout.target_start == 0 || layout.complete_offset == 0 ||
		end.total_length != (uint64) fd_stat.st_size ||
		end.frame_count != sequence || end.total_items != total_items)
		emit_error("upgrade relink file has inconsistent frame totals");
	COMP_CRC32C(crc, &end, offsetof(PgUpgradeRelinkFileEnd, file_crc));
	FIN_CRC32C(crc);
	if (!EQ_CRC32C(crc, end.file_crc))
		emit_error("upgrade relink file checksum does not match its contents");
	if (fstat(fd, &path_stat) != 0 || path_stat.st_size != fd_stat.st_size)
		emit_error("upgrade relink file changed during validation");

	/* The second pass merges one old and target database at a time. */
	{
		CatalogCursor old =
		{
			.fd = fd, .path = path, .kind = PG_UPGRADE_RELINK_OLD,
			.offset = layout.old_start, .end = layout.target_start,
			.buffer = palloc(buffer_size)
		};
		CatalogCursor target =
		{
			.fd = fd, .path = path, .kind = PG_UPGRADE_RELINK_TARGET,
			.offset = layout.target_start, .end = layout.complete_offset,
			.buffer = palloc(buffer_size)
		};

		PgUpgradeEmitWal(XLOG_UPGRADE_START, (const uint8 *) &layout.start,
						 sizeof(layout.start));
		merge_catalogs(&old, &target);
		if (old.offset != old.end || target.offset != target.end ||
			old.database_open || target.database_open)
			emit_error("upgrade catalog cursors did not consume the file");
		pfree(old.buffer);
		pfree(target.buffer);
	}
	if (fstat(fd, &path_stat) != 0 || path_stat.st_size != fd_stat.st_size ||
		path_stat.st_dev != fd_stat.st_dev || path_stat.st_ino != fd_stat.st_ino)
		emit_error("upgrade relink file changed during emission");
	PgUpgradeEmitWal(XLOG_UPGRADE_COMPLETE, (const uint8 *) &layout.complete,
					 sizeof(layout.complete));
	if (CloseTransientFile(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close upgrade relink file \"%s\": %m", path)));
	pfree(buffer);
}

void
PgUpgradeEmitWal(uint8 opcode, const uint8 *data, size_t length)
{
	MemoryContext caller = CurrentMemoryContext;

	PG_TRY();
	{
		if (!IsBinaryUpgrade || !superuser())
			ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
							errmsg("upgrade WAL emission requires a binary-upgrade superuser")));
		if (!IsTransactionBlock() || IsSubTransaction() || IsInParallelMode() || IsParallelWorker() ||
			RecoveryInProgress() || XactReadOnly)
			emit_error("emission requires an explicit, writable top-level transaction on the primary");
		if (emitter == NULL)
		{
			if (opcode != XLOG_UPGRADE_START)
				emit_error("upgrade emission requires START");
			create_emitter();
			start_window(data, length);
		}
		else
		{
			if (emitter->poisoned || emitter->complete)
				emit_error("the emission window is poisoned or already complete");
			MemoryContextSwitchTo(emitter->window_context);
			if (opcode == XLOG_UPGRADE_COMPLETE)
				complete_window(data, length);
			else
				emit_error("unexpected upgrade emission request");
		}
		MemoryContextSwitchTo(caller);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(caller);
		if (emitter != NULL)
			emitter->poisoned = true;
		PG_RE_THROW();
	}
	PG_END_TRY();
}
