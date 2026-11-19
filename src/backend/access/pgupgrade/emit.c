/*-------------------------------------------------------------------------
 *
 * emit.c
 *    Validate the relink input and emit upgrade WAL records ending with
 *    COMPLETE.
 *
 * A backend in the new cluster merge-joins the old and new relation
 * inventories, derives storage operations, captures relation pages and
 * auxiliary files, and emits the records in the caller's upgrade transaction.
 *
 * For each database, RELINK operations precede the SMGR create records and
 * full-page images for CREATE and RECREATE relations. INHERIT relations do
 * not require page images.
 *
 * COMPLETE is flushed before returning to pg_upgrade. The caller commits the
 * upgrade transaction and shuts down the new server to write the completion
 * checkpoint.
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
#include "access/parallel.h"
#include "access/pgupgrade_control.h"
#include "access/pgupgrade_emit.h"
#include "access/pgupgrade_wal.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "catalog/pg_control.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/storage_xlog.h"
#include "common/file_utils.h"
#include "common/int.h"
#include "common/pg_upgrade_data.h"
#include "common/relpath.h"
#include "miscadmin.h"
#include "port/pg_crc32c.h"
#include "postmaster/bgwriter.h"
#include "storage/fd.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/procnumber.h"
#include "utils/injection_point.h"
#include "utils/memutils.h"

typedef struct EmitOperation
{
	xl_pg_upgrade_relink_entry entry;
	uint8		persistence;
} EmitOperation;

typedef struct EmitDirectory
{
	xl_pg_upgrade_relink_entry entry;
	bool		old_present;
	bool		new_present;
} EmitDirectory;

typedef struct PgUpgradeEmitter
{
	MemoryContext transaction_context;
	MemoryContext database_context;
	MemoryContextCallback transaction_reset;
	xl_pg_upgrade_start input;
	Oid			old_database_oid;
	Oid			new_database_oid;
	Oid			new_default_tablespace;
	bool		have_new_database;
	EmitDirectory *directories;
	size_t		ndirectories;
	size_t		directory_capacity;
	EmitOperation *operations;
	size_t		noperations;
	size_t		operation_capacity;
	bool		poisoned;
	bool		complete;
	XLogRecPtr	complete_lsn;
} PgUpgradeEmitter;

static PgUpgradeEmitter *emitter;
static bool callbacks_registered;

/* Flush selected test prefixes before the existing injection action runs. */
static void
emit_test_point(const char *stage, Oid database, RelFileNumber relation,
				uint32 ordinal, XLogRecPtr end, const char *path)
{
#if defined(USE_ASSERT_CHECKING) && defined(USE_INJECTION_POINTS)
	char		name[64];

	if (getenv("PG_UPGRADE_TEST_INJECTION_POINTS") == NULL)
		return;
	snprintf(name, sizeof(name), "pg-upgrade-%s-%u-%u-%u",
			 stage, database, relation, ordinal);
	if (!IS_INJECTION_POINT_ATTACHED(name))
		return;
	if (!XLogRecPtrIsInvalid(end))
		XLogFlush(end);
	elog(LOG, "pg_upgrade test point %s xid %u database %u relation %u ordinal %u end %X/%X path %s",
		 name, GetCurrentTransactionIdIfAny(), database, relation, ordinal,
		 LSN_FORMAT_ARGS(end), path ? path : "-");
	INJECTION_POINT(name, NULL);
	CHECK_FOR_INTERRUPTS();
#endif
}

static const char *const upgrade_slru_dirs[] = {
	"pg_xact",
	"pg_multixact/offsets",
	"pg_multixact/members"
};

pg_noreturn static void
emit_error(const char *message)
{
	ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED),
			 errmsg("could not emit upgrade WAL: %s", message)));
}

static void
transaction_reset(void *arg)
{
	if (emitter == arg)
		emitter = NULL;
}

/*
 * Reject prepared upgrade transactions and reject commit unless emission
 * reached COMPLETE without an error. Commit arms the shutdown checkpoint
 * that finalizes the upgraded primary.
 */
static void
emit_xact_callback(XactEvent event, void *arg)
{
	if (emitter == NULL)
		return;
	if (event == XACT_EVENT_PRE_PREPARE)
		emit_error("an upgrade transaction cannot be prepared");
	if ((event == XACT_EVENT_PRE_COMMIT || event == XACT_EVENT_PARALLEL_PRE_COMMIT) &&
		(!emitter->complete || emitter->poisoned))
		emit_error("upgrade transaction has not reached COMPLETE");
	if (event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT)
		emitter->poisoned = true;
	if (event == XACT_EVENT_COMMIT)
	{
		Assert(emitter->complete && !emitter->poisoned);
		Assert(!XLogRecPtrIsInvalid(emitter->complete_lsn));
		ArmUpgradeCompletionCheckpoint(emitter->complete_lsn);
		emit_test_point("commit-response", InvalidOid, 0, 0,
					   InvalidXLogRecPtr, NULL);
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
									"WAL upgrade emission transaction", ALLOCSET_DEFAULT_SIZES);
	previous = MemoryContextSwitchTo(context);
	emitter = palloc0_object(PgUpgradeEmitter);
	emitter->transaction_context = context;
	emitter->transaction_reset.func = transaction_reset;
	emitter->transaction_reset.arg = emitter;
	MemoryContextRegisterResetCallback(context, &emitter->transaction_reset);
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
	uint32		test_ordinal = 0;

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
				 errmsg("relation segment \"%s\" changed after upgrade validation", path),
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

		CHECK_FOR_INTERRUPTS();
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
		emit_test_point("fpi", dboid, rfnum, ++test_ordinal, recptr, path);
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

#ifdef USE_ASSERT_CHECKING
	if (getenv("PG_UPGRADE_TEST_CHECKPOINT_AFTER_CONTROL_SNAPSHOT") != NULL)
	{
		RequestCheckpoint(CHECKPOINT_FORCE | CHECKPOINT_FAST | CHECKPOINT_WAIT);
		RequestCheckpoint(CHECKPOINT_FORCE | CHECKPOINT_FAST | CHECKPOINT_WAIT);
	}
#endif

	(void) PgUpgradeInsertRawFileRecord(path, buffer, PG_CONTROL_FILE_SIZE, 0);
	/*
	 * Record the current parameter and checksum state after the pg_control
	 * image.
	 */
	XLogReportCurrentControlState();
	emit_test_point("control", InvalidOid, 0, 0, GetXLogInsertRecPtr(), path);

	pfree(buffer);
}

static void
flush_slru(void)
{
	CheckPointCLOG();
	CheckPointCommitTs();
	CheckPointMultiXact();

	for (int i = 0; i < lengthof(upgrade_slru_dirs); i++)
	{
		DIR		   *dir;
		struct dirent *de;

		dir = AllocateDir(upgrade_slru_dirs[i]);
		if (dir == NULL)
			continue;

		while ((de = ReadDir(dir, upgrade_slru_dirs[i])) != NULL)
		{
			char		path[MAXPGPATH];

			if (de->d_name[0] == '.')
				continue;
			if (strlen(de->d_name) > 8)
				continue;
			if (strspn(de->d_name, "0123456789ABCDEF") != strlen(de->d_name))
				continue;

			snprintf(path, sizeof(path), "%s/%s", upgrade_slru_dirs[i], de->d_name);
			(void) fsync_fname(path, false);
		}
		FreeDir(dir);
		(void) fsync_fname(upgrade_slru_dirs[i], true);
	}
}

/*
 * RELINK entries for one database, or for shared relations, are split into
 * batches of at most UPGRADE_RELINK_MAX_ENTRIES entries. The begin flag marks
 * the first batch and the end flag marks the last.
 *
 * If another entry would exceed the limit, emit the full batch without the
 * end flag because more entries follow. Start the next batch without the
 * begin flag. The caller sets the end flag after appending the last entry.
 */
static void
append_relink_entry(xl_pg_upgrade_relink *batch, uint32 *count,
					const xl_pg_upgrade_relink_entry *entry, uint32 *test_ordinal)
{
	if (*count == UPGRADE_RELINK_MAX_ENTRIES)
	{
		PgUpgradeInsertRecord(XLOG_UPGRADE_RELINK, batch,
							  SizeOfPgUpgradeRelink +
							  *count * SizeOfPgUpgradeRelinkEntry);
		emit_test_point("relink", emitter->new_database_oid, 0,
					   ++*test_ordinal, GetXLogInsertRecPtr(), NULL);
		batch->flags = 0;
		*count = 0;
	}
	batch->entries[(*count)++] = *entry;
}

/* Emit one global or database scope with directories before relations. */
static void
emit_database_operations(void)
{
	xl_pg_upgrade_relink *batch = palloc0(SizeOfPgUpgradeRelink +
										  UPGRADE_RELINK_MAX_ENTRIES * SizeOfPgUpgradeRelinkEntry);
	uint32		count = 0;
	uint32		test_ordinal = 0;

	batch->flags = UPGRADE_RELINK_BEGIN;
	for (size_t i = 0; i < emitter->ndirectories; i++)
		append_relink_entry(batch, &count, &emitter->directories[i].entry, &test_ordinal);
	for (size_t i = 0; i < emitter->noperations; i++)
		append_relink_entry(batch, &count, &emitter->operations[i].entry, &test_ordinal);
	batch->flags |= UPGRADE_RELINK_END;
	PgUpgradeInsertRecord(XLOG_UPGRADE_RELINK, batch,
						  SizeOfPgUpgradeRelink +
						  count * SizeOfPgUpgradeRelinkEntry);
	emit_test_point("relink", emitter->new_database_oid, 0,
				   ++test_ordinal, GetXLogInsertRecPtr(), NULL);
	pfree(batch);
}

/*
 * Emit an SMGR create record and full-page images for a CREATE or RECREATE
 * relation.
 */
static void
capture_relation_storage(const EmitOperation *relation)
{
	const xl_pg_upgrade_key *key = &relation->entry.key;
	ForkNumber	fork = relation->persistence == 'u' ?
		INIT_FORKNUM : MAIN_FORKNUM;
	RelPathStr	base = GetRelationPath(key->database_oid, key->tablespace_oid,
									   key->filenumber, INVALID_PROC_NUMBER, fork);
	bool		partial = false;
	RelFileLocator locator =
	{key->tablespace_oid, key->database_oid, key->filenumber};

	log_smgrcreate(&locator, fork);
	emit_test_point("smgr", key->database_oid, key->filenumber, 0,
				   GetXLogInsertRecPtr(), base.str);

	for (uint32 segment = 0;; segment++)
	{
		char	   *path = segment == 0 ? pstrdup(base.str) : psprintf("%s.%u", base.str, segment);
		uint64		bytes;
		int			fd = open_regular(path, &bytes, segment != 0);

		CHECK_FOR_INTERRUPTS();
		if (fd < 0)
		{
			pfree(path);
			return;
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
		capture_relation_segment(path, key->tablespace_oid, key->database_oid,
								 key->filenumber, fork, segment, bytes / BLCKSZ);
		pfree(path);
	}
}

static void
read_exact_file(int fd, void *data, size_t length, off_t offset,
				const char *path, bool relink_input)
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
					 errmsg("could not read %s\"%s\": %m",
							relink_input ? "upgrade relink file " : "", path)));
		if (amount == 0)
		{
			if (!relink_input)
				emit_error("auxiliary file became shorter during capture");
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("upgrade relink file \"%s\" ends unexpectedly", path)));
		}
		done += amount;
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
		XLogRecPtr end;

		CHECK_FOR_INTERRUPTS();
		read_exact_file(fd, buffer, chunk, offset, path, false);
		if (version && memcmp(buffer, PG_MAJORVERSION "\n", chunk) != 0)
			emit_error("PG_VERSION contents do not match the new server");
		end = PgUpgradeInsertRawFileRecord(path, buffer, chunk, offset);

		if (slru)
			emit_test_point("slru", InvalidOid, 0, 0, end, path);
		offset += chunk;
	} while (offset < bytes);

	close_regular(fd, path);
	pfree(buffer);
}

static void
capture_database(void)
{
	bool		shared = emitter->have_new_database &&
		emitter->new_database_oid == InvalidOid;

	/*
	 * Emit RELINK and RAWFILE records before the SMGR create records and
	 * full-page images that rebuild CREATE and RECREATE relations.
	 */
	emit_database_operations();
	if (emitter->have_new_database)
	{
		char	   *directory = GetDatabasePath(emitter->new_database_oid,
												shared ? GLOBALTABLESPACE_OID : emitter->new_default_tablespace);
		char	   *path = psprintf("%s/pg_filenode.map", directory);

		emit_test_point("before-relmap", emitter->new_database_oid, 0, 0,
					   GetXLogInsertRecPtr(), path);
		capture_raw_file(path, false, false);
		emit_test_point("relmap", emitter->new_database_oid, 0, 0,
					   GetXLogInsertRecPtr(), path);
		pfree(path);
		if (!shared)
		{
			path = psprintf("%s/PG_VERSION", directory);
			capture_raw_file(path, true, false);
			pfree(path);
		}
		pfree(directory);
	}
	for (size_t i = 0; i < emitter->noperations; i++)
	{
		const EmitOperation *relation = &emitter->operations[i];

		if (relation->entry.operation == UPGRADE_RELINK_CREATE ||
			relation->entry.operation == UPGRADE_RELINK_RECREATE)
			capture_relation_storage(relation);
	}
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

static void
complete_transaction(void)
{
	/*
	 * After emitting WAL for all shared and per-database relations, capture
	 * PG_VERSION, pg_xact, and both pg_multixact SLRUs, then insert and flush
	 * COMPLETE.
	 */
	Assert(emitter->database_context == NULL);
	capture_raw_file("PG_VERSION", true, false);
	flush_slru();
	capture_slru("pg_xact", false);
	capture_slru("pg_multixact/offsets", false);
	capture_slru("pg_multixact/members", true);
	emitter->complete_lsn = PgUpgradeInsertRecord(XLOG_UPGRADE_COMPLETE,
												  &emitter->input.marker,
												  SizeOfPgUpgradeMarker);
	XLogFlush(emitter->complete_lsn);
#ifdef USE_ASSERT_CHECKING
	if (getenv("PG_UPGRADE_TEST_CRASH_AFTER_COMPLETE") != NULL)
		elog(PANIC, "test crash after pg_upgrade COMPLETE");
#endif
	emitter->complete = true;
	emit_test_point("complete", InvalidOid, 0, 0, emitter->complete_lsn, NULL);
}

static void
start_transaction(const xl_pg_upgrade_start *transaction)
{
	emitter->input = *transaction;
	BeginControlFileUpgrade();

	/* Assign the XID carried by all upgrade WAL records and the COMMIT record. */
	(void) GetCurrentTransactionId();
	flush_slru();
	emit_test_point("before-start", InvalidOid, 0, 0, InvalidXLogRecPtr, NULL);
#ifdef USE_ASSERT_CHECKING
	if (getenv("PG_UPGRADE_TEST_CHECKPOINT_BEFORE_START") != NULL)
		RequestCheckpoint(CHECKPOINT_FORCE | CHECKPOINT_FAST | CHECKPOINT_WAIT);
#endif
	PgUpgradeInsertRecord(XLOG_UPGRADE_START, &emitter->input,
						  SizeOfPgUpgradeStart);
	emit_test_point("start", InvalidOid, 0, 0, GetXLogInsertRecPtr(), NULL);
#ifdef USE_ASSERT_CHECKING
	if (getenv("PG_UPGRADE_TEST_CHECKPOINT_BEFORE_CONTROL") != NULL)
		RequestCheckpoint(CHECKPOINT_FORCE | CHECKPOINT_FAST | CHECKPOINT_WAIT);
#endif
	write_control_file_image();
}

pg_noreturn static void
relink_file_error(const char *path, const char *message)
{
	ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED),
			 errmsg("invalid upgrade relink file \"%s\": %s", path, message)));
}

static void
check_relink_file_header(const PgUpgradeRelinkFileHeader *header,
						 off_t file_size, const char *path)
{
	if (header->magic != PG_UPGRADE_RELINK_FILE_MAGIC ||
		header->version != PG_UPGRADE_RELINK_FILE_VERSION)
		relink_file_error(path, "incompatible format");
	if (header->new_section_offset < sizeof(*header) ||
		header->new_section_offset > (uint64) file_size)
		relink_file_error(path, "invalid new-section offset");
}

static void
check_relink_file_checksum(int fd, const PgUpgradeRelinkFileHeader *header,
						   off_t file_size, const char *path)
{
	PgUpgradeRelinkFileHeader copy = *header;
	char		buffer[BLCKSZ];
	off_t		offset = sizeof(copy);
	pg_crc32c	crc;

	copy.checksum = 0;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, &copy, sizeof(copy));
	while (offset < file_size)
	{
		size_t		length = Min((off_t) sizeof(buffer), file_size - offset);

		read_exact_file(fd, buffer, length, offset, path, true);
		COMP_CRC32C(crc, buffer, length);
		offset += length;
	}
	FIN_CRC32C(crc);
	if (!EQ_CRC32C(crc, header->checksum))
		relink_file_error(path, "checksum mismatch");
}

typedef struct RelinkInputCursor
{
	int			fd;
	const char *path;
	off_t		offset;
	off_t		end;
	PgUpgradeRelinkEntry *buffer;
	uint32		count;
	uint32		index;
	uint64		entries_remaining;
	bool		new_section;
	bool		have_previous_database;
	bool		database_open;
	bool		previous_inplace;
	Oid			previous_database;
	xl_pg_upgrade_key previous_relation;
	PgUpgradeRelinkDatabase database;
} RelinkInputCursor;

static RelinkInputCursor
make_relink_input_cursor(int fd, const char *path, off_t offset, off_t end,
						 PgUpgradeRelinkEntry *buffer, bool new_section)
{
	RelinkInputCursor cursor =
	{
		.fd = fd,
		.path = path,
		.offset = offset,
		.end = end,
		.buffer = buffer,
		.new_section = new_section
	};

	return cursor;
}

static void
validate_relink_database(RelinkInputCursor *cursor)
{
	const PgUpgradeRelinkDatabase *database = &cursor->database;
	uint8		allowed_flags = cursor->new_section ?
		PG_UPGRADE_RELINK_DEFAULT_INPLACE : PG_UPGRADE_RELINK_RECREATE_SCOPE;

	if (database->default_tablespace == InvalidOid ||
		(database->flags & ~allowed_flags) != 0 ||
		((database->flags & PG_UPGRADE_RELINK_RECREATE_SCOPE) != 0 &&
		 database->relation_count != 0) ||
		((database->flags & PG_UPGRADE_RELINK_DEFAULT_INPLACE) != 0 &&
		 (database->default_tablespace == DEFAULTTABLESPACE_OID ||
		  database->default_tablespace == GLOBALTABLESPACE_OID)) ||
		memcmp(database->reserved, "\0\0\0", sizeof(database->reserved)) != 0 ||
		(cursor->have_previous_database &&
		 database->database_oid <= cursor->previous_database))
		relink_file_error(cursor->path, "invalid database description");
	cursor->have_previous_database = true;
	cursor->previous_database = database->database_oid;
}

static void
validate_relink_entry(RelinkInputCursor *cursor,
					  const PgUpgradeRelinkEntry *entry)
{
	xl_pg_upgrade_key key = {entry->tablespace_oid, 0, entry->filenumber};
	uint8		allowed_flags = cursor->new_section ?
		PG_UPGRADE_RELINK_TRANSFERRED | UPGRADE_RELINK_INPLACE : 0;
	bool		inplace = (entry->flags & UPGRADE_RELINK_INPLACE) != 0;

	if (entry->reserved != 0 || entry->tablespace_oid == InvalidOid ||
		entry->filenumber == InvalidRelFileNumber ||
		PgUpgradeCompareKeys(&cursor->previous_relation, &key) >= 0 ||
		(entry->flags & ~allowed_flags) != 0 ||
		(cursor->new_section ?
		 (entry->persistence != 'p' && entry->persistence != 'u') ||
		 (entry->tablespace_oid == cursor->database.default_tablespace &&
		  inplace) ||
		 ((entry->tablespace_oid == DEFAULTTABLESPACE_OID ||
		   entry->tablespace_oid == GLOBALTABLESPACE_OID) && inplace) ||
		 (entry->tablespace_oid == cursor->previous_relation.tablespace_oid &&
		  inplace != cursor->previous_inplace) :
		 entry->persistence != 0))
		relink_file_error(cursor->path, "invalid relation entry");
	cursor->previous_relation = key;
	cursor->previous_inplace = inplace;
}

static bool
relink_input_next_database(RelinkInputCursor *cursor)
{
	uint64		entry_bytes;

	if (cursor->database_open)
		relink_file_error(cursor->path,
						  "advanced before the current database ended");
	if (cursor->offset == cursor->end)
		return false;
	if (cursor->offset > cursor->end ||
		cursor->end - cursor->offset < sizeof(cursor->database))
		relink_file_error(cursor->path, "database description is truncated");
	read_exact_file(cursor->fd, &cursor->database,
					sizeof(cursor->database), cursor->offset, cursor->path, true);
	cursor->offset += sizeof(cursor->database);
	validate_relink_database(cursor);
	entry_bytes = (uint64) cursor->database.relation_count *
		sizeof(PgUpgradeRelinkEntry);
	if (entry_bytes > (uint64) (cursor->end - cursor->offset))
		relink_file_error(cursor->path, "database entries exceed their section");
	cursor->entries_remaining = cursor->database.relation_count;
	cursor->count = cursor->index = 0;
	cursor->previous_inplace = false;
	MemSet(&cursor->previous_relation, 0, sizeof(cursor->previous_relation));
	cursor->database_open = true;
	return true;
}

static bool
relink_input_next_entry(RelinkInputCursor *cursor,
						PgUpgradeRelinkEntry *entry)
{
	if (!cursor->database_open)
		relink_file_error(cursor->path, "entry has no active database");
	if (cursor->entries_remaining == 0)
		return false;
	if (cursor->index == cursor->count)
	{
		cursor->count = Min(cursor->entries_remaining,
							PG_UPGRADE_RELINK_INPUT_BUFFER_ENTRIES);
		read_exact_file(cursor->fd, cursor->buffer,
						cursor->count * sizeof(PgUpgradeRelinkEntry),
						cursor->offset, cursor->path, true);
		cursor->offset += cursor->count * sizeof(PgUpgradeRelinkEntry);
		cursor->index = 0;
	}
	*entry = cursor->buffer[cursor->index++];
	validate_relink_entry(cursor, entry);
	cursor->entries_remaining--;
	return true;
}

static void
finish_relink_database(RelinkInputCursor *cursor)
{
	Assert(cursor->entries_remaining == 0);
	cursor->database_open = false;
}

static void
reserve_derived_operations(size_t required)
{
	size_t		capacity;

	if (required > PG_UINT32_MAX)
		emit_error("too many storage operations in one database");
	if (required <= emitter->operation_capacity)
		return;
	capacity = emitter->operation_capacity == 0 ? 128 :
		add_size(emitter->operation_capacity, emitter->operation_capacity);
	if (capacity < required || capacity > PG_UINT32_MAX)
		capacity = required;
	if (emitter->operations == NULL)
		emitter->operations = MemoryContextAllocExtended(
													 emitter->database_context,
													 mul_size(capacity, sizeof(EmitOperation)), MCXT_ALLOC_HUGE);
	else
		emitter->operations = repalloc_array_extended(emitter->operations,
													EmitOperation, capacity, MCXT_ALLOC_HUGE);
	emitter->operation_capacity = capacity;
}

static void
append_derived_operation(const EmitOperation *operation)
{
	reserve_derived_operations(add_size(emitter->noperations, 1));
	emitter->operations[emitter->noperations++] = *operation;
}

static int
compare_relink_entries(const PgUpgradeRelinkEntry *left,
					   const PgUpgradeRelinkEntry *right)
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

static xl_pg_upgrade_key
relink_key(const PgUpgradeRelinkEntry *entry, Oid database_oid)
{
	return (xl_pg_upgrade_key)
	{
		entry->tablespace_oid, database_oid, entry->filenumber
	};
}

static EmitDirectory *
find_or_add_directory(Oid tablespace)
{
	size_t		low = 0;
	size_t		high = emitter->ndirectories;

	while (low < high)
	{
		size_t		mid = low + (high - low) / 2;
		Oid			mid_tablespace =
			emitter->directories[mid].entry.key.tablespace_oid;

		if (mid_tablespace < tablespace)
			low = mid + 1;
		else
			high = mid;
	}
	if (low < emitter->ndirectories &&
		emitter->directories[low].entry.key.tablespace_oid == tablespace)
		return &emitter->directories[low];
	if (emitter->ndirectories == emitter->directory_capacity)
	{
		size_t		capacity = emitter->directory_capacity == 0 ? 16 :
			add_size(emitter->directory_capacity,
					 emitter->directory_capacity);

		if (emitter->directories == NULL)
			emitter->directories = MemoryContextAllocExtended(
														 emitter->database_context,
														 mul_size(capacity, sizeof(EmitDirectory)), MCXT_ALLOC_HUGE);
		else
			emitter->directories = repalloc_array_extended(emitter->directories,
														EmitDirectory, capacity, MCXT_ALLOC_HUGE);
		emitter->directory_capacity = capacity;
	}
	memmove(&emitter->directories[low + 1], &emitter->directories[low],
			(emitter->ndirectories - low) * sizeof(EmitDirectory));
	MemSet(&emitter->directories[low], 0, sizeof(EmitDirectory));
	emitter->directories[low].entry.key.tablespace_oid = tablespace;
	emitter->ndirectories++;
	return &emitter->directories[low];
}

static void
note_directory(Oid tablespace, bool old_present, bool new_present,
			   bool new_inplace)
{
	EmitDirectory *directory = find_or_add_directory(tablespace);

	directory->old_present |= old_present;
	directory->new_present |= new_present;
	if (new_inplace)
		directory->entry.flags = UPGRADE_RELINK_INPLACE;
}

static void
derive_relation_operations(RelinkInputCursor *old,
						   RelinkInputCursor *new_input)
{
	PgUpgradeRelinkEntry old_entry;
	PgUpgradeRelinkEntry new_entry;
	bool		have_old = false;
	bool		have_new = false;

	for (;;)
	{
		EmitOperation operation = {0};
		int			comparison;

		if (!have_old && old != NULL)
			have_old = relink_input_next_entry(old, &old_entry);
		if (!have_new && new_input != NULL)
			have_new = relink_input_next_entry(new_input, &new_entry);
		if (!have_old && !have_new)
			break;
		comparison = !have_old ? 1 : !have_new ? -1 :
			compare_relink_entries(&old_entry, &new_entry);
		if (comparison <= 0)
			note_directory(old_entry.tablespace_oid, true, false, false);
		if (comparison >= 0)
			note_directory(new_entry.tablespace_oid, false, true,
						   (new_entry.flags & UPGRADE_RELINK_INPLACE) != 0);
		if (comparison < 0)
		{
			operation.entry.key = relink_key(&old_entry,
											 emitter->old_database_oid);
			operation.entry.operation = UPGRADE_RELINK_DELETE;
			have_old = false;
		}
		else
		{
			operation.entry.key = relink_key(&new_entry,
										 emitter->new_database_oid);
			operation.entry.operation = comparison > 0 ?
				UPGRADE_RELINK_CREATE :
				(new_entry.flags & PG_UPGRADE_RELINK_TRANSFERRED) != 0 &&
				reference_transfer_mode() ? UPGRADE_RELINK_INHERIT :
				UPGRADE_RELINK_RECREATE;
			operation.persistence = new_entry.persistence;
			have_new = false;
			if (comparison == 0)
				have_old = false;
		}
		if (operation.persistence == 'u' &&
			operation.entry.operation == UPGRADE_RELINK_INHERIT)
			operation.entry.operation = UPGRADE_RELINK_RECREATE;
		append_derived_operation(&operation);
	}
}

static void
finish_directory_operations(bool recreate)
{
	if (emitter->ndirectories > PG_UINT32_MAX - emitter->noperations)
		emit_error("too many storage operations in one database");
	for (size_t i = 0; i < emitter->ndirectories; i++)
	{
		EmitDirectory *directory = &emitter->directories[i];

		Assert(directory->old_present || directory->new_present);
		directory->entry.key.database_oid = directory->new_present ?
			emitter->new_database_oid : emitter->old_database_oid;
		directory->entry.operation = !directory->old_present ?
			UPGRADE_RELINK_CREATE : !directory->new_present ?
			UPGRADE_RELINK_DELETE : recreate ?
			UPGRADE_RELINK_RECREATE : UPGRADE_RELINK_INHERIT;
	}
}

static void
derive_database(RelinkInputCursor *old, RelinkInputCursor *new_input)
{
	const PgUpgradeRelinkDatabase *old_database = old ? &old->database : NULL;
	const PgUpgradeRelinkDatabase *new_database =
		new_input ? &new_input->database : NULL;
	bool		recreate = old_database != NULL && new_database != NULL &&
		(old_database->flags & PG_UPGRADE_RELINK_RECREATE_SCOPE) != 0;
	MemoryContext previous;

	Assert(old_database == NULL || new_database == NULL ||
		   old_database->database_oid == new_database->database_oid);

	emitter->database_context = AllocSetContextCreate(emitter->transaction_context,
													  "WAL upgrade database",
													  ALLOCSET_DEFAULT_SIZES);
	previous = MemoryContextSwitchTo(emitter->database_context);
	emitter->old_database_oid = InvalidOid;
	emitter->new_database_oid = InvalidOid;
	emitter->new_default_tablespace = InvalidOid;
	emitter->have_new_database = new_database != NULL;
	emitter->ndirectories = emitter->directory_capacity = 0;
	emitter->directories = NULL;
	emitter->noperations = 0;
	emitter->operation_capacity = 0;
	emitter->operations = NULL;
	if (old_database != NULL)
		emitter->old_database_oid = old_database->database_oid;
	if (new_database != NULL)
	{
		emitter->new_database_oid = new_database->database_oid;
		emitter->new_default_tablespace =
			new_database->default_tablespace;
	}
	derive_relation_operations(old, new_input);
	if (old_database != NULL)
		note_directory(old_database->default_tablespace, true, false, false);
	if (new_database != NULL)
		note_directory(new_database->default_tablespace, false, true,
					   (new_database->flags &
						PG_UPGRADE_RELINK_DEFAULT_INPLACE) != 0);
	if (old != NULL)
		finish_relink_database(old);
	if (new_input != NULL)
		finish_relink_database(new_input);
	finish_directory_operations(recreate);
	capture_database();
	MemoryContextSwitchTo(previous);
	MemoryContextDelete(emitter->database_context);
	emitter->database_context = NULL;
	emitter->directories = NULL;
	emitter->ndirectories = emitter->directory_capacity = 0;
	emitter->operations = NULL;
	emitter->noperations = emitter->operation_capacity = 0;
}

static void
merge_relink_inputs(RelinkInputCursor *old, RelinkInputCursor *new_input)
{
	bool		have_old = relink_input_next_database(old);
	bool		have_new = relink_input_next_database(new_input);

	while (have_old || have_new)
	{
		int			comparison = !have_old ? 1 : !have_new ? -1 :
			(old->database.database_oid > new_input->database.database_oid) -
			(old->database.database_oid < new_input->database.database_oid);

		derive_database(comparison <= 0 ? old : NULL,
						comparison >= 0 ? new_input : NULL);
		if (comparison <= 0)
			have_old = relink_input_next_database(old);
		if (comparison >= 0)
			have_new = relink_input_next_database(new_input);
	}
}

void
PgUpgradeEmitWalFile(void)
{
	char		path[MAXPGPATH];
	struct stat path_stat;
	struct stat fd_stat;
	PgUpgradeRelinkFileHeader header;
	PgUpgradeRelinkEntry *old_buffer;
	PgUpgradeRelinkEntry *new_buffer;
	MemoryContext caller = CurrentMemoryContext;
	off_t		contents_end;
	off_t		new_start;
	int			fd;
	int			flags = O_RDONLY | PG_BINARY;

	PG_TRY();
	{
		if (!IsBinaryUpgrade || !superuser())
			ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
							errmsg("upgrade WAL emission requires a binary-upgrade superuser")));
		if (!IsTransactionBlock() || IsSubTransaction() || IsInParallelMode() ||
			IsParallelWorker() || RecoveryInProgress() || XactReadOnly)
			emit_error("upgrade WAL emission requires an explicit, writable "
					   "top-level transaction on the primary");
		if (emitter != NULL)
			emit_error("upgrade WAL emission is already active in this "
					   "upgrade transaction");

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
			relink_file_error(path, "path is not one regular file");
#ifndef WIN32
		if (fd_stat.st_uid != geteuid() || fd_stat.st_nlink != 1 ||
			(fd_stat.st_mode & (S_IRWXU | S_IRWXG | S_IRWXO)) !=
			(S_IRUSR | S_IWUSR))
			relink_file_error(path, "unsafe ownership or permissions");
#endif
		if (fd_stat.st_size < (off_t) sizeof(header))
			relink_file_error(path, "file is too short");
		contents_end = fd_stat.st_size;
		read_exact_file(fd, &header, sizeof(header), 0, path, true);
		check_relink_file_header(&header, contents_end, path);
		check_relink_file_checksum(fd, &header, contents_end, path);
		if (header.new_system_identifier != GetSystemIdentifier())
			relink_file_error(path, "new cluster system identifier does not match");
		if (!PgUpgradeStartMatchesServer(&header.start))
			relink_file_error(path, "START does not match this server");
		new_start = (off_t) header.new_section_offset;
		old_buffer = palloc_array(PgUpgradeRelinkEntry,
								  PG_UPGRADE_RELINK_INPUT_BUFFER_ENTRIES);
		new_buffer = palloc_array(PgUpgradeRelinkEntry,
								  PG_UPGRADE_RELINK_INPUT_BUFFER_ENTRIES);

		create_emitter();
		start_transaction(&header.start);

		/*
		 * Validate and merge the old and new relation inventories one database
		 * at a time.
		 */
		{
			RelinkInputCursor old = make_relink_input_cursor(fd, path,
													 sizeof(header), new_start,
													 old_buffer, false);
			RelinkInputCursor new_input = make_relink_input_cursor(fd, path,
														   new_start, contents_end,
														   new_buffer, true);

			merge_relink_inputs(&old, &new_input);
			if (old.offset != old.end || new_input.offset != new_input.end ||
				old.database_open || new_input.database_open)
				relink_file_error(path, "input cursors did not consume the file");
		}
		MemoryContextSwitchTo(emitter->transaction_context);
		complete_transaction();
		MemoryContextSwitchTo(caller);

		if (CloseTransientFile(fd) != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not close upgrade relink file \"%s\": %m", path)));
		pfree(old_buffer);
		pfree(new_buffer);
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
