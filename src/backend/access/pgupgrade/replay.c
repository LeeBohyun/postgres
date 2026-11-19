/*-------------------------------------------------------------------------
 *
 * replay.c
 *    Apply a WAL-logged pg_upgrade transaction.
 *
 * Replay upgrade records during standby and archive recovery. START checks
 * cluster compatibility and sets upgrade_started in pg_control. RELINK
 * prepares directories and relation files. Standard relation WAL rebuilds
 * recreated relations, and RAWFILE restores auxiliary files.
 *
 * Receiving COMPLETE does not finish replay. The matching upgrade transaction
 * commit records completion. Replay remains in progress until the following
 * shutdown checkpoint is persisted as a restartpoint and pg_control is marked
 * upgrade-finalized.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * src/backend/access/pgupgrade/replay.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/pgupgrade_config.h"
#include "access/pgupgrade_control.h"
#include "access/pgupgrade_wal.h"
#include "access/timeline.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogrecovery.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "catalog/pg_control.h"
#include "catalog/pg_tablespace_d.h"
#include "common/file_perm.h"
#include "common/file_utils.h"
#include "common/relpath.h"
#include "miscadmin.h"
#include "postmaster/bgwriter.h"
#include "storage/bufmgr.h"
#include "storage/copydir.h"
#include "storage/fd.h"
#include "storage/md.h"
#include "storage/procsignal.h"
#include "storage/reinit.h"
#include "utils/elog.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"


static bool replay_in_progress;

static void replay_complete(XLogReaderState *record);

bool
PgUpgradeReplayIsInProgress(void)
{
	return replay_in_progress;
}

void
PgUpgradeCheckRecoveryFinish(void)
{
	if (replay_in_progress)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot finish recovery before pg_upgrade is finalized"),
				 errhint("Discard this new-version attempt and retry from the retained old cluster.")));
}

void
PgUpgradeCheckRecoveryTarget(void)
{
	if (replay_in_progress)
		ereport(FATAL,
				(errmsg("requested recovery stop point is inside a pg_upgrade transaction"),
				 errdetail("The upgrade has begun but not completed at this point, so the cluster would be only partially upgraded."),
				 errhint("Choose a recovery target at or after the end of the pg_upgrade transaction, or remove the recovery target to replay the whole upgrade.")));
}

void
PgUpgradeSelectReplay(void)
{
	Assert(!replay_in_progress);
	replay_in_progress = true;
}

typedef enum PgUpgradeCheckpointState
{
	PG_UPGRADE_CHECKPOINT_NONE,
	PG_UPGRADE_CHECKPOINT_PENDING,
	PG_UPGRADE_CHECKPOINT_REPLAYED
} PgUpgradeCheckpointState;

/*
 * Finalize the committed upgrade transaction after its shutdown checkpoint
 * becomes a durable restartpoint.
 */
static PgUpgradeCheckpointState upgrade_complete_checkpoint_state;
static XLogRecPtr upgrade_complete_end_lsn = InvalidXLogRecPtr;
static XLogRecPtr upgrade_complete_checkpoint_lsn = InvalidXLogRecPtr;
static TimeLineID upgrade_complete_checkpoint_tli = 0;

typedef enum UpgradeRecordState
{
	UPGRADE_RECORD_NONE,
	UPGRADE_RECORD_STARTED,
	UPGRADE_RECORD_COMPLETE
} UpgradeRecordState;

static xl_pg_upgrade_start upgrade_start;
static UpgradeRecordState upgrade_record_state;
static HTAB *storage_roots;
static MemoryContext replay_context;
static bool old_datadir_inplace;
static bool old_datadir_at_boundary;
static const char *old_datadir;
static char old_tablespace_version_directory[MAXFNAMELEN];
static TransactionId upgrade_xid;

typedef enum ReplayRelinkState
{
	RELINK_INACTIVE,
	RELINK_STARTED,
	RELINK_DIRECTORIES,
	RELINK_RELATIONS
} ReplayRelinkState;

static ReplayRelinkState relink_state;

static void
sync_containing_directory(const char *path)
{
	char	   *directory = pstrdup(path);

	get_parent_directory(directory);
	fsync_fname(directory[0] != '\0' ? directory : ".", true);
	pfree(directory);
}

static void
make_directory(const char *path)
{
	struct stat st;
	char	   *writable = pstrdup(path);

	if (pg_mkdir_p(writable, pg_dir_create_mode) != 0 && errno != EEXIST)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not create upgrade directory \"%s\": %m", path)));
	if (lstat(path, &st) != 0 || !S_ISDIR(st.st_mode))
		elog(PANIC, "upgrade destination is not a directory: %s", path);
	fsync_fname(path, true);
	sync_containing_directory(path);
	pfree(writable);
}

static void
write_upgrade_file(const char *path, const void *data, Size length,
				   uint64 offset, bool sync_parent)
{
	int			fd = OpenTransientFile(path, O_WRONLY | O_CREAT | PG_BINARY |
									   (offset == 0 ? O_TRUNC : 0));

	if (fd < 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not open \"%s\": %m", path)));
	if (pg_pwrite(fd, data, length, (pgoff_t) offset) != (ssize_t) length)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not write \"%s\": %m", path)));
	if (pg_fsync(fd) != 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not fsync \"%s\": %m", path)));
	CloseTransientFile(fd);
	if (sync_parent && offset == 0)
		sync_containing_directory(path);
}

/*
 * Keep an existing pg_tblspc directory or symbolic link. For a missing entry,
 * create a directory if the in-place flag is set, or recreate the symbolic
 * link from the retained old cluster otherwise.
 */
static void
prepare_tablespace(const xl_pg_upgrade_relink_entry *entry)
{
	Oid			oid = entry->key.tablespace_oid;
	char		path[MAXPGPATH];
	struct stat st;

	if (oid == 0 || oid == DEFAULTTABLESPACE_OID ||
		oid == GLOBALTABLESPACE_OID)
		return;
	snprintf(path, sizeof(path), "pg_tblspc/%u", oid);
	if (lstat(path, &st) != 0)
	{
		char		source[MAXPGPATH];
		char		target[MAXPGPATH];
		ssize_t		length;

		if (errno != ENOENT)
			ereport(PANIC, (errcode_for_file_access(),
							errmsg("could not stat \"%s\": %m", path)));
		if (entry->flags & UPGRADE_RELINK_INPLACE)
			make_directory(path);
		else
		{
			if (old_datadir == NULL)
				elog(PANIC, "upgrade tablespace %u requires a local "
					 "destination mapping", oid);
			snprintf(source, sizeof(source), "%s/%s", old_datadir, path);
			length = readlink(source, target, sizeof(target) - 1);
			if (length <= 0 || length >= sizeof(target) - 1)
				elog(PANIC, "could not read retained tablespace link %s",
					 source);
			target[length] = '\0';
			if (!is_absolute_path(target))
				elog(PANIC, "retained tablespace link must have an absolute "
					 "target: %s", source);
			if (symlink(target, path) != 0)
				ereport(PANIC, (errcode_for_file_access(),
								errmsg("could not create \"%s\": %m", path)));
			fsync_fname("pg_tblspc", true);
		}
	}
	else if (!S_ISDIR(st.st_mode) && !S_ISLNK(st.st_mode))
		elog(PANIC, "invalid upgrade tablespace mapping: %s", path);
}

typedef struct ReplayFileIdentity
{
	uint64		device;
	uint64		file;
} ReplayFileIdentity;

static ReplayFileIdentity
directory_identity(const char *path, const struct stat *st)
{
	ReplayFileIdentity identity;

#ifdef WIN32
	BY_HANDLE_FILE_INFORMATION info;
	HANDLE		handle = pgwin32_open_handle(path, O_RDONLY, true);
	DWORD		error;

	if (handle == INVALID_HANDLE_VALUE)
		error = GetLastError();
	else if (!GetFileInformationByHandle(handle, &info))
	{
		error = GetLastError();
		CloseHandle(handle);
	}
	else if (!CloseHandle(handle))
		error = GetLastError();
	else
	{
		identity.device = info.dwVolumeSerialNumber;
		identity.file = ((uint64) info.nFileIndexHigh << 32) |
			info.nFileIndexLow;
		return identity;
	}

	_dosmaperr(error);
	ereport(PANIC, (errcode_for_file_access(),
					errmsg("could not read directory identity of \"%s\": %m",
						   path)));
#else
	identity.device = st->st_dev;
	identity.file = st->st_ino;
	return identity;
#endif
	pg_unreachable();
}

typedef struct ReplayStorageRoot
{
	ReplayFileIdentity identity;
	Oid			tablespace;
	bool		old_cluster;
} ReplayStorageRoot;

static void
storage_root_path(Oid tablespace, bool old_cluster, char *path)
{
	const char *datadir = old_cluster ? old_datadir : DataDir;

	if (tablespace == DEFAULTTABLESPACE_OID)
		snprintf(path, MAXPGPATH, "%s/base", datadir);
	else if (tablespace == GLOBALTABLESPACE_OID)
		snprintf(path, MAXPGPATH, "%s/global", datadir);
	else
		snprintf(path, MAXPGPATH, "%s/pg_tblspc/%u/%s", datadir, tablespace,
				 old_cluster ? old_tablespace_version_directory :
				 TABLESPACE_VERSION_DIRECTORY);
}

/*
 * Reject a storage directory used by different tablespaces. Old and new
 * storage directories must also be distinct, except when archive recovery
 * reuses DataDir for retained old storage.
 */
static void
check_storage_root(Oid tablespace, bool old_cluster)
{
	char		path[MAXPGPATH];
	struct stat st;
	ReplayFileIdentity identity;
	ReplayStorageRoot *root;
	bool		found;

	storage_root_path(tablespace, old_cluster, path);
	if (!old_cluster)
		make_directory(path);
	if (lstat(path, &st) != 0 || !S_ISDIR(st.st_mode))
		elog(PANIC, "upgrade storage root is not a local directory: %s",
			 path);
	identity = directory_identity(path, &st);
	root = hash_search(storage_roots, &identity, HASH_ENTER, &found);
	if (found && (root->tablespace != tablespace ||
				  (!old_datadir_inplace &&
				   root->old_cluster != old_cluster)))
		elog(PANIC, "upgrade destination aliases retained storage for "
			 "tablespace %u", tablespace);
	root->tablespace = tablespace;
	root->old_cluster = old_cluster;
}

/*
 * Check START against the new server and select retained old storage.
 * When set, pg_upgrade_standby_old_datadir must name a separate old data
 * directory at the final shutdown checkpoint. Otherwise, archive recovery
 * can use DataDir after checking its saved old control data against START.
 */
static void
bind_transaction(void)
{
	const xl_pg_upgrade_start *transaction = &upgrade_start;
	OldUpgradeControlData old;

	if (!PgUpgradeStartMatchesServer(transaction))
		elog(PANIC, "upgrade physical format does not match this server");
	if (pg_upgrade_standby_old_datadir != NULL &&
		pg_upgrade_standby_old_datadir[0] != '\0')
	{
		struct stat old_dir;
		struct stat new_dir;
		ReplayFileIdentity old_identity;
		ReplayFileIdentity new_identity;

		ReadOldUpgradeControlData(pg_upgrade_standby_old_datadir, &old);
		if (stat(pg_upgrade_standby_old_datadir, &old_dir) != 0 ||
			stat(DataDir, &new_dir) != 0)
			elog(PANIC, "upgrade requires a separate retained old data "
				 "directory");
		old_identity = directory_identity(pg_upgrade_standby_old_datadir,
									  &old_dir);
		new_identity = directory_identity(DataDir, &new_dir);
		if (old_identity.device == new_identity.device &&
			old_identity.file == new_identity.file)
			elog(PANIC, "upgrade requires a separate retained old data "
				 "directory");
		old_datadir = pg_upgrade_standby_old_datadir;
	}
	else if (GetUpgradeRecoveryMode() == UPGRADE_RECOVERY_ARCHIVE &&
			 PgUpgradeGetArchiveOldControlData(&old) &&
			 old.system_identifier == transaction->old_system_identifier &&
			 old.major_version == transaction->marker.old_major &&
			 old.control_version == transaction->old_control_version &&
			 old.catalog_version == transaction->old_catalog_version)
	{
		old_datadir = DataDir;
		old_datadir_inplace = true;
	}
	if (old_datadir != NULL)
	{
		List	   *history = readTimeLineHistory(transaction->old_tli);

		if (old.system_identifier != transaction->old_system_identifier ||
			old.major_version != transaction->marker.old_major ||
			old.control_version != transaction->old_control_version ||
			old.catalog_version != transaction->old_catalog_version ||
			old.wal_segment_size != transaction->wal_segment_size ||
			old.block_size != transaction->block_size ||
			old.blocks_per_segment != transaction->relseg_blocks ||
			old.wal_block_size != transaction->wal_block_size ||
			old.checkpoint_lsn >= transaction->boundary_lsn ||
			old.checkpoint_end_lsn > transaction->boundary_lsn ||
			tliOfPointInHistory(old.checkpoint_lsn, history) !=
			old.checkpoint_tli ||
			(old.checkpoint_end_lsn != InvalidXLogRecPtr &&
			 tliOfPointInHistory(old.checkpoint_end_lsn - 1, history) !=
			 old.checkpoint_end_tli))
			elog(PANIC, "old cluster identity or recovery state does not "
				 "match the upgrade boundary");
		list_free_deep(history);

		/*
		 * Segment transfer requires a shutdown checkpoint ending at
		 * boundary_lsn on old_tli.
		 */
		old_datadir_at_boundary = old.checkpoint_lsn == old.checkpoint_redo &&
			old.checkpoint_end_lsn == transaction->boundary_lsn &&
			old.checkpoint_tli == transaction->old_tli &&
			old.checkpoint_end_tli == transaction->old_tli;
		if (!old_datadir_inplace && !old_datadir_at_boundary)
			elog(PANIC, "retained old data directory does not match the "
				 "boundary");
		if (old.major_version >= 100000)
			snprintf(old_tablespace_version_directory,
					 sizeof(old_tablespace_version_directory),
					 "PG_%u_%u", old.major_version / 10000,
					 old.catalog_version);
		else
			snprintf(old_tablespace_version_directory,
					 sizeof(old_tablespace_version_directory),
					 "PG_%u.%u_%u", old.major_version / 10000,
					 old.major_version / 100 % 100, old.catalog_version);
	}
	{
		HASHCTL		control = {0};

		control.keysize = sizeof(ReplayFileIdentity);
		control.entrysize = sizeof(ReplayStorageRoot);
		control.hcxt = replay_context;
		storage_roots = hash_create("upgrade storage roots", 32, &control,
									HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	make_directory("pg_tblspc");
}

static void
inherited_filename(const xl_pg_upgrade_key *key, uint32 segment, char *path)
{
	RelPathStr	relative = GetRelationPath(key->database_oid, key->tablespace_oid,
										   key->filenumber, INVALID_PROC_NUMBER,
										   MAIN_FORKNUM);
	char		suffix[32] = "";
	char		root[MAXPGPATH];
	const char *filename = strrchr(relative.str, '/');

	if (old_datadir == NULL || !old_datadir_at_boundary)
		elog(PANIC, "segment transfer requires the old data directory at "
			 "its final shutdown checkpoint");
	if (segment != 0)
		snprintf(suffix, sizeof(suffix), ".%u", segment);
	storage_root_path(key->tablespace_oid, true, root);
	Assert(filename != NULL);
	if (key->tablespace_oid == GLOBALTABLESPACE_OID)
		snprintf(path, MAXPGPATH, "%s/%s%s", root, filename + 1, suffix);
	else
		snprintf(path, MAXPGPATH, "%s/%u/%s%s", root, key->database_oid,
				 filename + 1, suffix);
}

/* Remove each declared relation or database directory from the new cluster. */
static void
ReplayUpgradeDeleteRoot(const xl_pg_upgrade_key *entries, int count)
{
	const xl_pg_upgrade_key *entry = entries;
	char	   *path;
	struct stat st;

	path = GetDatabasePath(entry->database_oid, entry->tablespace_oid);
	if (lstat(path, &st) != 0)
	{
		if (errno != ENOENT)
			ereport(PANIC, (errcode_for_file_access(),
							errmsg("could not stat upgrade DELETE target \"%s\": %m", path)));
		pfree(path);
		return;
	}
	if (!S_ISDIR(st.st_mode))
		elog(PANIC, "upgrade DELETE target \"%s\" is not a directory", path);
	if (entry->filenumber == UPGRADE_RELINK_DIRECTORY_FILENUMBER)
	{
		DropDatabaseBuffers(entry->database_oid);
		ForgetDatabaseSyncRequests(entry->database_oid);
		XLogDropDatabase(entry->database_oid);
		WaitForProcSignalBarrier(EmitProcSignalBarrier(PROCSIGNAL_BARRIER_SMGRRELEASE));
		if (!rmtree(path, true))
			elog(PANIC, "could not remove upgrade DELETE target \"%s\"", path);
		get_parent_directory(path);
	}
	else
	{
		RelFileLocator *locators = palloc_array(RelFileLocator, count);
		DIR		   *dir;
		struct dirent *de;

		for (int i = 0; i < count; i++)
			locators[i] = (RelFileLocator)
		{
			entries[i].tablespace_oid,
				entries[i].database_oid, entries[i].filenumber
		};

		/*
		 * Unlink every declared new-cluster relation file before
		 * DropRelationFiles().
		 */
		dir = AllocateDir(path);
		while ((de = ReadDirExtended(dir, path, PANIC)) != NULL)
		{
			xl_pg_upgrade_key key = *entry;
			ForkNumber	fork;
			unsigned	segno;
			char	   *filename;

			if (!parse_filename_for_nontemp_relation(de->d_name,
													 &key.filenumber, &fork, &segno) ||
				bsearch(&key, entries, count, sizeof(*entries),
						PgUpgradeCompareKeys) == NULL)
				continue;
			filename = psprintf("%s/%s", path, de->d_name);
			if (unlink(filename) != 0 && errno != ENOENT)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not remove upgrade DELETE target \"%s\": %m",
								filename)));
			pfree(filename);
		}
		FreeDir(dir);
		DropRelationFiles(locators, count, true);
		pfree(locators);
	}
	fsync_fname(path, true);
	pfree(path);
}

/*
 * Place one inherited relation file with the selected transfer mode and fsync
 * the resulting file.
 */
static void
PlaceInheritedFile(const char *oldfile, const char *newfile, uint8 mode)
{
	switch (mode)
	{
		case UPGRADE_RELINK_MODE_LINK:
			if (link(oldfile, newfile) != 0)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not link \"%s\" to \"%s\": %m",
								oldfile, newfile)));
			break;

		case UPGRADE_RELINK_MODE_SWAP:
			if (rename(oldfile, newfile) != 0)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not move \"%s\" to \"%s\": %m",
								oldfile, newfile)));
			break;

		case UPGRADE_RELINK_MODE_CLONE:
		case UPGRADE_RELINK_MODE_COPY_FILE_RANGE:
			{
				int			save_errno;
				bool		cloning = mode == UPGRADE_RELINK_MODE_CLONE;
				const char *action = cloning ? "clone" : "copy_file_range";
				PGReflinkResult result = cloning ?
					pg_clone_file(oldfile, newfile, &save_errno) :
					pg_copy_file_range_all(oldfile, newfile, &save_errno);

				if (result == PG_REFLINK_ERROR)
				{
					errno = save_errno;
					ereport(PANIC,
							(errcode_for_file_access(),
							 errmsg("could not %s \"%s\" to \"%s\": %m",
									action, oldfile, newfile),
							 cloning ?
							 errhint("The old data directory and the new skeleton "
									 "must be on the same reflink-capable filesystem.") : 0));
				}
				if (result == PG_REFLINK_UNSUPPORTED)
					ereport(PANIC,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("%s not supported on this platform",
									cloning ? "file cloning" : action),
							 errhint("The primary used %s; a standby on this "
									 "platform cannot reproduce it.",
									 cloning ? "--clone" : "--copy-file-range")));
				break;
			}

		case UPGRADE_RELINK_MODE_COPY:
		default:
			copy_file(oldfile, newfile);
			break;
	}

	fsync_fname(newfile, false);
}

static bool
relation_uses_retained_storage(const xl_pg_upgrade_relink_entry *entry)
{
	return entry->operation == UPGRADE_RELINK_INHERIT;
}

/*
 * Place contiguous main-fork segments from retained old storage at the final
 * shutdown checkpoint, using the selected transfer mode. Stop at the first
 * missing segment after segment zero. Leave files in place when the old and
 * new paths are identical.
 *
 * The caller must fsync a nonempty pending_directory before finishing the
 * RELINK record.
 */
static void
PlaceInheritedRelation(const xl_pg_upgrade_relink_entry *entry, uint8 mode,
					   char *pending_directory)
{
	RelPathStr	base = GetRelationPath(entry->key.database_oid,
									   entry->key.tablespace_oid,
									   entry->key.filenumber,
									   INVALID_PROC_NUMBER, MAIN_FORKNUM);

	for (uint32 segment = 0;; segment++)
	{
		char		oldfile[MAXPGPATH];
		char	   *newfile;

		inherited_filename(&entry->key, segment, oldfile);
		if (segment != 0)
		{
			struct stat st;

			if (lstat(oldfile, &st) != 0)
			{
				if (errno == ENOENT)
					break;
				ereport(PANIC, (errcode_for_file_access(),
								errmsg("could not stat retained old file \"%s\": %m",
									   oldfile)));
			}
		}
		newfile = segment == 0 ? psprintf("%s/%s", DataDir, base.str) :
			psprintf("%s/%s.%u", DataDir, base.str, segment);
		if (strcmp(oldfile, newfile) != 0)
		{
			if (unlink(newfile) != 0 && errno != ENOENT)
				ereport(PANIC, (errcode_for_file_access(),
								errmsg("could not remove RELINK destination \"%s\": %m",
									   newfile)));
			PlaceInheritedFile(oldfile, newfile, mode);
			if (mode == UPGRADE_RELINK_MODE_SWAP)
			{
				sync_containing_directory(newfile);
				sync_containing_directory(oldfile);
			}
			else
			{
				get_parent_directory(newfile);
				if (strcmp(pending_directory, newfile) != 0)
				{
					if (pending_directory[0] != '\0')
						fsync_fname(pending_directory, true);
					strlcpy(pending_directory, newfile, MAXPGPATH);
				}
			}
		}
		pfree(newfile);
	}
}

/*
 * Validate RELINK field values and the flags allowed for each operation.
 * Require directory entries before relation entries across record batches.
 */
static void
validate_relink_entry(const xl_pg_upgrade_relink_entry *entry)
{
	bool		directory = PgUpgradeRelinkEntryIsDirectory(entry);

	if (entry->operation < UPGRADE_RELINK_INHERIT ||
		entry->operation > UPGRADE_RELINK_DELETE ||
		entry->key.tablespace_oid == InvalidOid || entry->reserved != 0)
		elog(PANIC, "unsupported RELINK file operation");

	if (directory)
	{
		if (relink_state == RELINK_RELATIONS ||
			(entry->flags & ~UPGRADE_RELINK_INPLACE) != 0 ||
			((entry->flags & UPGRADE_RELINK_INPLACE) != 0 &&
			 (entry->operation == UPGRADE_RELINK_DELETE ||
			  entry->key.tablespace_oid == DEFAULTTABLESPACE_OID ||
			  entry->key.tablespace_oid == GLOBALTABLESPACE_OID)))
			elog(PANIC, "invalid RELINK directory operation");
		relink_state = RELINK_DIRECTORIES;
	}
	else
	{
		if ((relink_state != RELINK_DIRECTORIES &&
			 relink_state != RELINK_RELATIONS) || entry->flags != 0)
			elog(PANIC, "invalid RELINK relation operation");
		relink_state = RELINK_RELATIONS;
	}
}

/*
 * Validate all entries in the RELINK record before applying any directory
 * or relation operations.
 */
static void
ReplayUpgradeRelink(XLogReaderState *record)
{
	Size		length = XLogRecGetDataLen(record);
	const char *data = XLogRecGetData(record);
	uint32		flags;
	int			count;
	xl_pg_upgrade_relink_entry *entries;
	xl_pg_upgrade_key *deletes;
	int			ndeletes = 0;
	int			transfer_mode;
	char		pending_directory[MAXPGPATH] = {0};

	if (upgrade_record_state != UPGRADE_RECORD_STARTED ||
		length < SizeOfPgUpgradeRelink ||
		(length - SizeOfPgUpgradeRelink) % SizeOfPgUpgradeRelinkEntry != 0)
		elog(PANIC, "invalid upgrade RELINK length");
	transfer_mode = pg_upgrade_standby_transfer_mode == PG_UPGRADE_XFER_MIRROR ?
		upgrade_start.transfer_mode : pg_upgrade_standby_transfer_mode;
	if (transfer_mode < UPGRADE_RELINK_MODE_CLONE ||
		transfer_mode > UPGRADE_RELINK_MODE_SWAP)
		elog(PANIC, "invalid RELINK transfer mode");
	memcpy(&flags, data, sizeof(flags));
	count = (length - SizeOfPgUpgradeRelink) / SizeOfPgUpgradeRelinkEntry;
	/*
	 * Require the begin flag on the first batch for a database or shared
	 * relations. Another begin flag is invalid until the end flag is seen.
	 */
	if (count > UPGRADE_RELINK_MAX_ENTRIES ||
		(flags & ~(UPGRADE_RELINK_BEGIN | UPGRADE_RELINK_END)) != 0 ||
		((flags & UPGRADE_RELINK_BEGIN) != 0) ==
		(relink_state != RELINK_INACTIVE))
		elog(PANIC, "invalid upgrade RELINK batch");
	if (flags & UPGRADE_RELINK_BEGIN)
		relink_state = RELINK_STARTED;
	entries = palloc_array(xl_pg_upgrade_relink_entry, count);
	memcpy(entries, data + SizeOfPgUpgradeRelink,
		   count * sizeof(xl_pg_upgrade_relink_entry));
	deletes = palloc_array(xl_pg_upgrade_key, count);
	for (int i = 0; i < count; i++)
		validate_relink_entry(&entries[i]);
	if ((flags & UPGRADE_RELINK_END) != 0 &&
		relink_state == RELINK_STARTED)
		elog(PANIC, "incomplete upgrade RELINK scope");
	if (flags & UPGRADE_RELINK_END)
		relink_state = RELINK_INACTIVE;

	/* Prepare destination tablespaces before removing stale directories. */
	for (int i = 0; i < count; i++)
	{
		const xl_pg_upgrade_relink_entry *entry = &entries[i];

		if (PgUpgradeRelinkEntryIsDirectory(entry) &&
			entry->operation != UPGRADE_RELINK_DELETE)
		{
			prepare_tablespace(entry);
			check_storage_root(entry->key.tablespace_oid, false);
			if (old_datadir != NULL &&
				entry->operation != UPGRADE_RELINK_CREATE)
				check_storage_root(entry->key.tablespace_oid, true);
		}

		if (entry->operation == UPGRADE_RELINK_DELETE ||
			entry->operation == UPGRADE_RELINK_CREATE ||
			entry->operation == UPGRADE_RELINK_RECREATE ||
			(!PgUpgradeRelinkEntryIsDirectory(entry) &&
			 relation_uses_retained_storage(entry) &&
			 !old_datadir_inplace))
			deletes[ndeletes++] = entry->key;
	}
	qsort(deletes, ndeletes, sizeof(*deletes), PgUpgradeCompareKeys);
	for (int first = 0; first < ndeletes;)
	{
		int			end = first + 1;

		while (end < ndeletes &&
			   deletes[end].tablespace_oid == deletes[first].tablespace_oid &&
			   deletes[end].database_oid == deletes[first].database_oid)
			end++;
		ReplayUpgradeDeleteRoot(deletes + first, end - first);
		first = end;
	}
	pfree(deletes);

	for (int i = 0; i < count; i++)
	{
		const xl_pg_upgrade_relink_entry *entry = &entries[i];
		char	   *newfile;

		if (PgUpgradeRelinkEntryIsDirectory(entry))
		{
			if (entry->operation != UPGRADE_RELINK_DELETE)
			{
				newfile = GetDatabasePath(entry->key.database_oid,
										  entry->key.tablespace_oid);
				make_directory(newfile);
				pfree(newfile);
			}
			continue;
		}
		if (relation_uses_retained_storage(entry))
			PlaceInheritedRelation(entry, transfer_mode, pending_directory);
	}
	if (pending_directory[0] != '\0')
		fsync_fname(pending_directory, true);
	pfree(entries);
}

/*
 * Accept COMPLETE only when replay reaches a commit record with the upgrade
 * transaction's XID.
 */
void
PgUpgradeReplayCommit(XLogReaderState *record)
{
	uint8		rmid = XLogRecGetRmid(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (upgrade_record_state == UPGRADE_RECORD_NONE)
		return;
	if (upgrade_record_state == UPGRADE_RECORD_COMPLETE &&
		rmid == RM_PG_UPGRADE_ID)
		elog(PANIC, "upgrade record follows COMPLETE");
	if (XLogRecGetXid(record) != upgrade_xid)
		return;
	if (rmid == RM_XACT_ID && (info & XLOG_XACT_OPMASK) == XLOG_XACT_ABORT)
		elog(PANIC, "upgrade transaction aborted");
	if (upgrade_record_state != UPGRADE_RECORD_COMPLETE)
		return;
	if (rmid != RM_XACT_ID || (info & XLOG_XACT_OPMASK) != XLOG_XACT_COMMIT ||
		XLogRecGetDataLen(record) < MinSizeOfXactCommit)
		elog(PANIC, "upgrade COMPLETE has no matching upgrade transaction commit");
	replay_complete(record);
	MemoryContextDelete(replay_context);
	upgrade_record_state = UPGRADE_RECORD_NONE;
	old_datadir_inplace = old_datadir_at_boundary = false;
	relink_state = RELINK_INACTIVE;
	storage_roots = NULL;
	replay_context = NULL;
	old_datadir = NULL;
	upgrade_xid = InvalidTransactionId;
}

void
pg_upgrade_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	xl_pg_upgrade_marker marker;
	const char *marker_error = NULL;

	/* Upgrade records must have the same transaction ID as START. */
	if (upgrade_record_state != UPGRADE_RECORD_NONE &&
		XLogRecGetXid(record) != upgrade_xid)
		elog(PANIC, "upgrade record has no matching START");
	/* Reject other record types until the last RELINK batch is replayed. */
	if (relink_state != RELINK_INACTIVE && info != XLOG_UPGRADE_RELINK)
		elog(PANIC, "record interrupts an upgrade RELINK scope");

	if (info == XLOG_UPGRADE_START)
	{
		xl_pg_upgrade_marker *xlrec = &marker;
		int			len;

		if (!PgUpgradeReadMarker(info, XLogRecGetData(record),
								 XLogRecGetDataLen(record), &marker,
								 &marker_error))
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("invalid pg_upgrade start record: %s", marker_error)));

		len = strnlen(xlrec->pg_version, sizeof(xlrec->pg_version));

		if (!replay_in_progress)
		{
			if (StandbyMode)
				ereport(FATAL,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("reached pg_upgrade boundary on standby; halting "
								"to apply the upgrade"),
						 errdetail("A --wal-upgrade was performed on the primary; "
								   "the standby cannot apply it while streaming."),
						 errhint("Install the new-version binaries and restart "
								 "this standby; it will replay the upgrade from the "
								 "end-of-upgrade checkpoint.")));
			else
				ereport(FATAL,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("pg_upgrade WAL encountered during replay"),
						 errhint("Restart this server to apply the pg_upgrade; "
								 "the upgrade cannot be replayed on a running standby.")));
		}

		if (upgrade_record_state != UPGRADE_RECORD_NONE)
			elog(PANIC, "duplicate upgrade START");
		memcpy(&upgrade_start, XLogRecGetData(record), SizeOfPgUpgradeStart);
		upgrade_xid = XLogRecGetXid(record);
		if (!TransactionIdIsNormal(upgrade_xid))
			elog(PANIC, "upgrade START requires an upgrade transaction");
		replay_context = AllocSetContextCreate(TopMemoryContext, "WAL upgrade replay",
											   ALLOCSET_DEFAULT_SIZES);
		relink_state = RELINK_INACTIVE;
		bind_transaction();
		upgrade_record_state = UPGRADE_RECORD_STARTED;

		SetControlFileUpgradeStarted();

		write_upgrade_file("PG_VERSION", xlrec->pg_version, len, 0, false);
	}
	else if (info == XLOG_UPGRADE_COMPLETE)
	{
		if (upgrade_record_state != UPGRADE_RECORD_STARTED ||
			relink_state != RELINK_INACTIVE ||
			XLogRecGetDataLen(record) != SizeOfPgUpgradeMarker ||
			memcmp(XLogRecGetData(record), &upgrade_start.marker, SizeOfPgUpgradeMarker) != 0)
			elog(PANIC, "invalid upgrade COMPLETE");
		upgrade_record_state = UPGRADE_RECORD_COMPLETE;
	}
	else if (info == XLOG_UPGRADE_RELINK)
		ReplayUpgradeRelink(record);
	else if (info == XLOG_UPGRADE_RAWFILE)
	{
		/*
		 * Write this chunk at its recorded offset. Offset zero truncates the
		 * destination before the first chunk.
		 */
		xl_pg_upgrade_rawfile *xlrec =
			(xl_pg_upgrade_rawfile *) XLogRecGetData(record);
		char	   *payload = (char *) xlrec + SizeOfPgUpgradeRawFile;
		char		path[MAXPGPATH];
		char	   *data;
		char	   *slash;

		if (XLogRecGetDataLen(record) < SizeOfPgUpgradeRawFile)
			elog(PANIC, "truncated rawfile record");
		if (xlrec->path_len >= MAXPGPATH)
			elog(PANIC, "rawfile path too long");
		if ((uint64) SizeOfPgUpgradeRawFile + xlrec->path_len + xlrec->data_len >
			XLogRecGetDataLen(record))
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("rawfile record overruns the record")));
		data = payload + xlrec->path_len;
		memcpy(path, payload, xlrec->path_len);
		path[xlrec->path_len] = '\0';
		if (strlen(path) != xlrec->path_len)
			elog(PANIC, "embedded NUL in upgrade rawfile path");

		/* Adopt control settings without replacing local recovery progress. */
		if (xlrec->path_len == strlen(XLOG_CONTROL_FILE) &&
			memcmp(payload, XLOG_CONTROL_FILE, xlrec->path_len) == 0)
		{
			if (xlrec->offset != 0 ||
				xlrec->data_len != PG_CONTROL_FILE_SIZE ||
				(uint64) SizeOfPgUpgradeRawFile + xlrec->path_len +
				xlrec->data_len != XLogRecGetDataLen(record))
				ereport(PANIC,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("invalid pg_control image in pg_upgrade WAL")));

			AdoptUpgradeControlFile(data, xlrec->data_len);
			return;
		}

		if (!path_is_safe_for_extraction(path))
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("unsafe rawfile path \"%s\"", path)));

		slash = strrchr(path, '/');
		if (slash != NULL)
		{
			*slash = '\0';
			if (mkdir(path, pg_dir_create_mode) != 0 && errno != EEXIST)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not create directory \"%s\": %m",
								path)));
			*slash = '/';
		}

		write_upgrade_file(path, data, xlrec->data_len, xlrec->offset, true);
	}
	else
		elog(PANIC, "unknown op code %u", info);
}

/* Record committed completion and mark its shutdown restartpoint pending. */
static void
replay_complete(XLogReaderState *record)
{
	TimeLineID	replay_tli;

	if (upgrade_complete_checkpoint_state != PG_UPGRADE_CHECKPOINT_NONE)
		elog(PANIC, "duplicate pg_upgrade COMPLETE before its checkpoint");
	(void) GetCurrentReplayRecPtr(&replay_tli);
	SetControlFileUpgradeComplete(record->EndRecPtr, replay_tli);
	upgrade_complete_end_lsn = record->EndRecPtr;
	upgrade_complete_checkpoint_state = PG_UPGRADE_CHECKPOINT_PENDING;
}

/* Remember the completion shutdown checkpoint for restartpoint verification. */
void
PgUpgradeCheckpointReplayed(const CheckPoint *checkpoint,
							XLogReaderState *record)
{
	if (upgrade_complete_checkpoint_state == PG_UPGRADE_CHECKPOINT_PENDING)
	{
		if (checkpoint->redo != record->ReadRecPtr ||
			record->ReadRecPtr < upgrade_complete_end_lsn)
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("shutdown checkpoint after pg_upgrade COMPLETE is invalid")));

		upgrade_complete_checkpoint_lsn = record->ReadRecPtr;
		upgrade_complete_checkpoint_tli = checkpoint->ThisTimeLineID;
		upgrade_complete_checkpoint_state = PG_UPGRADE_CHECKPOINT_REPLAYED;
	}
}

/* Persist the completion restartpoint and mark upgrade replay finalized. */
void
PgUpgradeCheckpointApplied(void)
{
	if (upgrade_complete_checkpoint_state != PG_UPGRADE_CHECKPOINT_REPLAYED)
		return;

	RequestCheckpoint(CHECKPOINT_FORCE | CHECKPOINT_FAST | CHECKPOINT_WAIT);

	if (upgrade_complete_checkpoint_state == PG_UPGRADE_CHECKPOINT_REPLAYED)
	{
		VerifyUpgradeRestartPoint(upgrade_complete_checkpoint_lsn,
								  upgrade_complete_checkpoint_tli);

		/*
		 * Persist removal of pg_upgrade.signal before marking replay
		 * finalized.
		 */
		durable_unlink(PG_UPGRADE_SIGNAL_FILE, PANIC);
		SetControlFileUpgradeFinalized();
		upgrade_complete_checkpoint_state = PG_UPGRADE_CHECKPOINT_NONE;
		replay_in_progress = false;
		ereport(LOG,
				(errmsg("persisted the post-upgrade restartpoint; pg_upgrade replay is complete")));
	}
}
