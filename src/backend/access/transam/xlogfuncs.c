/*-------------------------------------------------------------------------
 *
 * xlogfuncs.c
 *
 * PostgreSQL write-ahead log manager user interface functions
 *
 * This file contains WAL control and information functions.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/xlogfuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "access/htup_details.h"
#include "access/pgupgrade_wal.h"	/* XLogFlushUpgradeSLRU, emit fns */
#include "access/transam.h"		/* FirstNormalObjectId */
#include "access/xlog_internal.h"
#include "access/xlogbackup.h"
#include "access/xlogrecovery.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_database_d.h"	/* Template0DbOid */
#include "catalog/pg_largeobject_d.h"	/* LargeObject*Id (user-data catalogs) */
#include "catalog/pg_largeobject_metadata_d.h"
#include "catalog/pg_tablespace_d.h"	/* DEFAULTTABLESPACE_OID */
#include "catalog/pg_type.h"
#include "common/relpath.h"		/* PG_TBLSPC_DIR, TABLESPACE_VERSION_DIRECTORY */
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "replication/slot.h"
#include "utils/acl.h"
#include "replication/walreceiver.h"
#include "storage/fd.h"
#include "storage/latch.h"
#include "storage/standby.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/relmapper.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

/*
 * Backup-related variables.
 */
static BackupState *backup_state = NULL;
static StringInfo tablespace_map = NULL;

/* Session-level context for the SQL-callable backup functions */
static MemoryContext backupcontext = NULL;


/*
 * Return a string constant representing the recovery pause state. This is
 * used in system functions and views, and should *not* be translated.
 */
static const char *
GetRecoveryPauseStateString(RecoveryPauseState pause_state)
{
	const char *statestr = NULL;

	switch (pause_state)
	{
		case RECOVERY_NOT_PAUSED:
			statestr = "not paused";
			break;
		case RECOVERY_PAUSE_REQUESTED:
			statestr = "pause requested";
			break;
		case RECOVERY_PAUSED:
			statestr = "paused";
			break;
	}

	Assert(statestr != NULL);
	return statestr;
}

/*
 * pg_backup_start: set up for taking an on-line backup dump
 *
 * Essentially what this does is to create the contents required for the
 * backup_label file and the tablespace map.
 *
 * Permission checking for this function is managed through the normal
 * GRANT system.
 */
Datum
pg_backup_start(PG_FUNCTION_ARGS)
{
	text	   *backupid = PG_GETARG_TEXT_PP(0);
	bool		fast = PG_GETARG_BOOL(1);
	char	   *backupidstr;
	SessionBackupState status = get_backup_status();
	MemoryContext oldcontext;

	backupidstr = text_to_cstring(backupid);

	if (status == SESSION_BACKUP_RUNNING)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("a backup is already in progress in this session")));

	/*
	 * backup_state and tablespace_map need to be long-lived as they are used
	 * in pg_backup_stop().  These are allocated in a dedicated memory context
	 * child of TopMemoryContext, deleted at the end of pg_backup_stop().  If
	 * an error happens before ending the backup, memory would be leaked in
	 * this context until pg_backup_start() is called again.
	 */
	if (backupcontext == NULL)
	{
		backupcontext = AllocSetContextCreate(TopMemoryContext,
											  "on-line backup context",
											  ALLOCSET_START_SMALL_SIZES);
	}
	else
	{
		backup_state = NULL;
		tablespace_map = NULL;
		MemoryContextReset(backupcontext);
	}

	oldcontext = MemoryContextSwitchTo(backupcontext);
	backup_state = palloc0_object(BackupState);
	tablespace_map = makeStringInfo();
	MemoryContextSwitchTo(oldcontext);

	register_persistent_abort_backup_handler();
	do_pg_backup_start(backupidstr, fast, NULL, backup_state, tablespace_map);

	PG_RETURN_LSN(backup_state->startpoint);
}


/*
 * pg_backup_stop: finish taking an on-line backup.
 *
 * The first parameter (variable 'waitforarchive'), which is optional,
 * allows the user to choose if they want to wait for the WAL to be archived
 * or if we should just return as soon as the WAL record is written.
 *
 * This function stops an in-progress backup, creates backup_label contents and
 * it returns the backup stop LSN, backup_label and tablespace_map contents.
 *
 * The backup_label contains the user-supplied label string (typically this
 * would be used to tell where the backup dump will be stored), the starting
 * time, starting WAL location for the dump and so on.  It is the caller's
 * responsibility to write the backup_label and tablespace_map files in the
 * data folder that will be restored from this backup.
 *
 * Permission checking for this function is managed through the normal
 * GRANT system.
 */
Datum
pg_backup_stop(PG_FUNCTION_ARGS)
{
#define PG_BACKUP_STOP_V2_COLS 3
	TupleDesc	tupdesc;
	Datum		values[PG_BACKUP_STOP_V2_COLS] = {0};
	bool		nulls[PG_BACKUP_STOP_V2_COLS] = {0};
	bool		waitforarchive = PG_GETARG_BOOL(0);
	char	   *backup_label;
	SessionBackupState status = get_backup_status();

	/* Initialize attributes information in the tuple descriptor */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	if (status != SESSION_BACKUP_RUNNING)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("backup is not in progress"),
				 errhint("Did you call pg_backup_start()?")));

	Assert(backup_state != NULL);
	Assert(tablespace_map != NULL);

	/* Stop the backup */
	do_pg_backup_stop(backup_state, waitforarchive);

	/* Build the contents of backup_label */
	backup_label = build_backup_content(backup_state, false);

	values[0] = LSNGetDatum(backup_state->stoppoint);
	values[1] = CStringGetTextDatum(backup_label);
	values[2] = CStringGetTextDatum(tablespace_map->data);

	/* Deallocate backup-related variables */
	pfree(backup_label);

	/* Clean up the session-level state and its memory context */
	backup_state = NULL;
	tablespace_map = NULL;
	MemoryContextDelete(backupcontext);
	backupcontext = NULL;

	/* Returns the record as Datum */
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

/*
 * pg_switch_wal: switch to next xlog file
 *
 * Permission checking for this function is managed through the normal
 * GRANT system.
 */
Datum
pg_switch_wal(PG_FUNCTION_ARGS)
{
	XLogRecPtr	switchpoint;

	if (RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("recovery is in progress"),
				 errhint("WAL control functions cannot be executed during recovery.")));

	switchpoint = RequestXLogSwitch(false);

	/*
	 * As a convenience, return the WAL location of the switch record
	 */
	PG_RETURN_LSN(switchpoint);
}

/*
 * pg_log_standby_snapshot: call LogStandbySnapshot()
 *
 * Permission checking for this function is managed through the normal
 * GRANT system.
 */
Datum
pg_log_standby_snapshot(PG_FUNCTION_ARGS)
{
	XLogRecPtr	recptr;

	if (RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("recovery is in progress"),
				 errhint("%s cannot be executed during recovery.",
						 "pg_log_standby_snapshot()")));

	if (!XLogStandbyInfoActive())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_log_standby_snapshot() can only be used if \"wal_level\" >= \"replica\"")));

	recptr = LogStandbySnapshot();

	/*
	 * As a convenience, return the WAL location of the last inserted record
	 */
	PG_RETURN_LSN(recptr);
}

/*
 * pg_create_restore_point: a named point for restore
 *
 * Permission checking for this function is managed through the normal
 * GRANT system.
 */
Datum
pg_create_restore_point(PG_FUNCTION_ARGS)
{
	text	   *restore_name = PG_GETARG_TEXT_PP(0);
	char	   *restore_name_str;
	XLogRecPtr	restorepoint;

	if (RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("recovery is in progress"),
				 errhint("WAL control functions cannot be executed during recovery.")));

	if (!XLogIsNeeded())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("WAL level not sufficient for creating a restore point"),
				 errhint("\"wal_level\" must be set to \"replica\" or \"logical\" at server start.")));

	restore_name_str = text_to_cstring(restore_name);

	if (strlen(restore_name_str) >= MAXFNAMELEN)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("value too long for restore point (maximum %d characters)", MAXFNAMELEN - 1)));

	restorepoint = XLogRestorePoint(restore_name_str);

	/*
	 * As a convenience, return the WAL location of the restore point record
	 */
	PG_RETURN_LSN(restorepoint);
}

/*
 * Report the current WAL write location (same format as pg_backup_start etc)
 *
 * This is useful for determining how much of WAL is visible to an external
 * archiving process.  Note that the data before this point is written out
 * to the kernel, but is not necessarily synced to disk.
 */

/*
 * pg_upgrade --wal-upgrade window emission.  Driven from C by
 * EmitUpgradeWalWindow() below, reachable only through the binary-upgrade-gated
 * SQL entry point binary_upgrade_emit_wal_window().
 */

/*
 * Parse a relation file name "<rfnum>[_fork][.segno]" into its parts.
 * Returns true if it is a relation file, filling *rfnum, *forknum (0=main,
 * 1=fsm, 2=vm, 3=init) and *segno.  Returns false for non-relation files
 * (PG_VERSION, pg_filenode.map, pg_internal.init, etc.).
 */
static bool
parse_relfile_name(const char *name, RelFileNumber *rfnum,
				   uint8 *forknum, uint32 *segno)
{
	char		base[MAXPGPATH];
	char	   *p;
	unsigned long n;
	char	   *endp;

	if (name[0] < '1' || name[0] > '9')
		return false;			/* relation files start with a nonzero digit */

	strlcpy(base, name, sizeof(base));

	*segno = 0;
	p = strrchr(base, '.');
	if (p != NULL && p[1] >= '0' && p[1] <= '9')
	{
		*segno = (uint32) strtoul(p + 1, NULL, 10);
		*p = '\0';
	}

	*forknum = 0;
	if ((p = strstr(base, "_vm")) != NULL && p[3] == '\0')
	{
		*forknum = 2;
		*p = '\0';
	}
	else if ((p = strstr(base, "_fsm")) != NULL && p[4] == '\0')
	{
		*forknum = 1;
		*p = '\0';
	}
	else if ((p = strstr(base, "_init")) != NULL && p[5] == '\0')
	{
		*forknum = 3;
		*p = '\0';
	}

	errno = 0;
	n = strtoul(base, &endp, 10);
	if (errno != 0 || *endp != '\0' || n == 0 || n > PG_UINT32_MAX)
		return false;
	*rfnum = (RelFileNumber) n;
	return true;
}

/*
 * Is this relfilenumber a pinned system catalog that pg_upgrade transfers as a
 * file (like user data) rather than regenerating?
 *
 * pg_upgrade treats pg_largeobject and pg_largeobject_metadata this way and
 * copies/links their files like ordinary user relations (see
 * get_rel_infos_query() in src/bin/pg_upgrade/info.c).  pg_largeobject applies
 * for every source version; pg_largeobject_metadata only for old clusters >= v16
 * -- before v16 the aclitem on-disk format differed, so pg_upgrade regenerates
 * it and the window must carry it.
 *
 * Their relfilenumbers equal the pinned catalog/index OIDs unless pg_upgrade
 * rewrote them, in which case the number is >= 16384 and the ordinary
 * user-relation test already excludes them, so matching the pinned OIDs is exact.
 */
static bool
is_transferred_user_data_catalog(RelFileNumber rfnum, uint32 old_major)
{
	if (rfnum == LargeObjectRelationId || rfnum == LargeObjectLOidPNIndexId)
		return true;
	if (old_major >= 160000 &&
		(rfnum == LargeObjectMetadataRelationId ||
		 rfnum == LargeObjectMetadataOidIndexId))
		return true;
	return false;
}

/*
 * Capture every file in one PGDATA subdirectory (base/<dboid> or global)
 * as WAL.  Relation files -> XLOG_FPI records (replayed by xlog_redo through
 * the buffer manager).  pg_filenode.map -> XLOG_RELMAP_UPDATE.  PG_VERSION ->
 * XLOG_UPGRADE_RAWFILE.  Everything else (pg_internal.init and friends) is
 * regenerated by the server.
 */
static void
capture_dir_files(UpgradeRelfileBatch *relink,
				  const char *reldir, Oid tsoid, Oid dboid, uint32 old_major)
{
	DIR		   *dir;
	struct dirent *de;
	char		path[MAXPGPATH];

	/*
	 * Unlogged relations (identified by an _init fork) must not have their
	 * main/fsm/vm forks captured: end-of-recovery ResetUnloggedRelations()
	 * rebuilds the main fork from _init with O_CREAT|O_EXCL and would FATAL
	 * if RELFILE redo had already recreated it.  Capture only the _init fork.
	 *
	 * Pass 1: collect the set of relfilenumbers that have an _init fork.
	 */
	RelFileNumber *unlogged = NULL;
	int			n_unlogged = 0;
	int			unlogged_alloc = 0;

	dir = AllocateDir(reldir);
	if (dir == NULL)
		return;

	while ((de = ReadDir(dir, reldir)) != NULL)
	{
		RelFileNumber rfnum;
		uint8		forknum;
		uint32		segno;

		if (parse_relfile_name(de->d_name, &rfnum, &forknum, &segno) &&
			forknum == INIT_FORKNUM)
		{
			if (n_unlogged >= unlogged_alloc)
			{
				unlogged_alloc = (unlogged_alloc == 0) ? 16 : unlogged_alloc * 2;
				if (unlogged == NULL)
					unlogged = palloc_array(RelFileNumber, unlogged_alloc);
				else
					unlogged = repalloc_array(unlogged, RelFileNumber, unlogged_alloc);
			}
			unlogged[n_unlogged++] = rfnum;
		}
	}
	FreeDir(dir);

	/* Pass 2: capture files, skipping the main/fsm/vm forks of unlogged rels. */
	dir = AllocateDir(reldir);
	if (dir == NULL)
	{
		if (unlogged != NULL)
			pfree(unlogged);
		return;
	}

	while ((de = ReadDir(dir, reldir)) != NULL)
	{
		RelFileNumber rfnum;
		uint8		forknum;
		uint32		segno;

		if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
			continue;

		snprintf(path, sizeof(path), "%s/%s", reldir, de->d_name);

		if (parse_relfile_name(de->d_name, &rfnum, &forknum, &segno))
		{
			/*
			 * Unlogged relations: only the _init fork travels.  main/fsm/vm
			 * are rebuilt from _init by ResetUnloggedRelations() at end of
			 * recovery (O_CREAT|O_EXCL), which FATALs if redo already placed
			 * them, so they must enter neither the RELINK manifest nor the
			 * capture.  This holds for user and system relations alike, so it
			 * is checked before the user/system dispatch below -- otherwise a
			 * user relation (handled first) would have its main/fsm/vm forks
			 * manifested wholesale.
			 */
			if (forknum != INIT_FORKNUM)
			{
				bool		is_unlogged = false;

				for (int i = 0; i < n_unlogged; i++)
				{
					if (unlogged[i] == rfnum)
					{
						is_unlogged = true;
						break;
					}
				}
				if (is_unlogged)
					continue;
			}

			/*
			 * Only system relations are captured as an FPI in this stage.
			 * User relations (relfilenode >= FirstNormalObjectId) are
			 * linked/copied from the old datadir by pg_upgrade's transfer step,
			 * so they are already on disk and need not travel through WAL.
			 *
			 * For a streaming standby, the file's identity is instead recorded
			 * in the RELINK manifest: on redo the standby links it from its
			 * retained old datadir into the new skeleton.
			 */
			if (rfnum >= FirstNormalObjectId)
			{
				XLogUpgradeRelinkBatchAdd(relink, tsoid, dboid, rfnum,
										  forknum, segno);
				continue;
			}

			/*
			 * A few pinned system catalogs (pg_largeobject and, for old
			 * clusters >= v16, pg_largeobject_metadata) carry user large-object
			 * data that pg_upgrade transfers as files rather than regenerating,
			 * so they are relinked like ordinary user relations.
			 *
			 * template0 is the exception: pg_upgrade never processes it (the new
			 * initdb regenerates it fresh, with a different database OID than the
			 * old cluster), so nothing under it is inherited.  Capture its files
			 * like any regenerated catalog instead of relinking them.
			 */
			if (dboid != Template0DbOid &&
				is_transferred_user_data_catalog(rfnum, old_major))
			{
				XLogUpgradeRelinkBatchAdd(relink, tsoid, dboid, rfnum,
										  forknum, segno);
				continue;
			}

			XLogUpgradeCaptureRelfile(path, tsoid, dboid, rfnum,
									  forknum, segno);
		}
		else if (strcmp(de->d_name, "pg_filenode.map") == 0)
		{
			/*
			 * The relation map is logged as an XLOG_RELMAP_UPDATE record: its
			 * redo derives the destination from (dbid, tsid).
			 */
			RelationMapLogFromDir(dboid, tsoid, unconstify(char *, reldir));
		}
		else if (strcmp(de->d_name, "PG_VERSION") == 0)
			(void) XLogWriteUpgradeRawFile(path);
		/* else: pg_internal.init, etc. -- regenerated, skip */
	}
	FreeDir(dir);

	if (unlogged != NULL)
		pfree(unlogged);
}

/*
 * capture_all_relfiles()
 *
 * Capture the cluster's physical image as WAL: relation files under
 * base/<dboid>/ and global/, plus the relation maps and PG_VERSION, so
 * first-startup replay can reconstruct the cluster from an empty data
 * directory.  Recovery is anchored at CN, so these images are the sole source
 * of the catalogs.
 *
 * Internal C helper driven by EmitUpgradeWalWindow(); not SQL-callable.
 */
static void
capture_all_relfiles(uint32 old_major, uint8 transfer_mode)
{
	UpgradeRelfileBatch relink;
	DIR		   *basedir;
	struct dirent *de;

	XLogUpgradeRelinkBatchBegin(&relink, transfer_mode);

	/*
	 * Shared catalogs live in global/ (tablespace GLOBALTABLESPACE_OID, db
	 * 0).
	 */
	capture_dir_files(&relink, "global", GLOBALTABLESPACE_OID,
					  InvalidOid, old_major);

	/* Every database directory under base/. */
	basedir = AllocateDir("base");
	if (basedir != NULL)
	{
		while ((de = ReadDir(basedir, "base")) != NULL)
		{
			char		dbpath[MAXPGPATH];
			char	   *endp;
			unsigned long dboid;

			if (de->d_name[0] < '1' || de->d_name[0] > '9')
				continue;
			errno = 0;
			dboid = strtoul(de->d_name, &endp, 10);
			if (errno != 0 || *endp != '\0')
				continue;

			snprintf(dbpath, sizeof(dbpath), "base/%s", de->d_name);
			capture_dir_files(&relink, dbpath, DEFAULTTABLESPACE_OID,
							  (Oid) dboid, old_major);
		}
		FreeDir(basedir);
	}

	/*
	 * Relations in user-defined tablespaces.  Each pg_tblspc/<spcoid>
	 * symlinks to TABLESPACE_VERSION_DIRECTORY/<dboid>/<files>.  Walk each and
	 * capture the relfiles with tsoid=<spcoid> so RELFILE redo places them
	 * correctly.
	 */
	{
		DIR		   *tsdir = AllocateDir(PG_TBLSPC_DIR);

		if (tsdir != NULL)
		{
			struct dirent *tde;

			while ((tde = ReadDir(tsdir, PG_TBLSPC_DIR)) != NULL)
			{
				char		verpath[MAXPGPATH];
				char	   *endp;
				unsigned long spcoid;
				DIR		   *verdir;
				struct dirent *vde;

				if (tde->d_name[0] < '1' || tde->d_name[0] > '9')
					continue;
				errno = 0;
				spcoid = strtoul(tde->d_name, &endp, 10);
				if (errno != 0 || *endp != '\0')
					continue;

				/* pg_tblspc/<spcoid>/<TABLESPACE_VERSION_DIRECTORY>/ */
				snprintf(verpath, sizeof(verpath), "%s/%s/%s",
						 PG_TBLSPC_DIR, tde->d_name,
						 TABLESPACE_VERSION_DIRECTORY);

				verdir = AllocateDir(verpath);
				if (verdir == NULL)
					continue;

				while ((vde = ReadDir(verdir, verpath)) != NULL)
				{
					char		dbpath[MAXPGPATH];
					char	   *dendp;
					unsigned long dboid;

					if (vde->d_name[0] < '1' || vde->d_name[0] > '9')
						continue;
					errno = 0;
					dboid = strtoul(vde->d_name, &dendp, 10);
					if (errno != 0 || *dendp != '\0')
						continue;

					snprintf(dbpath, sizeof(dbpath), "%s/%s",
							 verpath, vde->d_name);
					capture_dir_files(&relink, dbpath, (Oid) spcoid,
									  (Oid) dboid, old_major);
				}
				FreeDir(verdir);
			}
			FreeDir(tsdir);
		}
	}

	/* Flush the manifest of user relations for the standby to relink. */
	XLogUpgradeRelinkBatchEnd(&relink);

	/* Top-level PG_VERSION. */
	(void) XLogWriteUpgradeRawFile("PG_VERSION");
}

/*
 * EmitUpgradeWalWindow()
 *
 * Emit the whole --wal-upgrade window as WAL in one gated backend call.  The
 * order is fixed:
 *
 *   1. flush SLRU dirty pages, without a checkpoint (a checkpoint here would
 *      displace CN, the recovery anchor).
 *   2. XLOG_UPGRADE_START, carrying the new PG_VERSION string.
 *   3. XLOG_UPGRADE_DIRTREE, before any file image.
 *   4. relation pages as XLOG_FPI, plus the RELINK manifest for user rels.
 *   5. SLRU segments as XLOG_UPGRADE_RAWFILE.
 *   6. XLOG_UPGRADE_COMPLETE and the durable upgrade_finalized flag, unless
 *      skip_complete is set (assert-build hook simulating a crash mid-window).
 *
 * Runs only in binary-upgrade mode.  The caller enforces IsBinaryUpgrade.
 */
void
EmitUpgradeWalWindow(uint32 old_major, uint32 new_major, uint8 transfer_mode,
					 bool skip_complete)
{
	if (RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("recovery is in progress")));

	/* no automatic checkpoint may displace CN while the window is emitted */
	SetAutoCheckpointSuppressed(true);
	PG_TRY();
	{
		/*
		 * Any migrated physical slot that pins the window was created by
		 * pg_upgrade before this call.
		 *
		 * Step 1: flush SLRU dirty pages durably.
		 */
		XLogFlushUpgradeSLRU();

		/*
		 * Mark the control file "upgrade started" durably before emitting
		 * START.  If the burst server dies after this point but before
		 * finalize, the new cluster's control file carries
		 * upgrade_started && !upgrade_finalized, which
		 * PerformWalUpgradeIfNeeded() refuses to auto-serve even if the
		 * START-bearing WAL is no longer present at first boot.
		 */
		SetControlFileUpgradeStarted();

		/* 2. START */
		(void) XLogWritePgUpgrade(true, old_major, new_major);

		/* 3. directory skeleton */
		(void) XLogWriteUpgradeDirSkel();

		/* 4. relation-file images + RELINK manifest */
		capture_all_relfiles(old_major, transfer_mode);

		/* 5. SLRU bulk images: pg_xact, multixact offsets, multixact members */
		(void) XLogWriteUpgradeSlruData(0);
		(void) XLogWriteUpgradeSlruData(1);
		(void) XLogWriteUpgradeSlruData(2);

		/* 6. COMPLETE + durable finalized flag (unless suppressed for testing) */
		if (!skip_complete)
		{
			(void) XLogWritePgUpgrade(false, old_major, new_major);
			SetControlFileUpgradeFinalized();
		}
	}
	PG_FINALLY();
	{
		SetAutoCheckpointSuppressed(false);
	}
	PG_END_TRY();
}

Datum
pg_current_wal_lsn(PG_FUNCTION_ARGS)
{
	XLogRecPtr	current_recptr;

	if (RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("recovery is in progress"),
				 errhint("WAL control functions cannot be executed during recovery.")));

	current_recptr = GetXLogWriteRecPtr();

	PG_RETURN_LSN(current_recptr);
}

/*
 * Report the current WAL insert location (same format as pg_backup_start etc)
 *
 * This function is mostly for debugging purposes.
 */
Datum
pg_current_wal_insert_lsn(PG_FUNCTION_ARGS)
{
	XLogRecPtr	current_recptr;

	if (RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("recovery is in progress"),
				 errhint("WAL control functions cannot be executed during recovery.")));

	current_recptr = GetXLogInsertRecPtr();

	PG_RETURN_LSN(current_recptr);
}

/*
 * Report the current WAL flush location (same format as pg_backup_start etc)
 *
 * This function is mostly for debugging purposes.
 */
Datum
pg_current_wal_flush_lsn(PG_FUNCTION_ARGS)
{
	XLogRecPtr	current_recptr;

	if (RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("recovery is in progress"),
				 errhint("WAL control functions cannot be executed during recovery.")));

	current_recptr = GetFlushRecPtr(NULL);

	PG_RETURN_LSN(current_recptr);
}

/*
 * Report the last WAL receive location (same format as pg_backup_start etc)
 *
 * This is useful for determining how much of WAL is guaranteed to be received
 * and synced to disk by walreceiver.
 */
Datum
pg_last_wal_receive_lsn(PG_FUNCTION_ARGS)
{
	XLogRecPtr	recptr;

	recptr = GetWalRcvFlushRecPtr(NULL, NULL);

	if (!XLogRecPtrIsValid(recptr))
		PG_RETURN_NULL();

	PG_RETURN_LSN(recptr);
}

/*
 * Report the last WAL replay location (same format as pg_backup_start etc)
 *
 * This is useful for determining how much of WAL is visible to read-only
 * connections during recovery.
 */
Datum
pg_last_wal_replay_lsn(PG_FUNCTION_ARGS)
{
	XLogRecPtr	recptr;

	recptr = GetXLogReplayRecPtr(NULL);

	if (!XLogRecPtrIsValid(recptr))
		PG_RETURN_NULL();

	PG_RETURN_LSN(recptr);
}

/*
 * Compute an xlog file name and decimal byte offset given a WAL location,
 * such as is returned by pg_backup_stop() or pg_switch_wal().
 */
Datum
pg_walfile_name_offset(PG_FUNCTION_ARGS)
{
	XLogSegNo	xlogsegno;
	uint32		xrecoff;
	XLogRecPtr	locationpoint = PG_GETARG_LSN(0);
	char		xlogfilename[MAXFNAMELEN];
	Datum		values[2];
	bool		isnull[2];
	TupleDesc	resultTupleDesc;
	HeapTuple	resultHeapTuple;
	Datum		result;

	if (RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("recovery is in progress"),
				 errhint("%s cannot be executed during recovery.",
						 "pg_walfile_name_offset()")));

	/*
	 * Construct a tuple descriptor for the result row.  This must match this
	 * function's pg_proc entry!
	 */
	resultTupleDesc = CreateTemplateTupleDesc(2);
	TupleDescInitEntry(resultTupleDesc, (AttrNumber) 1, "file_name",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(resultTupleDesc, (AttrNumber) 2, "file_offset",
					   INT4OID, -1, 0);

	TupleDescFinalize(resultTupleDesc);
	resultTupleDesc = BlessTupleDesc(resultTupleDesc);

	/*
	 * xlogfilename
	 */
	XLByteToSeg(locationpoint, xlogsegno, wal_segment_size);
	XLogFileName(xlogfilename, GetWALInsertionTimeLine(), xlogsegno,
				 wal_segment_size);

	values[0] = CStringGetTextDatum(xlogfilename);
	isnull[0] = false;

	/*
	 * offset
	 */
	xrecoff = XLogSegmentOffset(locationpoint, wal_segment_size);

	values[1] = UInt32GetDatum(xrecoff);
	isnull[1] = false;

	/*
	 * Tuple jam: Having first prepared your Datums, then squash together
	 */
	resultHeapTuple = heap_form_tuple(resultTupleDesc, values, isnull);

	result = HeapTupleGetDatum(resultHeapTuple);

	PG_RETURN_DATUM(result);
}

/*
 * Compute an xlog file name given a WAL location,
 * such as is returned by pg_backup_stop() or pg_switch_wal().
 */
Datum
pg_walfile_name(PG_FUNCTION_ARGS)
{
	XLogSegNo	xlogsegno;
	XLogRecPtr	locationpoint = PG_GETARG_LSN(0);
	char		xlogfilename[MAXFNAMELEN];

	if (RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("recovery is in progress"),
				 errhint("%s cannot be executed during recovery.",
						 "pg_walfile_name()")));

	XLByteToSeg(locationpoint, xlogsegno, wal_segment_size);
	XLogFileName(xlogfilename, GetWALInsertionTimeLine(), xlogsegno,
				 wal_segment_size);

	PG_RETURN_TEXT_P(cstring_to_text(xlogfilename));
}

/*
 * Extract the sequence number and the timeline ID from given a WAL file
 * name.
 */
Datum
pg_split_walfile_name(PG_FUNCTION_ARGS)
{
#define PG_SPLIT_WALFILE_NAME_COLS 2
	char	   *fname = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char	   *fname_upper;
	char	   *p;
	TimeLineID	tli;
	XLogSegNo	segno;
	Datum		values[PG_SPLIT_WALFILE_NAME_COLS] = {0};
	bool		isnull[PG_SPLIT_WALFILE_NAME_COLS] = {0};
	TupleDesc	tupdesc;
	HeapTuple	tuple;
	char		buf[256];
	Datum		result;

	fname_upper = pstrdup(fname);

	/* Capitalize WAL file name. */
	for (p = fname_upper; *p; p++)
		*p = pg_ascii_toupper((unsigned char) *p);

	if (!IsXLogFileName(fname_upper))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid WAL file name \"%s\"", fname)));

	XLogFromFileName(fname_upper, &tli, &segno, wal_segment_size);

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Convert to numeric. */
	snprintf(buf, sizeof buf, UINT64_FORMAT, segno);
	values[0] = DirectFunctionCall3(numeric_in,
									CStringGetDatum(buf),
									ObjectIdGetDatum(0),
									Int32GetDatum(-1));

	values[1] = Int64GetDatum(tli);

	tuple = heap_form_tuple(tupdesc, values, isnull);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);

#undef PG_SPLIT_WALFILE_NAME_COLS
}

/*
 * pg_wal_replay_pause - Request to pause recovery
 *
 * Permission checking for this function is managed through the normal
 * GRANT system.
 */
Datum
pg_wal_replay_pause(PG_FUNCTION_ARGS)
{
	if (!RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("recovery is not in progress"),
				 errhint("Recovery control functions can only be executed during recovery.")));

	if (PromoteIsTriggered())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("standby promotion is ongoing"),
				 errhint("%s cannot be executed after promotion is triggered.",
						 "pg_wal_replay_pause()")));

	SetRecoveryPause(true);

	/* wake up the recovery process so that it can process the pause request */
	WakeupRecovery();

	PG_RETURN_VOID();
}

/*
 * pg_wal_replay_resume - resume recovery now
 *
 * Permission checking for this function is managed through the normal
 * GRANT system.
 */
Datum
pg_wal_replay_resume(PG_FUNCTION_ARGS)
{
	if (!RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("recovery is not in progress"),
				 errhint("Recovery control functions can only be executed during recovery.")));

	if (PromoteIsTriggered())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("standby promotion is ongoing"),
				 errhint("%s cannot be executed after promotion is triggered.",
						 "pg_wal_replay_resume()")));

	SetRecoveryPause(false);

	PG_RETURN_VOID();
}

/*
 * pg_is_wal_replay_paused
 */
Datum
pg_is_wal_replay_paused(PG_FUNCTION_ARGS)
{
	if (!RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("recovery is not in progress"),
				 errhint("Recovery control functions can only be executed during recovery.")));

	PG_RETURN_BOOL(GetRecoveryPauseState() != RECOVERY_NOT_PAUSED);
}

/*
 * pg_get_wal_replay_pause_state - Returns the recovery pause state.
 *
 * Returned values:
 *
 * 'not paused' - if pause is not requested
 * 'pause requested' - if pause is requested but recovery is not yet paused
 * 'paused' - if recovery is paused
 */
Datum
pg_get_wal_replay_pause_state(PG_FUNCTION_ARGS)
{
	RecoveryPauseState state;

	if (!RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("recovery is not in progress"),
				 errhint("Recovery control functions can only be executed during recovery.")));

	state = GetRecoveryPauseState();

	/* get the recovery pause state */
	PG_RETURN_TEXT_P(cstring_to_text(GetRecoveryPauseStateString(state)));
}

/*
 * Returns timestamp of latest processed commit/abort record.
 *
 * When the server has been started normally without recovery the function
 * returns NULL.
 */
Datum
pg_last_xact_replay_timestamp(PG_FUNCTION_ARGS)
{
	TimestampTz xtime;

	xtime = GetLatestXTime();
	if (xtime == 0)
		PG_RETURN_NULL();

	PG_RETURN_TIMESTAMPTZ(xtime);
}

/*
 * Returns bool with current recovery mode, a global state.
 */
Datum
pg_is_in_recovery(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(RecoveryInProgress());
}

/*
 * Compute the difference in bytes between two WAL locations.
 */
Datum
pg_wal_lsn_diff(PG_FUNCTION_ARGS)
{
	Datum		result;

	result = DirectFunctionCall2(pg_lsn_mi,
								 PG_GETARG_DATUM(0),
								 PG_GETARG_DATUM(1));

	PG_RETURN_DATUM(result);
}

/*
 * Promotes a standby server.
 *
 * A result of "true" means that promotion has been completed if "wait" is
 * "true", or initiated if "wait" is false.
 */
Datum
pg_promote(PG_FUNCTION_ARGS)
{
	bool		wait = PG_GETARG_BOOL(0);
	int			wait_seconds = PG_GETARG_INT32(1);
	FILE	   *promote_file;
	TimestampTz end_time;

	if (!RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("recovery is not in progress"),
				 errhint("Recovery control functions can only be executed during recovery.")));

	if (wait_seconds <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("\"wait_seconds\" must not be negative or zero")));

	/* create the promote signal file */
	promote_file = AllocateFile(PROMOTE_SIGNAL_FILE, "w");
	if (!promote_file)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m",
						PROMOTE_SIGNAL_FILE)));

	if (FreeFile(promote_file))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m",
						PROMOTE_SIGNAL_FILE)));

	/* signal the postmaster */
	if (kill(PostmasterPid, SIGUSR1) != 0)
	{
		(void) unlink(PROMOTE_SIGNAL_FILE);
		ereport(ERROR,
				(errcode(ERRCODE_SYSTEM_ERROR),
				 errmsg("failed to send signal to postmaster: %m")));
	}

	/* return immediately if waiting was not requested */
	if (!wait)
		PG_RETURN_BOOL(true);

	/* wait for the amount of time wanted until promotion */
	end_time = TimestampTzPlusSeconds(GetCurrentTimestamp(), wait_seconds);
	while (GetCurrentTimestamp() < end_time)
	{
		int			rc;

		ResetLatch(MyLatch);

		if (!RecoveryInProgress())
			PG_RETURN_BOOL(true);

		CHECK_FOR_INTERRUPTS();

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   100L,
					   WAIT_EVENT_PROMOTE);

		/*
		 * Emergency bailout if postmaster has died.  This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (rc & WL_POSTMASTER_DEATH)
			ereport(FATAL,
					(errcode(ERRCODE_ADMIN_SHUTDOWN),
					 errmsg("terminating connection due to unexpected postmaster exit"),
					 errcontext("while waiting on promotion")));
	}

	ereport(WARNING,
			(errmsg_plural("server did not promote within %d second",
						   "server did not promote within %d seconds",
						   wait_seconds,
						   wait_seconds)));
	PG_RETURN_BOOL(false);
}

/*
 * pg_stat_get_recovery - returns information about WAL recovery state
 *
 * Returns NULL when not in recovery or when the caller lacks
 * pg_read_all_stats privileges; one row otherwise.
 */
Datum
pg_stat_get_recovery(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum	   *values;
	bool	   *nulls;

	/* Local copies of shared state */
	bool		promote_triggered;
	XLogRecPtr	last_replayed_read_lsn;
	XLogRecPtr	last_replayed_end_lsn;
	TimeLineID	last_replayed_tli;
	XLogRecPtr	replay_end_lsn;
	TimeLineID	replay_end_tli;
	TimestampTz recovery_last_xact_time;
	TimestampTz current_chunk_start_time;
	RecoveryPauseState pause_state;

	if (!RecoveryInProgress())
		PG_RETURN_NULL();

	if (!has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS))
		PG_RETURN_NULL();

	/* Take a lock to ensure value consistency */
	SpinLockAcquire(&XLogRecoveryCtl->info_lck);
	promote_triggered = XLogRecoveryCtl->SharedPromoteIsTriggered;
	last_replayed_read_lsn = XLogRecoveryCtl->lastReplayedReadRecPtr;
	last_replayed_end_lsn = XLogRecoveryCtl->lastReplayedEndRecPtr;
	last_replayed_tli = XLogRecoveryCtl->lastReplayedTLI;
	replay_end_lsn = XLogRecoveryCtl->replayEndRecPtr;
	replay_end_tli = XLogRecoveryCtl->replayEndTLI;
	recovery_last_xact_time = XLogRecoveryCtl->recoveryLastXTime;
	current_chunk_start_time = XLogRecoveryCtl->currentChunkStartTime;
	pause_state = XLogRecoveryCtl->recoveryPauseState;
	SpinLockRelease(&XLogRecoveryCtl->info_lck);

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	values = palloc0_array(Datum, tupdesc->natts);
	nulls = palloc0_array(bool, tupdesc->natts);

	values[0] = BoolGetDatum(promote_triggered);

	if (XLogRecPtrIsValid(last_replayed_read_lsn))
		values[1] = LSNGetDatum(last_replayed_read_lsn);
	else
		nulls[1] = true;

	if (XLogRecPtrIsValid(last_replayed_end_lsn))
		values[2] = LSNGetDatum(last_replayed_end_lsn);
	else
		nulls[2] = true;

	if (XLogRecPtrIsValid(last_replayed_end_lsn))
		values[3] = Int32GetDatum(last_replayed_tli);
	else
		nulls[3] = true;

	if (XLogRecPtrIsValid(replay_end_lsn))
		values[4] = LSNGetDatum(replay_end_lsn);
	else
		nulls[4] = true;

	if (XLogRecPtrIsValid(replay_end_lsn))
		values[5] = Int32GetDatum(replay_end_tli);
	else
		nulls[5] = true;

	if (recovery_last_xact_time != 0)
		values[6] = TimestampTzGetDatum(recovery_last_xact_time);
	else
		nulls[6] = true;

	if (current_chunk_start_time != 0)
		values[7] = TimestampTzGetDatum(current_chunk_start_time);
	else
		nulls[7] = true;

	values[8] = CStringGetTextDatum(GetRecoveryPauseStateString(pause_state));

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}
