/*
 *	pg_upgrade.c
 *
 *	main source file
 *
 *	Copyright (c) 2010-2026, PostgreSQL Global Development Group
 *	src/bin/pg_upgrade/pg_upgrade.c
 */

/*
 *	To simplify the upgrade process, we force certain system values to be
 *	identical between old and new clusters:
 *
 *	We control all assignments of pg_class.oid (and relfilenode) so toast
 *	oids are the same between old and new clusters.  This is important
 *	because toast oids are stored as toast pointers in user tables.
 *
 *	While pg_class.oid and pg_class.relfilenode are initially the same in a
 *	cluster, they can diverge due to CLUSTER, REINDEX, or VACUUM FULL. We
 *	control assignments of pg_class.relfilenode because we want the filenames
 *	to match between the old and new cluster.
 *
 *	We control assignment of pg_tablespace.oid because we want the oid to match
 *	between the old and new cluster.
 *
 *	We control all assignments of pg_type.oid because these oids are stored
 *	in user composite type values.
 *
 *	We control all assignments of pg_enum.oid because these oids are stored
 *	in user tables as enum values.
 *
 *	We control all assignments of pg_authid.oid because the oids are stored in
 *	pg_largeobject_metadata, which is copied via file transfer for upgrades
 *	from v16 and newer.
 *
 *	We control all assignments of pg_database.oid because we want the directory
 *	names to match between the old and new cluster.
 */



#include "postgres_fe.h"

#include <dirent.h>
#include <time.h>

#include "access/multixact.h"
#include "catalog/pg_class_d.h"
#include "catalog/pg_collation_d.h"
#include "common/file_perm.h"
#include "common/logging.h"
#include "common/restricted_token.h"
#include "fe_utils/string_utils.h"
#include "fe_utils/version.h"
#include "mb/pg_wchar.h"
#include "pg_upgrade.h"

/*
 * Maximum number of pg_restore actions (TOC entries) to process within one
 * transaction.  At some point we might want to make this user-controllable,
 * but for now a hard-wired setting will suffice.
 */
#define RESTORE_TRANSACTION_SIZE 1000

static void set_new_cluster_char_signedness(void);
static void set_locale_and_encoding(void);
static void prepare_new_cluster(void);
static void prepare_new_globals(void);
static void create_new_objects(void);
static void copy_xact_xlog_xid(void);
static void set_frozenxids(void);
static void make_outputdirs(char *pgdata);
static void setup(char *argv0);
static void resolve_new_bindir(const char *argv0);
static void create_new_cluster_via_initdb(const char *argv0);
static void create_logical_replication_slots(void);
static void create_conflict_detection_slot(void);
static void revert_wal_logged_disk_writes(void);

ClusterInfo old_cluster,
			new_cluster;
OSInfo		os_info;

char	   *output_files[] = {
	SERVER_LOG_FILE,
#ifdef WIN32
	/* unique file for pg_ctl start */
	SERVER_START_LOG_FILE,
#endif
	UTILITY_LOG_FILE,
	INTERNAL_LOG_FILE,
	NULL
};


int
main(int argc, char **argv)
{
	char	   *deletion_script_file_name = NULL;
	bool		migrate_logical_slots;

	/*
	 * pg_upgrade doesn't currently use common/logging.c, but initialize it
	 * anyway because we might call common code that does.
	 */
	pg_logging_init(argv[0]);
	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_upgrade"));

	/* Set default restrictive mask until new cluster permissions are read */
	umask(PG_MODE_MASK_OWNER);


	parseCommandLine(argc, argv);


	get_restricted_token();

	adjust_data_dir(&old_cluster);

	if (user_opts.initdb_new_cluster)
		create_new_cluster_via_initdb(argv[0]);

	adjust_data_dir(&new_cluster);

	/*
	 * Set mask based on PGDATA permissions, needed for the creation of the
	 * output directories with correct permissions.
	 */
	if (!GetDataDirectoryCreatePerm(new_cluster.pgdata))
		pg_fatal("could not read permissions of directory \"%s\": %m",
				 new_cluster.pgdata);

	umask(pg_mode_mask);

	/*
	 * This needs to happen after adjusting the data directory of the new
	 * cluster in adjust_data_dir().
	 */
	make_outputdirs(new_cluster.pgdata);

	setup(argv[0]);

	output_check_banner();

	check_cluster_versions();

	get_sock_dir(&old_cluster);
	get_sock_dir(&new_cluster);

	check_cluster_compatibility();

	/*
	 * LEE: the --wal-log-upgrade recovery anchor (CN) is captured later, at the
	 * end of the upgrade, from the checkpoint we take just before the full-page
	 * image burst — not from the initdb checkpoint.  See the end-of-upgrade
	 * block below.
	 */

	check_and_dump_old_cluster();


	/* -- NEW -- */
	start_postmaster(&new_cluster, true);

	check_new_cluster();
	report_clusters_compatible();

	pg_log(PG_REPORT,
		   "\n"
		   "Performing Upgrade\n"
		   "------------------");

	set_locale_and_encoding();

	prepare_new_cluster();

	stop_postmaster(false);

	/*
	 * Destructive Changes to New Cluster
	 */

	copy_xact_xlog_xid();
	set_new_cluster_char_signedness();

	/* New now using xids of the old system */

	/*
	 * LEE: the new cluster runs at wal_level=replica (initdb's default; we do
	 * not lower it to minimal).  Recovery is anchored at the end-of-upgrade
	 * checkpoint (CN) and never replays pg_restore's in-process WAL, so that
	 * restore-phase WAL is throwaway.  The whole cluster is instead captured as
	 * full-page images at the very end -- and because the server runs at
	 * replica, those images (and the persisted pg_control) are at a level a
	 * standby can recover from.  Verified: pg_controldata on a completed
	 * --wal-log-upgrade cluster shows wal_level=replica.
	 */
	start_postmaster(&new_cluster, true);

	prepare_new_globals();

	/*
	 * LEE: for --wal-log-upgrade we no longer emit any WAL markers here.  The
	 * entire upgrade image (SLRU segments, relation files, and the
	 * START/COMPLETE markers) is captured as full-page images at the very end,
	 * after everything is on disk and a CHECKPOINT has
	 * flushed all buffers.  See the end-of-upgrade block below.
	 */

	create_new_objects();

	/*
	 * LEE: write the matching XLOG_PG_UPGRADE_COMPLETE marker when
	 * --wal-log-upgrade is set.
	 */
	/*
	 * LEE: always stop the server before transferring relation files, exactly
	 * as stock pg_upgrade does.  (The earlier --wal-log-upgrade variant kept
	 * the server running through the transfer; that was wrong.  The transfer
	 * overwrites the pg_restore-built relation files -- e.g. freshly-built
	 * indexes -- with the old cluster's files by a raw copy/clone/link that
	 * bypasses the buffer manager, so with the server up, pg_restore's stale
	 * index pages would linger dirty in shared buffers and the capture-time
	 * CHECKPOINT would flush them back over the transferred files, corrupting
	 * the captured images -- GIN indexes were the visible symptom.  For
	 * --wal-log-upgrade we restart with a clean buffer pool afterward, so the
	 * capture reads exactly the transferred files.)
	 */
	stop_postmaster(false);

	/*
	 * Most failures happen in create_new_objects(), which has completed at
	 * this point.  We do this here because it is just before file transfer,
	 * which for --link will make it unsafe to start the old cluster once the
	 * new cluster is started, and for --swap will make it unsafe to start the
	 * old cluster at all.
	 */
	if (user_opts.transfer_mode == TRANSFER_MODE_LINK ||
		user_opts.transfer_mode == TRANSFER_MODE_SWAP)
		disable_old_cluster(user_opts.transfer_mode);

	transfer_all_new_tablespaces(&old_cluster.dbarr, &new_cluster.dbarr,
								 old_cluster.pgdata, new_cluster.pgdata);

	/*
	 * Set the new cluster's next OID.  This is the stock upstream step, run
	 * with the server down.  pg_restore consumes OIDs, so it must happen after
	 * the restore.  For --wal-log-upgrade it MUST also happen HERE, before the
	 * end-of-upgrade checkpoint below, so that checkpoint (CN) records the
	 * transplanted OID counter; recovery replays from CN and reproduces every
	 * counter for free.  (Doing it afterward, as a separate pg_resetwal on the
	 * already-sealed cluster, would be discarded on recovery, which
	 * re-initializes the counters from the CN checkpoint record -- that was the
	 * old --control-only design, and it left the new cluster's OID counter too
	 * low, risking collisions with preserved OIDs.)
	 */
	prep_status("Setting next OID for new cluster");
	if (user_opts.wal_log_upgrade)
	{
		/*
		 * LEE: for --wal-log-upgrade, also stamp the OLD cluster's system
		 * identifier here.  This pg_resetwal call resets the WAL, so pg_control
		 * AND the fresh WAL segment both get the old sysid -- consistent.  The
		 * burst server starts next and writes all upgrade WAL with that sysid,
		 * so a physical standby of the old cluster (same sysid) accepts it.
		 */
		uint64		old_sysid = get_control_system_identifier(&old_cluster);

		exec_prog(UTILITY_LOG_FILE, NULL, true, true,
				  "\"%s/pg_resetwal\" -o %u --system-identifier=" UINT64_FORMAT " \"%s\"",
				  new_cluster.bindir, old_cluster.controldata.chkpnt_nxtoid,
				  old_sysid, new_cluster.pgdata);
	}
	else
		exec_prog(UTILITY_LOG_FILE, NULL, true, true,
				  "\"%s/pg_resetwal\" -o %u \"%s\"",
				  new_cluster.bindir, old_cluster.controldata.chkpnt_nxtoid,
				  new_cluster.pgdata);
	check_ok();

	migrate_logical_slots = count_old_cluster_logical_slots();

	if (user_opts.wal_log_upgrade)
	{
		PGconn	   *conn;

		/*
		 * Restart with a fresh buffer pool for the WAL capture phase.  The
		 * server now reads the just-transplanted counters from pg_control, so
		 * the checkpoint we take below captures them.
		 */
		start_postmaster(&new_cluster, true);
		conn = connectToServer(&new_cluster, "template1");

		/*
		 * LEE: everything below is emitted AFTER all pg_upgrade work is
		 * complete, so the upgrade's own WAL is generated as one atomic burst
		 * at the end rather than interleaved with the restore.
		 *
		 * First force every dirty buffer and SLRU page to disk so the on-disk
		 * images we are about to capture are the authoritative final state.
		 * The CHECKPOINT flushes shared buffers; pg_flush_upgrade_slru() then
		 * runs a forced checkpoint and fsyncs the SLRU segment files so the
		 * committed transaction statuses are durable before we read them.
		 */
		PQclear(executeQueryOrDie(conn, "CHECKPOINT"));
		PQclear(executeQueryOrDie(conn, "SELECT pg_flush_upgrade_slru()"));

		/*
		 * LEE: capture CN — the LSN of the checkpoint we just took.  This, not
		 * the initdb checkpoint (C0), is the recovery anchor: replay starts here
		 * and applies ONLY the end-of-upgrade full-page images that follow, so
		 * it never re-runs pg_restore's CREATE DATABASE FILE_COPY records (which
		 * would need the template databases on disk).  Anchoring at CN is what
		 * lets the cluster be reconstructed from an empty data directory.
		 */
		{
			PGresult   *cres = executeQueryOrDie(conn,
												 "SELECT checkpoint_lsn, redo_lsn FROM pg_control_checkpoint()");

			strlcpy(user_opts.upgrade_recovery_lsn, PQgetvalue(cres, 0, 0),
					sizeof(user_opts.upgrade_recovery_lsn));
			strlcpy(user_opts.upgrade_recovery_redo_lsn, PQgetvalue(cres, 0, 1),
					sizeof(user_opts.upgrade_recovery_redo_lsn));
			PQclear(cres);
		}

		/*
		 * LEE: write the upgrade WAL as full-page images, all into the still-
		 * running server so they land in pg_wal/:
		 *  1. PG_UPGRADE_START (with PG_VERSION string)
		 *  2. RELFILE records (FPI of every relation file)
		 *  3. SLRU records   (FPI of pg_xact / pg_multixact segments)
		 *  4. PG_UPGRADE_COMPLETE (terminal marker)
		 *  5. pg_switch_wal() — seal the segment so all records are durable
		 *
		 * The XID/OID/multixact counters are NOT emitted as a separate record:
		 * they were transplanted into pg_control before the CHECKPOINT above, so
		 * the CN checkpoint record already carries them and recovery reproduces
		 * them.
		 */
		PQclear(executeQueryOrDie(conn,
								 "SELECT pg_write_pg_upgrade_start(%u, %u)",
								 old_cluster.major_version,
								 new_cluster.major_version));
		/*
		 * LEE: capture the directory-tree after-image immediately after START
		 * and before any file image, so replay recreates the full initdb
		 * directory skeleton (base, global, pg_xact, pg_multixact, and the
		 * transient runtime dirs) before a relfile/SLRU image is written into
		 * it.  This makes the skeleton part of the WAL, not something the
		 * recovering server must find already on disk.
		 */
		PQclear(executeQueryOrDie(conn,
								 "SELECT pg_write_upgrade_dirskel()"));
		PQclear(executeQueryOrDie(conn,
								 "SELECT pg_write_upgrade_relfile_data()"));
		PQclear(executeQueryOrDie(conn,
								 "SELECT pg_write_upgrade_slru_data(0)"));	/* pg_xact */
		PQclear(executeQueryOrDie(conn,
								 "SELECT pg_write_upgrade_slru_data(1)"));	/* pg_multixact/offsets */
		PQclear(executeQueryOrDie(conn,
								 "SELECT pg_write_upgrade_slru_data(2)"));	/* pg_multixact/members */
		/*
		 * LEE: test-only hook.  When PG_UPGRADE_TEST_SKIP_COMPLETE is set we omit
		 * the COMPLETE marker to simulate a crash mid-upgrade; first startup must
		 * then FATAL and leave the old cluster intact.  Never set in production.
		 */
		if (getenv("PG_UPGRADE_TEST_SKIP_COMPLETE") == NULL)
			PQclear(executeQueryOrDie(conn,
									 "SELECT pg_write_pg_upgrade_complete(%u, %u)",
									 old_cluster.major_version,
									 new_cluster.major_version));
		PQclear(executeQueryOrDie(conn, "SELECT pg_switch_wal()"));
		PQfinish(conn);

		/*
		 * LEE: immediate stop — no checkpoint, no WAL recycling.  All upgrade
		 * WAL stays intact in pg_wal/.  The rename to pg_wal_upgrade/ and the
		 * --upgrade-recovery pg_control rewrite happen at the very end, after
		 * all other pg_resetwal calls (which require pg_wal/ to exist).
		 */
		stop_postmaster_immediate();

		/*
		 * LEE: skip the disk writes.  Every byte pg_upgrade transferred into
		 * the new cluster (user relation files and the copied SLRU segments) is
		 * now captured as full-page images in pg_wal/.  Revert those on-disk
		 * files to an empty baseline so the persisted new cluster contains NONE
		 * of the upgrade data on disk — only in WAL.  First startup must then
		 * reconstruct the entire cluster purely from WAL replay, which is what
		 * proves the upgrade is fully WAL-recoverable.
		 */
		revert_wal_logged_disk_writes();

		/*
		 * LEE: the transferred-files manifest has served its purpose (the emit
		 * and revert phases read it).  Remove it so it does not linger in the
		 * live cluster.
		 */
		{
			char		manifest_path[MAXPGPATH];

			snprintf(manifest_path, sizeof(manifest_path),
					 "%s/pg_upgrade_transferred_files", new_cluster.pgdata);
			unlink(manifest_path);
		}
	}
	/* (the server was already stopped before the file transfer above) */

	/*
	 * Migrate replication slots to the new cluster.
	 *
	 * Note that we must migrate logical slots after resetting WAL because
	 * otherwise the required WAL would be removed and slots would become
	 * unusable.  There is a possibility that background processes might
	 * generate some WAL before we could create the slots in the new cluster
	 * but we can ignore that WAL as that won't be required downstream.
	 *
	 * The conflict detection slot is not affected by concerns related to WALs
	 * as it only retains the dead tuples. It is created here for consistency.
	 * Note that the new conflict detection slot uses the latest transaction
	 * ID as xmin, so it cannot protect dead tuples that existed before the
	 * upgrade. Additionally, commit timestamps and origin data are not
	 * preserved during the upgrade. So, even after creating the slot, the
	 * upgraded subscriber may be unable to detect conflicts or log relevant
	 * commit timestamps and origins when applying changes from the publisher
	 * occurred before the upgrade especially if those changes were not
	 * replicated. It can only protect tuples that might be deleted after the
	 * new cluster starts.
	 */
	if (migrate_logical_slots || old_cluster.sub_retain_dead_tuples)
	{
		start_postmaster(&new_cluster, true);

		if (migrate_logical_slots)
			create_logical_replication_slots();

		if (old_cluster.sub_retain_dead_tuples)
			create_conflict_detection_slot();

		stop_postmaster(false);
	}

	if (user_opts.do_sync)
	{
		prep_status("Sync data directory to disk");
		exec_prog(UTILITY_LOG_FILE, NULL, true, true,
				  "\"%s/initdb\" --sync-only %s \"%s\" --sync-method %s",
				  new_cluster.bindir,
				  (user_opts.transfer_mode == TRANSFER_MODE_SWAP) ?
				  "--no-sync-data-files" : "",
				  new_cluster.pgdata,
				  user_opts.sync_method);
		check_ok();
	}

	create_script_for_old_cluster_deletion(&deletion_script_file_name);

	/*
	 * LEE: for --wal-log-upgrade, the upgrade WAL in pg_wal/ must not be
	 * recycled by a server restart.  issue_warnings_and_set_wal_level()
	 * unconditionally starts/stops the server, which would checkpoint and
	 * recycle that WAL — so we skip it here and instead preserve the WAL and
	 * arm crash recovery from CN.  This is the atomic commit point: the whole
	 * upgrade is applied at once on next startup, with no midway rollback.
	 */
	if (user_opts.wal_log_upgrade)
	{
		/*
		 * LEE: arm crash recovery — set pg_control.checkPoint = CN (the
		 * end-of-upgrade checkpoint) and state = DB_IN_PRODUCTION.  We pass both
		 * the checkpoint record LSN and its redo LSN: for an online checkpoint
		 * the redo precedes the record (with an XLOG_CHECKPOINT_REDO marker
		 * there), and StartupXLOG uses pg_control.checkPointCopy.redo directly.
		 * The upgrade WAL stays in pg_wal/; on next startup StartupXLOG()
		 * crash-recovers from CN through PG_UPGRADE_COMPLETE, applying the whole
		 * upgrade at once.  No rename is needed — the WAL content itself (the
		 * START/COMPLETE records) is the signal.
		 */
		if (user_opts.upgrade_recovery_lsn[0])
		{
			prep_status("Setting pg_control for upgrade WAL recovery");
			exec_prog(UTILITY_LOG_FILE, NULL, true, true,
					  "\"%s/pg_resetwal\" --upgrade-recovery=%s,%s \"%s\"",
					  new_cluster.bindir,
					  user_opts.upgrade_recovery_lsn,
					  user_opts.upgrade_recovery_redo_lsn,
					  new_cluster.pgdata);
			check_ok();
		}
	}
	else
		issue_warnings_and_set_wal_level();

	pg_log(PG_REPORT,
		   "\n"
		   "Upgrade Complete\n"
		   "----------------");

	output_completion_banner(deletion_script_file_name);

	pg_free(deletion_script_file_name);

	cleanup_output_dirs();

	return 0;
}

/*
 * Create and assign proper permissions to the set of output directories
 * used to store any data generated internally, filling in log_opts in
 * the process.
 */
static void
make_outputdirs(char *pgdata)
{
	FILE	   *fp;
	char	  **filename;
	time_t		run_time = time(NULL);
	char		filename_path[MAXPGPATH];
	char		timebuf[128];
	struct timeval time;
	time_t		tt;
	int			len;

	log_opts.rootdir = (char *) pg_malloc0(MAXPGPATH);
	len = snprintf(log_opts.rootdir, MAXPGPATH, "%s/%s", pgdata, BASE_OUTPUTDIR);
	if (len >= MAXPGPATH)
		pg_fatal("directory path for new cluster is too long");

	/* BASE_OUTPUTDIR/$timestamp/ */
	gettimeofday(&time, NULL);
	tt = (time_t) time.tv_sec;
	strftime(timebuf, sizeof(timebuf), "%Y%m%dT%H%M%S", localtime(&tt));
	/* append milliseconds */
	snprintf(timebuf + strlen(timebuf), sizeof(timebuf) - strlen(timebuf),
			 ".%03d", (int) (time.tv_usec / 1000));
	log_opts.basedir = (char *) pg_malloc0(MAXPGPATH);
	len = snprintf(log_opts.basedir, MAXPGPATH, "%s/%s", log_opts.rootdir,
				   timebuf);
	if (len >= MAXPGPATH)
		pg_fatal("directory path for new cluster is too long");

	/* BASE_OUTPUTDIR/$timestamp/dump/ */
	log_opts.dumpdir = (char *) pg_malloc0(MAXPGPATH);
	len = snprintf(log_opts.dumpdir, MAXPGPATH, "%s/%s/%s", log_opts.rootdir,
				   timebuf, DUMP_OUTPUTDIR);
	if (len >= MAXPGPATH)
		pg_fatal("directory path for new cluster is too long");

	/* BASE_OUTPUTDIR/$timestamp/log/ */
	log_opts.logdir = (char *) pg_malloc0(MAXPGPATH);
	len = snprintf(log_opts.logdir, MAXPGPATH, "%s/%s/%s", log_opts.rootdir,
				   timebuf, LOG_OUTPUTDIR);
	if (len >= MAXPGPATH)
		pg_fatal("directory path for new cluster is too long");

	/*
	 * Ignore the error case where the root path exists, as it is kept the
	 * same across runs.
	 */
	if (mkdir(log_opts.rootdir, pg_dir_create_mode) < 0 && errno != EEXIST)
		pg_fatal("could not create directory \"%s\": %m", log_opts.rootdir);
	if (mkdir(log_opts.basedir, pg_dir_create_mode) < 0)
		pg_fatal("could not create directory \"%s\": %m", log_opts.basedir);
	if (mkdir(log_opts.dumpdir, pg_dir_create_mode) < 0)
		pg_fatal("could not create directory \"%s\": %m", log_opts.dumpdir);
	if (mkdir(log_opts.logdir, pg_dir_create_mode) < 0)
		pg_fatal("could not create directory \"%s\": %m", log_opts.logdir);

	len = snprintf(filename_path, sizeof(filename_path), "%s/%s",
				   log_opts.logdir, INTERNAL_LOG_FILE);
	if (len >= sizeof(filename_path))
		pg_fatal("directory path for new cluster is too long");

	if ((log_opts.internal = fopen_priv(filename_path, "a")) == NULL)
		pg_fatal("could not open log file \"%s\": %m", filename_path);

	/* label start of upgrade in logfiles */
	for (filename = output_files; *filename != NULL; filename++)
	{
		len = snprintf(filename_path, sizeof(filename_path), "%s/%s",
					   log_opts.logdir, *filename);
		if (len >= sizeof(filename_path))
			pg_fatal("directory path for new cluster is too long");
		if ((fp = fopen_priv(filename_path, "a")) == NULL)
			pg_fatal("could not write to log file \"%s\": %m", filename_path);

		fprintf(fp,
				"-----------------------------------------------------------------\n"
				"  pg_upgrade run on %s"
				"-----------------------------------------------------------------\n\n",
				ctime(&run_time));
		fclose(fp);
	}
}


/*
 * resolve_new_bindir()
 *
 * Idempotent helper: if new_cluster.bindir has not been set by the user via
 * -B, derive it from the path of the currently executing pg_upgrade binary.
 * Called early by create_new_cluster_via_initdb() so that the initdb path
 * is available before verify_directories() runs.
 */
static void
resolve_new_bindir(const char *argv0)
{
	if (!new_cluster.bindir)
	{
		char		exec_path[MAXPGPATH];

		if (find_my_exec(argv0, exec_path) < 0)
			pg_fatal("%s: could not find own program executable", argv0);
		/* Trim off program name and keep just the directory */
		*last_dir_separator(exec_path) = '\0';
		canonicalize_path(exec_path);
		new_cluster.bindir = pg_strdup(exec_path);
	}
}


/*
 * create_new_cluster_via_initdb()
 *
 * Implements --initdb: run initdb to create the new cluster before upgrading,
 * deriving WAL segment size, data checksums, encoding, and locale settings
 * from the old cluster so that check_control_data() passes.
 *
 * This runs before the normal verify_directories() / setup() path, so we
 * use a temporary log directory under the new bindir for the early server
 * start; make_outputdirs() will replace log_opts.logdir later.
 */
static void
create_new_cluster_via_initdb(const char *argv0)
{
	DbLocaleInfo *locale;
	PQExpBufferData cmd;
	char		tmp_logdir[MAXPGPATH];
	char	   *saved_logdir = log_opts.logdir;
	const char *encoding_name;

	/* LEE: pass argv0 (full path) so find_my_exec can locate the binary */
	resolve_new_bindir(argv0);

	/*
	 * Verify that initdb is present and executable before doing any work.
	 * The normal path checks this later inside verify_directories(), but we
	 * run before that, so fail early with a useful message.
	 */
	{
		char		initdb_path[MAXPGPATH];

		snprintf(initdb_path, sizeof(initdb_path), "%s/initdb",
				 new_cluster.bindir);
		if (validate_exec(initdb_path) != 0)
			pg_fatal("could not find \"initdb\" in \"%s\": %m\n"
					 "The --initdb option requires initdb to be present in the new cluster's bin directory.",
					 new_cluster.bindir);
	}

	old_cluster.major_version = get_pg_version(old_cluster.pgdata,
											   &old_cluster.major_version_str);

	/*
	 * get_control_data() selects pg_resetwal vs. pg_resetxlog via
	 * bin_version, which check_bindir() normally fills in later.  Seed it
	 * now so the right binary name is used in this early call.
	 */
	if (old_cluster.bin_version == 0)
		old_cluster.bin_version = old_cluster.major_version;

	/* Refuse to overwrite an existing cluster. */
	{
		char		verfile[MAXPGPATH];
		struct stat st;

		snprintf(verfile, sizeof(verfile), "%s/PG_VERSION",
				 new_cluster.pgdata);
		if (stat(verfile, &st) == 0)
			pg_fatal("new cluster data directory \"%s\" already contains a database system; "
					 "--initdb requires an empty or nonexistent directory",
					 new_cluster.pgdata);
	}

	get_control_data(&old_cluster);

	/* Set up a temporary log directory for the early server start. */
	snprintf(tmp_logdir, sizeof(tmp_logdir), "%s/pg_upgrade_initdb.log.d",
			 new_cluster.bindir);
	if (mkdir(tmp_logdir, pg_dir_create_mode) < 0 && errno != EEXIST)
		pg_fatal("could not create temporary log directory \"%s\": %m",
				 tmp_logdir);
	log_opts.logdir = tmp_logdir;

	if (!old_cluster.sockdir)
		old_cluster.sockdir = user_opts.socketdir ? user_opts.socketdir : ".";

	prep_status("Inspecting old cluster locale for new cluster creation");
	start_postmaster(&old_cluster, true);
	get_template0_info(&old_cluster);
	stop_postmaster(false);
	check_ok();

	locale = old_cluster.template0;
	encoding_name = pg_encoding_to_char(locale->db_encoding);

	prep_status("Creating new cluster with initdb");


	initPQExpBuffer(&cmd);
	appendPQExpBuffer(&cmd, "\"%s/initdb\" -D \"%s\" -N",
					  new_cluster.bindir, new_cluster.pgdata);
	appendPQExpBuffer(&cmd, " -U \"%s\"", os_info.user);
	appendPQExpBuffer(&cmd, " --wal-segsize=%u",
					  old_cluster.controldata.walseg / (1024 * 1024));

	/*
	 * Pass --data-checksums or --no-data-checksums explicitly.  Starting
	 * from PG18, initdb enables checksums by default, so we must mirror the
	 * old cluster's setting to avoid a mismatch that check_control_data()
	 * would reject.
	 */
	if (old_cluster.controldata.data_checksum_version != 0)
		appendPQExpBufferStr(&cmd, " --data-checksums");
	else
		appendPQExpBufferStr(&cmd, " --no-data-checksums");

	appendPQExpBuffer(&cmd, " --encoding=%s", encoding_name);
	appendPQExpBuffer(&cmd, " --locale-provider=%s",
					  collprovider_name(locale->db_collprovider));
	appendPQExpBuffer(&cmd, " --lc-collate=\"%s\" --lc-ctype=\"%s\"",
					  locale->db_collate, locale->db_ctype);

	if (locale->db_locale)
	{
		if (locale->db_collprovider == COLLPROVIDER_ICU)
			appendPQExpBuffer(&cmd, " --icu-locale=\"%s\"",
							  locale->db_locale);
		else if (locale->db_collprovider == COLLPROVIDER_BUILTIN)
			appendPQExpBuffer(&cmd, " --builtin-locale=\"%s\"",
							  locale->db_locale);
	}

	if (new_cluster.pgopts)
		appendPQExpBuffer(&cmd, " %s", new_cluster.pgopts);

	exec_prog(UTILITY_LOG_FILE, NULL, true, true, "%s", cmd.data);

	termPQExpBuffer(&cmd);
	log_opts.logdir = saved_logdir;

	check_ok();
}


static void
setup(char *argv0)
{
	/*
	 * make sure the user has a clean environment, otherwise, we may confuse
	 * libpq when we connect to one (or both) of the servers.
	 */
	check_pghost_envvar();

	/*
	 * In case the user hasn't specified the directory for the new binaries
	 * with -B, default to using the path of the currently executed pg_upgrade
	 * binary.
	 */
	resolve_new_bindir(argv0);

	verify_directories();

	/* no postmasters should be running, except for a live check */
	if (pid_lock_file_exists(old_cluster.pgdata))
	{
		/*
		 * If we have a postmaster.pid file, try to start the server.  If it
		 * starts, the pid file was stale, so stop the server.  If it doesn't
		 * start, assume the server is running.  If the pid file is left over
		 * from a server crash, this also allows any committed transactions
		 * stored in the WAL to be replayed so they are not lost, because WAL
		 * files are not transferred from old to new servers.  We later check
		 * for a clean shutdown.
		 */
		if (start_postmaster(&old_cluster, false))
			stop_postmaster(false);
		else
		{
			if (!user_opts.check)
				pg_fatal("There seems to be a postmaster servicing the old cluster.\n"
						 "Please shutdown that postmaster and try again.");
			else
				user_opts.live_check = true;
		}
	}

	/* same goes for the new postmaster */
	if (pid_lock_file_exists(new_cluster.pgdata))
	{
		if (start_postmaster(&new_cluster, false))
			stop_postmaster(false);
		else
			pg_fatal("There seems to be a postmaster servicing the new cluster.\n"
					 "Please shutdown that postmaster and try again.");
	}
}

/*
 * Set the new cluster's default char signedness using the old cluster's
 * value.
 */
static void
set_new_cluster_char_signedness(void)
{
	bool		new_char_signedness;

	/*
	 * Use the specified char signedness if specified. Otherwise we inherit
	 * the source database's signedness.
	 */
	if (user_opts.char_signedness != -1)
		new_char_signedness = (user_opts.char_signedness == 1);
	else
		new_char_signedness = old_cluster.controldata.default_char_signedness;

	/* Change the char signedness of the new cluster, if necessary */
	if (new_cluster.controldata.default_char_signedness != new_char_signedness)
	{
		prep_status("Setting the default char signedness for new cluster");

		exec_prog(UTILITY_LOG_FILE, NULL, true, true,
				  "\"%s/pg_resetwal\" --char-signedness %s \"%s\"",
				  new_cluster.bindir,
				  new_char_signedness ? "signed" : "unsigned",
				  new_cluster.pgdata);

		check_ok();
	}
}

/*
 * Copy locale and encoding information into the new cluster's template0.
 *
 * We need to copy the encoding, datlocprovider, datcollate, datctype, and
 * datlocale. We don't need datcollversion because that's never set for
 * template0.
 */
static void
set_locale_and_encoding(void)
{
	PGconn	   *conn_new_template1;
	char	   *datcollate_literal;
	char	   *datctype_literal;
	char	   *datlocale_literal = NULL;
	DbLocaleInfo *locale = old_cluster.template0;

	prep_status("Setting locale and encoding for new cluster");

	/* escape literals with respect to new cluster */
	conn_new_template1 = connectToServer(&new_cluster, "template1");

	datcollate_literal = PQescapeLiteral(conn_new_template1,
										 locale->db_collate,
										 strlen(locale->db_collate));
	datctype_literal = PQescapeLiteral(conn_new_template1,
									   locale->db_ctype,
									   strlen(locale->db_ctype));

	if (locale->db_locale)
		datlocale_literal = PQescapeLiteral(conn_new_template1,
											locale->db_locale,
											strlen(locale->db_locale));
	else
		datlocale_literal = "NULL";

	/* update template0 in new cluster */
	if (GET_MAJOR_VERSION(new_cluster.major_version) >= 1700)
		PQclear(executeQueryOrDie(conn_new_template1,
								  "UPDATE pg_catalog.pg_database "
								  "  SET encoding = %d, "
								  "      datlocprovider = '%c', "
								  "      datcollate = %s, "
								  "      datctype = %s, "
								  "      datlocale = %s "
								  "  WHERE datname = 'template0' ",
								  locale->db_encoding,
								  locale->db_collprovider,
								  datcollate_literal,
								  datctype_literal,
								  datlocale_literal));
	else if (GET_MAJOR_VERSION(new_cluster.major_version) >= 1500)
		PQclear(executeQueryOrDie(conn_new_template1,
								  "UPDATE pg_catalog.pg_database "
								  "  SET encoding = %d, "
								  "      datlocprovider = '%c', "
								  "      datcollate = %s, "
								  "      datctype = %s, "
								  "      daticulocale = %s "
								  "  WHERE datname = 'template0' ",
								  locale->db_encoding,
								  locale->db_collprovider,
								  datcollate_literal,
								  datctype_literal,
								  datlocale_literal));
	else
		PQclear(executeQueryOrDie(conn_new_template1,
								  "UPDATE pg_catalog.pg_database "
								  "  SET encoding = %d, "
								  "      datcollate = %s, "
								  "      datctype = %s "
								  "  WHERE datname = 'template0' ",
								  locale->db_encoding,
								  datcollate_literal,
								  datctype_literal));

	PQfreemem(datcollate_literal);
	PQfreemem(datctype_literal);
	if (locale->db_locale)
		PQfreemem(datlocale_literal);

	PQfinish(conn_new_template1);

	check_ok();
}


static void
prepare_new_cluster(void)
{
	/*
	 * It would make more sense to freeze after loading the schema, but that
	 * would cause us to lose the frozenxids restored by the load. We use
	 * --analyze so autovacuum doesn't update statistics later
	 */
	prep_status("Analyzing all rows in the new cluster");
	exec_prog(UTILITY_LOG_FILE, NULL, true, true,
			  "\"%s/vacuumdb\" %s --all --analyze %s",
			  new_cluster.bindir, cluster_conn_opts(&new_cluster),
			  log_opts.verbose ? "--verbose" : "");
	check_ok();

	/*
	 * We do freeze after analyze so pg_statistic is also frozen. template0 is
	 * not frozen here, but data rows were frozen by initdb, and we set its
	 * datfrozenxid, relfrozenxids, and relminmxid later to match the new xid
	 * counter later.
	 */
	prep_status("Freezing all rows in the new cluster");
	exec_prog(UTILITY_LOG_FILE, NULL, true, true,
			  "\"%s/vacuumdb\" %s --all --freeze %s",
			  new_cluster.bindir, cluster_conn_opts(&new_cluster),
			  log_opts.verbose ? "--verbose" : "");
	check_ok();
}


static void
prepare_new_globals(void)
{
	/*
	 * Before we restore anything, set frozenxids of initdb-created tables.
	 */
	set_frozenxids();

	/*
	 * Now restore global objects (roles and tablespaces).
	 */
	prep_status("Restoring global objects in the new cluster");

	exec_prog(UTILITY_LOG_FILE, NULL, true, true,
			  "\"%s/psql\" " EXEC_PSQL_ARGS " %s -f \"%s/%s\"",
			  new_cluster.bindir, cluster_conn_opts(&new_cluster),
			  log_opts.dumpdir,
			  GLOBALS_DUMP_FILE);
	check_ok();
}


static void
create_new_objects(void)
{
	int			dbnum;
	PGconn	   *conn_new_template1;
	/* LEE: LSN snapshot before and after pg_restore to measure WAL generated */
	PGresult   *lsn_res;
	uint64		lsn_before = 0,
				lsn_after = 0;

	prep_status_progress("Restoring database schemas in the new cluster");

	/*
	 * Ensure that any changes to template0 are fully written out to disk
	 * prior to restoring the databases.  This is necessary because we use the
	 * FILE_COPY strategy to create the databases (which testing has shown to
	 * be faster), and when the server is in binary upgrade mode, it skips the
	 * checkpoints this strategy ordinarily performs.
	 */
	conn_new_template1 = connectToServer(&new_cluster, "template1");
	PQclear(executeQueryOrDie(conn_new_template1, "CHECKPOINT"));

	/*
	 * LEE: snapshot the WAL position before pg_restore so we can measure how
	 * many bytes the schema restore generates.  Only for --wal-log-upgrade;
	 * without it this whole measurement is skipped so the flow matches stock
	 * pg_upgrade exactly.
	 */
	if (user_opts.wal_log_upgrade)
	{
		lsn_res = executeQueryOrDie(conn_new_template1,
									"SELECT pg_current_wal_lsn() - '0/0'");
		lsn_before = strtoull(PQgetvalue(lsn_res, 0, 0), NULL, 10);
		PQclear(lsn_res);
	}

	PQfinish(conn_new_template1);

	/*
	 * We cannot process the template1 database concurrently with others,
	 * because when it's transiently dropped, connection attempts would fail.
	 * So handle it in a separate non-parallelized pass.
	 */
	for (dbnum = 0; dbnum < old_cluster.dbarr.ndbs; dbnum++)
	{
		char		sql_file_name[MAXPGPATH],
					log_file_name[MAXPGPATH];
		DbInfo	   *old_db = &old_cluster.dbarr.dbs[dbnum];
		const char *create_opts;

		/* Process only template1 in this pass */
		if (strcmp(old_db->db_name, "template1") != 0)
			continue;

		pg_log(PG_STATUS, "%s", old_db->db_name);
		snprintf(sql_file_name, sizeof(sql_file_name), DB_DUMP_FILE_MASK, old_db->db_oid);
		snprintf(log_file_name, sizeof(log_file_name), DB_DUMP_LOG_FILE_MASK, old_db->db_oid);

		/*
		 * template1 database will already exist in the target installation,
		 * so tell pg_restore to drop and recreate it; otherwise we would fail
		 * to propagate its database-level properties.
		 */
		create_opts = "--clean --create";

		exec_prog(log_file_name,
				  NULL,
				  true,
				  true,
				  "\"%s/pg_restore\" %s %s --exit-on-error --verbose "
				  "--transaction-size=%d "
				  "--dbname postgres \"%s/%s\"",
				  new_cluster.bindir,
				  cluster_conn_opts(&new_cluster),
				  create_opts,
				  RESTORE_TRANSACTION_SIZE,
				  log_opts.dumpdir,
				  sql_file_name);

		break;					/* done once we've processed template1 */
	}

	for (dbnum = 0; dbnum < old_cluster.dbarr.ndbs; dbnum++)
	{
		char		sql_file_name[MAXPGPATH],
					log_file_name[MAXPGPATH];
		DbInfo	   *old_db = &old_cluster.dbarr.dbs[dbnum];
		const char *create_opts;
		int			txn_size;

		/* Skip template1 in this pass */
		if (strcmp(old_db->db_name, "template1") == 0)
			continue;

		pg_log(PG_STATUS, "%s", old_db->db_name);
		snprintf(sql_file_name, sizeof(sql_file_name), DB_DUMP_FILE_MASK, old_db->db_oid);
		snprintf(log_file_name, sizeof(log_file_name), DB_DUMP_LOG_FILE_MASK, old_db->db_oid);

		/*
		 * postgres database will already exist in the target installation, so
		 * tell pg_restore to drop and recreate it; otherwise we would fail to
		 * propagate its database-level properties.
		 */
		if (strcmp(old_db->db_name, "postgres") == 0)
			create_opts = "--clean --create";
		else
			create_opts = "--create";

		/*
		 * In parallel mode, reduce the --transaction-size of each restore job
		 * so that the total number of locks that could be held across all the
		 * jobs stays in bounds.
		 */
		txn_size = RESTORE_TRANSACTION_SIZE;
		if (user_opts.jobs > 1)
		{
			txn_size /= user_opts.jobs;
			/* Keep some sanity if -j is huge */
			txn_size = Max(txn_size, 10);
		}

		parallel_exec_prog(log_file_name,
						   NULL,
						   "\"%s/pg_restore\" %s %s --exit-on-error --verbose "
						   "--transaction-size=%d "
						   "--dbname template1 \"%s/%s\"",
						   new_cluster.bindir,
						   cluster_conn_opts(&new_cluster),
						   create_opts,
						   txn_size,
						   log_opts.dumpdir,
						   sql_file_name);
	}

	/* reap all children */
	while (reap_child(true) == true)
		;

	end_progress_output();
	check_ok();

	/*
	 * LEE: measure WAL bytes generated by pg_restore for debugging.  We
	 * reconnect briefly to read the current WAL position and compute the
	 * delta since lsn_before.  Only for --wal-log-upgrade; skipped otherwise so
	 * the flow matches stock pg_upgrade exactly.
	 */
	if (user_opts.wal_log_upgrade)
	{
		conn_new_template1 = connectToServer(&new_cluster, "template1");
		lsn_res = executeQueryOrDie(conn_new_template1,
									"SELECT pg_current_wal_lsn() - '0/0'");
		lsn_after = strtoull(PQgetvalue(lsn_res, 0, 0), NULL, 10);
		PQclear(lsn_res);
		PQfinish(conn_new_template1);

		log_opts.pg_upgrade_wal_bytes = lsn_after - lsn_before;
		pg_log(PG_VERBOSE, "pg_upgrade_wal_bytes: " UINT64_FORMAT,
			   log_opts.pg_upgrade_wal_bytes);
	}

	/* update new_cluster info now that we have objects in the databases */
	get_db_rel_and_slot_infos(&new_cluster);
}

/*
 * Delete the given subdirectory contents from the new cluster
 */
static void
remove_new_subdir(const char *subdir, bool rmtopdir)
{
	char		new_path[MAXPGPATH];

	prep_status("Deleting files from new %s", subdir);

	snprintf(new_path, sizeof(new_path), "%s/%s", new_cluster.pgdata, subdir);
	if (!rmtree(new_path, rmtopdir))
		pg_fatal("could not delete directory \"%s\"", new_path);

	check_ok();
}

/*
 * Copy the files from the old cluster into it
 */
static void
copy_subdir_files(const char *old_subdir, const char *new_subdir)
{
	char		old_path[MAXPGPATH];
	char		new_path[MAXPGPATH];

	remove_new_subdir(new_subdir, true);

	snprintf(old_path, sizeof(old_path), "%s/%s", old_cluster.pgdata, old_subdir);
	snprintf(new_path, sizeof(new_path), "%s/%s", new_cluster.pgdata, new_subdir);

	prep_status("Copying old %s to new server", old_subdir);

	exec_prog(UTILITY_LOG_FILE, NULL, true, true,
#ifndef WIN32
			  "cp -Rf \"%s\" \"%s\"",
#else
	/* flags: everything, no confirm, quiet, overwrite read-only */
			  "xcopy /e /y /q /r \"%s\" \"%s\\\"",
#endif
			  old_path, new_path);

	check_ok();
}

static void
copy_xact_xlog_xid(void)
{
	/*
	 * Copy old commit logs to new data dir. pg_clog has been renamed to
	 * pg_xact in post-10 clusters.
	 */
	copy_subdir_files("pg_xact", "pg_xact");

	/*
	 * LEE: these are the stock upstream pg_resetwal commands, identical for
	 * --wal-log-upgrade.  We deliberately do NOT use --control-only: they run
	 * before pg_restore, so the WAL they reset is throwaway, and for
	 * --wal-log-upgrade the counter values they write are captured later by the
	 * end-of-upgrade checkpoint (CN) that recovery replays from -- no separate
	 * WAL record or WAL-preserving flag is needed.
	 */
	prep_status("Setting oldest XID for new cluster");
	exec_prog(UTILITY_LOG_FILE, NULL, true, true,
			  "\"%s/pg_resetwal\" -f -u %u \"%s\"",
			  new_cluster.bindir, old_cluster.controldata.chkpnt_oldstxid,
			  new_cluster.pgdata);
	check_ok();

	/* set the next transaction id and epoch of the new cluster */
	prep_status("Setting next transaction ID and epoch for new cluster");
	exec_prog(UTILITY_LOG_FILE, NULL, true, true,
			  "\"%s/pg_resetwal\" -f -x %u \"%s\"",
			  new_cluster.bindir, old_cluster.controldata.chkpnt_nxtxid,
			  new_cluster.pgdata);
	exec_prog(UTILITY_LOG_FILE, NULL, true, true,
			  "\"%s/pg_resetwal\" -f -e %u \"%s\"",
			  new_cluster.bindir, old_cluster.controldata.chkpnt_nxtepoch,
			  new_cluster.pgdata);
	/* must reset commit timestamp limits also */
	exec_prog(UTILITY_LOG_FILE, NULL, true, true,
			  "\"%s/pg_resetwal\" -f -c %u,%u \"%s\"",
			  new_cluster.bindir,
			  old_cluster.controldata.chkpnt_nxtxid,
			  old_cluster.controldata.chkpnt_nxtxid,
			  new_cluster.pgdata);
	check_ok();

	/* Copy or convert pg_multixact files */
	Assert(new_cluster.controldata.cat_ver >= MULTIXACTOFFSET_FORMATCHANGE_CAT_VER);
	if (old_cluster.controldata.cat_ver >= MULTIXACTOFFSET_FORMATCHANGE_CAT_VER)
	{
		/* No change in multixact format, just copy the files */
		MultiXactId new_nxtmulti = old_cluster.controldata.chkpnt_nxtmulti;
		MultiXactOffset new_nxtmxoff = old_cluster.controldata.chkpnt_nxtmxoff;

		copy_subdir_files("pg_multixact/offsets", "pg_multixact/offsets");
		copy_subdir_files("pg_multixact/members", "pg_multixact/members");

		prep_status("Setting next multixact ID and offset for new cluster");

		/*
		 * we preserve all files and contents, so we must preserve both "next"
		 * counters here and the oldest multi present on system.
		 */
		exec_prog(UTILITY_LOG_FILE, NULL, true, true,
				  "\"%s/pg_resetwal\" -O %" PRIu64 " -m %u,%u \"%s\"",
				  new_cluster.bindir, new_nxtmxoff, new_nxtmulti,
				  old_cluster.controldata.chkpnt_oldstMulti,
				  new_cluster.pgdata);
		check_ok();
	}
	else
	{
		/* Conversion is needed */
		MultiXactId nxtmulti;
		MultiXactId oldstMulti;
		MultiXactOffset nxtmxoff;

		/*
		 * Determine the range of multixacts to convert.
		 */
		nxtmulti = old_cluster.controldata.chkpnt_nxtmulti;
		oldstMulti = old_cluster.controldata.chkpnt_oldstMulti;
		/* handle wraparound */
		if (nxtmulti < FirstMultiXactId)
			nxtmulti = FirstMultiXactId;
		if (oldstMulti < FirstMultiXactId)
			oldstMulti = FirstMultiXactId;

		/*
		 * Remove the files created by initdb in the new cluster.
		 * rewrite_multixacts() will create new ones.
		 */
		remove_new_subdir("pg_multixact/members", false);
		remove_new_subdir("pg_multixact/offsets", false);

		/*
		 * Create new pg_multixact files, converting old ones if needed.
		 */
		prep_status("Converting pg_multixact files");
		nxtmxoff = rewrite_multixacts(oldstMulti, nxtmulti);
		check_ok();

		prep_status("Setting next multixact ID and offset for new cluster");
		exec_prog(UTILITY_LOG_FILE, NULL, true, true,
				  "\"%s/pg_resetwal\" -O %" PRIu64 " -m %u,%u \"%s\"",
				  new_cluster.bindir,
				  nxtmxoff, nxtmulti, oldstMulti,
				  new_cluster.pgdata);
		check_ok();
	}

	/*
	 * Now reset the WAL archives in the new cluster.  This is the stock
	 * pg_upgrade step and runs identically for --wal-log-upgrade: it happens
	 * before pg_restore, so the WAL it resets is throwaway either way.
	 */
	prep_status("Resetting WAL archives");
	exec_prog(UTILITY_LOG_FILE, NULL, true, true,
	/* use timeline 1 to match controldata and no WAL history file */
			  "\"%s/pg_resetwal\" -l 00000001%s \"%s\"", new_cluster.bindir,
			  old_cluster.controldata.nextxlogfile + 8,
			  new_cluster.pgdata);
	check_ok();
}


/*
 *	set_frozenxids()
 *
 * This is called on the new cluster before we restore anything.
 * Its purpose is to ensure that all initdb-created
 * vacuumable tables have relfrozenxid/relminmxid matching the old cluster's
 * xid/mxid counters.  We also initialize the datfrozenxid/datminmxid of the
 * built-in databases to match.
 *
 * As we create user tables later, their relfrozenxid/relminmxid fields will
 * be restored properly by the binary-upgrade restore script.  Likewise for
 * user-database datfrozenxid/datminmxid.
 */
static void
set_frozenxids(void)
{
	int			dbnum;
	PGconn	   *conn,
			   *conn_template1;
	PGresult   *dbres;
	int			ntups;
	int			i_datname;
	int			i_datallowconn;

	prep_status("Setting frozenxid and minmxid counters in new cluster");

	conn_template1 = connectToServer(&new_cluster, "template1");

	/* set pg_database.datfrozenxid */
	PQclear(executeQueryOrDie(conn_template1,
							  "UPDATE pg_catalog.pg_database "
							  "SET	datfrozenxid = '%u'",
							  old_cluster.controldata.chkpnt_nxtxid));

	/* set pg_database.datminmxid */
	PQclear(executeQueryOrDie(conn_template1,
							  "UPDATE pg_catalog.pg_database "
							  "SET	datminmxid = '%u'",
							  old_cluster.controldata.chkpnt_nxtmulti));

	/* get database names */
	dbres = executeQueryOrDie(conn_template1,
							  "SELECT	datname, datallowconn "
							  "FROM	pg_catalog.pg_database");

	i_datname = PQfnumber(dbres, "datname");
	i_datallowconn = PQfnumber(dbres, "datallowconn");

	ntups = PQntuples(dbres);
	for (dbnum = 0; dbnum < ntups; dbnum++)
	{
		char	   *datname = PQgetvalue(dbres, dbnum, i_datname);
		char	   *datallowconn = PQgetvalue(dbres, dbnum, i_datallowconn);

		/*
		 * We must update databases where datallowconn = false, e.g.
		 * template0, because autovacuum increments their datfrozenxids,
		 * relfrozenxids, and relminmxid even if autovacuum is turned off, and
		 * even though all the data rows are already frozen.  To enable this,
		 * we temporarily change datallowconn.
		 */
		if (strcmp(datallowconn, "f") == 0)
			PQclear(executeQueryOrDie(conn_template1,
									  "ALTER DATABASE %s ALLOW_CONNECTIONS = true",
									  quote_identifier(datname)));

		conn = connectToServer(&new_cluster, datname);

		/* set pg_class.relfrozenxid */
		PQclear(executeQueryOrDie(conn,
								  "UPDATE	pg_catalog.pg_class "
								  "SET	relfrozenxid = '%u' "
		/* only heap, materialized view, and TOAST are vacuumed */
								  "WHERE	relkind IN ("
								  CppAsString2(RELKIND_RELATION) ", "
								  CppAsString2(RELKIND_MATVIEW) ", "
								  CppAsString2(RELKIND_TOASTVALUE) ")",
								  old_cluster.controldata.chkpnt_nxtxid));

		/* set pg_class.relminmxid */
		PQclear(executeQueryOrDie(conn,
								  "UPDATE	pg_catalog.pg_class "
								  "SET	relminmxid = '%u' "
		/* only heap, materialized view, and TOAST are vacuumed */
								  "WHERE	relkind IN ("
								  CppAsString2(RELKIND_RELATION) ", "
								  CppAsString2(RELKIND_MATVIEW) ", "
								  CppAsString2(RELKIND_TOASTVALUE) ")",
								  old_cluster.controldata.chkpnt_nxtmulti));
		PQfinish(conn);

		/* Reset datallowconn flag */
		if (strcmp(datallowconn, "f") == 0)
			PQclear(executeQueryOrDie(conn_template1,
									  "ALTER DATABASE %s ALLOW_CONNECTIONS = false",
									  quote_identifier(datname)));
	}

	PQclear(dbres);

	PQfinish(conn_template1);

	check_ok();
}

/*
 * unlink every regular file in one directory (optionally keeping one name).
 * Directories are left in place.  Returns the number of files removed.
 */
static int
wipe_dir_files(const char *dirpath, const char *keep)
{
	DIR		   *dir;
	struct dirent *de;
	int			n = 0;

	dir = opendir(dirpath);
	if (dir == NULL)
		return 0;
	while ((de = readdir(dir)) != NULL)
	{
		char		path[MAXPGPATH];
		struct stat st;

		if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
			continue;
		if (keep != NULL && strcmp(de->d_name, keep) == 0)
			continue;

		snprintf(path, sizeof(path), "%s/%s", dirpath, de->d_name);
		if (lstat(path, &st) != 0)
			continue;
		if (!S_ISREG(st.st_mode))
			continue;			/* leave subdirectories/symlinks alone */

		if (unlink(path) == 0)
			n++;
		else if (errno != ENOENT)
			pg_fatal("could not unlink \"%s\": %m", path);
	}
	closedir(dir);
	return n;
}

/*
 * revert_wal_logged_disk_writes()
 *
 * LEE: for --wal-log-upgrade, "skip the disk writes".  By the time this runs
 * (server stopped via immediate stop, WAL sealed in pg_wal/) the ENTIRE
 * physical image of the new cluster has been captured as full-page / raw-file
 * images in the upgrade WAL:
 *
 *   - all relation files (user + system catalogs + global) -> RELFILE_DATA
 *   - pg_filenode.map, PG_VERSION                           -> RAWFILE
 *   - pg_xact / pg_multixact segments                       -> SLRU_DATA
 *   - XID/OID/multixact counters                            -> the CN checkpoint
 *
 * We now wipe those files from disk, leaving only the directory skeleton, the
 * top-level PG_VERSION, and global/pg_control (the entry point recovery needs
 * before it can read any WAL).  The persisted new cluster therefore looks like
 * a fresh, pre-initdb directory tree with empty folders; first startup rebuilds
 * everything purely by replaying the upgrade WAL.  This is what makes the
 * recovery verifiable end to end: if any image were missing, the data would
 * come back wrong or the cluster would fail to start.
 *
 * Files are unlinked (not truncated) so --link mode does not destroy the old
 * cluster's data via shared inodes; replay recreates each file from scratch.
 *
 * NOTE: removing the DIRECTORIES (base/<dboid>, the SLRU dirs) in addition to
 * their files is NOT required for correctness -- replay would happily reuse
 * directories left on disk.  We do it purely for TESTING: wiping the directory
 * tree forces the XLOG_UPGRADE_DIRSKEL redo path to actually recreate every
 * directory, proving it can rebuild the skeleton from WAL alone (the property a
 * standby depends on).  If those rmdir()s were dropped, the upgrade would still
 * be correct; the DIRSKEL replay would just become a no-op instead of being
 * exercised.
 */
static void
revert_wal_logged_disk_writes(void)
{
	char		path[MAXPGPATH];
	DIR		   *basedir;
	struct dirent *de;

	prep_status("Skipping disk writes (wiping data files; only WAL remains)");

	/* Shared catalogs + relmap in global/, but KEEP pg_control. */
	snprintf(path, sizeof(path), "%s/global", new_cluster.pgdata);
	wipe_dir_files(path, "pg_control");

	/*
	 * Every database directory under base/.  We remove the directory itself
	 * too, not just its files -- but only to exercise the DIRSKEL redo path
	 * (see the function header note); it is not needed for correctness, since
	 * XLOG_UPGRADE_DIRSKEL redo recreates base/<dboid> on replay anyway.
	 */
	snprintf(path, sizeof(path), "%s/base", new_cluster.pgdata);
	basedir = opendir(path);
	if (basedir != NULL)
	{
		while ((de = readdir(basedir)) != NULL)
		{
			if (de->d_name[0] < '1' || de->d_name[0] > '9')
				continue;
			snprintf(path, sizeof(path), "%s/base/%s",
					 new_cluster.pgdata, de->d_name);
			wipe_dir_files(path, NULL);
			if (rmdir(path) != 0 && errno != ENOENT)
				pg_fatal("could not remove directory \"%s\": %m", path);
		}
		closedir(basedir);
	}

	/*
	 * SLRU segments (pg_xact, pg_multixact) — captured as SLRU_DATA.  As with
	 * base/ above, removing the directories (not just their files) is a
	 * testing measure only (see the function header note): DIRSKEL redo
	 * recreates them before the SLRU images replay.  Order matters: leaf dirs
	 * before the pg_multixact parent.
	 */
	{
		static const char *const slru_dirs[] = {
			"pg_xact", "pg_multixact/offsets", "pg_multixact/members",
			"pg_multixact"
		};

		for (int d = 0; d < (int) lengthof(slru_dirs); d++)
		{
			snprintf(path, sizeof(path), "%s/%s",
					 new_cluster.pgdata, slru_dirs[d]);
			wipe_dir_files(path, NULL);
			if (rmdir(path) != 0 && errno != ENOENT)
				pg_fatal("could not remove directory \"%s\": %m", path);
		}
	}

	check_ok();
}

/*
 * create_logical_replication_slots()
 *
 * Similar to create_new_objects() but only restores logical replication slots.
 */
static void
create_logical_replication_slots(void)
{
	prep_status_progress("Restoring logical replication slots in the new cluster");

	for (int dbnum = 0; dbnum < old_cluster.dbarr.ndbs; dbnum++)
	{
		DbInfo	   *old_db = &old_cluster.dbarr.dbs[dbnum];
		LogicalSlotInfoArr *slot_arr = &old_db->slot_arr;
		PGconn	   *conn;
		PQExpBuffer query;

		/* Skip this database if there are no slots */
		if (slot_arr->nslots == 0)
			continue;

		conn = connectToServer(&new_cluster, old_db->db_name);
		query = createPQExpBuffer();

		pg_log(PG_STATUS, "%s", old_db->db_name);

		for (int slotnum = 0; slotnum < slot_arr->nslots; slotnum++)
		{
			LogicalSlotInfo *slot_info = &slot_arr->slots[slotnum];

			/* Constructs a query for creating logical replication slots */
			appendPQExpBufferStr(query,
								 "SELECT * FROM "
								 "pg_catalog.pg_create_logical_replication_slot(");
			appendStringLiteralConn(query, slot_info->slotname, conn);
			appendPQExpBufferStr(query, ", ");
			appendStringLiteralConn(query, slot_info->plugin, conn);

			appendPQExpBuffer(query, ", false, %s, %s);",
							  slot_info->two_phase ? "true" : "false",
							  slot_info->failover ? "true" : "false");

			PQclear(executeQueryOrDie(conn, "%s", query->data));

			resetPQExpBuffer(query);
		}

		PQfinish(conn);

		destroyPQExpBuffer(query);
	}

	end_progress_output();
	check_ok();

	return;
}

/*
 * create_conflict_detection_slot()
 *
 * Create a replication slot to retain information necessary for conflict
 * detection such as dead tuples, commit timestamps, and origins, for migrated
 * subscriptions with retain_dead_tuples enabled.
 */
static void
create_conflict_detection_slot(void)
{
	PGconn	   *conn_new_template1;

	prep_status("Creating the replication conflict detection slot");

	conn_new_template1 = connectToServer(&new_cluster, "template1");
	PQclear(executeQueryOrDie(conn_new_template1, "SELECT pg_catalog.binary_upgrade_create_conflict_detection_slot()"));
	PQfinish(conn_new_template1);

	check_ok();
}
