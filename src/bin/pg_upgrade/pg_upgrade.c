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
static char *detect_old_cluster_archive_command(void);
static void write_wal_upgrade_archive_conf(const char *archive_command);
static void create_logical_replication_slots(void);
static void create_conflict_detection_slot(void);

ClusterInfo old_cluster,
			new_cluster;
OSInfo		os_info;

/*
 * For --wal-upgrade, the old cluster's archive_command carried forward to the
 * new cluster (NULL if it was not archiving).  Detected while the old server is
 * up; used to archive the upgrade window so PITR spans the upgrade.
 */
static char *old_cluster_archive_command = NULL;

/*
 * Fault-injection hook: when set, suppress the XLOG_UPGRADE_COMPLETE record
 * (and its durable marker), leaving an incomplete window as a crash mid-window
 * would.  Gated on USE_ASSERT_CHECKING, so a production (non-assert) build
 * ignores the environment variable entirely and can never emit an incomplete
 * window.
 */
static inline bool
wal_upgrade_skip_complete(void)
{
#ifdef USE_ASSERT_CHECKING
	return getenv("PG_UPGRADE_TEST_SKIP_COMPLETE") != NULL;
#else
	return false;
#endif
}

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

	/*
	 * A --wal-upgrade lifecycle subcommand acts on the running old cluster
	 * and exits without running an upgrade.  new_cluster.bindir locates
	 * pg_ctl.
	 */
	if (user_opts.revertable_op != REVERTABLE_OP_NONE)
	{
		resolve_new_bindir(argv[0]);
		perform_revertable_op();
		return 0;
	}


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
	 * The --wal-upgrade recovery anchor (CN) is the checkpoint taken just
	 * before the end-of-upgrade full-page image burst, not the initdb
	 * checkpoint.  See the end-of-upgrade block below.
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

	start_postmaster(&new_cluster, true);

	prepare_new_globals();

	create_new_objects();

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
	 * Set the new cluster's next OID, after the restore that consumes OIDs.
	 * For --wal-upgrade this must precede the end-of-upgrade checkpoint below,
	 * so that checkpoint (CN) records the transplanted OID counter and
	 * recovery from CN reproduces it.
	 */
	if (user_opts.wal_upgrade && old_cluster_archive_command == NULL)
	{
		/*
		 * Streaming path (no archive_command carried to the new cluster): the
		 * reset also positions the new cluster's WAL at the old cluster's next
		 * segment (nextxlogfile, timeline 1), so the reset-written
		 * DB_SHUTDOWNED checkpoint lands on a byte-deterministic segment
		 * boundary (redo == checkPoint == CN) that a fresh standby skeleton can
		 * derive locally from its retained old datadir.  Plain pg_resetwal -l
		 * only floors the start upward, so after the restore (whose checkpoint
		 * is at a high segment) it would be ignored; --wal-upgrade-exact makes
		 * the -l target authoritative and forces the position down to
		 * nextxlogfile.
		 */
		prep_status("Setting next OID and CN log position for new cluster");
		exec_prog(UTILITY_LOG_FILE, NULL, true, true,
		/* timeline 1 to match controldata and emit no WAL history file */
				  "\"%s/pg_resetwal\" --wal-upgrade-exact -o %u -l 00000001%s \"%s\"",
				  new_cluster.bindir, old_cluster.controldata.chkpnt_nxtoid,
				  old_cluster.controldata.nextxlogfile + 8,
				  new_cluster.pgdata);
		check_ok();
	}
	else
	{
		/*
		 * Plain upgrade, or --wal-upgrade with archiving: just set the OID.  On
		 * the --wal-upgrade archive path CN need not be byte-deterministic
		 * (PITR finds it by scanning WAL), so no exact WAL positioning.
		 */
		prep_status("Setting next OID for new cluster");
		exec_prog(UTILITY_LOG_FILE, NULL, true, true,
				  "\"%s/pg_resetwal\" -o %u \"%s\"",
				  new_cluster.bindir, old_cluster.controldata.chkpnt_nxtoid,
				  new_cluster.pgdata);
		check_ok();
	}

	migrate_logical_slots = count_old_cluster_logical_slots();

	if (user_opts.wal_upgrade)
	{
		PGconn	   *conn;

		/* Window's last segment; the archive barrier waits for it to drain. */
		char		upgrade_window_last_seg[MAXPGPATH] = {0};

		/*
		 * The old cluster's archive_command is carried forward only on the
		 * --initdb path.  Without it the upgrade window is generated but never
		 * archived, so PITR could not roll across the upgrade boundary; warn
		 * rather than report success on an upgrade that cannot be recovered.
		 */
		if (old_cluster_archive_command == NULL)
			pg_log(PG_WARNING,
				   "--wal-upgrade did not configure WAL archiving for the new cluster; "
				   "the upgrade window will not be archived and cannot be recovered by PITR. "
				   "Use --initdb so the old cluster's archive_command is carried forward, "
				   "or configure archiving on the new cluster before it is needed.");

		/*
		 * Restart with a fresh buffer pool for the WAL capture phase.  The
		 * server now reads the just-transplanted counters from pg_control, so
		 * the checkpoint below captures them.
		 */
		start_postmaster(&new_cluster, true);
		conn = connectToServer(&new_cluster, "template1");

		/*
		 * The window is emitted after all pg_upgrade work is done, as one
		 * burst rather than interleaved with the restore.
		 *
		 * A streaming standby anchors recovery at CN, so the window must be
		 * pinned in pg_wal/ by a slot whose restart_lsn is at or before CN.
		 * Migrating the old cluster's physical slots with immediately_reserve
		 * reserves each restart_lsn at the current insert position, so this
		 * loop must stay ahead of the emit call below: reserving after CN
		 * would silently break retention.
		 *
		 * Without a physical slot no pin is needed: the absence of one means
		 * no standby is connected.
		 */
		for (int slotnum = 0; slotnum < old_cluster.phys_slot_arr.nslots; slotnum++)
		{
			PhysicalSlotInfo *slot = &old_cluster.phys_slot_arr.slots[slotnum];
			PGresult   *slotres;

			pg_log(PG_VERBOSE, "migrating physical replication slot \"%s\"",
				   slot->slotname);

			/*
			 * Best-effort: a slot that cannot be recreated (a name collision, or
			 * a slot that was invalid on an old major and could not be filtered
			 * out) must not abort the upgrade, so warn and continue rather than
			 * executeQueryOrDie().  Losing a migrated slot only means that standby
			 * must be re-provisioned conventionally; the primary's upgrade is
			 * unaffected.
			 */
			slotres = PQexec(conn,
							 psprintf("SELECT pg_create_physical_replication_slot('%s', true, false)",
									  slot->slotname));
			if (PQresultStatus(slotres) != PGRES_TUPLES_OK)
			{
				pg_log(PG_WARNING,
					   "could not migrate physical replication slot \"%s\": %s",
					   slot->slotname, PQerrorMessage(conn));
				pg_log(PG_WARNING,
					   "The standby that used it must be re-provisioned after the upgrade.");
			}
			PQclear(slotres);
		}

		/*
		 * Emit the entire window with a single binary-upgrade-gated backend
		 * call; EmitUpgradeWalWindow() documents the order of the records it
		 * writes.  The checkpoint it takes first is CN, the recovery anchor, so
		 * replay starts there and applies only the end-of-upgrade images that
		 * follow, never pg_restore's own WAL.
		 *
		 * The XID/OID/multixact counters are not emitted separately: they were
		 * transplanted into pg_control before CN, so the CN checkpoint record
		 * carries them.  CN's LSN is not recorded either; first startup derives
		 * it from the WAL (PerformWalUpgradeIfNeeded), which is what lets a
		 * physical standby find the same anchor in the streamed WAL.
		 */
		PQclear(executeQueryOrDie(conn,
								  "SELECT binary_upgrade_emit_wal_window(%u, %u, %d, %s)",
								  old_cluster.major_version,
								  new_cluster.major_version,
								  (int) user_opts.transfer_mode,
								  wal_upgrade_skip_complete() ? "true" : "false"));

		/*
		 * Capture the segment holding PG_UPGRADE_COMPLETE before the switch,
		 * so the wait-for-archive barrier below targets the window's last
		 * segment (CN..COMPLETE).  pg_switch_wal() then seals that segment so
		 * it is archivable.
		 */
		if (old_cluster_archive_command != NULL)
		{
			PGresult   *res;

			res = executeQueryOrDie(conn,
									"SELECT pg_walfile_name(pg_current_wal_lsn())");
			strlcpy(upgrade_window_last_seg, PQgetvalue(res, 0, 0),
					sizeof(upgrade_window_last_seg));
			PQclear(res);
		}

		PQclear(executeQueryOrDie(conn, "SELECT pg_switch_wal()"));

		/*
		 * When the window is being archived, wait until the archiver has
		 * drained the window's last segment (holding PG_UPGRADE_COMPLETE,
		 * captured above) before shutting the burst server down.  Only
		 * CN..COMPLETE must reach the archive; pre-CN pg_restore WAL is
		 * irrelevant and legitimately recycled.
		 */
		if (old_cluster_archive_command != NULL)
		{
			PGresult   *res;

			/*
			 * Distinguish a persistent archiving failure from a transient one
			 * the archiver retries and recovers from.
			 * pg_stat_archiver.last_failed_wal is a sticky high-water mark: set
			 * on any failure and never cleared on a later success, so it cannot
			 * show whether archiving is currently failing.  Give up only once a
			 * failure has been recorded and last_archived_wal makes no forward
			 * progress for a bounded number of consecutive polls; a transient
			 * failure that drains advances last_archived and resets the stall
			 * counter.
			 */
			char		prev_archived[MAXPGPATH] = {0};
			int64		prev_failed = -1;
			int			stalled_polls = 0;

			/* ~30s of no progress while failures mount -> give up (100ms poll) */
#define UPGRADE_ARCHIVE_STALL_LIMIT 300

			prep_status("Waiting for the upgrade window to be archived");
			for (;;)
			{
				char		last_archived[MAXPGPATH];
				int64		failed_count;

				res = executeQueryOrDie(conn,
										"SELECT coalesce(last_archived_wal, ''), "
										"failed_count "
										"FROM pg_stat_archiver");
				strlcpy(last_archived, PQgetvalue(res, 0, 0), sizeof(last_archived));
				failed_count = strtoi64(PQgetvalue(res, 0, 1), NULL, 10);
				PQclear(res);

				/*
				 * last_archived_wal advances in segment-name order, so once
				 * it has reached (>=) the window's last segment, CN..COMPLETE
				 * is archived.
				 */
				if (last_archived[0] != '\0' &&
					strcmp(last_archived, upgrade_window_last_seg) >= 0)
					break;

				/*
				 * Persistent-failure detector.  The stall counter is driven
				 * by ABSENCE OF PROGRESS, not by catching the exact poll on
				 * which the archiver bumps its failure count: the archiver
				 * fails far less often than the 100ms poll rate, so keying on
				 * failed_count > prev_failed would reset the counter on
				 * almost every poll and never trip.  Instead: increment on
				 * every poll where last_archived_wal did not advance AND at
				 * least one archive failure has been recorded (failed_count >
				 * 0); reset the moment last_archived_wal advances.  A
				 * transient failure the archiver recovers from advances
				 * last_archived and clears the counter; a truly stuck
				 * archive_command makes no progress and trips the fatal after
				 * UPGRADE_ARCHIVE_STALL_LIMIT polls.  This also covers an
				 * archive that never produces any segment (fails from the
				 * first): last_archived stays "" across polls, which is "no
				 * advance", so the counter still climbs.  prev_failed >= 0
				 * means we have a prior poll to compare against (skip the
				 * first).
				 */
				if (failed_count > 0 &&
					prev_failed >= 0 &&
					strcmp(last_archived, prev_archived) == 0)
				{
					if (++stalled_polls >= UPGRADE_ARCHIVE_STALL_LIMIT)
						pg_fatal("archive_command is persistently failing while "
								 "archiving the upgrade window (no progress past "
								 "WAL file %s); the upgrade cannot be made "
								 "recoverable by PITR",
								 last_archived[0] != '\0' ? last_archived : "(none)");
				}
				else
					stalled_polls = 0;

				strlcpy(prev_archived, last_archived, sizeof(prev_archived));
				prev_failed = failed_count;

				pg_usleep(100000);	/* 100ms */
			}
			check_ok();
		}

		PQfinish(conn);

		/*
		 * The shutdown checkpoint lands past XLOG_UPGRADE_COMPLETE, so first
		 * startup's PerformWalUpgradeIfNeeded() guard (checkpoint > CN) skips
		 * the window replay.
		 */
		stop_postmaster(false);
	}

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
	 * issue_warnings_and_set_wal_level() starts and stops the new server once
	 * more to report extension updates and write a final wal_level record.  For
	 * --wal-upgrade that extra start/stop would checkpoint and recycle the
	 * upgrade WAL still pinned in pg_wal/, so skip it.
	 */
	if (!user_opts.wal_upgrade)
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

	/* Pass argv0 (full path) so find_my_exec can locate the binary */
	resolve_new_bindir(argv0);

	/*
	 * Verify that initdb is present and executable before doing any work. The
	 * normal path checks this later inside verify_directories(), but we run
	 * before that, so fail early with a useful message.
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
	 * bin_version, which check_bindir() normally fills in later.  Seed it now
	 * so the right binary name is used in this early call.
	 */
	if (old_cluster.bin_version == 0)
		old_cluster.bin_version = old_cluster.major_version;

	/*
	 * Refuse to clobber an already-populated new data directory.  --initdb is
	 * meant to create the new cluster from scratch, so an existing PG_VERSION
	 * there means the operator pointed at the wrong directory or a leftover
	 * from a previous attempt; either way, silently removing a database
	 * system is too dangerous.  Fail with pg_upgrade's own clear message (not
	 * initdb's "directory not empty") so the operator can remove it
	 * deliberately.
	 */
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

	/*
	 * While the old server is up, capture its archive_command so it can be
	 * carried forward to the new cluster: the upgrade window and post-upgrade
	 * WAL then flow to the same archive, making the upgrade recoverable by
	 * ordinary archive-based PITR with no extra operator action.
	 */
	if (user_opts.wal_upgrade)
		old_cluster_archive_command = detect_old_cluster_archive_command();
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
	 * Pass --data-checksums or --no-data-checksums explicitly.  Starting from
	 * PG18, initdb enables checksums by default, so we must mirror the old
	 * cluster's setting to avoid a mismatch that check_control_data() would
	 * reject.
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

	/*
	 * If the old cluster was archiving, enable the same archiving in the new
	 * cluster's postgresql.conf, so both the burst server and the auto-served
	 * upgraded cluster archive to the same place.  postgresql.conf (rather
	 * than -o flags) lets the archive_command be quoted correctly.
	 */
	if (old_cluster_archive_command != NULL)
		write_wal_upgrade_archive_conf(old_cluster_archive_command);
}

/*
 * Read the old cluster's archive_command over a connection to the running
 * old server.  Returns a pg_strdup'd copy if archiving was configured
 * (archive_mode <> off AND archive_command non-empty), else NULL.  Used to
 * carry the old cluster's archiving forward to the new cluster.
 */
static char *
detect_old_cluster_archive_command(void)
{
	PGconn	   *conn = connectToServer(&old_cluster, "template1");
	PGresult   *res;
	char	   *mode;
	char	   *cmd;
	char	   *result = NULL;

	res = executeQueryOrDie(conn,
							"SELECT current_setting('archive_mode'), "
							"current_setting('archive_command')");
	mode = PQgetvalue(res, 0, 0);
	cmd = PQgetvalue(res, 0, 1);

	/*
	 * archive_mode is off/on/always; archive_command defaults to '' (or the
	 * placeholder "(disabled)" on some builds).  Only carry a real command.
	 */
	if (strcmp(mode, "off") != 0 &&
		cmd[0] != '\0' &&
		strcmp(cmd, "(disabled)") != 0)
		result = pg_strdup(cmd);

	PQclear(res);
	PQfinish(conn);
	return result;
}

/*
 * Append archive settings to the new cluster's postgresql.conf so the
 * upgrade window and the post-upgrade WAL reach the archive.  archive_command
 * is the old cluster's command, carried forward.  Only called when the old
 * cluster was archiving.
 */
static void
write_wal_upgrade_archive_conf(const char *archive_command)
{
	char		conf_path[MAXPGPATH];
	FILE	   *fp;
	const char *p;

	snprintf(conf_path, sizeof(conf_path), "%s/postgresql.conf",
			 new_cluster.pgdata);

	fp = fopen(conf_path, "a");
	if (fp == NULL)
		pg_fatal("could not open \"%s\" to enable WAL archiving: %m", conf_path);

	/* archive_command is emitted as a single-quoted GUC value; double any '. */
	fputs("\n# added by pg_upgrade --wal-upgrade (carried from the old cluster)\n"
		  "archive_mode = on\n"
		  "archive_command = '", fp);
	for (p = archive_command; *p; p++)
	{
		if (*p == '\'')
			fputc('\'', fp);
		fputc(*p, fp);
	}
	fputs("'\n", fp);

	if (fclose(fp) != 0)
		pg_fatal("could not write \"%s\": %m", conf_path);
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

	/* LSN snapshot before and after pg_restore to measure WAL generated */
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
	 * Snapshot the WAL position before pg_restore to measure the bytes the
	 * schema restore generates.  Only for --wal-upgrade; otherwise skipped so
	 * the flow matches stock pg_upgrade.
	 */
	if (user_opts.wal_upgrade)
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
	 * Measure WAL bytes generated by pg_restore: read the current WAL
	 * position and compute the delta since lsn_before.  Only for
	 * --wal-upgrade; skipped otherwise so the flow matches stock pg_upgrade.
	 */
	if (user_opts.wal_upgrade)
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

	prep_status("Setting oldest XID for new cluster");
	exec_prog(UTILITY_LOG_FILE, NULL, true, true,
			  "\"%s/pg_resetwal\" -f -u %u \"%s\"",
			  new_cluster.bindir,
			  old_cluster.controldata.chkpnt_oldstxid,
			  new_cluster.pgdata);
	check_ok();

	/* set the next transaction id and epoch of the new cluster */
	prep_status("Setting next transaction ID and epoch for new cluster");
	exec_prog(UTILITY_LOG_FILE, NULL, true, true,
			  "\"%s/pg_resetwal\" -f -x %u \"%s\"",
			  new_cluster.bindir,
			  old_cluster.controldata.chkpnt_nxtxid,
			  new_cluster.pgdata);
	exec_prog(UTILITY_LOG_FILE, NULL, true, true,
			  "\"%s/pg_resetwal\" -f -e %u \"%s\"",
			  new_cluster.bindir,
			  old_cluster.controldata.chkpnt_nxtepoch,
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
	 * Now reset the WAL archives in the new cluster.  This positions the new
	 * cluster's WAL at the old cluster's next segment.
	 *
	 * For --wal-upgrade this reset also (re)assigns the new cluster's system
	 * identifier, not forced to the old cluster's value.  It rewrites both
	 * the control file and the fresh WAL segment header from the same
	 * ControlFile.system_identifier, so pg_control and the burst WAL stay
	 * consistent -- all that replay requires (recovery validates the WAL's
	 * xlp_sysid against pg_control, not its numeric value).  A standby is
	 * re-provisioned from a fresh skeleton stamped with this sysid, so it
	 * need not match the pre-upgrade cluster.  Same "new cluster gets a new
	 * sysid" behavior as stock pg_upgrade.
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
