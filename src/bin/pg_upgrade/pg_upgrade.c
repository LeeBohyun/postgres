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
#include <fcntl.h>
#include <time.h>

#include "access/multixact.h"
#include "access/xlog_internal.h"
#include "catalog/pg_class_d.h"
#include "catalog/pg_collation_d.h"
#include "catalog/pg_control.h"
#include "common/controldata_utils.h"
#include "common/file_perm.h"
#include "common/file_utils.h"
#include "common/logging.h"
#include "common/restricted_token.h"
#include "fe_utils/simple_list.h"
#include "fe_utils/string_utils.h"
#include "fe_utils/version.h"
#include "mb/pg_wchar.h"
#include "pg_upgrade.h"
#include "upgrade_catalogs.h"
#include "prepare_upgrade.h"

StaticAssertDecl((int) TRANSFER_MODE_CLONE == UPGRADE_RELINK_MODE_CLONE,
				 "TRANSFER_MODE_CLONE must match UPGRADE_RELINK_MODE_CLONE");
StaticAssertDecl((int) TRANSFER_MODE_COPY == UPGRADE_RELINK_MODE_COPY,
				 "TRANSFER_MODE_COPY must match UPGRADE_RELINK_MODE_COPY");
StaticAssertDecl((int) TRANSFER_MODE_COPY_FILE_RANGE == UPGRADE_RELINK_MODE_COPY_FILE_RANGE,
				 "TRANSFER_MODE_COPY_FILE_RANGE must match UPGRADE_RELINK_MODE_COPY_FILE_RANGE");
StaticAssertDecl((int) TRANSFER_MODE_LINK == UPGRADE_RELINK_MODE_LINK,
				 "TRANSFER_MODE_LINK must match UPGRADE_RELINK_MODE_LINK");
StaticAssertDecl((int) TRANSFER_MODE_SWAP == UPGRADE_RELINK_MODE_SWAP,
				 "TRANSFER_MODE_SWAP must match UPGRADE_RELINK_MODE_SWAP");

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
static void copy_wal_timeline_history(void);
static void stage_old_checkpoint_wal(SimpleStringList *segments);
static void wait_for_wal_archive(PGconn *conn, const char *segment);
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

/* Old cluster's archive_command, read while preparing --initdb. */
static char *old_cluster_archive_command = NULL;
static char *initdb_logdir = NULL;

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
	bool		perform_handoff;
	bool		migrate_logical_slots;
	UpgradePreparation *preparation = NULL;
	SimpleStringList old_checkpoint_segments = {NULL, NULL};

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
	set_old_cluster_endpoint();
	perform_handoff = user_opts.wal_upgrade && !user_opts.check &&
		!user_opts.live_check;
	if (perform_handoff)
		prepare_pg_upgrade_handoff();

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

	if (user_opts.wal_upgrade && !user_opts.check)
		preparation = create_upgrade_preparation(&old_cluster, &new_cluster,
												 user_opts.transfer_mode);
	check_and_dump_old_cluster(preparation);

	/* -- NEW -- */
	/* Start the new-major server for target compatibility checks. */
	start_postmaster(&new_cluster, true);

	check_new_cluster();
	report_clusters_compatible();

	/*
	 * Perform HANDOFF after target checks and before preparing the upgrade
	 * source.
	 */
	if (perform_handoff)
	{
		stop_postmaster(false);
		perform_pg_upgrade_handoff();
	}

	/*
	 * Reload the stopped local old cluster's final-checkpoint control data
	 * while preserving the multixact offset width read from its running
	 * server.
	 */
	if (user_opts.wal_upgrade && !user_opts.live_check)
	{
		int			nxtmxoff_size =
			old_cluster.controldata.chkpnt_nxtmxoff_size;

		memset(&old_cluster.controldata, 0, sizeof(old_cluster.controldata));
		old_cluster.controldata.chkpnt_nxtmxoff_size = nxtmxoff_size;
		get_control_data(&old_cluster);
	}

	if (user_opts.wal_upgrade && !user_opts.check)
		prepare_upgrade_source(preparation);

	/* Restart the new-major server for target preparation after HANDOFF. */
	if (perform_handoff)
		start_postmaster(&new_cluster, true);

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

	if (user_opts.wal_upgrade)
	{
		prepare_upgrade_catalogs(preparation, &new_cluster,
								 UPGRADE_CATALOG_NEW);
		finish_upgrade_preparation(preparation);
	}

	stop_postmaster(false);


	if (user_opts.transfer_mode == TRANSFER_MODE_LINK ||
		user_opts.transfer_mode == TRANSFER_MODE_SWAP)
		disable_old_cluster(user_opts.transfer_mode);

	transfer_all_new_tablespaces(&old_cluster.dbarr, &new_cluster.dbarr,
								 old_cluster.pgdata, new_cluster.pgdata);

	if (user_opts.wal_upgrade)
	{
		prep_status("Setting next OID and preparing upgrade WAL");

		/*
		 * Write the upgrade checkpoint in the first whole segment after the
		 * final old checkpoint ends, on the old checkpoint's timeline.
		 */
		exec_prog(UTILITY_LOG_FILE, NULL, true, true,
				  "\"%s/pg_resetwal\" --wal-upgrade-exact -o %" PRIu64 " -l %s \"%s\"",
				  new_cluster.bindir,
				  old_cluster.controldata.chkpnt_nxtoid,
				  old_cluster.controldata.upgrade_start_wal_file,
				  new_cluster.pgdata);
		copy_wal_timeline_history();
		if (old_cluster_archive_command != NULL)
			stage_old_checkpoint_wal(&old_checkpoint_segments);
		check_ok();
	}
	else
	{
		prep_status("Setting next OID for new cluster");
		exec_prog(UTILITY_LOG_FILE, NULL, true, true,
				  "\"%s/pg_resetwal\" -o %" PRIu64 " \"%s\"",
				  new_cluster.bindir, old_cluster.controldata.chkpnt_nxtoid,
				  new_cluster.pgdata);
		check_ok();
	}
	if (user_opts.wal_upgrade)
		bind_upgrade_preparation(preparation);

	migrate_logical_slots = count_old_cluster_logical_slots();

	/*
	 * Reserve physical slots and emit the completed cluster as an upgrade WAL
	 * window.  With archiving enabled, wait for the window and its completion
	 * checkpoint to reach the archive.
	 */
	if (user_opts.wal_upgrade)
	{
		PGconn	   *conn;

		char		upgrade_window_last_seg[MAXPGPATH] = {0};

		if (old_cluster_archive_command == NULL)
			pg_log(PG_WARNING,
				   "--wal-upgrade did not configure WAL archiving for the new cluster; "
				   "the upgrade window will not be archived and cannot be recovered by PITR. "
				   "Use --initdb so the old cluster's archive_command is carried forward, "
				   "or configure archiving on the new cluster before it is needed.");

		if (old_cluster_archive_command != NULL)
			write_wal_upgrade_archive_conf(old_cluster_archive_command);

		/* Start the new-major server that emits the upgrade window. */
		start_postmaster(&new_cluster, true);
		conn = connectToServer(&new_cluster, "template1");

		if (old_checkpoint_segments.head != NULL)
		{
			SimpleStringListCell *segment;

			/*
			 * Wait for archival of every staged HANDOFF and checkpoint
			 * segment.
			 */
			prep_status("Archiving the final old-cluster checkpoint");
			for (segment = old_checkpoint_segments.head; segment != NULL;
				 segment = segment->next)
				wait_for_wal_archive(conn, segment->val);
			simple_string_list_destroy(&old_checkpoint_segments);
			check_ok();
		}

		/* Recreate and reserve each physical slot before window emission. */
		for (int slotnum = 0; slotnum < old_cluster.phys_slot_arr.nslots; slotnum++)
		{
			PhysicalSlotInfo *slot = &old_cluster.phys_slot_arr.slots[slotnum];

			pg_log(PG_VERBOSE, "migrating physical replication slot \"%s\"",
				   slot->slotname);

			PQclear(executeQueryOrDie(conn,
									  "SELECT pg_create_physical_replication_slot('%s', true, false)",
									  slot->slotname));
		}

		emit_upgrade_wal(preparation, conn);

		free_upgrade_preparation(preparation);

		if (old_cluster_archive_command != NULL)
		{
			PGresult   *res;

			/* Finalize the committed window with a shutdown checkpoint. */
			PQfinish(conn);
			stop_postmaster(false);

			/* Restart the archiver before switching the checkpoint's segment. */
			start_postmaster(&new_cluster, true);
			conn = connectToServer(&new_cluster, "template1");
			res = executeQueryOrDie(conn,
									"SELECT pg_walfile_name(pg_current_wal_lsn())");
			strlcpy(upgrade_window_last_seg, PQgetvalue(res, 0, 0),
					sizeof(upgrade_window_last_seg));
			PQclear(res);
		}

		PQclear(executeQueryOrDie(conn, "SELECT pg_switch_wal()"));

		if (old_cluster_archive_command != NULL)
		{
			prep_status("Waiting for the upgrade window to be archived");
			wait_for_wal_archive(conn, upgrade_window_last_seg);
			check_ok();
		}

		PQfinish(conn);

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
				  (user_opts.transfer_mode == TRANSFER_MODE_SWAP &&
				   !user_opts.wal_upgrade) ?
				  "--no-sync-data-files" : "",
				  new_cluster.pgdata,
				  user_opts.sync_method);
		check_ok();
	}

	create_script_for_old_cluster_deletion(&deletion_script_file_name);

	if (!user_opts.wal_upgrade)
		issue_warnings_and_set_wal_level();

	pg_log(PG_REPORT,
		   "\n"
		   "Upgrade Complete\n"
		   "----------------");

	output_completion_banner(deletion_script_file_name);

	pg_free(deletion_script_file_name);

	if (initdb_logdir != NULL && !log_opts.retain &&
		!rmtree(initdb_logdir, true))
		rmtree(initdb_logdir, true);

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


static void
resolve_new_bindir(const char *argv0)
{
	if (!new_cluster.bindir)
	{
		char		exec_path[MAXPGPATH];

		if (find_my_exec(argv0, exec_path) < 0)
			pg_fatal("%s: could not find own program executable", argv0);
		*last_dir_separator(exec_path) = '\0';
		canonicalize_path(exec_path);
		new_cluster.bindir = pg_strdup(exec_path);
	}
}


static void
create_new_cluster_via_initdb(const char *argv0)
{
	DbLocaleInfo *locale;
	PQExpBufferData cmd;
	char	   *saved_logdir = log_opts.logdir;
	const char *encoding_name;
	bool		keep_old_running;

	resolve_new_bindir(argv0);

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

	if (old_cluster.bin_version == 0)
		old_cluster.bin_version = old_cluster.major_version;

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
	keep_old_running = user_opts.wal_upgrade;

	initdb_logdir = psprintf("%s/pg_upgrade_initdb-XXXXXX",
							 user_opts.socketdir);
	if (mkdtemp(initdb_logdir) == NULL)
		pg_fatal("could not create temporary log directory \"%s\": %m",
				 initdb_logdir);
	log_opts.logdir = initdb_logdir;

	if (!old_cluster.sockdir)
		old_cluster.sockdir = user_opts.socketdir ? user_opts.socketdir : ".";

	prep_status("Inspecting old cluster locale for new cluster creation");
	start_postmaster(&old_cluster, true);
	get_template0_info(&old_cluster);
	if (user_opts.wal_upgrade)
		old_cluster_archive_command = detect_old_cluster_archive_command();
	/* Keep the old-major server running for checks, dump, and HANDOFF. */
	if (!keep_old_running)
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

/* Copy history files for recovery on the retained source timeline. */
static void
copy_wal_timeline_history(void)
{
	char		waldir[MAXPGPATH];
	DIR		   *dir;
	struct dirent *de;

	snprintf(waldir, sizeof(waldir), "%s/pg_wal", old_cluster.pgdata);
	dir = opendir(waldir);
	if (dir == NULL)
		pg_fatal("could not open directory \"%s\": %m", waldir);
	while ((de = readdir(dir)) != NULL)
	{
		char		src[MAXPGPATH];
		char		dst[MAXPGPATH];

		if (!IsTLHistoryFileName(de->d_name))
			continue;
		snprintf(src, sizeof(src), "%s/%s", waldir, de->d_name);
		snprintf(dst, sizeof(dst), "%s/pg_wal/%s", new_cluster.pgdata,
				 de->d_name);
		copyFile(src, dst, "pg_wal", de->d_name);
	}
	closedir(dir);
}

/* Read an old-major record header across WAL page and segment headers. */
static void
read_old_wal_header(XLogRecPtr ptr, XLogRecord *record)
{
	size_t		copied = 0;
	uint32		wal_segsz = old_cluster.controldata.walseg;

	while (copied < SizeOfXLogRecord)
	{
		char		path[MAXPGPATH];
		char		name[MAXFNAMELEN];
		XLogSegNo	segno;
		size_t		len = Min(SizeOfXLogRecord - copied,
							  XLOG_BLCKSZ - ptr % XLOG_BLCKSZ);
		int			fd;

		XLByteToSeg(ptr, segno, wal_segsz);
		XLogFileName(name, old_cluster.controldata.chkpnt_tli, segno, wal_segsz);
		snprintf(path, sizeof(path), "%s/pg_wal/%s", old_cluster.pgdata, name);
		fd = open(path, O_RDONLY | PG_BINARY, 0);
		if (fd < 0)
			pg_fatal("could not open old checkpoint WAL file \"%s\": %m", path);
		if (pread(fd, (char *) record + copied, len,
				  (off_t) (ptr % wal_segsz)) != (ssize_t) len)
			pg_fatal("could not read old checkpoint WAL header from \"%s\": %m", path);
		if (close(fd) != 0)
			pg_fatal("could not close old checkpoint WAL file \"%s\": %m", path);
		copied += len;
		ptr += len;
		if (ptr % XLOG_BLCKSZ == 0)
			ptr += ptr % wal_segsz == 0 ? SizeOfXLogLongPHD : SizeOfXLogShortPHD;
	}
}

/*
 * Stage the segments containing HANDOFF and the final old checkpoint in the
 * new-major pg_wal directory. The new archiver copies them into the continuous
 * archive before the upgrade window is emitted.
 */
static void
stage_old_checkpoint_wal(SimpleStringList *segments)
{
	ControlData *control = &old_cluster.controldata;
	XLogRecord	checkpoint;
	XLogSegNo	first;
	XLogSegNo	last;
	XLogSegNo	segno;

	read_old_wal_header(control->chkpnt_redo_lsn, &checkpoint);
	if (checkpoint.xl_rmid != RM_XLOG_ID ||
		(checkpoint.xl_info & ~XLR_INFO_MASK) != XLOG_CHECKPOINT_SHUTDOWN ||
		checkpoint.xl_prev == InvalidXLogRecPtr ||
		checkpoint.xl_prev >= control->chkpnt_redo_lsn)
		pg_fatal("invalid final old checkpoint WAL header");

	/* HANDOFF may start in the segment before the shutdown checkpoint. */
	XLByteToSeg(checkpoint.xl_prev, first, control->walseg);
	XLByteToPrevSeg(control->shutdown_checkpoint_end_lsn, last, control->walseg);
	if (first > last || last - first > 1)
		pg_fatal("invalid final old checkpoint WAL segment range");
	for (segno = first; segno <= last; segno++)
	{
		char		name[MAXFNAMELEN];
		char		src[MAXPGPATH];
		char		dst[MAXPGPATH];
		char		status[MAXPGPATH];
		struct stat st;
		int			fd;

		XLogFileName(name, control->chkpnt_tli, segno, control->walseg);
		snprintf(status, sizeof(status), "%s/pg_wal/archive_status/%s.done",
				 old_cluster.pgdata, name);
		if (stat(status, &st) == 0)
		{
			if (!S_ISREG(st.st_mode))
				pg_fatal("invalid old archive status file \"%s\"", status);
			continue;
		}
		if (errno != ENOENT)
			pg_fatal("could not inspect old archive status \"%s\": %m", status);

		snprintf(src, sizeof(src), "%s/pg_wal/%s", old_cluster.pgdata, name);
		snprintf(dst, sizeof(dst), "%s/pg_wal/%s", new_cluster.pgdata, name);
		if (stat(src, &st) != 0 || !S_ISREG(st.st_mode) ||
			st.st_size != control->walseg)
			pg_fatal("old checkpoint WAL file \"%s\" is missing or has an invalid size",
					 src);
		snprintf(status, sizeof(status), "%s/pg_wal/archive_status/%s.done",
				 new_cluster.pgdata, name);
		if (stat(status, &st) == 0)
			pg_fatal("old checkpoint archive status already exists: \"%s\"", status);
		if (errno != ENOENT)
			pg_fatal("could not inspect archive status \"%s\": %m", status);
		/* Reject an existing destination instead of replacing archived WAL. */
		copyFile(src, dst, "pg_wal", name);
		if (fsync_fname(dst, false) != 0 || fsync_parent_path(dst) != 0)
			pg_fatal("could not synchronize staged old checkpoint WAL file \"%s\"",
					 dst);

		snprintf(status, sizeof(status), "%s/pg_wal/archive_status/%s.ready",
				 new_cluster.pgdata, name);
		fd = open(status, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY,
				  pg_file_create_mode);
		if (fd < 0)
			pg_fatal("could not create old checkpoint archive status \"%s\": %m",
					 status);
		if (close(fd) != 0 || fsync_fname(status, false) != 0 ||
			fsync_parent_path(status) != 0)
			pg_fatal("could not synchronize old checkpoint archive status \"%s\": %m",
					 status);
		simple_string_list_append(segments, name);
	}
}

static bool
wal_archive_file_exists(const char *path)
{
	struct stat st;

	if (stat(path, &st) == 0)
	{
		if (!S_ISREG(st.st_mode))
			pg_fatal("invalid WAL archive file \"%s\"", path);
		return true;
	}
	if (errno != ENOENT)
		pg_fatal("could not inspect WAL archive file \"%s\": %m", path);
	return false;
}

static void
wait_for_wal_archive(PGconn *conn, const char *segment)
{
	char		done[MAXPGPATH];
	char		ready[MAXPGPATH];
	char		wal[MAXPGPATH];
	char		prev_archived[MAXPGPATH] = {0};
	int64		prev_failed = -1;
	int64		failures_at_progress = 0;

	snprintf(done, sizeof(done), "%s/pg_wal/archive_status/%s.done",
			 new_cluster.pgdata, segment);
	snprintf(ready, sizeof(ready), "%s/pg_wal/archive_status/%s.ready",
			 new_cluster.pgdata, segment);
	snprintf(wal, sizeof(wal), "%s/pg_wal/%s",
			 new_cluster.pgdata, segment);
	for (;;)
	{
		PGresult   *res;
		char		last_archived[MAXPGPATH];
		int64		failed_count;

		/* Stop at .done, or after cleanup removes staged WAL and .ready. */
		if (wal_archive_file_exists(done) ||
			(!wal_archive_file_exists(ready) &&
			 !wal_archive_file_exists(wal)))
			return;
		res = executeQueryOrDie(conn,
								"SELECT coalesce(last_archived_wal, ''), failed_count "
								"FROM pg_stat_archiver");
		strlcpy(last_archived, PQgetvalue(res, 0, 0), sizeof(last_archived));
		failed_count = strtoi64(PQgetvalue(res, 0, 1), NULL, 10);
		PQclear(res);
		if (prev_failed < 0 || failed_count < prev_failed ||
			strcmp(last_archived, prev_archived) != 0)
			failures_at_progress = failed_count;
		else if (failed_count - failures_at_progress >= 3)
			pg_fatal("archive_command is persistently failing while archiving "
					 "WAL file %s; the upgrade cannot be made recoverable by PITR",
					 segment);
		strlcpy(prev_archived, last_archived, sizeof(prev_archived));
		prev_failed = failed_count;
		pg_usleep(100000);
	}
}

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

	if (strcmp(mode, "off") != 0 &&
		cmd[0] != '\0' &&
		strcmp(cmd, "(disabled)") != 0)
		result = pg_strdup(cmd);

	PQclear(res);
	PQfinish(conn);
	return result;
}

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
	if (os_info.running_cluster != &old_cluster &&
		pid_lock_file_exists(old_cluster.pgdata))
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
	if (old_cluster.controldata.chkpnt_nxtmxoff_size == (int) sizeof(uint64))
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
	else if (old_cluster.controldata.chkpnt_nxtmxoff_size ==
			 (int) sizeof(uint32))
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
	else
		pg_fatal("old cluster has unsupported multixact offset width %d",
				 old_cluster.controldata.chkpnt_nxtmxoff_size);

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
