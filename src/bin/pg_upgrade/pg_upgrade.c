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

#include <time.h>

#include "access/multixact.h"
#include "catalog/pg_class_d.h"
#include "catalog/pg_collation_d.h"
#include "common/file_perm.h"
#include "common/logging.h"
#include "common/restricted_token.h"
#include "common/string.h"
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
static void make_outputdirs(const char *output_root);
static void check_bin_dirs(const char *argv0);
static void check_clusters_stopped(void);
static void prepare_initdb(void);
static void create_initdb_parents(char *path);
static void finalize_initdb(void);
static void set_initdb_directory_permissions(const char *path);
static void set_output_permissions(void);
static void get_old_cluster_initdb_info(void);
static void build_new_cluster_initdb_cmd(PQExpBuffer cmd);
static void create_new_cluster_via_initdb(void);
static void create_logical_replication_slots(void);
static void create_conflict_detection_slot(void);

ClusterInfo old_cluster,
			new_cluster;
OSInfo		os_info;

/*
 * Track directories created by --initdb and the original permissions of
 * existing target and external WAL directories. Check-mode cleanup uses
 * this state to remove initialized data and newly created directories,
 * preserve pre-existing directories, and restore their permissions.
 */
static char *initdb_target;
static char *initdb_state_file;
static char *initdb_waldir;
static char *initdb_wal_parent;
static bool initdb_wal_existed;
static mode_t initdb_wal_mode;
static bool initdb_succeeded;
static bool initdb_target_created;
static bool initdb_output_root_created;
static mode_t initdb_target_mode;
static bool initdb_restore_mode;
static char **initdb_created_parents;
static int	initdb_num_created_parents;

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

	/*
	 * Make sure the user has a clean environment, otherwise, we may confuse
	 * libpq when we connect to one (or both) of the servers.
	 */
	check_pghost_envvar();

	/*
	 * Resolve old and new binary dirs before adjust_data_dir(), because
	 * adjust_data_dir() might need to call the binaries.
	 */
	check_bin_dirs(argv[0]);

	adjust_data_dir(&old_cluster);

	if (user_opts.initdb_new_cluster)
	{
		prepare_initdb();
		create_new_cluster_via_initdb();
	}
	else
		adjust_data_dir(&new_cluster);

	/*
	 * Use the permissions selected by initdb, including explicit group
	 * access.
	 */
	if (!GetDataDirectoryCreatePerm(new_cluster.pgdata))
		pg_fatal("could not read permissions of directory \"%s\": %m",
				 new_cluster.pgdata);
	umask(pg_mode_mask);

	if (user_opts.initdb_new_cluster)
	{
		for (int i = 0; i < initdb_num_created_parents; i++)
			set_initdb_directory_permissions(initdb_created_parents[i]);
		set_output_permissions();
		if (initdb_output_root_created)
			set_initdb_directory_permissions(log_opts.rootdir);
	}
	else
		make_outputdirs(new_cluster.pgdata);

	check_current_dir_write_permissions();

	check_data_dir(&old_cluster);
	if (!user_opts.initdb_new_cluster)
		check_data_dir(&new_cluster);
	check_clusters_stopped();

	output_check_banner();

	check_cluster_versions();

	get_sock_dir(&old_cluster);
	get_sock_dir(&new_cluster);

	check_cluster_compatibility();

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

	/* -- NEW -- */
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
	 * Assuming OIDs are only used in system tables, there is no need to
	 * restore the OID counter because we have not transferred any OIDs from
	 * the old system, but we do it anyway just in case.  We do it late here
	 * because there is no need to have the schema load use new oids.
	 */
	prep_status("Setting next OID for new cluster");
	exec_prog(UTILITY_LOG_FILE, NULL, true, true,
			  "\"%s/pg_resetwal\" -o %" PRIu64 " \"%s\"",
			  new_cluster.bindir, old_cluster.controldata.chkpnt_nxtoid,
			  new_cluster.pgdata);
	check_ok();

	migrate_logical_slots = count_old_cluster_logical_slots();

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

	sync_new_cluster(user_opts.transfer_mode != TRANSFER_MODE_SWAP);

	create_script_for_old_cluster_deletion(&deletion_script_file_name);

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
 * Sync the new cluster after shutdown, unless --no-sync is specified.
 * A completed --swap upgrade can skip syncing data files here because they
 * have already been synced.
 */
void
sync_new_cluster(bool sync_data_files)
{
	if (!user_opts.do_sync)
		return;

	prep_status("Sync data directory to disk");
	exec_prog(UTILITY_LOG_FILE, NULL, true, true,
			  "\"%s/initdb\" --sync-only %s \"%s\" --sync-method %s",
			  new_cluster.bindir,
			  sync_data_files ? "" : "--no-sync-data-files",
			  new_cluster.pgdata,
			  user_opts.sync_method);
	check_ok();
}


/*
 * Create and assign proper permissions to the set of output directories
 * used to store any data generated internally, filling in log_opts in
 * the process.
 */
static void
make_outputdirs(const char *output_root)
{
	FILE	   *fp;
	char	  **filename;
	time_t		run_time = time(NULL);
	char		filename_path[MAXPGPATH];
	char		timebuf[128];
	struct timeval time;
	time_t		tt;
	int			len;

	pg_free(log_opts.rootdir);
	log_opts.rootdir = (char *) pg_malloc0(MAXPGPATH);
	len = snprintf(log_opts.rootdir, MAXPGPATH, "%s/%s", output_root, BASE_OUTPUTDIR);
	if (len >= MAXPGPATH)
		pg_fatal("directory path for new cluster is too long");

	/* BASE_OUTPUTDIR/$timestamp/ */
	gettimeofday(&time, NULL);
	tt = (time_t) time.tv_sec;
	strftime(timebuf, sizeof(timebuf), "%Y%m%dT%H%M%S", localtime(&tt));
	/* append milliseconds */
	snprintf(timebuf + strlen(timebuf), sizeof(timebuf) - strlen(timebuf),
			 ".%03d", (int) (time.tv_usec / 1000));
	pg_free(log_opts.basedir);
	log_opts.basedir = (char *) pg_malloc0(MAXPGPATH);
	len = snprintf(log_opts.basedir, MAXPGPATH, "%s/%s", log_opts.rootdir,
				   timebuf);
	if (len >= MAXPGPATH)
		pg_fatal("directory path for new cluster is too long");

	/* BASE_OUTPUTDIR/$timestamp/dump/ */
	pg_free(log_opts.dumpdir);
	log_opts.dumpdir = (char *) pg_malloc0(MAXPGPATH);
	len = snprintf(log_opts.dumpdir, MAXPGPATH, "%s/%s/%s", log_opts.rootdir,
				   timebuf, DUMP_OUTPUTDIR);
	if (len >= MAXPGPATH)
		pg_fatal("directory path for new cluster is too long");

	/* BASE_OUTPUTDIR/$timestamp/log/ */
	pg_free(log_opts.logdir);
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
 * Resolve the new binary directory from pg_upgrade's path when -B or
 * PGBINNEW was not specified, then validate both clusters' binaries.
 */
static void
check_bin_dirs(const char *argv0)
{
	if (!new_cluster.bindir)
	{
		char		exec_path[MAXPGPATH];

		if (find_my_exec(argv0, exec_path) < 0)
			pg_fatal("%s: could not find own program executable", argv0);
		/* Trim off program name and keep just path */
		*last_dir_separator(exec_path) = '\0';
		canonicalize_path(exec_path);
		new_cluster.bindir = pg_strdup(exec_path);
	}

	check_bin_dir(&old_cluster, false);
	check_bin_dir(&new_cluster, true);
}


/*
 * Before creating logs or changing permissions, check that the target
 * directory is missing or empty except for pg_upgrade_output.d.
 */
static void
prepare_initdb(void)
{
	int			status = pg_check_dir_except(new_cluster.pgdata, BASE_OUTPUTDIR);
	struct stat st;

	if (status < 0)
		pg_fatal("could not access directory \"%s\": %m", new_cluster.pgdata);
	if (status > 1)
		pg_fatal("new cluster data directory \"%s\" is not empty",
				 new_cluster.pgdata);

	initdb_target = make_absolute_path(new_cluster.pgdata);
	if (!initdb_target)
		exit(1);
	{
		char	   *output_root = psprintf("%s/%s", initdb_target, BASE_OUTPUTDIR);

		initdb_output_root_created = stat(output_root, &st) != 0 && errno == ENOENT;
		pg_free(output_root);
	}
	if (status != 0)
	{
		if (stat(initdb_target, &st) != 0)
			pg_fatal("could not read permissions of directory \"%s\": %m",
					 initdb_target);
		initdb_target_mode = st.st_mode & 07777;
	}

	/*
	 * atexit() runs handlers in reverse registration order. Register these
	 * before starting a server so its shutdown handler runs before data
	 * cleanup. Restore the target's original permissions last so cleanup can
	 * still access the directory.
	 */
	atexit(finalize_initdb);
	atexit(cleanup_initdb_cluster);
	if (status == 0)
	{
		char	   *parent = pg_strdup(initdb_target);

		get_parent_directory(parent);
		create_initdb_parents(parent);
		pg_free(parent);
		if (mkdir(initdb_target, PG_DIR_MODE_OWNER) != 0)
			pg_fatal("could not create directory \"%s\": %m", initdb_target);
		initdb_target_created = true;
	}
	else
	{
		if (chmod(initdb_target, PG_DIR_MODE_OWNER) != 0)
			pg_fatal("could not change permissions of directory \"%s\": %m",
					 initdb_target);
		initdb_restore_mode = user_opts.check;
	}

	/* Keep early logs private until initdb's selected permissions are known. */
	make_outputdirs(initdb_target);

	initdb_state_file = psprintf("%s/initdb_state", log_opts.logdir);
	pg_free(new_cluster.pgdata);
	new_cluster.pgdata = new_cluster.pgconfig = initdb_target;
}

/*
 * Track parent directories created by this run so we can apply initdb's
 * permissions and remove them after --check if empty.
 */
static void
create_initdb_parents(char *path)
{
	struct stat st;
	char	   *parent;
	char	   *created;

	if (stat(path, &st) == 0)
	{
		if (!S_ISDIR(st.st_mode))
			pg_fatal("\"%s\" is not a directory", path);
		return;
	}
	if (errno != ENOENT)
		pg_fatal("could not access directory \"%s\": %m", path);
	parent = pg_strdup(path);
	get_parent_directory(parent);
	if (parent[0] == '\0' || strcmp(parent, path) == 0)
		pg_fatal("could not access directory \"%s\"", path);
	create_initdb_parents(parent);
	pg_free(parent);

	created = pg_strdup(path);
	initdb_created_parents = pg_realloc_array(initdb_created_parents, char *,
											  initdb_num_created_parents + 1);
	if (mkdir(path, PG_DIR_MODE_OWNER) != 0)
	{
		if (errno != EEXIST || stat(path, &st) != 0 || !S_ISDIR(st.st_mode))
			pg_fatal("could not create directory \"%s\": %m", path);
		pg_free(created);
		return;
	}
	initdb_created_parents[initdb_num_created_parents++] = created;
}

/* Runs after log cleanup on success; failed checks keep their logs. */
static void
finalize_initdb(void)
{
	bool		failed = false;

	if (!user_opts.check)
		return;
	for (int i = initdb_num_created_parents; i >= 0; i--)
	{
		const char *path = i == initdb_num_created_parents ? initdb_target :
			initdb_created_parents[i];

		if (i == initdb_num_created_parents && !initdb_target_created)
			continue;
		if (rmdir(path) != 0 && errno != ENOENT &&
			errno != ENOTEMPTY && errno != EEXIST)
		{
			pg_log(PG_WARNING, "could not remove directory \"%s\": %m", path);
			failed = true;
		}
	}
	if (initdb_restore_mode)
	{
		if (chmod(initdb_target, initdb_target_mode) != 0)
		{
			pg_log(PG_WARNING, "could not restore permissions of directory \"%s\": %m",
				   initdb_target);
			failed = true;
		}
		else
			initdb_restore_mode = false;
	}
	if (failed)
	{
		fflush(NULL);
		_exit(1);
	}
}

/* Apply initdb's permissions without clearing inherited setgid. */
static void
set_initdb_directory_permissions(const char *path)
{
	mode_t		mode = pg_dir_create_mode;
#ifndef WIN32
	struct stat st;

	/* Keep the group inheritance of directories we created under setgid. */
	if (stat(path, &st) != 0)
		pg_fatal("could not read permissions of directory \"%s\": %m", path);
	mode |= st.st_mode & S_ISGID;
#endif
	if (chmod(path, mode) != 0)
		pg_fatal("could not change permissions of directory \"%s\": %m", path);
}

/*
 * Set permissions on this run's output directory, its log and dump
 * subdirectories, and log files to match initdb's selected permissions.
 */
static void
set_output_permissions(void)
{
	const char *dirs[] = {log_opts.basedir, log_opts.dumpdir, log_opts.logdir, NULL};
	char		path[MAXPGPATH];

	for (const char **dir = dirs; *dir; dir++)
		set_initdb_directory_permissions(*dir);
	for (char **file = output_files; *file; file++)
	{
		snprintf(path, sizeof(path), "%s/%s", log_opts.logdir, *file);
		if (chmod(path, pg_file_create_mode) != 0)
			pg_fatal("could not change permissions of file \"%s\": %m", path);
	}
}

/*
 * Before initdb, the target contained only pg_upgrade_output.d.
 * Remove initialized data after --check, preserving that log directory.
 */
void
cleanup_initdb_cluster(void)
{
	struct stat st;
	char		path[MAXPGPATH];

	if (!user_opts.check || !initdb_succeeded)
		return;

	snprintf(path, sizeof(path), "%s/postmaster.pid", initdb_target);
	if (os_info.running_cluster == &new_cluster ||
		lstat(path, &st) == 0 || (errno != ENOENT && errno != ENOTDIR))
	{
		pg_log(PG_WARNING, "cannot confirm new server shutdown, retaining cluster at \"%s\"", initdb_target);
		goto fail;
	}

	if (initdb_waldir[0])
	{
		if (!rmtree(initdb_waldir, !initdb_wal_existed) ||
			(initdb_wal_existed && chmod(initdb_waldir, initdb_wal_mode) != 0))
			goto fail;
		strlcpy(path, initdb_waldir, sizeof(path));
		get_parent_directory(path);
		while (!initdb_wal_existed && strcmp(path, initdb_wal_parent) != 0)
		{
			if (rmdir(path) != 0 && errno != ENOENT)
			{
				if (errno == ENOTEMPTY || errno == EEXIST)
					break;
				pg_log(PG_WARNING, "could not remove directory \"%s\": %m", path);
				goto fail;
			}
			get_parent_directory(path);
		}
	}

	if (!rmtree_except(initdb_target, BASE_OUTPUTDIR))
		goto fail;
	initdb_succeeded = false;
	return;

fail:
	pg_log(PG_WARNING, "could not clean up initialization under \"%s\"", initdb_target);
	/* _exit() skips the finalizer registered to restore target permissions. */
	finalize_initdb();
	fflush(NULL);
	_exit(1);
}


/*
 * Read the old cluster's control data and template0 settings for initdb.
 */
static void
get_old_cluster_initdb_info(void)
{
	old_cluster.major_version = get_pg_version(old_cluster.pgdata,
											   &old_cluster.major_version_str);

	if (!old_cluster.sockdir)
		old_cluster.sockdir = user_opts.socketdir ? user_opts.socketdir : ".";

	/*
	 * Allow a running old server for --check, as check_clusters_stopped()
	 * does.
	 */
	if (pid_lock_file_exists(old_cluster.pgdata))
	{
		if (start_postmaster(&old_cluster, false))
			stop_postmaster(false);
		else if (!user_opts.check)
			pg_fatal("There seems to be a postmaster servicing the old cluster.\n"
					 "Please shutdown that postmaster and try again.");
		else
			user_opts.live_check = true;
	}

	get_sock_dir(&old_cluster);
	get_control_data(&old_cluster);

	prep_status("Examining old cluster settings");
	if (!user_opts.live_check)
		start_postmaster(&old_cluster, true);
	get_template0_info(&old_cluster);
	if (!user_opts.live_check)
		stop_postmaster(false);
	check_ok();
}


/*
 * Build the initdb command using the old cluster's settings and any
 * arguments supplied with --initdb-options.
 */
static void
build_new_cluster_initdb_cmd(PQExpBuffer cmd)
{
	DbLocaleInfo *locale = old_cluster.template0;
	const char *encoding_name = pg_encoding_to_char(locale->db_encoding);
	char		initdb_path[MAXPGPATH];

	snprintf(initdb_path, sizeof(initdb_path), "%s/initdb", new_cluster.bindir);

	prep_status("Constructing new cluster initdb command");

	initPQExpBuffer(cmd);

	/*
	 * Build the command with appendShellString() for every value that comes
	 * from outside our control: the username is from the command line, and
	 * the encoding and locale strings are read from the old cluster's
	 * template0. This prevents shell metacharacters in any of them from
	 * breaking out of their argument when the command is run through the
	 * shell.
	 */
	appendShellString(cmd, initdb_path);
	appendPQExpBufferStr(cmd, " --for-pg-upgrade=");
	appendShellString(cmd, initdb_state_file);
	appendPQExpBufferStr(cmd, " -N -D ");
	appendShellString(cmd, new_cluster.pgdata);
	appendPQExpBufferStr(cmd, " -U ");
	appendShellString(cmd, os_info.user);
	appendPQExpBuffer(cmd, " --wal-segsize=%u",
					  old_cluster.controldata.walseg / (1024 * 1024));

	/*
	 * Pass --data-checksums or --no-data-checksums explicitly.  Starting from
	 * PG18, initdb enables checksums by default, so we must mirror the old
	 * cluster's setting to avoid a mismatch that check_control_data() would
	 * reject.
	 */
	if (old_cluster.controldata.data_checksum_version != 0)
		appendPQExpBufferStr(cmd, " --data-checksums");
	else
		appendPQExpBufferStr(cmd, " --no-data-checksums");

	appendPQExpBufferStr(cmd, " --encoding=");
	appendShellString(cmd, encoding_name);
	appendPQExpBufferStr(cmd, " --locale-provider=");
	appendShellString(cmd, collprovider_name(locale->db_collprovider));
	appendPQExpBufferStr(cmd, " --lc-collate=");
	appendShellString(cmd, locale->db_collate);
	appendPQExpBufferStr(cmd, " --lc-ctype=");
	appendShellString(cmd, locale->db_ctype);

	if (locale->db_locale)
	{
		if (locale->db_collprovider == COLLPROVIDER_ICU)
		{
			appendPQExpBufferStr(cmd, " --icu-locale=");
			appendShellString(cmd, locale->db_locale);
		}
		else if (locale->db_collprovider == COLLPROVIDER_BUILTIN)
		{
			appendPQExpBufferStr(cmd, " --builtin-locale=");
			appendShellString(cmd, locale->db_locale);
		}
	}

	for (int i = 0; i < user_opts.num_initdb_options; i++)
	{
		appendPQExpBufferChar(cmd, ' ');
		appendShellString(cmd, user_opts.initdb_options[i]);
	}

	check_ok();
}


/* Initialize the new cluster before checking compatibility with the old cluster. */
static void
create_new_cluster_via_initdb(void)
{
	PQExpBufferData cmd;

	get_old_cluster_initdb_info();
	build_new_cluster_initdb_cmd(&cmd);

	prep_status("Creating new cluster with initdb");

	exec_prog(UTILITY_LOG_FILE, NULL, true, true, "%s", cmd.data);

	/*
	 * --initdb-options may override -D. Verify that the requested target
	 * contains a valid cluster before enabling its automatic cleanup after
	 * --check.
	 */
	check_data_dir(&new_cluster);
	if (user_opts.check)
	{
		FILE	   *state = fopen(initdb_state_file, "r");
		char	   *header = state ? pg_get_line(state, NULL) : NULL;
		int			existed;
		unsigned int mode;

		initdb_waldir = state ? pg_get_line(state, NULL) : NULL;
		initdb_wal_parent = state ? pg_get_line(state, NULL) : NULL;
		if (!header || !initdb_waldir || !initdb_wal_parent ||
			sscanf(header, "%d %o", &existed, &mode) != 2 ||
			(existed != 0 && existed != 1))
			pg_fatal("could not read initialization state from \"%s\", retaining cluster",
					 initdb_state_file);
		if (fclose(state) != 0)
			pg_fatal("could not close file \"%s\": %m", initdb_state_file);
		pg_strip_crlf(initdb_waldir);
		pg_strip_crlf(initdb_wal_parent);
		initdb_wal_existed = existed;
		initdb_wal_mode = mode;
		pg_free(header);
	}
	initdb_succeeded = true;

	termPQExpBuffer(&cmd);
	check_ok();
}




static void
check_clusters_stopped(void)
{
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

	/* now reset the wal archives in the new cluster */
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
