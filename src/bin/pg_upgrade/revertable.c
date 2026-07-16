/*
 *	revertable.c
 *
 *	LEE: revertable-upgrade lifecycle subcommands for --wal-log-upgrade.
 *
 *	A --wal-log-upgrade new cluster is held (DB_UPGRADE_QUARANTINED) after its
 *	window replays to XLOG_PG_UPGRADE_COMPLETE, instead of going live.  These
 *	subcommands are how the operator resolves that hold:
 *
 *	  --status     -D new   report the new cluster's lifecycle state
 *	  --commit     -D new   finalize: start new_dir (recovery goes live),
 *	                        verify it is up, THEN stamp old_dir superseded
 *	  --rollback   -D new   discard the quarantined new_dir (old_dir untouched)
 *	  --delete-old -d old   delete an old_dir that a commit has superseded
 *
 *	The old cluster is marked "superseded" by renaming its control file to
 *	pg_control.old (as stock disable_old_cluster() does): a durable, on-disk
 *	mark that (a) proves a commit completed and (b) stops the old vN binary
 *	from starting.  --delete-old refuses unless it sees that mark.
 *
 *	Copyright (c) 2010-2025, PostgreSQL Global Development Group
 *	src/bin/pg_upgrade/revertable.c
 */

#include "postgres_fe.h"

#include <sys/stat.h>

#include "access/xlog_internal.h"	/* XLOG_CONTROL_FILE */
#include "catalog/pg_control.h"
#include "common/controldata_utils.h"
#include "common/file_perm.h"
#include "pg_upgrade.h"

#define UPGRADE_COMMIT_SENTINEL "pg_upgrade_commit.signal"

/*
 * Read the DB state recorded in a cluster's control file.  Fatals if the
 * control file cannot be read or its CRC is bad -- we must not act on a
 * cluster we cannot reliably identify.
 */
static DBState
read_db_state(const char *datadir)
{
	ControlFileData *cf;
	bool		crc_ok;
	DBState		state;

	cf = get_controlfile(datadir, &crc_ok);
	if (cf == NULL)
		pg_fatal("could not read control file in \"%s\"", datadir);
	if (!crc_ok)
		pg_fatal("control file in \"%s\" has a bad CRC", datadir);

	state = cf->state;
	pg_free(cf);
	return state;
}

/*
 * Run "pg_ctl <verb>" against a data directory; return true on success.
 *
 * Uses the NEW cluster's binaries (new_cluster.bindir): the lifecycle
 * subcommands only ever start/stop the NEW cluster (reconstructed at the new
 * major version), never the old one -- the old cluster is only ever renamed
 * (superseded stamp) or rm -rf'd, never started by us.  If a future op ever
 * needs to run pg_ctl against the old cluster it must pass the old bindir
 * explicitly rather than reuse this helper.
 *
 * We shell out via system() rather than reuse pg_upgrade's exec_prog() /
 * start_postmaster() on purpose: those carry upgrade-run-specific setup (output
 * dirs, IsBinaryUpgrade options, PGOPTIONS, atexit stop hooks) that does not
 * apply to a standalone lifecycle subcommand acting on an already-built cluster.
 * datadir/logfile come from -D/-d and an internal path, and are double-quoted;
 * pg_ctl's own -w handles wait semantics.
 */
static bool
run_pg_ctl(const char *verb, const char *datadir, const char *logfile)
{
	char		cmd[MAXPGPATH * 3];
	int			rc;

	if (logfile)
		snprintf(cmd, sizeof(cmd),
				 "\"%s/pg_ctl\" -w -D \"%s\" -l \"%s\" %s",
				 new_cluster.bindir, datadir, logfile, verb);
	else
		snprintf(cmd, sizeof(cmd),
				 "\"%s/pg_ctl\" -w -D \"%s\" %s",
				 new_cluster.bindir, datadir, verb);

	fflush(NULL);
	rc = system(cmd);
	return rc == 0;
}

/* Does old_dir carry the "superseded by commit" stamp? */
static bool
old_cluster_superseded(const char *old_datadir)
{
	char		path[MAXPGPATH];
	struct stat st;

	snprintf(path, sizeof(path), "%s/%s.old", old_datadir, XLOG_CONTROL_FILE);
	return stat(path, &st) == 0;
}

/*
 * --status: report the new cluster's lifecycle state.
 */
static void
do_status(void)
{
	DBState		state = read_db_state(new_cluster.pgdata);

	switch (state)
	{
		case DB_UPGRADE_QUARANTINED:
			pg_log(PG_REPORT, "new cluster \"%s\" is QUARANTINED (held, not serving)",
				   new_cluster.pgdata);
			pg_log(PG_REPORT, "Run \"pg_upgrade --commit\" to adopt it, or \"--rollback\" to discard it.");
			break;
		case DB_IN_PRODUCTION:
			pg_log(PG_REPORT, "new cluster \"%s\" is COMMITTED (live)",
				   new_cluster.pgdata);
			break;
		default:
			pg_log(PG_REPORT, "new cluster \"%s\" is not in a pg_upgrade quarantine (state code %d)",
				   new_cluster.pgdata, (int) state);
			break;
	}
}

/*
 * --commit: finalize a quarantined new cluster, then stamp old_dir superseded.
 *
 * STRICT ORDER (the stamp must be last): start new_dir so recovery finalizes
 * and it goes live, verify it is live, and ONLY THEN mark old_dir.  If new_dir
 * failed to come up, we must not stamp/disable old_dir, or the operator would
 * be left with no startable cluster.
 */
static void
do_commit(void)
{
	char		sentinel[MAXPGPATH];
	char		logfile[MAXPGPATH];
	char		old_ctl[MAXPGPATH],
				old_ctl_new[MAXPGPATH];
	FILE	   *fp;

	/*
	 * Gate: you cannot commit a cluster whose upgrade has not been applied yet.
	 * Committing means finalizing an already-reconstructed cluster, so the new
	 * cluster must be HELD in quarantine -- i.e. it was started once, replayed
	 * its upgrade window to COMPLETE, and is holding (DB_UPGRADE_QUARANTINED).
	 * This is also the gate that confines --commit to real --wal-log-upgrade
	 * clusters: no ordinary cluster is ever in this state, so --commit refuses a
	 * random -D.  (A never-started cluster is still pending -- start it first.)
	 */
	if (read_db_state(new_cluster.pgdata) != DB_UPGRADE_QUARANTINED)
		pg_fatal("new cluster \"%s\" is not held in pg_upgrade quarantine\n"
				 "Start it once so it reconstructs and holds, then commit; "
				 "or this is not a --wal-log-upgrade cluster.",
				 new_cluster.pgdata);

	/* Drop the commit sentinel so startup finalizes instead of re-holding. */
	snprintf(sentinel, sizeof(sentinel), "%s/%s", new_cluster.pgdata,
			 UPGRADE_COMMIT_SENTINEL);
	fp = fopen(sentinel, "w");
	if (fp == NULL)
		pg_fatal("could not create commit sentinel \"%s\": %m", sentinel);
	fclose(fp);

	/* Start the new cluster: recovery replays to COMPLETE and finalizes. */
	snprintf(logfile, sizeof(logfile), "%s/pg_upgrade_commit.log",
			 new_cluster.pgdata);
	prep_status("Committing: starting new cluster to finalize recovery");
	if (!run_pg_ctl("start", new_cluster.pgdata, logfile))
	{
		/* leave old_dir untouched; the commit did not take */
		(void) unlink(sentinel);
		pg_fatal("could not start new cluster to finalize commit; "
				 "old cluster is untouched (see \"%s\")", logfile);
	}
	check_ok();

	/* Verify the finalized cluster is actually live. */
	if (read_db_state(new_cluster.pgdata) != DB_IN_PRODUCTION)
	{
		run_pg_ctl("stop", new_cluster.pgdata, NULL);
		pg_fatal("new cluster did not reach production state after commit; "
				 "old cluster is untouched");
	}

	/* Stop it again; the operator restarts it as the live cluster. */
	run_pg_ctl("stop", new_cluster.pgdata, NULL);

	/*
	 * New cluster is verified live -> NOW stamp old_dir superseded (rename its
	 * control file).  This is the point of no return (C4).
	 */
	if (old_cluster.pgdata != NULL && old_cluster.pgdata[0] != '\0')
	{
		snprintf(old_ctl, sizeof(old_ctl), "%s/%s",
				 old_cluster.pgdata, XLOG_CONTROL_FILE);
		snprintf(old_ctl_new, sizeof(old_ctl_new), "%s/%s.old",
				 old_cluster.pgdata, XLOG_CONTROL_FILE);
		prep_status("Marking old cluster as superseded");
		if (rename(old_ctl, old_ctl_new) != 0)
			pg_log(PG_WARNING,
				   "commit succeeded but could not stamp old cluster \"%s\": %m",
				   old_cluster.pgdata);
		else
			check_ok();
	}

	pg_log(PG_REPORT, "\nCommit complete.  The new cluster is now the live cluster.");
	pg_log(PG_REPORT, "Start it with: pg_ctl -D \"%s\" start", new_cluster.pgdata);
	if (old_cluster.pgdata != NULL && old_cluster.pgdata[0] != '\0')
		pg_log(PG_REPORT, "The old cluster at \"%s\" is superseded; remove it with \"pg_upgrade --delete-old -d %s\".",
			   old_cluster.pgdata, old_cluster.pgdata);
}

/*
 * --rollback: discard a quarantined new cluster.  old_dir was never touched.
 */
static void
do_rollback(void)
{
	DBState		state = read_db_state(new_cluster.pgdata);

	if (state != DB_UPGRADE_QUARANTINED)
		pg_fatal("new cluster \"%s\" is not held in pg_upgrade quarantine "
				 "(refusing to roll back)", new_cluster.pgdata);

	prep_status("Rolling back: discarding new cluster \"%s\"", new_cluster.pgdata);
	if (!rmtree(new_cluster.pgdata, true))
		pg_fatal("could not remove new cluster directory \"%s\"", new_cluster.pgdata);
	check_ok();

	pg_log(PG_REPORT, "\nRollback complete.  The new cluster was discarded.");
	pg_log(PG_REPORT, "The old cluster is untouched; start it with your original binary.");
}

/*
 * --delete-old: delete an old cluster that a commit has superseded.
 */
static void
do_delete_old(void)
{
	if (old_cluster.pgdata == NULL || old_cluster.pgdata[0] == '\0')
		pg_fatal("--delete-old requires the old cluster data directory (-d)");

	if (!old_cluster_superseded(old_cluster.pgdata))
		pg_fatal("old cluster \"%s\" has not been superseded by a committed upgrade; "
				 "refusing to delete", old_cluster.pgdata);

	prep_status("Deleting superseded old cluster \"%s\"", old_cluster.pgdata);
	if (!rmtree(old_cluster.pgdata, true))
		pg_fatal("could not remove old cluster directory \"%s\"", old_cluster.pgdata);
	check_ok();

	pg_log(PG_REPORT, "\nOld cluster deleted.");
}

/*
 * --signal-handoff: connect to the LIVE old primary and write the
 * streaming-handoff trigger into its (old-format) WAL.  This does NOT push to
 * each standby directly -- it emits a WAL record, which propagates to streaming
 * standbys through the normal WAL path (in Neon, primary -> safekeepers ->
 * standby).  On replaying it, a standby shuts down cleanly, ready for the
 * new-version binary swap / re-provision.  Run this BEFORE stopping the old
 * primary and running pg_upgrade.
 *
 * Unlike the other subcommands (which act on stopped clusters), this one
 * requires the old primary to be RUNNING.  The target major version passed to
 * the trigger is this pg_upgrade binary's own major (the new version the
 * standby will converge to).
 */
static void
do_signal_handoff(void)
{
	PGconn	   *conn;
	PGresult   *res;
	char		query[128];

	if (old_cluster.pgdata == NULL || old_cluster.pgdata[0] == '\0')
		pg_fatal("--signal-handoff requires the old cluster data directory (-d)");

	/*
	 * The old primary is RUNNING here.  Flag a live check so get_sock_dir()
	 * reads the actual socket directory (and port) from the server's
	 * postmaster.pid, rather than defaulting to the current directory.
	 */
	user_opts.live_check = true;
	get_sock_dir(&old_cluster);

	prep_status("Signaling handoff on the old primary (port %d)",
				old_cluster.port);

	conn = connectToServer(&old_cluster, "template1");

	snprintf(query, sizeof(query),
			 "SELECT pg_write_pg_upgrade_handoff(%d)", PG_MAJORVERSION_NUM);
	res = executeQueryOrDie(conn, "%s", query);
	PQclear(res);
	PQfinish(conn);
	check_ok();

	pg_log(PG_REPORT,
		   "\nHandoff trigger written to the old primary's WAL.  It propagates to\n"
		   "streaming standbys (via the safekeepers in Neon), which replay it and\n"
		   "shut down.  Now stop the old primary and run\n"
		   "\"pg_upgrade --wal-log-upgrade ...\"; then re-provision each standby\n"
		   "from the delivered upgrade window.");
}

/*
 * Generate the revertable-upgrade lifecycle scripts, in the same style as
 * create_script_for_old_cluster_deletion(): self-contained shell scripts with
 * all paths baked in, so the operator runs a path-free script rather than
 * re-typing -b/-B/-d/-D.  Written at the end of a --wal-log-upgrade run.
 *
 *   pg_upgrade_commit.sh   -> pg_upgrade --commit   (adopt new, stamp old)
 *   pg_upgrade_rollback.sh -> pg_upgrade --rollback (discard new, keep old)
 *
 * (delete_old_cluster.sh is still produced by the stock deletion-script path.)
 */
void
create_revertable_scripts(void)
{
	char	   *commit_file;
	char	   *rollback_file;
	FILE	   *script;
	const char *newbin = new_cluster.bindir;

	commit_file = psprintf("%spg_upgrade_commit.%s", SCRIPT_PREFIX, SCRIPT_EXT);
	rollback_file = psprintf("%spg_upgrade_rollback.%s", SCRIPT_PREFIX, SCRIPT_EXT);

	/* --- commit script --- */
	prep_status("Writing revertable-upgrade scripts");
	if ((script = fopen_priv(commit_file, "w")) == NULL)
		pg_fatal("could not open file \"%s\": %m", commit_file);
#ifndef WIN32
	fprintf(script, "#!/bin/sh\n\n");
#endif
	fprintf(script,
			"# Adopt the upgraded cluster: bring it live and mark the old\n"
			"# cluster superseded.  The old cluster is retained; remove it\n"
			"# afterwards with delete_old_cluster.%s once you are confident.\n\n",
			SCRIPT_EXT);
	fprintf(script, "%c%s%cpg_upgrade%c --commit -b %c%s%c -B %c%s%c -d %c%s%c -D %c%s%c\n",
			PATH_QUOTE, newbin, PATH_SEPARATOR, PATH_QUOTE,
			PATH_QUOTE, old_cluster.bindir, PATH_QUOTE,
			PATH_QUOTE, newbin, PATH_QUOTE,
			PATH_QUOTE, old_cluster.pgdata, PATH_QUOTE,
			PATH_QUOTE, new_cluster.pgdata, PATH_QUOTE);
	fclose(script);

	/* --- rollback script --- */
	if ((script = fopen_priv(rollback_file, "w")) == NULL)
		pg_fatal("could not open file \"%s\": %m", rollback_file);
#ifndef WIN32
	fprintf(script, "#!/bin/sh\n\n");
#endif
	fprintf(script,
			"# Discard the upgraded cluster.  The old cluster was never\n"
			"# touched; start it again with its original (old) binaries.\n\n");
	fprintf(script, "%c%s%cpg_upgrade%c --rollback -D %c%s%c\n",
			PATH_QUOTE, newbin, PATH_SEPARATOR, PATH_QUOTE,
			PATH_QUOTE, new_cluster.pgdata, PATH_QUOTE);
	fclose(script);

#ifndef WIN32
	if (chmod(commit_file, S_IRWXU) != 0)
		pg_fatal("could not add execute permission to file \"%s\": %m", commit_file);
	if (chmod(rollback_file, S_IRWXU) != 0)
		pg_fatal("could not add execute permission to file \"%s\": %m", rollback_file);
#endif

	pg_free(commit_file);
	pg_free(rollback_file);
	check_ok();
}

/*
 * Dispatch a revertable-upgrade lifecycle subcommand and exit.  Called from
 * main() before the normal upgrade flow when user_opts.revertable_op is set.
 */
void
perform_revertable_op(void)
{
	switch (user_opts.revertable_op)
	{
		case REVERTABLE_OP_STATUS:
			do_status();
			break;
		case REVERTABLE_OP_COMMIT:
			do_commit();
			break;
		case REVERTABLE_OP_ROLLBACK:
			do_rollback();
			break;
		case REVERTABLE_OP_DELETE_OLD:
			do_delete_old();
			break;
		case REVERTABLE_OP_SIGNAL_HANDOFF:
			do_signal_handoff();
			break;
		case REVERTABLE_OP_NONE:
			break;				/* not reached */
	}
}
