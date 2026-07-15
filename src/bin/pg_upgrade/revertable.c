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
 * Stamp the new cluster's control file as DB_UPGRADE_QUARANTINED.
 *
 * Called at the end of a --wal-log-upgrade run.  pg_upgrade's internal server
 * work leaves the control state at DB_IN_PRODUCTION, which is untruthful: the
 * on-disk data files have been reverted and the cluster cannot serve until its
 * WAL window is replayed.  Recording the quarantine state here makes the state
 * honest BEFORE any startup, so "pg_upgrade --status/--commit/--rollback" read
 * the correct state on a freshly-upgraded, never-started cluster.
 */
void
mark_new_cluster_quarantined(void)
{
	ControlFileData *cf;
	bool		crc_ok;

	cf = get_controlfile(new_cluster.pgdata, &crc_ok);
	if (cf == NULL || !crc_ok)
		pg_fatal("could not read control file in \"%s\" to mark quarantine",
				 new_cluster.pgdata);

	cf->state = DB_UPGRADE_QUARANTINED;
	update_controlfile(new_cluster.pgdata, cf, true);
	pg_free(cf);
}

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

/* Run "pg_ctl <verb>" against a data directory; return true on success. */
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
	DBState		state = read_db_state(new_cluster.pgdata);
	char		sentinel[MAXPGPATH];
	char		logfile[MAXPGPATH];
	char		old_ctl[MAXPGPATH],
				old_ctl_new[MAXPGPATH];
	FILE	   *fp;

	/*
	 * Refuse only if the cluster is already live/committed.  We accept both a
	 * pre-stamped DB_UPGRADE_QUARANTINED cluster (the normal primary case) and
	 * a cluster that merely has a pending upgrade window in pg_wal but was never
	 * stamped -- e.g. a fresh skeleton fed the WAL (the standby/replay case).
	 * In the latter the state is whatever initdb left (DB_SHUTDOWNED); the
	 * commit sentinel makes first startup finalize the window either way, and if
	 * there is no window the backend refuses to start with its own clear error.
	 */
	if (state == DB_IN_PRODUCTION)
		pg_fatal("new cluster \"%s\" is already live (nothing to commit)",
				 new_cluster.pgdata);

	/* Drop the commit sentinel so first startup finalizes instead of re-holding. */
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
		case REVERTABLE_OP_NONE:
			break;				/* not reached */
	}
}
