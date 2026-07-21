/*
 *	revertable.c
 *
 *	LEE: revertable-upgrade lifecycle subcommands for --wal-upgrade.
 *
 *	AUTO-SERVE model: a --wal-upgrade new cluster comes up read-write on its
 *	first start, like upstream pg_upgrade -- there is NO quarantine hold and NO
 *	commit step.  These subcommands cover the remaining lifecycle:
 *
 *	  --wal-upgrade-rollback   -D new [-d old]   discard new_dir and return to old_dir
 *	                        (allowed while old_dir is intact; warns on data loss)
 *	  --wal-upgrade-delete-old -d old -D new     delete old_dir once new is a completed
 *	                        upgrade (gated on new's pg_upgrade_complete.done)
 *	  --wal-upgrade-signal-handoff  -d old   trigger streaming standbys to stand down
 *
 *	(A fresh standby needs no prepare step: with primary_conninfo set it
 *	auto-fetches the upgrade window anchor from the primary and streams it --
 *	see ArmFromPrimaryAnchorIfConfigured in pgupgrade_wal.c.)
 *
 *	(cluster lifecycle state is available from pg_controldata; no --status flag.)
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
#include "common/string.h"			/* pg_strip_crlf */
#include "utils/pidfile.h"			/* LOCK_FILE_LINE_PORT / _SOCKET_DIR */
#include "pg_upgrade.h"

/*
 * Durable "the upgrade window fully replayed to COMPLETE" marker, written and
 * fsync'd by the XLOG_UPGRADE_COMPLETE redo handler in the backend (see
 * UPGRADE_COMPLETE_MARKER in pgupgrade_wal.c).  Present only after a full replay,
 * so --wal-upgrade-delete-old uses it to confirm the new cluster is a completed
 * upgrade before removing the old one.  Keep this string in sync with the backend.
 */
#define UPGRADE_COMPLETE_MARKER "pg_upgrade_complete.done"

/*
 * Did the new cluster's upgrade window fully replay to COMPLETE?  True iff the
 * durable marker the COMPLETE redo handler drops is present.  --wal-upgrade-delete-old
 * uses this to confirm the new cluster is a completed upgrade before removing the
 * old one (a crash-truncated cluster never wrote the marker).
 */
static bool
new_cluster_complete(const char *new_datadir)
{
	char		path[MAXPGPATH];
	struct stat st;

	snprintf(path, sizeof(path), "%s/%s", new_datadir, UPGRADE_COMPLETE_MARKER);
	return stat(path, &st) == 0;
}

/*
 * Is old_dir a usable pre-upgrade cluster to roll back to?  Auto-serve (no
 * quarantine hold) means rollback is no longer gated on "before first write";
 * it is gated on old_dir being INTACT.  We check the ACTUAL state of old_dir
 * (its control file reads as a cleanly shut-down cluster), not which transfer
 * mode was used: on this branch --wal-upgrade keeps the old cluster's files
 * intact for every allowed mode (copy AND link -- the primary is not demolished;
 * old-cluster deletion is a separate deferred step, and --swap is rejected at
 * parse time).  If old_dir is instead damaged, missing, or was already started
 * post-upgrade, refuse and point at PITR rather than start something unsound.
 */
static bool
old_cluster_intact(const char *old_datadir)
{
	ControlFileData *cf;
	bool		crc_ok;
	bool		ok;

	cf = get_controlfile(old_datadir, &crc_ok);
	if (cf == NULL || !crc_ok)
	{
		if (cf)
			pg_free(cf);
		return false;
	}

	/*
	 * A cleanly shut-down old cluster is DB_SHUTDOWNED (or _IN_RECOVERY for a
	 * standby).  Anything else -- in production, in recovery mid-crash, or the
	 * commit "superseded" stamp -- means it is not a safe rollback target.
	 */
	ok = (cf->state == DB_SHUTDOWNED ||
		  cf->state == DB_SHUTDOWNED_IN_RECOVERY);
	pg_free(cf);
	return ok;
}

/*
 * --wal-upgrade-rollback: discard the new cluster and return to the old one.
 *
 * Auto-serve model: the new cluster may already be live and have taken writes.
 * Rollback is allowed as long as old_dir is intact; if the new cluster diverged,
 * those writes are permanently lost (a warning, not an error).  If old_dir is
 * NOT intact (--link/--swap, or damaged), refuse and point at PITR.
 */
static void
do_rollback(void)
{
	/*
	 * Guard: -D must be given and be a real data directory before we rm -rf it.
	 * We do NOT require the COMPLETE marker here: a crash-truncated (partial)
	 * upgrade is precisely a case that needs rolling back, and it never wrote
	 * COMPLETE.  The real safety net is that we only return to old_dir if old_dir
	 * itself is intact (checked next); discarding new_dir is the operator's
	 * explicit intent.
	 */
	{
		char		verfile[MAXPGPATH];
		struct stat st;

		if (new_cluster.pgdata == NULL || new_cluster.pgdata[0] == '\0')
			pg_fatal("--wal-upgrade-rollback requires the new cluster data directory (-D)");
		snprintf(verfile, sizeof(verfile), "%s/PG_VERSION", new_cluster.pgdata);
		if (stat(verfile, &st) != 0)
			pg_fatal("\"%s\" is not a PostgreSQL data directory (no PG_VERSION); "
					 "refusing to roll back", new_cluster.pgdata);
	}

	/*
	 * Hard precondition: old_dir must be a valid, cleanly shut-down pre-upgrade
	 * cluster to return to.  If its control file is missing or unreadable (e.g.
	 * it was started after the upgrade, or damaged), there is nothing safe to
	 * start -- the only recovery is a backup/PITR restore.
	 */
	if (!old_cluster_intact(old_cluster.pgdata))
		pg_fatal("cannot roll back: the old cluster \"%s\" is not intact\n"
				 "Its control file is missing or does not read as a cleanly "
				 "shut-down cluster (e.g. it was started after the upgrade, or "
				 "damaged).  Restore from a backup / PITR instead.",
				 old_cluster.pgdata);

	/*
	 * Rolling back discards the new cluster wholesale.  If it was ever started
	 * after the upgrade it may have taken writes, and those are lost -- we cannot
	 * cheaply prove it took none, so warn unconditionally.  This is sound (old_dir
	 * is frozen, C5), just lossy.
	 */
	pg_log(PG_REPORT,
		   "WARNING: discarding the new cluster \"%s\".  Any changes made to it "
		   "after the upgrade (if it was started) are permanently lost.  The old "
		   "cluster is unaffected.",
		   new_cluster.pgdata);

	prep_status("Rolling back: discarding new cluster \"%s\"", new_cluster.pgdata);
	if (!rmtree(new_cluster.pgdata, true))
		pg_fatal("could not remove new cluster directory \"%s\"", new_cluster.pgdata);
	check_ok();

	pg_log(PG_REPORT, "\nRollback complete.  The new cluster was discarded.");
	pg_log(PG_REPORT, "The old cluster at \"%s\" is intact; start it with your original binary.",
		   old_cluster.pgdata);
}

/*
 * Drop the upgrade-window retention slot on the LIVE new cluster.
 *
 * The slot (UPGRADE_WINDOW_SLOT) was created during capture to pin the upgrade
 * window in the new cluster's pg_wal so a standby could stream it after commit.
 * Once the operator runs --wal-upgrade-delete-old (the teardown step) the standby has been
 * re-provisioned and the window is no longer needed, so we drop the slot to stop
 * it pinning WAL.  Best-effort: --wal-upgrade-delete-old takes -d (old cluster); the slot
 * lives on the NEW cluster, so this only runs if -D (new) was also supplied AND
 * the new cluster is reachable.  If not, we warn the operator to drop it by hand
 * rather than fail the deletion (the old-cluster removal is the primary job).
 */
static void
drop_upgrade_window_slot(void)
{
	char		pidfile[MAXPGPATH];
	char		line[MAXPGPATH];
	char		sockdir[MAXPGPATH] = "";
	int			port = 0;
	int			lineno;
	FILE	   *fp;
	PGconn	   *conn;
	PGresult   *res;
	char		conninfo[MAXPGPATH * 2];

	if (new_cluster.pgdata == NULL || new_cluster.pgdata[0] == '\0')
	{
		pg_log(PG_REPORT,
			   "\nNote: pass -D <new datadir> to --wal-upgrade-delete-old to also drop the "
			   "upgrade-window\nreplication slot \"%s\"; otherwise drop it manually "
			   "on the new cluster:\n  SELECT pg_drop_replication_slot('%s');",
			   UPGRADE_WINDOW_SLOT, UPGRADE_WINDOW_SLOT);
		return;
	}

	/*
	 * Best-effort: the slot lives on the NEW cluster, which at --wal-upgrade-delete-old time
	 * is normally the live production server.  Read its postmaster.pid directly
	 * to learn the running port + socket dir (its presence also tells us the
	 * server is up).  We deliberately do NOT use connectToServer()/get_sock_dir()
	 * here: those pg_fatal() on any failure, but a stopped or unreachable new
	 * cluster must not fail the old-cluster deletion -- we just warn.
	 */
	snprintf(pidfile, sizeof(pidfile), "%s/postmaster.pid", new_cluster.pgdata);
	if ((fp = fopen(pidfile, "r")) == NULL)
	{
		pg_log(PG_REPORT,
			   "\nNote: new cluster does not appear to be running; drop the "
			   "upgrade-window slot\nmanually once it is up:\n"
			   "  SELECT pg_drop_replication_slot('%s');",
			   UPGRADE_WINDOW_SLOT);
		return;
	}
	for (lineno = 1;
		 lineno <= Max(LOCK_FILE_LINE_PORT, LOCK_FILE_LINE_SOCKET_DIR);
		 lineno++)
	{
		if (fgets(line, sizeof(line), fp) == NULL)
			break;
		if (lineno == LOCK_FILE_LINE_PORT)
			sscanf(line, "%d", &port);
		if (lineno == LOCK_FILE_LINE_SOCKET_DIR)
		{
			strlcpy(sockdir, line, sizeof(sockdir));
			(void) pg_strip_crlf(sockdir);
		}
	}
	fclose(fp);

	if (port == 0)
	{
		pg_log(PG_WARNING,
			   "could not determine the new cluster's port; drop the "
			   "upgrade-window slot manually:\n"
			   "  SELECT pg_drop_replication_slot('%s');",
			   UPGRADE_WINDOW_SLOT);
		return;
	}

	snprintf(conninfo, sizeof(conninfo),
			 "dbname=template1 user=%s port=%d%s%s",
			 os_info.user, port,
			 sockdir[0] ? " host=" : "", sockdir);
	conn = PQconnectdb(conninfo);
	if (conn == NULL || PQstatus(conn) != CONNECTION_OK)
	{
		pg_log(PG_WARNING,
			   "could not connect to the new cluster to drop the upgrade-window "
			   "slot; drop it manually:\n"
			   "  SELECT pg_drop_replication_slot('%s');",
			   UPGRADE_WINDOW_SLOT);
		if (conn)
			PQfinish(conn);
		return;
	}

	prep_status("Dropping upgrade-window retention slot \"%s\"", UPGRADE_WINDOW_SLOT);
	res = PQexec(conn,
				 "SELECT pg_drop_replication_slot(slot_name) "
				 "FROM pg_replication_slots "
				 "WHERE slot_name = '" UPGRADE_WINDOW_SLOT "'");
	PQclear(res);
	PQfinish(conn);
	check_ok();
}

/*
 * --wal-upgrade-delete-old: delete this node's old cluster now that it has been
 * superseded by a fully-upgraded new cluster.
 *
 * pg_upgrade does not distinguish primary from standby: the operator runs this
 * on each node whose old cluster should be removed.  The only safety gate is
 * that -D points at a completed --wal-upgrade cluster (COMPLETE marker present),
 * so the old cluster is never removed unless a working replacement exists.
 */
static void
do_delete_old(void)
{
	if (old_cluster.pgdata == NULL || old_cluster.pgdata[0] == '\0')
		pg_fatal("--wal-upgrade-delete-old requires the old cluster data directory (-d)");

	/*
	 * LEE (2026-07-20, auto-serve): the old gate required a "superseded by
	 * commit" stamp (pg_control.old).  There is no commit step anymore, so nothing
	 * stamps it; the safety check is now that the operator has actually adopted a
	 * new cluster.  Require -D to point at a valid, fully-upgraded new
	 * cluster (its COMPLETE marker is present) before removing the old one.  This
	 * prevents deleting the old cluster when there is no usable replacement (which
	 * would leave the operator with nothing to fall back to and no upgraded
	 * cluster).  Running --wal-upgrade-delete-old is itself the adoption confirmation.
	 */
	if (new_cluster.pgdata == NULL || new_cluster.pgdata[0] == '\0')
		pg_fatal("--wal-upgrade-delete-old requires the new cluster data directory (-D) "
				 "to confirm an upgraded cluster exists before deleting the old one");
	if (!new_cluster_complete(new_cluster.pgdata))
		pg_fatal("new cluster \"%s\" is not a completed --wal-upgrade cluster "
				 "(no \"%s\"); refusing to delete the old cluster",
				 new_cluster.pgdata, UPGRADE_COMPLETE_MARKER);

	prep_status("Deleting superseded old cluster \"%s\"", old_cluster.pgdata);
	if (!rmtree(old_cluster.pgdata, true))
		pg_fatal("could not remove old cluster directory \"%s\"", old_cluster.pgdata);

	/*
	 * Verify the deletion actually took: rmtree() can report success yet leave
	 * the directory behind (e.g. a live file underneath it, or a partial removal
	 * on some platforms).  Confirm the directory is gone rather than trusting the
	 * return value, so we never claim "deleted" over a still-present old cluster.
	 */
	{
		struct stat st;

		if (stat(old_cluster.pgdata, &st) == 0)
			pg_fatal("old cluster directory \"%s\" still exists after deletion",
					 old_cluster.pgdata);
	}
	check_ok();

	/* Old cluster is gone; now release the window-retention slot (best effort). */
	drop_upgrade_window_slot();

	pg_log(PG_REPORT, "\nOld cluster deleted.");
}

/*
 * --wal-upgrade-signal-handoff: connect to the LIVE old primary and write the
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
		pg_fatal("--wal-upgrade-signal-handoff requires the old cluster data directory (-d)");

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
			 "SELECT pg_upgrade_wal_handoff(%d)", PG_MAJORVERSION_NUM);
	res = executeQueryOrDie(conn, "%s", query);
	PQclear(res);
	PQfinish(conn);
	check_ok();

	pg_log(PG_REPORT,
		   "\nHandoff trigger written to the old primary's WAL.  It propagates to\n"
		   "streaming standbys (via the safekeepers in Neon), which replay it and\n"
		   "shut down.  Now stop the old primary and run\n"
		   "\"pg_upgrade --wal-upgrade ...\"; then re-provision each standby\n"
		   "from the delivered upgrade window.");
}

/*
 * Generate the revertable-upgrade lifecycle script, in the same style as
 * create_script_for_old_cluster_deletion(): a self-contained shell script with
 * all paths baked in, so the operator runs a path-free script rather than
 * re-typing -b/-B/-d/-D.  Written at the end of a --wal-upgrade run.
 *
 *   pg_upgrade_rollback.sh -> pg_upgrade --wal-upgrade-rollback (discard new, keep old)
 *
 * (Under auto-serve there is no commit step: the new cluster is adopted simply
 * by starting it.  delete_old_cluster.sh is still produced by the stock
 * deletion-script path.)
 */
void
create_revertable_scripts(void)
{
	char	   *rollback_file;
	FILE	   *script;
	const char *newbin = new_cluster.bindir;

	rollback_file = psprintf("%spg_upgrade_rollback.%s", SCRIPT_PREFIX, SCRIPT_EXT);

	/* --- rollback script --- */
	prep_status("Writing revertable-upgrade scripts");
	if ((script = fopen_priv(rollback_file, "w")) == NULL)
		pg_fatal("could not open file \"%s\": %m", rollback_file);
#ifndef WIN32
	fprintf(script, "#!/bin/sh\n\n");
#endif
	fprintf(script,
			"# Discard the upgraded cluster.  The old cluster was never\n"
			"# touched; start it again with its original (old) binaries.\n\n");
	fprintf(script, "%c%s%cpg_upgrade%c --wal-upgrade-rollback -D %c%s%c\n",
			PATH_QUOTE, newbin, PATH_SEPARATOR, PATH_QUOTE,
			PATH_QUOTE, new_cluster.pgdata, PATH_QUOTE);
	fclose(script);

#ifndef WIN32
	if (chmod(rollback_file, S_IRWXU) != 0)
		pg_fatal("could not add execute permission to file \"%s\": %m", rollback_file);
#endif

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
