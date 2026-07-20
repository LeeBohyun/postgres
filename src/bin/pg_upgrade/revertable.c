/*
 *	revertable.c
 *
 *	LEE: revertable-upgrade lifecycle subcommands for --wal-log-upgrade.
 *
 *	AUTO-SERVE model: a --wal-log-upgrade new cluster comes up read-write on its
 *	first start, like upstream pg_upgrade -- there is NO quarantine hold and NO
 *	commit step.  These subcommands cover the remaining lifecycle:
 *
 *	  --wal-log-rollback   -D new [-d old]   discard new_dir and return to old_dir
 *	                        (allowed while old_dir is intact; warns on data loss)
 *	  --wal-log-delete-old -d old -D new     delete old_dir once new is a completed
 *	                        upgrade (gated on new's pg_upgrade_complete.done)
 *	  --wal-log-prepare-standby -D new   stage a fresh skeleton to STREAM the window
 *	                        (fallback; a skeleton with primary_conninfo auto-fetches
 *	                        the anchor on its own -- see ArmFromPrimaryAnchorIfConfigured)
 *	  --wal-log-signal-handoff  -d old   trigger streaming standbys to stand down
 *	  --wal-log-commit     obsolete no-op (kept only to print a helpful message)
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
 * fsync'd by the XLOG_PG_UPGRADE_COMPLETE redo handler in the backend (see
 * UPGRADE_COMPLETE_MARKER in pgupgrade_wal.c).  Present only after a full replay,
 * so --wal-log-delete-old uses it to confirm the new cluster is a completed
 * upgrade before removing the old one.  Keep this string in sync with the backend.
 */
#define UPGRADE_COMPLETE_MARKER "pg_upgrade_complete.done"

/*
 * Did the new cluster's upgrade window fully replay to COMPLETE?  True iff the
 * durable marker the COMPLETE redo handler drops is present.  --wal-log-delete-old
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
 * --wal-log-commit: OBSOLETE under auto-serve.  The new cluster comes up
 * read-write on first start (there is no quarantine hold to release), so there
 * is nothing to commit -- a cluster is adopted simply by starting it, and backed
 * out with --wal-log-rollback (gated on old_dir integrity).  The option is kept
 * only so an operator or script that still passes it gets a clear message rather
 * than an "unknown option" error.
 */
static void
do_commit(void)
{
	pg_log(PG_REPORT,
		   "--wal-log-commit is no longer required: the new cluster auto-serves "
		   "on first start.  Just start it to adopt the upgrade; use "
		   "\"pg_upgrade --wal-log-rollback\" to back out while the old cluster "
		   "is intact.");
}

/*
 * Is old_dir a usable pre-upgrade cluster to roll back to?  Auto-serve (no
 * quarantine hold) means rollback is no longer gated on "before first write";
 * it is gated on old_dir being INTACT.  We check the ACTUAL state of old_dir
 * (its control file reads as a cleanly shut-down cluster), not which transfer
 * mode was used: on this branch --wal-log-upgrade keeps the old cluster's files
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
 * --wal-log-rollback: discard the new cluster and return to the old one.
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
			pg_fatal("--wal-log-rollback requires the new cluster data directory (-D)");
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
 * Once the operator runs --wal-log-delete-old (the teardown step) the standby has been
 * re-provisioned and the window is no longer needed, so we drop the slot to stop
 * it pinning WAL.  Best-effort: --wal-log-delete-old takes -d (old cluster); the slot
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
			   "\nNote: pass -D <new datadir> to --wal-log-delete-old to also drop the "
			   "upgrade-window\nreplication slot \"%s\"; otherwise drop it manually "
			   "on the new cluster:\n  SELECT pg_drop_replication_slot('%s');",
			   UPGRADE_WINDOW_SLOT, UPGRADE_WINDOW_SLOT);
		return;
	}

	/*
	 * Best-effort: the slot lives on the NEW cluster, which at --wal-log-delete-old time
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

	/*
	 * Emit the set-wide delete-authorize signal into the live new primary's WAL
	 * FIRST (before dropping the slot): it streams to NEW standbys, which on
	 * replay mark their own old cluster delete-authorized, so the operator can run
	 * "pg_upgrade --wal-log-delete-old" on each standby without extra ceremony.  This is a
	 * no-op if there are no standbys.  Best-effort: a failure here must not fail
	 * the primary's own old-cluster deletion (already done by the caller).
	 */
	prep_status("Signaling standbys that the old cluster may be deleted");
	res = PQexec(conn, "SELECT pg_write_pg_upgrade_delete_authorize()");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		pg_log(PG_WARNING,
			   "could not emit the set-wide delete-authorize signal: %s"
			   "standbys must be told to delete their old clusters manually",
			   PQerrorMessage(conn));
	PQclear(res);
	check_ok();

	prep_status("Dropping upgrade-window retention slot \"%s\"", UPGRADE_WINDOW_SLOT);
	res = PQexec(conn,
				 "SELECT pg_drop_replication_slot(slot_name) "
				 "FROM pg_replication_slots "
				 "WHERE slot_name = '" UPGRADE_WINDOW_SLOT "'");
	PQclear(res);
	PQfinish(conn);
	check_ok();
}

/* Did a replayed set-wide delete-authorize signal land in the new cluster? */
static bool
delete_authorized_by_signal(const char *new_datadir)
{
	char		path[MAXPGPATH];
	struct stat st;

	if (new_datadir == NULL || new_datadir[0] == '\0')
		return false;
	snprintf(path, sizeof(path), "%s/%s", new_datadir, "pg_upgrade_delete_authorized");
	return stat(path, &st) == 0;
}

/*
 * --wal-log-delete-old: delete an old cluster that a commit has superseded.
 *
 * On a STANDBY, the new cluster may also carry a replayed set-wide
 * delete-authorize signal (pg_upgrade_delete_authorized), emitted by --wal-log-delete-old
 * on the primary.  We report that as the authorizing reason, but still require
 * the old dir to be superseded (the real safety gate): the signal is a fleet-wide
 * "go", not a license to delete a still-live cluster.
 */
static void
do_delete_old(void)
{
	if (old_cluster.pgdata == NULL || old_cluster.pgdata[0] == '\0')
		pg_fatal("--wal-log-delete-old requires the old cluster data directory (-d)");

	/*
	 * LEE (2026-07-20, auto-serve): the old gate required a "superseded by
	 * commit" stamp (pg_control.old) that --wal-log-commit wrote.  Commit is gone,
	 * so nothing stamps it; the safety check is now that the operator has actually
	 * adopted a new cluster.  Require -D to point at a valid, fully-upgraded new
	 * cluster (its COMPLETE marker is present) before removing the old one.  This
	 * prevents deleting the old cluster when there is no usable replacement (which
	 * would leave the operator with nothing to fall back to and no upgraded
	 * cluster).  Running --wal-log-delete-old is itself the adoption confirmation.
	 */
	if (new_cluster.pgdata == NULL || new_cluster.pgdata[0] == '\0')
		pg_fatal("--wal-log-delete-old requires the new cluster data directory (-D) "
				 "to confirm an upgraded cluster exists before deleting the old one");
	if (!new_cluster_complete(new_cluster.pgdata))
		pg_fatal("new cluster \"%s\" is not a completed --wal-log-upgrade cluster "
				 "(no \"%s\"); refusing to delete the old cluster",
				 new_cluster.pgdata, UPGRADE_COMPLETE_MARKER);

	if (delete_authorized_by_signal(new_cluster.pgdata))
		pg_log(PG_REPORT,
			   "set-wide delete-authorize signal present; proceeding to delete this node's old cluster");

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
 * --wal-log-signal-handoff: connect to the LIVE old primary and write the
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
		pg_fatal("--wal-log-signal-handoff requires the old cluster data directory (-d)");

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
 * --wal-log-prepare-standby: stamp a fresh skeleton (-D) so it can STREAM the upgrade
 * window from the LIVE primary (named by the standard primary_conninfo GUC in the
 * skeleton's config) -- no cp of WAL.
 *
 * Runs on the standby host AFTER the primary has been committed and is live as
 * the new version, and while it still RETAINS the upgrade window (pinned by the
 * UPGRADE_WINDOW_SLOT retention slot; --wal-log-delete-old drops it).  We connect to the
 * primary, learn the three facts a fresh skeleton needs to stream the window --
 * its system identifier, its timeline, and the window anchor CN -- and drop them
 * into the skeleton as UPGRADE_STREAM_ANCHOR, then wire up streaming config
 * (standby.signal + primary_conninfo + primary_slot_name).  First startup then
 * arms the control file from the anchor (before the walreceiver connects, so the
 * sysid check passes) and streams the window from CN.
 *
 * Why the anchor is needed at all: a fresh initdb skeleton has its own random
 * sysid, so its walreceiver would reject the primary ("system identifier
 * differs") before any WAL flows, and it would not know where the window starts.
 */

/*
 * Read the primary_conninfo GUC from a data directory's config, returning a
 * palloc'd copy of the value (without surrounding quotes) or NULL if unset.  We
 * check postgresql.auto.conf first (ALTER SYSTEM wins at load time), then
 * postgresql.conf.  This is a deliberately small parser: it matches an active
 * (non-comment) "primary_conninfo" setting and extracts the single-quoted value.
 * It avoids adding a pg_upgrade flag that would just duplicate this standard GUC.
 */
static char *
read_primary_conninfo_from(const char *path)
{
	FILE	   *fp;
	char		line[4096];
	char	   *result = NULL;

	if ((fp = fopen(path, "r")) == NULL)
		return NULL;
	while (fgets(line, sizeof(line), fp) != NULL)
	{
		char	   *p = line;
		char	   *q;

		while (*p == ' ' || *p == '\t')
			p++;
		if (strncmp(p, "primary_conninfo", 16) != 0)
			continue;
		p += 16;
		while (*p == ' ' || *p == '\t')
			p++;
		if (*p != '=')
			continue;
		p++;
		while (*p == ' ' || *p == '\t')
			p++;
		if (*p != '\'')
			continue;			/* expect a single-quoted value */
		p++;
		q = strchr(p, '\'');
		if (q == NULL)
			continue;
		*q = '\0';
		if (result)
			pg_free(result);	/* a later line overrides an earlier one */
		result = pg_strdup(p);
	}
	fclose(fp);
	return result;
}

/* primary_conninfo: auto.conf (ALTER SYSTEM) overrides postgresql.conf. */
static char *
read_primary_conninfo(const char *datadir)
{
	char		path[MAXPGPATH];
	char	   *v;

	snprintf(path, sizeof(path), "%s/postgresql.auto.conf", datadir);
	v = read_primary_conninfo_from(path);
	if (v != NULL)
		return v;
	snprintf(path, sizeof(path), "%s/postgresql.conf", datadir);
	return read_primary_conninfo_from(path);
}

static void
do_prepare_standby(void)
{
	PGconn	   *conn;
	PGresult   *res;
	char	   *sysid;
	char	   *tli;
	char	   *anchor;			/* "cn_hi/cn_lo/redo_hi/redo_lo" */
	char		anchor_path[MAXPGPATH];
	char		autoconf_path[MAXPGPATH];
	char		signal_path[MAXPGPATH];
	FILE	   *fp;

	char	   *primary_conninfo;

	if (new_cluster.pgdata == NULL || new_cluster.pgdata[0] == '\0')
		pg_fatal("--wal-log-prepare-standby requires the new (skeleton) data directory (-D)");

	/*
	 * The primary is named by the standard primary_conninfo GUC in the skeleton's
	 * config -- exactly as for any streaming standby.  We read it rather than add
	 * a dedicated pg_upgrade flag (it would only duplicate the GUC).  Set it in
	 * the skeleton's postgresql.conf or postgresql.auto.conf before running this.
	 */
	primary_conninfo = read_primary_conninfo(new_cluster.pgdata);
	if (primary_conninfo == NULL)
		pg_fatal("no primary_conninfo found in \"%s\"\n"
				 "Set primary_conninfo (in postgresql.conf or postgresql.auto.conf) "
				 "to the live primary, as for any standby, then re-run --wal-log-prepare-standby.",
				 new_cluster.pgdata);

	prep_status("Preparing standby to stream the upgrade window from the primary");

	conn = PQconnectdb(primary_conninfo);
	if (conn == NULL || PQstatus(conn) != CONNECTION_OK)
		pg_fatal("could not connect to the primary (primary_conninfo): %s",
				 conn ? PQerrorMessage(conn) : "out of memory");

	/* System identifier of the live primary (must match for streaming). */
	res = executeQueryOrDie(conn, "SELECT system_identifier FROM pg_control_system()");
	sysid = pg_strdup(PQgetvalue(res, 0, 0));
	PQclear(res);

	/* Primary's current timeline. */
	res = executeQueryOrDie(conn, "SELECT timeline_id FROM pg_control_checkpoint()");
	tli = pg_strdup(PQgetvalue(res, 0, 0));
	PQclear(res);

	/* Window anchor CN (cn_lsn/redo) from the retained window on the primary. */
	res = executeQueryOrDie(conn, "SELECT pg_upgrade_window_anchor()");
	if (PQgetisnull(res, 0, 0))
		pg_fatal("the primary is not retaining a pg_upgrade window "
				 "(no anchor); is it a live --wal-log-upgrade primary that "
				 "has not yet run --wal-log-delete-old?");
	anchor = pg_strdup(PQgetvalue(res, 0, 0));
	PQclear(res);
	PQfinish(conn);

	/*
	 * Write the streaming anchor the backend consumes at first startup:
	 *   <sysid> <cn_hi>/<cn_lo> <redo_hi>/<redo_lo> <tli>
	 * pg_upgrade_window_anchor() already returns "cn_hi/cn_lo/redo_hi/redo_lo",
	 * so reshape it into two slash LSNs for readability/parse symmetry.
	 */
	{
		unsigned int a, b, c, d;

		if (sscanf(anchor, "%X/%X/%X/%X", &a, &b, &c, &d) != 4)
			pg_fatal("primary returned a malformed window anchor \"%s\"", anchor);

		snprintf(anchor_path, sizeof(anchor_path), "%s/%s",
				 new_cluster.pgdata, "pg_upgrade_stream.anchor");
		if ((fp = fopen(anchor_path, "w")) == NULL)
			pg_fatal("could not create streaming anchor \"%s\": %m", anchor_path);
		fprintf(fp, "%s %X/%X %X/%X %s\n", sysid, a, b, c, d, tli);
		fclose(fp);
	}

	/*
	 * Point the walreceiver at the retention slot so the window is guaranteed
	 * present.  primary_conninfo is already set by the operator (we read it
	 * above), so we only add primary_slot_name.
	 */
	snprintf(autoconf_path, sizeof(autoconf_path), "%s/postgresql.auto.conf",
			 new_cluster.pgdata);
	if ((fp = fopen(autoconf_path, "a")) == NULL)
		pg_fatal("could not append to \"%s\": %m", autoconf_path);
	fprintf(fp, "\n# added by pg_upgrade --wal-log-prepare-standby\n");
	fprintf(fp, "primary_slot_name = '%s'\n", UPGRADE_WINDOW_SLOT);
	fclose(fp);

	/* standby.signal so first startup enters standby (streaming) mode. */
	snprintf(signal_path, sizeof(signal_path), "%s/standby.signal",
			 new_cluster.pgdata);
	if ((fp = fopen(signal_path, "w")) == NULL)
		pg_fatal("could not create \"%s\": %m", signal_path);
	fclose(fp);

	check_ok();

	pg_log(PG_REPORT,
		   "\nStandby prepared.  Start it to stream the upgrade window from the primary:\n"
		   "  pg_ctl -D \"%s\" start\n"
		   "It streams + replays the window, then continues as a hot standby of the primary.",
		   new_cluster.pgdata);

	pg_free(primary_conninfo);
	pg_free(sysid);
	pg_free(tli);
	pg_free(anchor);
}

/*
 * Generate the revertable-upgrade lifecycle scripts, in the same style as
 * create_script_for_old_cluster_deletion(): self-contained shell scripts with
 * all paths baked in, so the operator runs a path-free script rather than
 * re-typing -b/-B/-d/-D.  Written at the end of a --wal-log-upgrade run.
 *
 *   pg_upgrade_commit.sh   -> pg_upgrade --wal-log-commit   (adopt new, stamp old)
 *   pg_upgrade_rollback.sh -> pg_upgrade --wal-log-rollback (discard new, keep old)
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
	fprintf(script, "%c%s%cpg_upgrade%c --wal-log-commit -b %c%s%c -B %c%s%c -d %c%s%c -D %c%s%c\n",
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
	fprintf(script, "%c%s%cpg_upgrade%c --wal-log-rollback -D %c%s%c\n",
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
		case REVERTABLE_OP_PREPARE_STANDBY:
			do_prepare_standby();
			break;
		case REVERTABLE_OP_NONE:
			break;				/* not reached */
	}
}
