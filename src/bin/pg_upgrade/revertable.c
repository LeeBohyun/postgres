/*
 *	revertable.c
 *
 *	LEE: --wal-upgrade lifecycle subcommand (signal-handoff).
 *
 *	AUTO-SERVE model: a --wal-upgrade new cluster comes up read-write on its
 *	first start, like upstream pg_upgrade -- there is NO quarantine hold and NO
 *	commit step.  This file implements the one remaining lifecycle subcommand:
 *
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

#include <stdlib.h>				/* system() */

#include "pg_upgrade.h"

/*
 * --wal-upgrade-signal-handoff: connect to the LIVE old primary and write the
 * streaming-handoff trigger into its (old-format) WAL.  This does NOT push to
 * each standby directly -- it emits a WAL record, which propagates to streaming
 * standbys through the normal WAL path (in Neon, primary -> safekeepers ->
 * standby).  On replaying it, a standby shuts down cleanly, ready for the
 * new-version binary swap / re-provision.  Run this BEFORE stopping the old
 * primary and running pg_upgrade.
 *
 * Unlike --wal-upgrade itself (which acts on stopped clusters), this one
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
	if (old_cluster.bindir == NULL || old_cluster.bindir[0] == '\0')
		pg_fatal("--wal-upgrade-signal-handoff requires the old cluster bin directory (-b)\n"
				 "(needed to shut the old primary down at the handoff point)");

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

	/*
	 * Gate writes BEFORE emitting the handoff so no user WAL follows it.
	 *
	 * 1. Terminate every existing CLIENT backend except our own session.  This
	 *    stops any in-flight write transaction from committing after the handoff
	 *    record.  We deliberately filter on backend_type = 'client backend' so we
	 *    do NOT terminate the walsender(s) feeding streaming standbys -- those
	 *    must stay up to carry the handoff record to the standbys.  (Autovacuum
	 *    and other background workers are left alone; they are torn down by the
	 *    fast shutdown a moment later.)
	 * 2. Immediately after, emit the handoff and fast-stop the primary.  A fast
	 *    shutdown refuses new connections from the instant it begins, so together
	 *    these close the window in which a user transaction could write WAL past
	 *    the handoff.
	 */
	res = executeQueryOrDie(conn,
							"SELECT pg_terminate_backend(pid) "
							"FROM pg_stat_activity "
							"WHERE pid <> pg_backend_pid() "
							"AND backend_type = 'client backend'");
	PQclear(res);

	snprintf(query, sizeof(query),
			 "SELECT pg_upgrade_wal_handoff(%d)", PG_MAJORVERSION_NUM);
	res = executeQueryOrDie(conn, "%s", query);
	PQclear(res);
	PQfinish(conn);
	check_ok();

	/*
	 * Shut the old primary down IMMEDIATELY, at the handoff point.  This is the
	 * whole reason signal-handoff drives the shutdown itself rather than leaving
	 * it to the operator: any transaction that committed after the handoff record
	 * would append old-format WAL *after* the handoff marker, so the handoff would
	 * no longer be the clean end of the old stream that streaming standbys stop
	 * at.  Client backends were already terminated above (and the fast shutdown
	 * refuses new connections), so nothing writes past the handoff; the shutdown
	 * checkpoint lands right after it, making the handoff the last meaningful
	 * record.  The remaining shutdown checkpoint carries no user data.
	 *
	 * Use system() rather than exec_prog() here: the lifecycle subcommand runs
	 * before make_outputdirs() has set log_opts.logdir, so exec_prog() (which
	 * writes to that directory) would fail with a "(null)/..." log path.  A direct
	 * pg_ctl invocation needs no output directory.
	 */
	{
		char		cmd[MAXPGPATH * 2];
		int			rc;

		prep_status("Shutting down the old primary at the handoff point");
		snprintf(cmd, sizeof(cmd), "\"%s/pg_ctl\" -w -D \"%s\" -m fast stop",
				 old_cluster.bindir, old_cluster.pgdata);
		rc = system(cmd);
		if (rc != 0)
			pg_fatal("could not shut down the old primary at the handoff point "
					 "(command \"%s\" returned %d)\n"
					 "The handoff record was written; stop the old primary "
					 "manually before running \"pg_upgrade --wal-upgrade\".",
					 cmd, rc);
		check_ok();
	}

	pg_log(PG_REPORT,
		   "\nHandoff trigger written to the old primary's WAL and the primary was\n"
		   "shut down at that point.  The trigger propagates to streaming standbys\n"
		   "(via the safekeepers in Neon), which replay it and shut down.  Now run\n"
		   "\"pg_upgrade --wal-upgrade ...\"; then re-provision each standby\n"
		   "from the delivered upgrade window.");
}

/*
 * Dispatch a --wal-upgrade lifecycle subcommand and exit.  Called from main()
 * before the normal upgrade flow when user_opts.revertable_op is set.
 */
void
perform_revertable_op(void)
{
	switch (user_opts.revertable_op)
	{
		case REVERTABLE_OP_SIGNAL_HANDOFF:
			do_signal_handoff();
			break;
		case REVERTABLE_OP_NONE:
			break;				/* not reached */
	}
}
