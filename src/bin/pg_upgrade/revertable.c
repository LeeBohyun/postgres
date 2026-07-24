/*
 *	revertable.c
 *
 *	--wal-upgrade lifecycle subcommand (signal-handoff).
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
 *	Copyright (c) 2010-2026, PostgreSQL Global Development Group
 *	src/bin/pg_upgrade/revertable.c
 */

#include "postgres_fe.h"

#include <stdlib.h>				/* system() */

#include "pg_upgrade.h"

/*
 * --wal-upgrade-signal-handoff: connect to the LIVE old primary and write the
 * streaming-handoff trigger into its (old-format) WAL.  This does NOT push to
 * each standby directly -- it emits a WAL record, which propagates to streaming
 * standbys through the normal WAL/replication path.  On replaying it, a standby
 * shuts down cleanly, ready for the
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
	char		sigpath[MAXPGPATH];
	FILE	   *f;
	char		cmd[MAXPGPATH * 2];
	int			rc;

	if (old_cluster.pgdata == NULL || old_cluster.pgdata[0] == '\0')
		pg_fatal("--wal-upgrade-signal-handoff requires the old cluster data directory (-d)");
	if (old_cluster.bindir == NULL || old_cluster.bindir[0] == '\0')
		pg_fatal("--wal-upgrade-signal-handoff requires the old cluster bin directory (-b)\n"
				 "(needed to shut the old primary down at the handoff point)");

	/*
	 * Emit the handoff during the primary's own shutdown, not from a client
	 * session.  A client-issued handoff cannot be the guaranteed end of the
	 * old WAL stream: a backend past its commit point, or a fresh connection
	 * racing the terminate/shutdown, can flush a commit record after the
	 * marker, so a standby that stopped at the handoff would be missing an
	 * acknowledged commit.
	 *
	 * Instead drop a sentinel and let the server emit the record itself.  On
	 * a fast shutdown the postmaster drains every backend (flushing their
	 * commit WAL) before the checkpointer runs ShutdownXLOG(); ShutdownXLOG()
	 * then emits XLOG_UPGRADE_HANDOFF just before the shutdown checkpoint,
	 * while WAL senders are still streaming.  By construction all user WAL
	 * precedes the handoff, which precedes the shutdown checkpoint -- so the
	 * handoff is the exact, race-free end of the old stream that streaming
	 * standbys stop at. The sentinel records the target major version (this
	 * pg_upgrade binary's own major, the version the standby converges to).
	 */
	snprintf(sigpath, sizeof(sigpath), "%s/pg_upgrade_handoff.pending",
			 old_cluster.pgdata);

	prep_status("Arming pg_upgrade handoff on the old primary");
	f = fopen(sigpath, "w");
	if (f == NULL)
		pg_fatal("could not create handoff signal file \"%s\": %m", sigpath);
	fprintf(f, "%d\n", PG_MAJORVERSION_NUM);
	if (fclose(f) != 0)
		pg_fatal("could not write handoff signal file \"%s\": %m", sigpath);
	check_ok();

	/*
	 * Fast-stop the old primary; ShutdownXLOG() consumes the sentinel and
	 * emits the handoff.  system() rather than exec_prog(): the lifecycle
	 * subcommand runs before make_outputdirs() has set log_opts.logdir, so
	 * exec_prog() would fail on a "(null)/..." log path.
	 */
	prep_status("Shutting down the old primary at the handoff point");
	snprintf(cmd, sizeof(cmd), "\"%s/pg_ctl\" -w -D \"%s\" -m fast stop",
			 old_cluster.bindir, old_cluster.pgdata);
	rc = system(cmd);
	if (rc != 0)
	{
		unlink(sigpath);
		pg_fatal("could not shut down the old primary at the handoff point "
				 "(command \"%s\" returned %d)\n"
				 "Stop the old primary manually before running "
				 "\"pg_upgrade --wal-upgrade\".",
				 cmd, rc);
	}
	check_ok();

	pg_log(PG_REPORT,
		   "\nHandoff trigger written to the old primary's WAL as it shut down.\n"
		   "The trigger propagates to streaming standbys through the normal\n"
		   "WAL/replication path, which replay it and shut down.  Now run\n"
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
