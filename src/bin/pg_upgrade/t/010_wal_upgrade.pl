# Copyright (c) 2025-2026, PostgreSQL Global Development Group

# Tests for "pg_upgrade --wal-upgrade" on a single (primary-only) cluster.
#
# --wal-upgrade captures the whole upgrade as WAL so the change is
# revertable, then leaves the new cluster as a normal, fully-upgraded cluster
# that comes up read-write on its first start like any upgraded cluster.  This
# test drives the two primary-only paths and, at
# each step, asserts the cluster's control-file STATE transitions the way the
# state machine says it should and ends in the expected state:
#
#   happy path:  old (vN)  --pg_upgrade-->  new shut down cleanly, "in
#                production" state, control checkpoint past the upgrade window
#                --first start-->  auto-serves, not in recovery, data preserved,
#                target major version.
#
#   crash path:  old (vN)  --pg_upgrade with COMPLETE suppressed-->  a window
#                with START but no COMPLETE on disk  --first start-->  FATAL
#                (never serves a half-built catalog); the old cluster stays
#                intact and startable, and the dead-end new cluster is discarded
#                with rm -rf (there is no revert interface).
#
# Cross-version: when $ENV{oldinstall} is set the old cluster is built with an
# older major's binaries (checksums are pre-18 so pass -k there); otherwise the
# test runs same-version, exercising the state machine without a real gap.
#
# ---------------------------------------------------------------------------
# ASSUMPTIONS AND INVARIANTS --wal-upgrade relies on (why this works):
#
#  1. The window is self-contained and atomic.  pg_upgrade writes the whole
#     upgrade as a WAL "window": an end-of-upgrade checkpoint (CN), then
#     XLOG_UPGRADE_START, the system-relation/SLRU/rawfile/dirtree images, and
#     XLOG_UPGRADE_COMPLETE.  Recovery anchors at CN and either reaches COMPLETE
#     (fully upgraded) or it did not -- there is no half-upgraded steady state.
#
#  2. COMPLETE is the atomicity boundary.  A window with START but no COMPLETE
#     (crash mid-upgrade) must NEVER be served: the catalog is half-built and
#     the cluster auto-serves read-write at end of recovery, so serving it would
#     expose a corrupt catalog.  First startup FATALs in that case.  The durable
#     pg_control upgrade_finalized flag (paired with a control checkpoint past CN)
#     distinguishes a finished upgrade from a crashed partial one even if a torn
#     final WAL page hides the COMPLETE record.
#
#  3. The old cluster stays intact on the copy-family transfer modes.  With
#     copy/clone/copy_file_range the old datadir's files are only read, so a
#     failed or crashed upgrade can always fall back to starting the old cluster
#     (the crash path below asserts this).  (link/swap deliberately give that up,
#     as in stock pg_upgrade.)
#
#  4. Counters ride in the CN checkpoint.  The XID/OID/multixact counters are
#     transplanted into pg_control before CN rather than WAL-logged separately,
#     so the CN checkpoint record reproduces them on replay.

use strict;
use warnings FATAL => 'all';

use File::Path qw(rmtree);
use File::Copy qw(copy);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# The upgrade output files (delete_old_cluster.sh etc.) are written relative to
# the current directory, so run pg_upgrade from a writable scratch dir.
chdir ${PostgreSQL::Test::Utils::tmp_check};

# Read the "Database cluster state" line out of pg_controldata for $datadir,
# using $node's bindir so we read it with the matching-version tool.
sub cluster_state
{
	my ($node, $datadir) = @_;
	my $bindir = $node->config_data('--bindir');
	my ($stdout, $stderr) =
	  run_command([ "$bindir/pg_controldata", '-D', $datadir ]);
	return $1 if $stdout =~ /Database cluster state:\s+(.*)/;
	return '';
}

# True if pg_control records the --wal-upgrade window as finalized (the durable
# flag that replaced the old pg_upgrade_complete.done marker file).
sub upgrade_finalized
{
	my ($node, $datadir) = @_;
	my $bindir = $node->config_data('--bindir');
	my ($stdout, $stderr) =
	  run_command([ "$bindir/pg_controldata", '-D', $datadir ]);
	return ($stdout =~ /wal-upgrade window finalized:\s+yes/) ? 1 : 0;
}

# Build an old cluster and populate it with a little data whose survival we can
# check after the upgrade.  Returns the $old node (stopped).
sub setup_old
{
	my ($name) = @_;
	my $old =
	  PostgreSQL::Test::Cluster->new($name, install_path => $ENV{oldinstall});

	# Checksums are enabled by default from v18 on, but not before, so pass
	# '-k' on older installs so a checksum-on new cluster can be upgraded.
	if (defined($ENV{oldinstall}))
	{
		$old->init(extra => ['-k']);
	}
	else
	{
		$old->init;
	}

	$old->start;
	$old->safe_psql(
		'postgres', qq{
		CREATE TABLE t (id int primary key, note text);
		INSERT INTO t SELECT g, 'row ' || g FROM generate_series(1, 500) g;
		CREATE TABLE toasted (id int, big text);
		INSERT INTO toasted
		  SELECT g, repeat('abcdef0123456789', 2000) FROM generate_series(1, 200) g;
		CREATE DATABASE extra_db;
	});
	$old->stop;
	return $old;
}

# The arguments common to every pg_upgrade --wal-upgrade invocation here.
sub upgrade_cmd
{
	my ($old, $new, @extra) = @_;
	return [
		'pg_upgrade', '--no-sync',
		'--old-datadir' => $old->data_dir,
		'--new-datadir' => $new->data_dir,
		'--old-bindir' => $old->config_data('--bindir'),
		'--new-bindir' => $new->config_data('--bindir'),
		'--socketdir' => $new->host,
		'--old-port' => $old->port,
		'--new-port' => $new->port,
		'--initdb',
		'--wal-upgrade',
		@extra,
	];
}

# The framework's init() writes port/socket settings into postgresql.conf.  A
# --initdb-created new cluster skipped init(), so append them before starting.
sub add_conn_conf
{
	my ($new) = @_;
	my $conf = $new->data_dir . '/postgresql.conf';
	open(my $fh, '>>', $conf) or die "could not open $conf: $!";
	print $fh "\n# added by test to start the --initdb-created cluster\n";
	print $fh "port = " . $new->port . "\n";
	print $fh "listen_addresses = ''\n";
	print $fh "unix_socket_directories = '" . $new->host . "'\n";
	close($fh);
}

#
# 1. HAPPY PATH: upgrade auto-serves and preserves data.
#
{
	my $old = setup_old('old_happy');
	# Do NOT init() the new node: --wal-upgrade --initdb creates it.
	my $new = PostgreSQL::Test::Cluster->new('new_happy');

	ok(!-d $new->data_dir,
		'happy: new data directory does not exist before --initdb');

	command_ok(upgrade_cmd($old, $new),
		'happy: pg_upgrade --wal-upgrade --initdb succeeds');

	# pg_upgrade shut the new cluster down cleanly, so its control file records
	# a shut-down / in-production state, not a recovery/upgrade state.
	my $state = cluster_state($new, $new->data_dir);
	like($state, qr/^(shut down|in production)$/,
		"happy: new cluster control state is shut-down/in-production ($state)");

	# pg_control records the window as finalized once it reached COMPLETE.
	ok(upgrade_finalized($new, $new->data_dir),
		'happy: pg_control upgrade_finalized set after a full upgrade');

	# The new cluster comes up read-write on its first start, without re-replaying
	# the window.
	add_conn_conf($new);
	$new->start;

	is($new->safe_psql('postgres', 'SELECT pg_is_in_recovery()'),
		'f', 'happy: upgraded cluster is not in recovery');

	# Data survived and the extra database carried over.
	is($new->safe_psql('postgres', 'SELECT count(*) FROM t'),
		'500', 'happy: user table data preserved');
	is($new->safe_psql('postgres', 'SELECT count(*) FROM toasted'),
		'200', 'happy: toasted table data preserved');
	is( $new->safe_psql(
			'postgres',
			"SELECT count(*) FROM pg_database WHERE datname = 'extra_db'"),
		'1',
		'happy: user database carried over');

	# It reports the new major version.
	my $newver = $new->safe_psql('postgres',
		"SELECT current_setting('server_version_num')::int / 10000");
	ok($newver >= 18, "happy: new cluster reports target major ($newver)");

	# A restart comes up the same way (starting an upgraded cluster is idempotent).
	$new->restart;
	is($new->safe_psql('postgres', 'SELECT pg_is_in_recovery()'),
		'f', 'happy: still serving after restart');
	is($new->safe_psql('postgres', 'SELECT count(*) FROM t'),
		'500', 'happy: data stable across restart');

	# The durable pg_control upgrade_finalized flag persists across the restart.
	# It lets a finalized cluster whose window tail is later torn or trimmed still
	# be recognized as finalized rather than as a crashed mid-window upgrade.
	ok(upgrade_finalized($new, $new->data_dir),
		'happy: durable upgrade_finalized flag set after restart');

	$new->stop;
	$old->clean_node;
	$new->clean_node;
}

#
# 1b. LINK MODE: window is still schema-only, and rollback is impossible.
#
# --link hardlinks user relations into the new cluster instead of copying them,
# and (as in stock pg_upgrade) disables the old cluster: global/pg_control is
# renamed to pg_control.old before the transfer, and once the new cluster starts
# the shared inodes make the old cluster unsafe to run.  Two things must hold:
#
#   (a) No user relation PAGES are WAL-logged.  Regardless of transfer mode the
#       window carries only pg_upgrade-rewritten SYSTEM files (as ordinary FPI
#       full-page images) plus the RELINK manifest for user relations (identities
#       only, no data); user data never rides the WAL.  We scan the whole window
#       with pg_waldump and assert the RELINK manifest is present and that no
#       page image names a user relfilenumber.
#
#   (b) Rollback to the old cluster is impossible: pg_control is gone (renamed to
#       .old), so the old datadir will not start.
#
{
	my $old = setup_old('old_link');
	my $new = PostgreSQL::Test::Cluster->new('new_link');

	# This subtest inspects the WINDOW's contents in the new cluster's pg_wal,
	# so the window must be retained there.  pg_upgrade retains it only when a
	# physical slot is migrated (a standby is expected); with neither a slot nor
	# archiving the window is unused and recycled.  So give the old cluster a
	# physical slot, which pg_upgrade migrates and uses to pin the window.
	# (setup_old inits with wal_level=minimal; a slot needs >= replica.)
	$old->append_conf('postgresql.conf', "wal_level = replica\n");
	$old->start;
	$old->safe_psql('postgres',
		"SELECT pg_create_physical_replication_slot('link_stby_slot', true)");
	$old->stop;

	# Fingerprint + control state of the intact old cluster, for contrast.
	ok(-f $old->data_dir . '/global/pg_control',
		'link: old cluster has pg_control before the upgrade');

	command_ok(upgrade_cmd($old, $new, '--link'),
		'link: pg_upgrade --wal-upgrade --initdb --link succeeds');

	# (a) Scan the whole window in the new cluster's pg_wal.  User relations must
	# be represented ONLY by the RELINK manifest (identities, no data); their
	# pages must never be WAL-logged.  System relations DO travel, as ordinary
	# FPI records whose blkrefs name only system relfilenumbers.
	#
	# The page images are large (the whole system catalog) and can span a
	# segment boundary, so we must dump the WHOLE pg_wal directory in one
	# invocation (pg_waldump -p follows records across segments); a per-segment
	# scan would miss a record that starts in one segment and ends in the next.
	# pg_waldump errors at the end of available WAL -- that is expected, so read
	# both stdout and stderr and parse whatever records it emitted.
	my $bindir = $new->config_data('--bindir');
	my $waldir = $new->data_dir . '/pg_wal';
	# pg_waldump -p needs an explicit start LSN.  Use the start of the lowest
	# segment present so it dumps everything (following records across segment
	# boundaries) to the end of available WAL.
	opendir(my $wd, $waldir) or die "opendir $waldir: $!";
	my @segs = sort grep { /^[0-9A-F]{24}$/ } readdir $wd;
	closedir $wd;
	# Segment file is TTTTTTTT LLLLLLLL SSSSSSSS (timeline, log, seg); the byte
	# offset of its first record is (log, seg*WalSegSz) i.e. LSN "log/seg000000".
	my ($loghex, $seghex) = $segs[0] =~ /^[0-9A-F]{8}([0-9A-F]{8})([0-9A-F]{8})$/;
	my $start = sprintf('%X/%s000028', hex($loghex), substr($seghex, 6, 2));
	my ($stdout, $stderr);
	IPC::Run::run [ "$bindir/pg_waldump", '-p', $waldir, '-s', $start ], '>',
	  \$stdout, '2>', \$stderr;
	my $dump = $stdout . $stderr;
	my $saw_relink = ($dump =~ /UPGRADE_RELINK/);

	# System relation pages travel as ordinary FPI records (log_newpages), so
	# look for those rather than an upgrade-specific record type.  Match "FPI"
	# but not "FPI_FOR_HINT", which is unrelated hint-bit logging.
	my $saw_fpi = ($dump =~ /desc: FPI(?!_FOR_HINT)/);

	# The window must stay schema-only: no page image may name a user
	# relfilenumber (>= FirstNormalObjectId).  blkrefs read "rel TS/DB/RELNUM".
	my $saw_user_page = 0;
	while ($dump =~ m{rel \d+/\d+/(\d+)}g)
	{
		$saw_user_page = 1 if $1 >= 16384;
	}
	ok($saw_relink, 'link: window carries the RELINK manifest for user relations');
	ok($saw_fpi,
		'link: window carries FPI records (system relation full-page images)');
	is($saw_user_page, 0,
		'link: no user relation pages are WAL-logged (window is schema-only)');

	# (b) The old cluster is disabled: pg_control renamed to .old.
	ok(!-f $old->data_dir . '/global/pg_control',
		'link: old cluster pg_control removed (renamed to .old)');
	ok(-f $old->data_dir . '/global/pg_control.old',
		'link: old cluster pg_control saved as .old');

	# Bring the new cluster up (this is what makes the old cluster unsafe); the
	# old datadir then can no longer be started.
	add_conn_conf($new);
	$new->start;
	is($new->safe_psql('postgres', 'SELECT count(*) FROM t'),
		'500', 'link: user data present on the new cluster (hardlinked in)');

	my $oldbin = $old->config_data('--bindir');
	my $ret = run_log(
		[ "$oldbin/pg_ctl", '-D', $old->data_dir, '-w', '-t', '10', 'start' ]);
	ok(!$ret, 'link: old cluster refuses to start (rollback impossible)');
	# If it somehow started, stop it so it does not leak into later tests.
	run_log([ "$oldbin/pg_ctl", '-D', $old->data_dir, 'stop' ]) if $ret;

	$new->stop;
	$old->clean_node;
	$new->clean_node;
}

#
# 2. CRASH PATH: a window with START but no COMPLETE must be refused.
#
# PG_UPGRADE_TEST_SKIP_COMPLETE (a test-only fault-injection hook) makes
# pg_upgrade emit the whole upgrade window but omit the COMPLETE marker,
# simulating a crash after START.  The new cluster must FATAL on first start
# (never serve a half-built catalog) and the old cluster must stay intact.  The
# dead-end new cluster carries no state worth keeping (there is no revert
# interface), so it is discarded with rm -rf.
#
# The hook is gated on USE_ASSERT_CHECKING (a production build ignores the env
# var and can never emit an incomplete window), so this section only runs on a
# cassert build.
#
SKIP:
{
	skip 'crash-path fault injection requires a cassert build', 6
	  unless check_pg_config(qr/^#define USE_ASSERT_CHECKING 1/);

	my $old = setup_old('old_crash');

	# Fingerprint the old cluster so the post-crash check can confirm it is
	# undamaged.
	$old->start;
	my $old_fp =
	  $old->safe_psql('postgres', 'SELECT count(*), sum(id) FROM t');
	$old->stop;

	my $new = PostgreSQL::Test::Cluster->new('new_crash');

	# Upgrade with COMPLETE suppressed.  pg_upgrade itself still succeeds; the
	# missing COMPLETE only bites at the new cluster's first start.
	{
		local $ENV{PG_UPGRADE_TEST_SKIP_COMPLETE} = '1';
		command_ok(upgrade_cmd($old, $new),
			'crash: pg_upgrade succeeds even with COMPLETE suppressed');
	}

	# A partial window never sets the finalized flag; that absence is what marks
	# the cluster as not finalized.
	ok(!upgrade_finalized($new, $new->data_dir),
		'crash: upgrade_finalized flag unset on a partial window');

	# First start must FAIL (fail_ok so we can assert on it instead of bailing).
	add_conn_conf($new);
	my $started = $new->start(fail_ok => 1);
	is($started, 0, 'crash: new cluster refuses to start on a partial window');

	# And it must FATAL specifically because the window is incomplete -- not
	# for some unrelated reason.
	my $log = slurp_file($new->logfile);
	like(
		$log,
		qr/pg_upgrade WAL is incomplete|found START without COMPLETE/,
		'crash: startup FATALed with the "incomplete window" message');

	# The old cluster is untouched: it starts and its data is identical.
	$old->start;
	is($old->safe_psql('postgres', 'SELECT count(*), sum(id) FROM t'),
		$old_fp, 'crash: old cluster intact and startable after the failed upgrade');
	$old->stop;

	# The dead-end new cluster is discarded (rm -rf); there is no revert
	# interface, and the old cluster is the source of truth.
	rmtree($new->data_dir);
	ok(!-d $new->data_dir,
		'crash: half-upgraded new data directory discarded');

	$old->clean_node;
	$new->clean_node;
}

#
# 3. BACKUP-GAP / PITR PATH: recover the transactions executed after the
#    upgrade went live from a pre-upgrade base backup + archived WAL, ACROSS
#    the upgrade boundary, WITHOUT a new base backup and WITHOUT a standby.
#
# The operator runs a single primary with a periodic base backup and continuous
# WAL archiving.  The upgrade procedure is:
#
#   1. stop the old server
#   2. pg_upgrade --wal-upgrade
#   3. start the new server
#   4. the application connects and executes transactions   <-- must survive
#   5. take a new base backup
#
# If the storage is lost after step 4 but before step 5, upstream pg_upgrade
# loses the step-4 transactions: the pre-upgrade base backup cannot be rolled
# forward across the version boundary, and no post-upgrade base backup exists
# yet.  On a large database step 5 can take a long time, so that gap is wide.
#
# With --wal-upgrade the upgrade itself is WAL, so the claim is: restore the
# last pre-upgrade base backup, replay archived WAL up to the upgrade, replay
# the upgrade window, then keep replaying the WAL generated on the new version
# -- recovering the step-4 transactions with no new base backup.  This test
# encodes that workflow end to end.
{
	# A single archive both the old and the new cluster feed, and that the
	# PITR restore reads back -- one continuous WAL history on disk.
	my $archive = "${PostgreSQL::Test::Utils::tmp_check}/pitr_archive";
	rmtree($archive);
	mkdir($archive) or die "could not create $archive: $!";
	# Archive atomically (copy to a temp name, then rename) so an immediate
	# stop that kills the archiver mid-copy can never leave a truncated segment
	# in the archive for the PITR restore to trip over.
	my $arch_cmd =
	  "cp \"%p\" \"$archive/%f.tmp\" && mv \"$archive/%f.tmp\" \"$archive/%f\"";
	my $restore_cmd = "cp \"$archive/%f\" \"%p\"";

	# Old cluster with WAL archiving on.
	my $old =
	  PostgreSQL::Test::Cluster->new('old_pitr', install_path => $ENV{oldinstall});
	if (defined($ENV{oldinstall}))
	{
		$old->init(extra => ['-k'], allows_streaming => 1);
	}
	else
	{
		$old->init(allows_streaming => 1);
	}
	$old->append_conf('postgresql.conf',
		"archive_mode = on\narchive_command = '$arch_cmd'\n");
	$old->start;

	# Pre-upgrade data.
	$old->safe_psql(
		'postgres', qq{
		CREATE TABLE t (id int primary key, note text);
		INSERT INTO t SELECT g, 'pre ' || g FROM generate_series(1, 500) g;
	});

	# Step 0: the periodic pre-upgrade base backup the operator already has.
	$old->backup('pitr_base');

	# Capture the old cluster's final WAL position.  Phase 1 of recovery stops
	# here: at the boundary, past all pre-upgrade transactions, before the
	# upgrade window (which lives at a later, non-contiguous segment).
	my $old_end_lsn =
	  $old->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

	$old->stop;    # step 1

	# Step 2: pg_upgrade --wal-upgrade --initdb creates the new cluster.
	my $new = PostgreSQL::Test::Cluster->new('new_pitr');
	command_ok(upgrade_cmd($old, $new),
		'pitr: pg_upgrade --wal-upgrade --initdb succeeds');

	# The upgraded cluster inherits the old cluster's archive_command
	# automatically (pg_upgrade --wal-upgrade carries it forward), so the upgrade
	# window and the post-upgrade WAL flow to the SAME archive with no extra
	# option.  Verify that landed in the new cluster's config.
	add_conn_conf($new);
	my $newconf = slurp_file($new->data_dir . '/postgresql.conf');
	like($newconf, qr/archive_mode\s*=\s*on/,
		'pitr: upgraded cluster inherited archive_mode');

	# The upgrade window (CN..COMPLETE) must have reached the archive; the burst
	# server's wait-for-archive barrier guarantees this.  Confirm the window
	# segments are present by scanning the archive for the START/COMPLETE records.
	my $bindir = $new->config_data('--bindir');
	my ($start_seg, $complete_seg) = ('', '');
	opendir(my $ad, $archive) or die "opendir $archive: $!";
	for my $f (sort grep { /^[0-9A-F]{24}$/ } readdir $ad)
	{
		my ($out) = run_command([ "$bindir/pg_waldump", "$archive/$f" ]);
		$start_seg = $f if $out =~ /PG_UPGRADE_START/;
		$complete_seg = $f if $out =~ /PG_UPGRADE_COMPLETE/;
	}
	closedir $ad;
	ok($start_seg ne '' && $complete_seg ne '',
		"pitr: upgrade window archived (START in $start_seg, COMPLETE in $complete_seg)");

	# Step 3: start the upgraded cluster (auto-serves), archiving its tail.
	$new->start;

	# On the backup/PITR path the window's durable home is the archive, so no
	# retention slot is created at all (the wait-for-archive barrier guarantees
	# the window is archived before shutdown; a slot would only pin pg_wal/
	# forever with no standby to consume it).  Confirm no physical slot exists.
	is($new->safe_psql('postgres',
			"SELECT count(*) FROM pg_replication_slots WHERE slot_type = 'physical'"),
		'0', 'pitr: no retention slot on the archive path');

	# Step 4: the application executes transactions on the new version.  These
	# are the ones the backup gap loses upstream.
	$new->safe_psql('postgres',
		"INSERT INTO t SELECT g, 'post ' || g FROM generate_series(501, 800) g;"
	);
	$new->safe_psql('postgres',
		'CREATE TABLE only_on_new (x int); INSERT INTO only_on_new VALUES (42);');

	# Push the step-4 WAL out to the archive (switch + checkpoint + switch), the
	# way continuous archiving would before the operator got to step 5.
	$new->safe_psql('postgres',
		'SELECT pg_switch_wal(); CHECKPOINT; SELECT pg_switch_wal();');
	# Give the archiver a moment to drain the tail.
	$new->poll_query_until('postgres',
		"SELECT last_archived_wal IS NOT NULL FROM pg_stat_archiver");

	# DISASTER: storage lost after step 4, before the step-5 base backup.  The
	# new cluster's data dir (and its local pg_wal upgrade window) are gone; only
	# the pre-upgrade base backup and the archive survive off host.
	$new->stop('immediate');
	my $new_datadir = $new->data_dir;
	rmtree($new_datadir);
	ok(!-d $new_datadir, 'pitr: upgraded cluster storage lost before new backup');

	# ---- RECOVERY: two-phase PITR across the upgrade boundary ----
	#
	# Phase 1 (OLD binary): restore the pre-upgrade base backup and replay
	# archived OLD WAL up to the upgrade boundary, stopping there WITHOUT
	# promoting (a promotion would fork a new timeline and clobber the state
	# Phase 2 needs).  This recovers every pre-upgrade transaction, including
	# those written after the base backup.
	# Same-version test: old and new binaries are identical, so one node drives
	# both phases.  For a real cross-version run ($ENV{oldinstall} set), Phase 1
	# must use the OLD binary (to read the old-version restored backup) and Phase
	# 2 the NEW binary (to replay the window).  The restore node is a NEW-version
	# node so its start()/psql use the new binaries; Phase 1 is driven with the
	# OLD bindir explicitly via raw pg_ctl.
	my $old_bindir = $old->config_data('--bindir');
	my $restore = PostgreSQL::Test::Cluster->new('restore_pitr');
	$restore->init_from_backup($old, 'pitr_base');

	# The boundary is the old cluster's final WAL position, captured above.
	# Stopping there keeps Phase 1 on timeline 1, past every pre-upgrade
	# transaction, without entering the (non-contiguous) upgrade window.
	$restore->append_conf('postgresql.conf',
		"restore_command = '$restore_cmd'\n"
		  . "recovery_target_lsn = '$old_end_lsn'\n"
		  . "recovery_target_inclusive = on\n"
		  . "recovery_target_action = 'shutdown'\n");
	open(my $s1, '>', $restore->data_dir . '/recovery.signal') or die $!;
	close($s1);

	# Phase 1 runs the OLD binary and stops itself at the recovery target
	# (recovery_target_action=shutdown).  Drive it with a raw pg_ctl rather than
	# $node->start, because the node framework expects start() to leave a running
	# server with a live postmaster.pid; a self-terminating recovery would leave
	# its pid bookkeeping inconsistent and bail the whole test.
	# pg_ctl --wait returns non-zero when the server exits instead of staying up
	# (which is exactly what recovery_target_action=shutdown does), so ignore the
	# exit code and assert on the log instead.
	my $p1log = "${PostgreSQL::Test::Utils::tmp_check}/pitr_phase1.log";
	PostgreSQL::Test::Utils::system_log(
		"$old_bindir/pg_ctl", '--wait',
		'--pgdata' => $restore->data_dir,
		'--log' => $p1log,
		'--options' => "--cluster-name=restore_pitr_p1",
		'start');
	like(slurp_file($p1log), qr/shutdown at recovery target/,
		'pitr phase 1: old binary stopped at the upgrade boundary (no promote)');

	# Phase 2 (NEW binary): stage the window segments (CN..COMPLETE) into pg_wal/
	# so the new binary's local scan re-anchors recovery at CN, then replay the
	# window + the archived post-upgrade tail.  No recovery_target this time.
	#
	# This copy mirrors a real operator's recovery step, not a test shortcut: on
	# the archive-PITR path the window-detection scan (PerformWalUpgradeIfNeeded)
	# reads the local pg_wal/ before archive recovery is wired up, so the window
	# segments must physically be present there when the new binary starts -- they
	# cannot yet be pulled from the archive via restore_command at that point (the
	# post-upgrade tail, which IS fetched by restore_command, still is).  Fetching
	# the window from the archive too is future work (move upgrade detection after
	# archive-recovery init).  The streaming-standby path (011) needs no such
	# staging: it receives the window over replication.
	my $rwal = $restore->data_dir . '/pg_wal';
	opendir(my $ad2, $archive) or die $!;
	for my $f (sort grep { /^[0-9A-F]{24}$/ } readdir $ad2)
	{
		next if $f lt $start_seg or $f gt $complete_seg;
		copy("$archive/$f", "$rwal/$f") or die "stage $f: $!";
	}
	closedir $ad2;

	# Rewrite recovery config: new binary, restore_command for the tail, follow
	# to the latest timeline, and crucially NO recovery_target (a stale target
	# before CN would abort as "stop point before consistent recovery point").
	my $conf = $restore->data_dir . '/postgresql.conf';
	# Strip the phase-1 recovery_target_* lines, keep base settings.
	my $txt = slurp_file($conf);
	$txt =~ s/^recovery_target.*\n//mg;
	open(my $cw, '>', $conf) or die $!;
	print $cw $txt;
	print $cw "recovery_target_timeline = 'latest'\n";
	# A PITR/standby replay requires the recovery node's resource GUCs to be at
	# least the primary's (a PARAMETER_CHANGE record in the window carries the
	# primary's values, e.g. max_connections=100).  The no-initdb synthesized
	# pg_control comes up with this binary's defaults (max_connections=10), which
	# are lower, so recovery would abort ("insufficient parameter settings").
	# Match the primary's values, exactly as an operator provisioning a restore
	# node must.
	print $cw "max_connections = 100\n";
	print $cw "max_worker_processes = 8\n";
	print $cw "max_wal_senders = 10\n";
	print $cw "max_prepared_transactions = 0\n";
	print $cw "max_locks_per_transaction = 128\n";
	close($cw);
	open(my $s2, '>', $restore->data_dir . '/recovery.signal') or die $!;
	close($s2);

	# Cross-version: Phase 1 left an OLD-version pg_control that this NEW binary
	# would reject at the version gate.  Drop the pg_upgrade.signal sentinel next
	# to the staged window so the new binary, seeing recovery.signal + this
	# sentinel in checkDataDir(), synthesizes a new-version pg_control before the
	# gate.  (recovery.signal, not primary_conninfo, so this is the archive path.)
	# Same-version needs no synthesis and is unaffected (control file already
	# matches), but the sentinel is harmless there.
	open(my $u2, '>', $restore->data_dir . '/pg_upgrade.signal') or die $!;
	close($u2);

	my $p2 = $restore->start(fail_ok => 1);
	is($p2, 1, 'pitr phase 2: new binary replays the window + tail and comes up');

  SKIP:
	{
		skip 'phase 2 did not start', 3 if $p2 != 1;

		# The server accepts read-only connections at consistency, but recovery
		# is still replaying the window + tail.  Wait for recovery to finish
		# (promotion) before checking data, else we observe a mid-replay state.
		$restore->poll_query_until('postgres', 'SELECT NOT pg_is_in_recovery()')
		  or die 'phase 2 recovery did not finish';

		# The step-4 transactions survived without a new base backup.
		is($restore->safe_psql('postgres', 'SELECT count(*) FROM t'),
			'800', 'pitr: pre- and post-upgrade rows both recovered');
		is( $restore->safe_psql(
				'postgres', "SELECT to_regclass('only_on_new') IS NOT NULL"),
			't', 'pitr: post-upgrade DDL recovered');
		is($restore->safe_psql('postgres', 'SELECT x FROM only_on_new'),
			'42', 'pitr: post-upgrade row recovered');

		$restore->stop;
	}

	$old->clean_node;
	$restore->clean_node;
}

done_testing();
