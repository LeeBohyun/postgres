# Copyright (c) 2025-2026, PostgreSQL Global Development Group

# Tests for "pg_upgrade --wal-upgrade" on a single (primary-only) cluster.
#
# --wal-upgrade captures the whole upgrade as WAL so the change is
# revertable, then leaves the new cluster as a normal, fully-upgraded cluster
# that AUTO-SERVES read-write on its first start (there is no "commit" step and
# no not-serving hold).  This test drives the two primary-only paths and, at
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
#                intact and startable, and --wal-upgrade-rollback discards new.
#
# Cross-version: when $ENV{oldinstall} is set the old cluster is built with an
# older major's binaries (checksums are pre-18 so pass -k there); otherwise the
# test runs same-version, exercising the state machine without a real gap.

use strict;
use warnings FATAL => 'all';

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
		  SELECT g, repeat(md5(g::text), 200) FROM generate_series(1, 200) g;
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

	# State oracle #1: pg_upgrade did a clean shutdown of the new cluster, so
	# its control file must record a shut-down "in production" state -- NOT a
	# left-over recovery/upgrade state.  This is the terminal state before the
	# very first start.
	my $state = cluster_state($new, $new->data_dir);
	like($state, qr/^(shut down|in production)$/,
		"happy: new cluster control state is shut-down/in-production ($state)");

	# The COMPLETE marker proves the whole upgrade window reached COMPLETE.
	ok(-f $new->data_dir . '/pg_upgrade_complete.done',
		'happy: COMPLETE marker present after a full upgrade');

	# First start: the new cluster must AUTO-SERVE (come up read-write) with no
	# commit step and without re-replaying the window.
	add_conn_conf($new);
	$new->start;

	# State oracle #2: it is a live primary, not stuck in recovery.
	is($new->safe_psql('postgres', 'SELECT pg_is_in_recovery()'),
		'f', 'happy: auto-served cluster is not in recovery');

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

	# State oracle #3: it really is the new major version.
	my $newver = $new->safe_psql('postgres',
		"SELECT current_setting('server_version_num')::int / 10000");
	ok($newver >= 18, "happy: new cluster reports target major ($newver)");

	# A restart stays live and stable (auto-serve is idempotent, never re-holds).
	$new->restart;
	is($new->safe_psql('postgres', 'SELECT pg_is_in_recovery()'),
		'f', 'happy: still serving after restart');
	is($new->safe_psql('postgres', 'SELECT count(*) FROM t'),
		'500', 'happy: data stable across restart');

	$new->stop;
	$old->clean_node;
	$new->clean_node;
}

#
# 2. CRASH PATH: a window with START but no COMPLETE must be refused.
#
# PG_UPGRADE_TEST_SKIP_COMPLETE (a test-only backend/frontend hook) makes
# pg_upgrade emit the whole upgrade window but omit the COMPLETE marker,
# simulating a crash after START.  The new cluster must FATAL on first start
# (never serve a half-built catalog), the old cluster must stay intact, and
# --wal-upgrade-rollback must discard the dead-end new cluster.
#
{
	my $old = setup_old('old_crash');

	# Fingerprint the intact old cluster so we can prove it is undamaged.
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

	# State oracle: the partial window left NO COMPLETE marker -- that absence
	# is the ground truth distinguishing a half-built cluster from a good one.
	ok(!-f $new->data_dir . '/pg_upgrade_complete.done',
		'crash: COMPLETE marker absent on a partial window');

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

	# Rollback discards the dead-end new cluster (old_dir is intact so this is
	# allowed) and removes the new data directory.
	command_ok(
		[
			'pg_upgrade',
			'--old-datadir' => $old->data_dir,
			'--new-datadir' => $new->data_dir,
			'--old-bindir' => $old->config_data('--bindir'),
			'--new-bindir' => $new->config_data('--bindir'),
			'--socketdir' => $new->host,
			'--old-port' => $old->port,
			'--new-port' => $new->port,
			'--wal-upgrade-rollback',
		],
		'crash: --wal-upgrade-rollback discards the half-upgraded new cluster');
	ok(!-d $new->data_dir,
		'crash: rollback removed the half-upgraded new data directory');

	$old->clean_node;
	$new->clean_node;
}

done_testing();
