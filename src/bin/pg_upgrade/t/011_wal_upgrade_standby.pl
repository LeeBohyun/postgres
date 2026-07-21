# Copyright (c) 2025-2026, PostgreSQL Global Development Group

# Tests for "pg_upgrade --wal-upgrade" delivering the upgrade to a physical
# standby by STREAMING the upgrade window from the live, upgraded primary.
#
# Under --wal-upgrade the whole upgrade is captured as WAL (the CN..COMPLETE
# "window"), and a physical replication slot (the retention slot) pins that
# window in the primary's pg_wal/ so it survives the upgrade and stays
# streamable.  A fresh new-version skeleton that points primary_conninfo at the
# upgraded primary AUTO-FETCHES the window anchor over the replication
# connection (the PG_UPGRADE_WINDOW_ANCHOR command), arms its control file at
# CN, and streams the window forward -- becoming a hot standby that serves the
# upgraded data.  No operator "prepare" step and no
# hand-copied WAL are required.
#
# This test drives that path and, at each step, asserts the state transitions
# land where the state machine says they should:
#
#   primary:   old (vN)  --pg_upgrade--> auto-served new primary (not in
#              recovery), retention slot present, window pinned in pg_wal/.
#   standby:   fresh new-version skeleton + primary_conninfo  --start-->
#              auto-armed from the primary, STREAMED the window, came up in
#              recovery (pg_is_in_recovery = t), converged to the primary's data.
#   negative:  a fresh new-version skeleton with NO primary_conninfo and NO
#              local window starts as an ordinary (empty) cluster -- there is no
#              silent alternative delivery path (e.g. via archive).
#
# Cross-version: when $ENV{oldinstall} is set the old primary is built with an
# older major's binaries; otherwise the test runs same-version.  Either way the
# STANDBY skeleton is always the new (in-tree) version, since streaming a window
# into an older-version standby is not the supported direction.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# pg_upgrade writes output files relative to the current directory.
chdir ${PostgreSQL::Test::Utils::tmp_check};

# Config that lets a node act as a streaming-replication primary.
# Use values >= what PostgreSQL::Test::Cluster's allows_streaming default sets
# (max_wal_senders = 10), so a hot standby replaying this primary's WAL does not
# abort with "insufficient parameter settings".
my $primary_conf = q{
wal_level = replica
max_wal_senders = 10
max_replication_slots = 10
hot_standby = on
};

#
# 1. Old primary with data -> upgrade -> auto-served new primary.
#
my $old =
  PostgreSQL::Test::Cluster->new('old', install_path => $ENV{oldinstall});
if (defined($ENV{oldinstall}))
{
	# Checksums default on from v18; pass -k on older installs so a checksum-on
	# new cluster can be upgraded.
	$old->init(allows_streaming => 1, extra => ['-k']);
}
else
{
	$old->init(allows_streaming => 1);
}
$old->append_conf('postgresql.conf', $primary_conf);
$old->start;
$old->safe_psql(
	'postgres', qq{
	CREATE TABLE t (id int primary key, v text);
	INSERT INTO t SELECT g, 'v' || g FROM generate_series(1, 2000) g;
	CREATE INDEX ON t (v);
	CREATE TABLE toasted (id int, big text);
	INSERT INTO toasted
	  SELECT g, repeat(md5(g::text), 300) FROM generate_series(1, 300) g;
});
my $want = $old->safe_psql('postgres',
	'SELECT count(*), sum(hashtext(v)::bigint) FROM t');
$old->stop;

# The new primary is created by --initdb; do NOT init() it here.
my $new = PostgreSQL::Test::Cluster->new('new');

command_ok(
	[
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
	],
	'primary: pg_upgrade --wal-upgrade --initdb succeeds');

# The --initdb-created new cluster skipped init(); append the settings the test
# harness needs to start and stream from it.
my $conf = $new->data_dir . '/postgresql.conf';
open(my $fh, '>>', $conf) or die "could not open $conf: $!";
print $fh "\n# added by test\n";
print $fh "port = " . $new->port . "\n";
print $fh "listen_addresses = '"
  . ($PostgreSQL::Test::Utils::windows_os ? '127.0.0.1' : '') . "'\n";
print $fh "unix_socket_directories = '" . $new->host . "'\n";
print $fh $primary_conf;
close($fh);

# Trust replication + normal connections locally so the standby can connect.
$new->append_conf('pg_hba.conf',
	"local replication all trust\nhost replication all 127.0.0.1/32 trust\nhost replication all ::1/128 trust\n"
);

$new->start;

# State oracle: the upgraded primary auto-serves (not in recovery) with data.
is($new->safe_psql('postgres', 'SELECT pg_is_in_recovery()'),
	'f', 'primary: auto-served, not in recovery');
is($new->safe_psql('postgres', 'SELECT count(*), sum(hashtext(v)::bigint) FROM t'),
	$want, 'primary: data preserved after upgrade');

# State oracle: the retention slot that pins the upgrade window exists on the
# live primary, so the window is streamable to a standby.
my $nslots = $new->safe_psql('postgres',
	"SELECT count(*) FROM pg_replication_slots WHERE slot_type = 'physical'");
ok($nslots >= 1, "primary: retention slot present ($nslots physical slot(s))");

#
# 2. Fresh new-version skeleton streams the window (auto-anchor path).
#
# Build a skeleton with the *new* binaries, then reduce it to a bare skeleton
# (as the real feature does on the standby) so it has nothing but a control file
# and must obtain everything by streaming.  It is given ONLY primary_conninfo:
# no pre-staged anchor file.
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init;    # always the new/in-tree version

# A hot standby must run with WAL/replication GUCs at least as high as the
# primary's, or recovery aborts with "insufficient parameter settings".
$standby->append_conf('postgresql.conf', $primary_conf);

# Wipe the skeleton's user/global relation files but keep pg_control, matching
# the feature's standby re-provision (base/ and global/ relfiles removed).
my $sdir = $standby->data_dir;
for my $g (glob("$sdir/base/*/[0-9]*"))     { unlink $g; }
for my $g (glob("$sdir/global/[0-9]*"))     { unlink $g; }
unlink "$sdir/global/pg_filenode.map" if -e "$sdir/global/pg_filenode.map";

# connstr without a dbname yields a plain "port=N host=..." with no embedded
# quotes, which is what a replication connection needs and is safe to wrap in
# single quotes in postgresql.conf.
my $conninfo = $new->connstr;
$standby->append_conf('postgresql.conf',
	"primary_conninfo = '$conninfo'\n");
$standby->set_standby_mode;    # writes standby.signal

# There must be NO pre-staged anchor: this is the pure auto-fetch path.
ok(!-f "$sdir/pg_upgrade_stream.anchor",
	'standby: no pre-staged anchor file (auto-fetch path)');

$standby->start;

# State oracle: the standby armed from the primary and streamed the window.
my $slog = slurp_file($standby->logfile);
like(
	$slog,
	qr/auto-armed streaming standby from primary/,
	'standby: auto-armed from the primary over replication');
like(
	$slog,
	qr/started streaming|streaming WAL/,
	'standby: STREAMED the window (no cp, no prepare step)');

# State oracle: it is a hot standby (in recovery) that converged to the primary.
# First wait until the standby has replayed the whole upgrade window and reached
# a consistent, query-answerable hot-standby state -- only then does pg_class et
# al. exist and does it accept read-only connections.  poll_query_until against
# the streamed table is the natural readiness signal.
$standby->poll_query_until('postgres',
	'SELECT count(*) = 2000 FROM t')
  or die "standby did not converge to the upgraded data in time";

# Then make sure it is fully caught up with the primary's current WAL.
$new->wait_for_catchup($standby, 'replay', $new->lsn('insert'));

is($standby->safe_psql('postgres', 'SELECT pg_is_in_recovery()'),
	't', 'standby: in recovery (hot standby)');
is( $standby->safe_psql(
		'postgres', 'SELECT count(*), sum(hashtext(v)::bigint) FROM t'),
	$want,
	'standby: converged to the upgraded primary data');

$standby->stop;

#
# 3. Negative: a fresh new-version skeleton with NO primary and NO local window
#    starts as an ordinary cluster -- no silent alternative delivery path.
#
my $lonely = PostgreSQL::Test::Cluster->new('lonely');
$lonely->init;
$lonely->start;
is($lonely->safe_psql('postgres', 'SELECT pg_is_in_recovery()'),
	'f', 'negative: plain new cluster with no primary is a normal live cluster');
is($lonely->safe_psql('postgres', "SELECT count(*) FROM pg_tables WHERE tablename = 't'"),
	'0', 'negative: no upgrade window was silently applied');
$lonely->stop;

$new->stop;

done_testing();
