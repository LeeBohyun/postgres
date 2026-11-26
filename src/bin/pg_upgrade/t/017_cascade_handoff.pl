# Copyright (c) 2026, PostgreSQL Global Development Group

# Primary completion confirms direct receipt. Each relay must independently
# retain the old-version WAL until its downstream standbys have received it.

use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use WalUpgradeTest qw(
  make_old_standby_chain
  wait_for_old_replay
  wait_for_physical_slot
);

chdir $PostgreSQL::Test::Utils::tmp_check;

my $replication_conf = q{
wal_level = replica
max_wal_senders = 10
max_replication_slots = 10
hot_standby = on
listen_addresses = '127.0.0.1'
wal_receiver_status_interval = 0
};
my $replication_hba = "host replication upgrade_repl 127.0.0.1/32 trust\n";

my $primary = PostgreSQL::Test::Cluster->new('receipt_primary',
	install_path => $ENV{oldinstall});
$primary->init(
	allows_streaming => 1,
	force_initdb => defined($ENV{oldinstall}));
$primary->adjust_conf('postgresql.conf', 'autovacuum_worker_slots', undef)
  if $primary->pg_version < 18;
$primary->append_conf('postgresql.conf', $replication_conf);
$primary->append_conf('pg_hba.conf', $replication_hba);
$primary->append_conf('postgresql.conf', "wal_sender_shutdown_timeout = '1s'")
  if $primary->pg_version >= 19;
$primary->start;
$primary->safe_psql(
	'postgres', q{
	CREATE ROLE upgrade_repl LOGIN REPLICATION;
	CREATE TABLE receipt_data AS SELECT 42 AS value;
	SELECT pg_create_physical_replication_slot('relay_slot', true);
});
$primary->backup('receipt_base');

my ($relay, $leaf) = make_old_standby_chain(
	source => $primary,
	backup_name => 'receipt_base',
	install_path => $ENV{oldinstall},
	postgresql_conf => $replication_conf,
	replication_hba => $replication_hba,
	replication_user => 'upgrade_repl',
	standbys => [
		{
			name => 'receipt_relay',
			slot => 'relay_slot',
			accepts_replication => 1,
		},
		{
			name => 'receipt_leaf',
			slot => 'leaf_slot',
			create_slot => 1,
		},
	],
	after_start => sub {
		my ($parent, $standby, $spec) = @_;
		wait_for_physical_slot($parent, $spec->{slot}, $spec->{name})
		  or die "standby $spec->{name} did not start streaming";
	});
wait_for_old_replay($primary, $relay);
wait_for_old_replay($primary, $leaf);

$leaf->stop;
$relay->poll_query_until('postgres',
	"SELECT NOT active FROM pg_replication_slots WHERE slot_name = 'leaf_slot'"
) or die 'leaf slot did not become inactive';
$primary->stop;

my $new = PostgreSQL::Test::Cluster->new('receipt_upgraded_primary');
my $relay_log_offset = -s $relay->logfile;
command_ok(
	[
		'pg_upgrade', '--initdb', '--wal-upgrade', '--copy',
		'--old-datadir' => $primary->data_dir,
		'--new-datadir' => $new->data_dir,
		'--old-bindir' => $primary->config_data('--bindir'),
		'--new-bindir' => $new->config_data('--bindir'),
		'--socketdir' => $new->host,
		'--old-port' => $primary->port,
		'--new-port' => $new->port,
	],
	'primary preparation completes after direct relay receipt with the leaf offline'
);
$primary->_update_pid(0);

# HANDOFF backports vary in wording; verify the receipt state with SQL below.
my $waiting = qr/waiting for 1 physical .*pg_upgrade handoff/;
my $paused = q{SELECT pg_get_wal_replay_pause_state() = 'paused'};
my $waiting_for_leaf = q{
	SELECT pg_get_wal_replay_pause_state() = 'not paused'
	   AND NOT active
	   AND restart_lsn < (SELECT min_recovery_end_lsn
	                      FROM pg_control_recovery())
	FROM pg_replication_slots WHERE slot_name = 'leaf_slot'
};
$relay->wait_for_log($waiting, $relay_log_offset);
is($relay->safe_psql('postgres', $waiting_for_leaf),
	't', 'primary completion leaves the relay waiting for the offline leaf');

# The persisted checkpoint must restore the downstream wait after restart.
$relay_log_offset = -s $relay->logfile;
$relay->restart;
$relay->wait_for_log($waiting, $relay_log_offset);
is($relay->safe_psql('postgres', $waiting_for_leaf),
	't', 'relay restart does not bypass the missing leaf receipt');

my $leaf_log_offset = -s $leaf->logfile;
$leaf->start;
my $checkpoint_message = 'the final old-major shutdown checkpoint';
$leaf->wait_for_log(
	qr/reached $checkpoint_message; pausing recovery for pg_upgrade/,
	$leaf_log_offset);
$relay->wait_for_log(qr/restored pg_upgrade pause from $checkpoint_message/,
	$relay_log_offset);
ok( $leaf->poll_query_until('postgres', $paused),
	'leaf reaches its HANDOFF pause after reconnecting');
ok( $relay->poll_query_until('postgres', $paused),
	'relay reaches its HANDOFF pause after leaf receipt');
is( $relay->safe_psql(
		'postgres', q{
	SELECT restart_lsn >= (SELECT min_recovery_end_lsn
	                       FROM pg_control_recovery())
	FROM pg_replication_slots WHERE slot_name = 'leaf_slot'
}),
	't',
	'leaf receipt covers the final checkpoint before the relay is ready');
for my $node ($relay, $leaf)
{
	is($node->safe_psql('postgres', 'TABLE receipt_data'),
		'42', $node->name . ' retains the source data');
}

$leaf->stop;
$relay->stop;
done_testing();
