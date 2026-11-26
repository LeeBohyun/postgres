# Copyright (c) 2026, PostgreSQL Global Development Group

# Check HANDOFF propagation through a cascaded physical standby.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# pg_upgrade writes its output files in the current directory.
chdir ${PostgreSQL::Test::Utils::tmp_check};

my $lsn_pattern = qr/[0-9A-F]+\/[0-9A-F]+/i;

sub wait_for_physical_slot
{
	my ($parent, $slot, $child) = @_;
	return $parent->poll_query_until(
		'postgres', qq{
		SELECT count(*) = 1
		FROM pg_replication_slots s
		JOIN pg_stat_replication r ON r.pid = s.active_pid
		WHERE s.slot_name = '$slot'
		  AND s.slot_type = 'physical'
		  AND NOT s.temporary
		  AND s.restart_lsn IS NOT NULL
		  AND s.wal_status IS DISTINCT FROM 'lost'
		  AND r.application_name = '$child'
		  AND r.state = 'streaming'
	});
}

sub handoff_control_state
{
	my ($node) = @_;
	return $node->safe_psql(
		'postgres', q{
		SELECT s.system_identifier,
		       c.checkpoint_lsn,
		       c.checkpoint_lsn = c.redo_lsn,
		       c.timeline_id,
		       r.min_recovery_end_lsn,
		       r.min_recovery_end_lsn > c.checkpoint_lsn,
		       r.min_recovery_end_timeline = c.timeline_id,
		       pg_is_in_recovery(),
		       pg_get_wal_replay_pause_state()
		FROM pg_control_system() s,
		     pg_control_checkpoint() c,
		     pg_control_recovery() r
	});
}

my $replication_conf = q{
wal_level = replica
max_wal_senders = 10
max_replication_slots = 10
hot_standby = on
listen_addresses = '127.0.0.1'
wal_receiver_status_interval = 0
};
my $replication_hba = "host replication upgrade_repl 127.0.0.1/32 trust\n";

my $primary = PostgreSQL::Test::Cluster->new('cascade_primary',
	install_path => $ENV{oldinstall});
$primary->init(allows_streaming => 1, force_initdb => 1);
$primary->adjust_conf('postgresql.conf', 'autovacuum_worker_slots', undef)
  if $primary->pg_version < 18;
$primary->append_conf('postgresql.conf', $replication_conf);
$primary->append_conf('pg_hba.conf', $replication_hba);
$primary->start;

if ($primary->safe_psql('postgres',
		q{SELECT current_setting('server_version_num')::int >= 190000}) eq 't'
  )
{
	$primary->append_conf('postgresql.conf',
		"wal_sender_shutdown_timeout = '1s'");
	$primary->reload;
}
$primary->safe_psql(
	'postgres', q{
	CREATE ROLE upgrade_repl LOGIN REPLICATION SUPERUSER;
	CREATE TABLE handoff_data (id int primary key, payload text);
	INSERT INTO handoff_data
	  SELECT g, repeat(md5(g::text), 10) FROM generate_series(1, 100) g;
	SELECT pg_create_physical_replication_slot('slot_a', true);
});
$primary->backup('cascade_base');

my $relay = PostgreSQL::Test::Cluster->new('cascade_relay',
	install_path => $ENV{oldinstall});
$relay->init_from_backup($primary, 'cascade_base', has_streaming => 1);
$relay->append_conf('postgresql.conf', $replication_conf);
$relay->append_conf('pg_hba.conf', $replication_hba);
$relay->append_conf('postgresql.auto.conf',
		"primary_conninfo = 'host="
	  . $primary->host
	  . " port="
	  . $primary->port
	  . " user=upgrade_repl application_name=cascade_relay'\n"
	  . "primary_slot_name = 'slot_a'\n");
$relay->start;
ok(wait_for_physical_slot($primary, 'slot_a', 'cascade_relay'),
	'primary streams to the relay through slot_a');

$relay->safe_psql('postgres',
	"SELECT pg_create_physical_replication_slot('slot_b', true)");

my $leaf = PostgreSQL::Test::Cluster->new('cascade_leaf',
	install_path => $ENV{oldinstall});
$leaf->init_from_backup($primary, 'cascade_base', has_streaming => 1);
$leaf->append_conf('postgresql.conf', $replication_conf);
$leaf->append_conf('postgresql.auto.conf',
		"primary_conninfo = 'host=127.0.0.1 port="
	  . $relay->port
	  . " user=upgrade_repl application_name=cascade_leaf'\n"
	  . "primary_slot_name = 'slot_b'\n");

$leaf->start;
ok(wait_for_physical_slot($relay, 'slot_b', 'cascade_leaf'),
	'relay streams to the leaf through slot_b');
my $system_identifier = $primary->safe_psql('postgres',
	'SELECT system_identifier FROM pg_control_system()');

# Disconnect the leaf before HANDOFF. The relay must retain the boundary for it.
$leaf->stop;

my $new_primary = PostgreSQL::Test::Cluster->new('cascade_new_primary');
$primary->stop;
my @upgrade_command = (
	'pg_upgrade', '--no-sync', '--initdb', '--wal-upgrade',
	'--copy',
	'--old-datadir' => $primary->data_dir,
	'--new-datadir' => $new_primary->data_dir,
	'--old-bindir' => $primary->config_data('--bindir'),
	'--new-bindir' => $new_primary->config_data('--bindir'),
	'--socketdir' => $new_primary->host,
	'--old-port' => $primary->port,
	'--new-port' => $new_primary->port,);
command_ok(\@upgrade_command,
	'primary prepares the upgrade while the cascade leaf is disconnected');
$primary->_update_pid(0);

is( $relay->safe_psql(
		'postgres',
		q{SELECT pg_is_in_recovery(), pg_get_wal_replay_pause_state(),
		         count(*), sum(id) FROM handoff_data}),
	't|not paused|100|5050',
	'relay remains queryable while child receipt is pending');

$leaf->start;

my $paused_query = q{SELECT pg_get_wal_replay_pause_state() = 'paused'};
ok($leaf->poll_query_until('postgres', $paused_query),
	'leaf: recovery pauses at HANDOFF');
ok( $relay->poll_query_until('postgres', $paused_query),
	'relay: recovery pauses after the leaf acknowledgement');

my $relay_control_state = handoff_control_state($relay);
my @relay_control = split(/\|/, $relay_control_state);
ok( @relay_control == 9
	  && $relay_control[0] eq $system_identifier
	  && $relay_control[1] =~ /\A$lsn_pattern\z/
	  && $relay_control[2] eq 't'
	  && $relay_control[3] =~ /\A\d+\z/
	  && $relay_control[4] =~ /\A$lsn_pattern\z/
	  && $relay_control[5] eq 't'
	  && $relay_control[6] eq 't'
	  && $relay_control[7] eq 't'
	  && $relay_control[8] eq 'paused',
	'relay: HANDOFF checkpoint and restartpoint are durable');
is(handoff_control_state($leaf),
	$relay_control_state, 'leaf: durable HANDOFF state matches the relay');
my $checkpoint_end = $relay_control[4];

$leaf->stop;
ok( $relay->poll_query_until(
		'postgres',
		q{SELECT NOT active FROM pg_replication_slots WHERE slot_name = 'slot_b'}
	),
	'relay observes the leaf disconnect');
$leaf->start;
ok( $leaf->poll_query_until('postgres', $paused_query),
	'leaf: restart restores the HANDOFF pause');
is(handoff_control_state($leaf),
	$relay_control_state,
	'leaf: restart preserves the durable HANDOFF state');
ok( $relay->poll_query_until(
		'postgres',
		"SELECT r.replay_lsn >= '$checkpoint_end'::pg_lsn "
		  . "FROM pg_replication_slots s JOIN pg_stat_replication r "
		  . "ON r.pid = s.active_pid WHERE s.slot_name = 'slot_b'"),
	'relay: slot_b reports replay through the checkpoint end');

$leaf->stop;
ok(-f $relay->data_dir . '/postmaster.pid', 'relay remains available');
$relay->stop;

done_testing();
