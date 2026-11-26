# Copyright (c) 2026, PostgreSQL Global Development Group

# HANDOFF over TCP, durable pause, and subsequent first-start upgrade replay.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

chdir $PostgreSQL::Test::Utils::tmp_check;

my @old_options =
  defined($ENV{oldinstall}) ? (install_path => $ENV{oldinstall}) : ();
my $old = PostgreSQL::Test::Cluster->new('handoff_primary', @old_options);
$old->init(allows_streaming => 1, force_initdb => defined($ENV{oldinstall}));
$old->adjust_conf('postgresql.conf', 'autovacuum_worker_slots', undef)
  if $old->pg_version < 18;
$old->append_conf('postgresql.conf', "listen_addresses = '127.0.0.1'");
$old->append_conf('postgresql.conf', "wal_sender_shutdown_timeout = '1s'")
  if $old->pg_version >= 19;
$old->append_conf('pg_hba.conf',
	'host replication handoff_repl 127.0.0.1/32 trust');
$old->start;
$old->safe_psql(
	'postgres', q{
  CREATE ROLE handoff_repl LOGIN REPLICATION;
  CREATE TABLE handoff_data AS SELECT generate_series(1, 100) AS id;
  SELECT pg_create_physical_replication_slot('handoff_slot', true);
});
$old->backup('handoff_base');
my $retained =
  PostgreSQL::Test::Cluster->new('handoff_standby', @old_options);
$retained->init_from_backup($old, 'handoff_base', has_streaming => 1);
$retained->append_conf('postgresql.auto.conf',
		"primary_conninfo = 'host=127.0.0.1 port="
	  . $old->port
	  . " user=handoff_repl application_name=handoff_standby'\n"
	  . "primary_slot_name = 'handoff_slot'\n");
$retained->start;
ok( $old->poll_query_until(
		'postgres', q{
  SELECT count(*) = 1 FROM pg_stat_replication
  WHERE application_name = 'handoff_standby' AND state = 'streaming'
}),
	'old standby streams over TCP with a replication role');
$old->wait_for_catchup('handoff_standby');
$old->stop;

my $new = PostgreSQL::Test::Cluster->new('handoff_new_primary');
command_ok(
	[
		'pg_upgrade', '--initdb', '--wal-upgrade', '--no-sync', '--copy',
		'--initdb-options=-c wal_keep_size=1GB -c max_connections=10 -c autovacuum=off',
		'--old-datadir' => $old->data_dir,
		'--new-datadir' => $new->data_dir,
		'--old-bindir' => $old->config_data('--bindir'),
		'--new-bindir' => $new->config_data('--bindir'),
		'--socketdir' => $old->host,
		'--old-port' => $old->port,
		'--new-port' => $new->port,
	],
	'pg_upgrade completes HANDOFF and prepares the new primary');

my $paused = q{SELECT pg_get_wal_replay_pause_state() = 'paused'};
ok( $retained->poll_query_until('postgres', $paused),
	'standby pauses at the final HANDOFF checkpoint');
is( $retained->safe_psql(
		'postgres', q{
  SELECT c.checkpoint_lsn = c.redo_lsn,
         r.min_recovery_end_lsn > c.checkpoint_lsn,
         r.min_recovery_end_timeline = c.timeline_id
  FROM pg_control_checkpoint() c, pg_control_recovery() r
}),
	't|t|t',
	'HANDOFF checkpoint and its end are durable');
$retained->restart;
ok($retained->poll_query_until('postgres', $paused),
	'HANDOFF pause survives restart');
$retained->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
ok( $retained->poll_query_until(
		'postgres', q{SELECT pg_get_wal_replay_pause_state() = 'not paused'}),
	'explicit resume cancels HANDOFF');
$retained->stop;
ok(-f $new->data_dir . '/pg_upgrade_relink',
	'upgrade commands remain pending until first startup');

$new->append_conf('postgresql.conf', 'port = ' . $new->port);
$new->append_conf('postgresql.conf',
	"unix_socket_directories = '" . $new->host . "'");
$new->append_conf('postgresql.conf', "listen_addresses = '127.0.0.1'");
$new->append_conf('pg_hba.conf', 'host replication all 127.0.0.1/32 trust');
$new->start;
ok( !-f $new->data_dir . '/pg_upgrade_relink',
	'first startup finalizes upgrade WAL emission');
is( $new->safe_psql('postgres', 'SELECT count(*), sum(id) FROM handoff_data'),
	'100|5050',
	'new primary preserves the source data');

my $replica = PostgreSQL::Test::Cluster->new('handoff_new_standby');
$replica->init(allows_streaming => 1);
$replica->append_conf('postgresql.conf', 'max_connections = 100');
$replica->append_conf('postgresql.conf',
		"primary_conninfo = '"
	  . $new->connstr . "'\n"
	  . "primary_slot_name = ''\nwal_receiver_create_temp_slot = off\n"
	  . "pg_upgrade_standby_old_datadir = '"
	  . $retained->data_dir
	  . "'\n");
$replica->append_conf('pg_upgrade.signal', '');
$replica->set_standby_mode;
$replica->start;
is( $replica->safe_psql(
		'postgres', 'SELECT count(*), sum(id) FROM handoff_data'),
	'100|5050',
	'new standby replays upgrade WAL from the HANDOFF boundary');
$replica->restart;
is( $replica->safe_psql(
		'postgres', q{
  SELECT pg_is_in_recovery(), count(*), sum(id) FROM handoff_data
}),
	't|100|5050',
	'finalized standby survives restart');
$replica->stop;
$new->stop;

done_testing();
