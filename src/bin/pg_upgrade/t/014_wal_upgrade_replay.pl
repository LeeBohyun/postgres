# Copyright (c) 2026, PostgreSQL Global Development Group

# Replay an upgrade from a retained old-version standby and verify the
# upgraded data, completion restartpoint, subsequent WAL, and promotion.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use FindBin;
use lib $FindBin::RealBin;
use WalUpgradeTest qw(require_wal_upgrade_source);

my $old = PostgreSQL::Test::Cluster->new('old_manual_replay',
	install_path => $ENV{oldinstall});
require_wal_upgrade_source($old);
$old->init(extra => ['--data-checksums'], allows_streaming => 1);
$old->start;
$old->safe_psql('postgres', q{
	CREATE TABLE upgrade_data (id int PRIMARY KEY, payload text);
	INSERT INTO upgrade_data
	SELECT g, repeat(md5(g::text), 200) FROM generate_series(1, 100) g;
	CREATE UNLOGGED TABLE upgrade_unlogged AS SELECT 42 AS value;
	CREATE SEQUENCE upgrade_sequence;
	SELECT setval('upgrade_sequence', 1234);
});
my $fingerprint = $old->safe_psql('postgres', q{
	SELECT count(*), md5(string_agg(id::text || payload, '' ORDER BY id))
	FROM upgrade_data;
});
$old->safe_psql('postgres',
	"SELECT pg_create_physical_replication_slot('manual_replay_slot', true)");
$old->backup('replay_source');
my $retained = PostgreSQL::Test::Cluster->new('retained_manual_replay',
	install_path => $ENV{oldinstall});
$retained->init_from_backup($old, 'replay_source', has_streaming => 1);
$retained->append_conf('postgresql.auto.conf',
	"primary_slot_name = 'manual_replay_slot'");
$retained->start;
$old->wait_for_replay_catchup($retained);
$old->stop;

my $primary = PostgreSQL::Test::Cluster->new('primary_manual_replay');
chdir ${PostgreSQL::Test::Utils::tmp_check};
command_ok([
	'pg_upgrade', '--no-sync', '--initdb', '--wal-upgrade', '--copy',
	'--initdb-options=-c wal_keep_size=128MB',
	'--old-datadir' => $old->data_dir,
	'--new-datadir' => $primary->data_dir,
	'--old-bindir' => $old->config_data('--bindir'),
	'--new-bindir' => $primary->config_data('--bindir'),
	'--socketdir' => $primary->host,
	'--old-port' => $old->port,
	'--new-port' => $primary->port ],
	'primary emits the upgrade transaction');

# HANDOFF persists the final old checkpoint before pausing this standby.
$retained->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
	or die 'retained standby did not pause at the final source checkpoint';
$retained->stop;

my $listen = $PostgreSQL::Test::Cluster::use_tcp ? $primary->host : '';
my $socketdir = $PostgreSQL::Test::Cluster::use_tcp ? '' : $primary->host;
$primary->append_conf('postgresql.conf',
	'port = ' . $primary->port
	  . "\nlisten_addresses = '$listen'\nunix_socket_directories = '$socketdir'\n"
	  . "wal_keep_size = '128MB'\n");
$primary->start;

my $standby = PostgreSQL::Test::Cluster->new('standby_manual_replay');
$standby->init(allows_streaming => 1);
$standby->append_conf('postgresql.conf',
	  "pg_upgrade_standby_old_datadir = '" . $retained->data_dir . "'\n"
	. "primary_conninfo = '" . $primary->connstr . "'\n"
	. "primary_slot_name = ''\nwal_receiver_create_temp_slot = off\n"
	. "max_connections = 100\nmax_worker_processes = 8\n"
	. "max_wal_senders = 10\nmax_locks_per_transaction = 128\n");
$standby->append_conf('pg_upgrade.signal', '');
$standby->set_standby_mode;
$standby->start;
is($standby->safe_psql('postgres', q{
	SELECT count(*), md5(string_agg(id::text || payload, '' ORDER BY id))
	FROM upgrade_data;
}), $fingerprint, 'standby replay preserves heap and toasted values');
is($standby->safe_psql('postgres',
	'SET enable_seqscan = off; SELECT count(*) FROM upgrade_data WHERE id BETWEEN 1 AND 100'),
	'100', 'standby replay preserves the primary-key index');
my ($control) = run_command([
	$standby->installed_command('pg_controldata'), $standby->data_dir ]);
like($control, qr/wal-upgrade transaction finalized:\s+yes/,
	'standby persists the upgrade completion checkpoint before serving');
ok(!-f $standby->data_dir . '/pg_upgrade.signal',
	'completed replay removes the upgrade signal');

$primary->safe_psql('postgres',
	"INSERT INTO upgrade_data VALUES (101, 'after upgrade')");
$standby->poll_query_until('postgres',
	'SELECT count(*) = 101 FROM upgrade_data')
	or die 'standby did not replay post-upgrade writes';
$standby->restart;
is($standby->safe_psql('postgres', 'SELECT count(*) FROM upgrade_data'),
	'101', 'standby retains upgraded data after restart');

$standby->promote;
is($standby->safe_psql('postgres', 'SELECT count(*) FROM upgrade_unlogged'),
	'0', 'promotion resets unlogged data');
is($standby->safe_psql('postgres', "SELECT nextval('upgrade_sequence')"),
	'1235', 'promoted standby retains sequence state');
$standby->stop;
$primary->stop;

done_testing();
