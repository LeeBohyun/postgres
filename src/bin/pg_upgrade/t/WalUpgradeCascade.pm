# Copyright (c) 2026, PostgreSQL Global Development Group

# Exercise a two-level cascade after the direct-standby cases in TAP 011.

package WalUpgradeCascade;

use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use WalUpgradeTest qw(
  make_old_standby_chain
  make_upgrade_standby
  slot_is_inactive_at
  upgrade_finalized
  wait_for_old_replay
  wait_for_physical_slot
  wait_for_slotless_streaming
);

# pg_upgrade writes its output files in the current directory.
chdir ${PostgreSQL::Test::Utils::tmp_check};

my $replication_conf = q{
wal_level = replica
max_wal_senders = 10
max_replication_slots = 10
hot_standby = on
listen_addresses = '127.0.0.1'
wal_keep_size = 64MB
wal_receiver_status_interval = 0
};
my $new_version_conf = $replication_conf . q{
fsync = on
max_connections = 100
max_slot_wal_keep_size = -1
idle_replication_slot_timeout = 0
wal_receiver_status_interval = 1s
};
my $replication_hba = "host replication upgrade_repl 127.0.0.1/32 trust\n";

my $old_primary = PostgreSQL::Test::Cluster->new('old_primary',
	install_path => $ENV{oldinstall});
$old_primary->init(allows_streaming => 1, force_initdb => 1, extra => ['-k']);
$old_primary->append_conf('postgresql.conf', $replication_conf);
$old_primary->append_conf('pg_hba.conf', $replication_hba);
$old_primary->start;
my $old_supports_handoff_slot_fencing = $old_primary->safe_psql('postgres',
	q{SELECT current_setting('server_version_num')::int >= 190000}) eq 't';
$old_primary->safe_psql(
	'postgres', q{
	CREATE ROLE upgrade_repl LOGIN REPLICATION;
	CREATE TABLE cascade_data (id int primary key, payload text);
	CREATE SEQUENCE cascade_sequence;
	INSERT INTO cascade_data
	  SELECT g, repeat(md5(g::text), 10) FROM generate_series(1, 500) g;
	SELECT pg_create_physical_replication_slot('slot_a', true);
});
my $fingerprint =
  'SELECT count(*), sum(id), sum(length(payload)) FROM cascade_data';
my $expected = $old_primary->safe_psql('postgres', $fingerprint);
$old_primary->backup('cascade_base');
my @standby_topology = (
	{
		name => 'old_relay',
		slot => 'slot_a',
		accepts_replication => 1,
		message => 'old primary streams to the relay through slot_a',
	},
	{
		name => 'old_leaf',
		slot => 'slot_b',
		create_slot => 1,
		message => 'old relay streams to the leaf through slot_b',
	},);
my ($old_relay, $old_leaf) = make_old_standby_chain(
	source => $old_primary,
	backup_name => 'cascade_base',
	install_path => $ENV{oldinstall},
	postgresql_conf => $replication_conf,
	replication_hba => $replication_hba,
	replication_user => 'upgrade_repl',
	standbys => \@standby_topology,
	after_start => sub {
		my ($parent, $standby, $spec) = @_;
		wait_for_physical_slot($parent, $spec->{slot}, $spec->{name})
		  or die $spec->{message};
	});
wait_for_old_replay($old_primary, $old_relay);
wait_for_old_replay($old_primary, $old_leaf);

$old_leaf->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
my $paused = q{SELECT pg_get_wal_replay_pause_state() = 'paused'};
$old_leaf->poll_query_until('postgres', $paused)
  or die 'old leaf did not pause replay before HANDOFF';
my $leaf_replay_before =
  $old_leaf->safe_psql('postgres', 'SELECT pg_last_wal_replay_lsn()');
my $relay_handoff_log_offset = -s $old_relay->logfile;

my $new_primary = PostgreSQL::Test::Cluster->new('new_primary');
$old_primary->stop;
my $leaf_stopped_during_relay_restart = 0;
my $leaf_handoff_log_offset;
my @upgrade_command = (
	'pg_upgrade',
	'--old-datadir' => $old_primary->data_dir,
	'--new-datadir' => $new_primary->data_dir,
	'--old-bindir' => $old_primary->config_data('--bindir'),
	'--new-bindir' => $new_primary->config_data('--bindir'),
	'--socketdir' => $new_primary->host,
	'--new-port' => $new_primary->port,
	'--initdb',
	'--wal-upgrade',
	'--copy',);

$leaf_handoff_log_offset = -s $old_leaf->logfile;
$relay_handoff_log_offset = -s $old_relay->logfile;
command_ok(\@upgrade_command,
	'primary waits for durable relay receipt of HANDOFF and shutdown checkpoint'
);
$old_primary->_update_pid(0);

$old_relay->wait_for_log(
	qr/reached the final old-major shutdown checkpoint; pausing recovery for pg_upgrade/,
	$relay_handoff_log_offset);
ok( $old_relay->poll_query_until('postgres', $paused),
	'relay pauses after durable leaf receipt of HANDOFF and shutdown checkpoint'
);
if ($old_supports_handoff_slot_fencing)
{
	my $advance_stderr;

	$old_leaf->stop;
	$leaf_stopped_during_relay_restart = 1;
	$old_relay->poll_query_until('postgres',
		"SELECT NOT active FROM pg_replication_slots WHERE slot_name = 'slot_b'"
	) or die 'slot_b did not become inactive';
	isnt(
		$old_relay->psql(
			'postgres',
			"SELECT pg_replication_slot_advance('slot_b', pg_last_wal_replay_lsn())",
			stderr => \$advance_stderr),
		0,
		'HANDOFF rejects manual advancement of slot_b');

	# Test pause restoration from durable slot_b state while the leaf is offline.
	my $relay_restart_log_offset = -s $old_relay->logfile;
	$old_relay->stop;
	$old_relay->start;
	$old_relay->wait_for_log(
		qr/restored pg_upgrade pause from the final old-major shutdown checkpoint/,
		$relay_restart_log_offset);
	ok( $old_relay->poll_query_until('postgres', $paused)
		  && $old_relay->poll_query_until(
			'postgres', q{
			SELECT NOT active
			  AND restart_lsn >= (SELECT min_recovery_end_lsn
			                      FROM pg_control_recovery())
			FROM pg_replication_slots WHERE slot_name = 'slot_b'
			}),
		'old relay restores its pause and durable slot_b receipt');
}
if ($leaf_stopped_during_relay_restart)
{
	$old_leaf->start;
	$old_leaf->wait_for_log(
		qr/reached the final old-major shutdown checkpoint; pausing recovery for pg_upgrade/,
		$leaf_handoff_log_offset);
	ok( $old_leaf->poll_query_until('postgres', $paused),
		'old leaf reaches its HANDOFF pause after the root upgrade completes'
	);
}
else
{
	is( join(
			'|',
			$old_leaf->safe_psql(
				'postgres', 'SELECT pg_get_wal_replay_pause_state()'),
			$old_leaf->safe_psql(
				'postgres', 'SELECT pg_last_wal_replay_lsn()')),
		"paused|$leaf_replay_before",
		'root upgrade completes while leaf replay remains paused below HANDOFF'
	);
}

my $relay_checkpoint = $old_relay->safe_psql('postgres',
	'SELECT checkpoint_lsn FROM pg_control_checkpoint()');
ok( $old_relay->poll_query_until(
		'postgres', q{
		SELECT restart_lsn >= (SELECT min_recovery_end_lsn
		                        FROM pg_control_recovery())
		FROM pg_replication_slots WHERE slot_name = 'slot_b'
	}),
	'leaf durably receives the shutdown checkpoint before the old relay stops'
);
$old_relay->stop;

$new_primary->append_conf('postgresql.conf',
		"port = "
	  . $new_primary->port . "\n"
	  . "listen_addresses = '127.0.0.1'\n"
	  . "unix_socket_directories = '"
	  . $new_primary->host . "'\n"
	  . $new_version_conf);
$new_primary->append_conf('pg_hba.conf', $replication_hba);
$new_primary->start;

my $replay_start_lsn = $new_primary->safe_psql('postgres',
	"SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = 'slot_a'"
);
is(slot_is_inactive_at($new_primary, 'slot_a', $replay_start_lsn),
	't', 'new primary retains slot_a at the upgrade replay start LSN');
is($new_primary->safe_psql('postgres', $fingerprint),
	$expected, 'new primary contains the upgraded data');

my $new_relay = make_upgrade_standby(
	name => 'new_relay',
	source => $new_primary,
	old_datadir => $old_relay->data_dir,
	postgresql_conf => $new_version_conf,
	replication_user => 'upgrade_repl',
	replication_hba => $replication_hba,
	allows_streaming => 1);
$new_relay->start;
$new_primary->wait_for_replay_catchup($new_relay);
ok( wait_for_slotless_streaming($new_primary, 'new_relay'),
	'new relay replays without a named or temporary slot');
ok( upgrade_finalized($new_relay)
	  && slot_is_inactive_at($new_relay, 'slot_b', $replay_start_lsn) eq 't'
	  && $new_relay->safe_psql('postgres', $fingerprint) eq $expected,
	'new relay finalizes with upgraded data and retained slot_b');

$new_relay->restart;
ok( upgrade_finalized($new_relay)
	  && slot_is_inactive_at($new_relay, 'slot_b', $replay_start_lsn) eq 't',
	'finalized relay restart retains slot_b at the replay start LSN');

if (!$leaf_stopped_during_relay_restart)
{
	$leaf_handoff_log_offset = -s $old_leaf->logfile;
	$old_leaf->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
	$old_leaf->wait_for_log(
		qr/reached the final old-major shutdown checkpoint; pausing recovery for pg_upgrade/,
		$leaf_handoff_log_offset);
	ok($old_leaf->poll_query_until('postgres', $paused),
		'old leaf reaches its HANDOFF pause after the relay upgrades');
}
my $leaf_checkpoint = $old_leaf->safe_psql('postgres',
	'SELECT checkpoint_lsn FROM pg_control_checkpoint()');
is($leaf_checkpoint, $relay_checkpoint,
	'old relay and leaf retain the same shutdown checkpoint');
$old_leaf->stop;

my $new_leaf = make_upgrade_standby(
	name => 'new_leaf',
	source => $new_relay,
	old_datadir => $old_leaf->data_dir,
	postgresql_conf => $new_version_conf,
	replication_user => 'upgrade_repl',
	replication_hba => $replication_hba,
	allows_streaming => 1);
$new_leaf->start;
$new_relay->wait_for_replay_catchup($new_leaf, $new_primary);
ok( wait_for_slotless_streaming($new_relay, 'new_leaf'),
	'new leaf replays without a named or temporary slot');
ok( upgrade_finalized($new_leaf)
	  && $new_leaf->safe_psql('postgres', $fingerprint) eq $expected,
	'new leaf finalizes with upgraded data');
ok( slot_is_inactive_at($new_primary, 'slot_a', $replay_start_lsn) eq 't'
	  && slot_is_inactive_at($new_relay, 'slot_b', $replay_start_lsn) eq 't',
	'upgrade replay leaves both migrated slots at the replay start LSN');

$new_leaf->adjust_conf('postgresql.conf', 'primary_slot_name', "'slot_b'");
$new_leaf->reload;
ok(wait_for_physical_slot($new_relay, 'slot_b', 'new_leaf'),
	'leaf switches its WAL receiver to slot_b');
$new_relay->adjust_conf('postgresql.conf', 'primary_slot_name', "'slot_a'");
$new_relay->reload;
ok( wait_for_physical_slot($new_primary, 'slot_a', 'new_relay'),
	'relay switches its WAL receiver to slot_a after the leaf');

$new_primary->safe_psql('postgres', "SELECT setval('cascade_sequence', 501)");
$new_primary->safe_psql('postgres', 'SELECT pg_switch_wal()');
$new_primary->wait_for_replay_catchup($new_relay);
$new_relay->wait_for_replay_catchup($new_leaf, $new_primary);
is( $new_leaf->safe_psql(
		'postgres', 'SELECT last_value FROM cascade_sequence'),
	'501',
	'new WAL replicates through the cascade');
ok( $new_primary->poll_query_until('postgres',
		"SELECT restart_lsn > '$replay_start_lsn'::pg_lsn FROM pg_replication_slots WHERE slot_name = 'slot_a'"
	  )
	  && $new_relay->poll_query_until(
		'postgres',
		"SELECT restart_lsn > '$replay_start_lsn'::pg_lsn FROM pg_replication_slots WHERE slot_name = 'slot_b'"
	  ),
	'both migrated slots advance after leaf-first activation');

$new_leaf->stop;
$new_relay->stop;
$new_primary->stop;

1;
