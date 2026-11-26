# Copyright (c) 2026, PostgreSQL Global Development Group

# Retain upgrade WAL on the new primary and relay without wal_keep_size.

use strict;
use warnings FATAL => 'all';

use File::Copy qw(copy);
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
fsync = on
wal_level = replica
wal_keep_size = 0
min_wal_size = '32MB'
max_wal_size = '64MB'
max_connections = 100
max_wal_senders = 10
max_replication_slots = 10
max_slot_wal_keep_size = -1
hot_standby = on
listen_addresses = '127.0.0.1'
wal_receiver_status_interval = 0
};
my $replication_hba = "host replication upgrade_repl 127.0.0.1/32 trust\n";

my $primary = PostgreSQL::Test::Cluster->new('retention_primary',
	install_path => $ENV{oldinstall});
$primary->init(
	allows_streaming => 1,
	force_initdb => defined($ENV{oldinstall}));
$primary->adjust_conf('postgresql.conf', 'autovacuum_worker_slots', undef)
  if $primary->pg_version < 18;
$primary->append_conf('postgresql.conf', $replication_conf);
$primary->append_conf('pg_hba.conf', $replication_hba);
$primary->start;
$primary->safe_psql(
	'postgres', q{
	CREATE ROLE upgrade_repl LOGIN REPLICATION;
	CREATE TABLE retention_data AS SELECT 42 AS value;
	CREATE SEQUENCE retention_sequence;
	SELECT pg_create_physical_replication_slot('relay_slot', true);
});
$primary->backup('retention_base');
my ($relay, $leaf) = make_old_standby_chain(
	source => $primary,
	backup_name => 'retention_base',
	install_path => $ENV{oldinstall},
	postgresql_conf => $replication_conf,
	replication_hba => $replication_hba,
	replication_user => 'upgrade_repl',
	standbys => [
		{
			name => 'retention_relay',
			slot => 'relay_slot',
			accepts_replication => 1
		},
		{ name => 'retention_leaf', slot => 'leaf_slot', create_slot => 1 },
	],
	after_start => sub {
		my ($parent, $standby, $spec) = @_;
		wait_for_physical_slot($parent, $spec->{slot}, $spec->{name})
		  or die "standby $spec->{name} did not start streaming";
	});
wait_for_old_replay($primary, $relay);
wait_for_old_replay($primary, $leaf);
$primary->stop;

sub upgrade_command
{
	my ($new, @options) = @_;
	return [
		'pg_upgrade', '--initdb', '--wal-upgrade', '--copy', @options,
		'--old-datadir' => $primary->data_dir,
		'--new-datadir' => $new->data_dir,
		'--old-bindir' => $primary->config_data('--bindir'),
		'--new-bindir' => $new->config_data('--bindir'),
		'--socketdir' => $new->host,
		'--old-port' => $primary->port,
		'--new-port' => $new->port,
	];
}

subtest 'reserved physical slot names fail before HANDOFF' => sub {
	plan skip_all => 'requires a source version before PG19'
	  if $primary->pg_version >= 19;
	$primary->start;
	$primary->safe_psql('postgres',
		q{SELECT pg_create_physical_replication_slot('pg_conflict_detection', true)}
	);
	$primary->stop;
	my $new = PostgreSQL::Test::Cluster->new('reserved_physical_slot');
	command_checks_all(
		upgrade_command($new),
		1,
		[
			qr/physical replication slot "pg_conflict_detection" uses a name reserved in the new cluster/
		],
		[],
		'reject a physical slot name reserved by the target version');
	ok( !-f $primary->data_dir . '/pg_upgrade_handoff.pending',
		'reserved name failure does not leave an armed HANDOFF');
	ok( !-f $new->data_dir . '/PG_VERSION',
		'reserved name fails during checks, while target cleanup is still allowed'
	);
	$primary->start;
	$primary->safe_psql('postgres',
		q{SELECT pg_drop_replication_slot('pg_conflict_detection')});
	$primary->stop;
};

for my $case (
	[
		'relay_slot',
		10,
		qr/physical replication slot "relay_slot" already exists in the new cluster/
	],
	[
		'additional_target_slot', 1,
		qr/"max_replication_slots" must be at least 2/
	])
{
	my ($slot, $capacity, $error) = @$case;
	subtest "existing target slot $slot is checked before HANDOFF" => sub {
		my $new = PostgreSQL::Test::Cluster->new("existing_$slot");
		$new->init(allows_streaming => 1);
		$new->append_conf('postgresql.conf', $replication_conf);
		$new->append_conf('postgresql.conf',
			"max_replication_slots = $capacity");
		$new->start;
		$new->safe_psql('postgres',
			"SELECT pg_create_physical_replication_slot('$slot', false)");
		$new->stop;
		my $command = upgrade_command($new);
		@$command = grep { $_ ne '--initdb' } @$command;
		command_checks_all($command, 1, [$error], [],
			'reject the target slot conflict or insufficient capacity');
		is( $relay->safe_psql(
				'postgres', 'SELECT pg_get_wal_replay_pause_state()'),
			'not paused',
			'target-slot failure does not initiate HANDOFF');
		ok(-f $new->data_dir . "/pg_replslot/$slot/state",
			'preflight failure preserves the existing target slot state');
	};
}

# These are persistent settings, not just pg_upgrade's startup overrides.
for my $case (
	[
		'max_replication_slots', 0,
		qr/"max_replication_slots" must be at least 1/
	],
	[ 'max_slot_wal_keep_size', 0, qr/"max_slot_wal_keep_size" must be -1/ ],
	[
		'idle_replication_slot_timeout', 1,
		qr/"idle_replication_slot_timeout" must be 0/
	],
	[ 'fsync', 'off', qr/"fsync" must be on/ ],
	[
		'wal_level', 'minimal',
		qr/"wal_level" must be replica or logical/, ' -c max_wal_senders=0'
	])
{
	my ($setting, $value, $message, $extra) = @$case;
	$extra //= '';
	my $new = PostgreSQL::Test::Cluster->new("bad_$setting");
	command_checks_all(
		upgrade_command(
			$new, '--check', "--initdb-options=-c $setting=$value$extra"),
		1,
		[$message],
		[],
		"reject unsafe $setting before HANDOFF");
	ok( !-f $primary->data_dir . '/pg_upgrade_handoff.pending',
		'configuration failure does not arm HANDOFF');
}

my $new_primary = PostgreSQL::Test::Cluster->new('retention_new_primary');
command_ok(upgrade_command($new_primary),
	'primary preparation recreates its physical slot');
$primary->_update_pid(0);
for my $node ($relay, $leaf)
{
	ok( $node->poll_query_until(
			'postgres', q{SELECT pg_get_wal_replay_pause_state() = 'paused'}),
		$node->name . ' reaches HANDOFF');
}
$leaf->stop;
$relay->stop;
$new_primary->append_conf('postgresql.conf', $replication_conf);
$new_primary->append_conf('postgresql.conf', 'port = ' . $new_primary->port);
$new_primary->append_conf('postgresql.conf',
	"unix_socket_directories = '" . $new_primary->host . "'");
$new_primary->append_conf('pg_hba.conf', $replication_hba);
$new_primary->start;
my $replay_start = $new_primary->safe_psql(
	'postgres', q{
	SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = 'relay_slot'
});
like($replay_start, qr{^[0-9A-F]+/[0-9A-F]+$},
	'primary slot reserves a valid replay-start LSN');

my @new_nodes = ($new_primary);
my @old_nodes = ($primary, $relay, $leaf);
for my $index (1, 2)
{
	my $parent = $new_nodes[-1];
	my $new = PostgreSQL::Test::Cluster->new("retention_new_$index");
	$new->init(allows_streaming => 1);
	$new->append_conf('postgresql.conf', $replication_conf);
	$new->append_conf('pg_hba.conf', $replication_hba);
	$new->append_conf('postgresql.auto.conf',
			"primary_conninfo = 'host=127.0.0.1 port="
		  . $parent->port
		  . " user=upgrade_repl application_name="
		  . $new->name . "'\n"
		  . "primary_slot_name = ''\nwal_receiver_create_temp_slot = off\n"
		  . "pg_upgrade_standby_transfer_mode = mirror\n"
		  . "pg_upgrade_standby_old_datadir = '"
		  . $old_nodes[$index]->data_dir
		  . "'\n");
	$new->append_conf('pg_upgrade.signal', '');
	$new->set_standby_mode;

	if ($index == 1)
	{
		# Corrupt only the checksum, then restore the retained slot exactly.
		my $state = $relay->data_dir . '/pg_replslot/leaf_slot/state';
		copy($state, "$state.saved") or die "save slot state: $!";
		open my $file, '+<:raw', $state or die "open slot state: $!";
		seek($file, 4, 0) or die "seek slot checksum: $!";
		read($file, my $byte, 1) == 1 or die "read slot checksum: $!";
		seek($file, 4, 0) or die "seek slot checksum: $!";
		print {$file} chr(ord($byte) ^ 1) or die "write slot checksum: $!";
		close $file or die "close slot state: $!";
		ok(!$new->start(fail_ok => 1),
			'invalid retained slot state prevents relay startup');
		like(
			slurp_file($new->logfile),
			qr/old replication slot file .* has an invalid checksum/,
			'corrupt slot state is diagnosed before upgrade replay');
		copy("$state.saved", $state) or die "restore slot state: $!";
		unlink "$state.saved" or die "remove saved slot state: $!";
	}
	$new->start;
	is( $new->safe_psql('postgres', 'TABLE retention_data'),
		'42',
		$new->name
		  . ' reconstructs source data without consuming a migrated slot');
	push @new_nodes, $new;
}

# Exceed max_wal_size so the initial segment needs a slot reservation even
# after checkpoint cleanup. Sequence WAL uses a built-in resource manager.
for (1 .. 8)
{
	$new_primary->safe_psql('postgres', 'SELECT pg_switch_wal()');
}
my $target_lsn = $new_primary->lsn('insert');
$new_primary->wait_for_catchup($new_nodes[1], 'replay', $target_lsn);
$new_nodes[1]->wait_for_catchup($new_nodes[2], 'replay', $target_lsn);

my $filename = $new_primary->safe_psql('postgres',
	"SELECT pg_walfile_name('$replay_start'::pg_lsn)");
for my $edge ([ $new_nodes[0], 'relay_slot' ], [ $new_nodes[1], 'leaf_slot' ])
{
	my ($parent, $slot) = @$edge;
	my $check = qq{
		SELECT NOT active AND restart_lsn = '$replay_start'::pg_lsn
		   AND wal_status IN ('reserved', 'extended')
		FROM pg_replication_slots WHERE slot_name = '$slot'
	};
	is($parent->safe_psql('postgres', $check),
		't', "$slot remains inactive at replay start");
	$parent->safe_psql('postgres', 'CHECKPOINT');
	$parent->restart;
	is($parent->safe_psql('postgres', $check),
		't', "$slot retains its reservation across checkpoint and restart");
	ok( -f $parent->data_dir . "/pg_wal/$filename",
		"$slot keeps the replay-start WAL available");
}

# Activate migrated slots from the leaf upward, after all nodes have replayed
# the upgrade using connections that do not consume those reservations.
$new_nodes[2]
  ->append_conf('postgresql.auto.conf', "primary_slot_name = 'leaf_slot'");
$new_nodes[2]->restart;
ok(wait_for_physical_slot($new_nodes[1], 'leaf_slot', $new_nodes[2]->name),
	'leaf activates the relay slot after upgrade replay');
$new_nodes[1]
  ->append_conf('postgresql.auto.conf', "primary_slot_name = 'relay_slot'");
$new_nodes[1]->restart;
ok( wait_for_physical_slot($new_primary, 'relay_slot', $new_nodes[1]->name),
	'relay activates the primary slot after its leaf is ready');
$new_primary->safe_psql(
	'postgres',
	q{
  SELECT setval('retention_sequence', 77);
  UPDATE retention_data SET value = 84;
  CREATE TABLE post_upgrade_heap (id int primary key, payload text);
  INSERT INTO post_upgrade_heap VALUES (1, 'after cascade upgrade');
});
$target_lsn = $new_primary->lsn('insert');
$new_primary->wait_for_catchup($new_nodes[1], 'replay', $target_lsn);
$new_nodes[1]->wait_for_catchup($new_nodes[2], 'replay', $target_lsn);
is( $new_nodes[2]
	  ->safe_psql('postgres', 'SELECT last_value FROM retention_sequence'),
	'77',
	'later WAL reaches the upgraded leaf through the cascade');

is($new_nodes[2]->safe_psql('postgres', 'TABLE retention_data'),
	'84', 'ordinary heap updates reach the upgraded leaf');
is( $new_nodes[2]
	  ->safe_psql('postgres', 'SELECT payload FROM post_upgrade_heap'),
	'after cascade upgrade',
	'new heap and index WAL reach the upgraded leaf');

$_->stop for reverse @new_nodes;
done_testing();
