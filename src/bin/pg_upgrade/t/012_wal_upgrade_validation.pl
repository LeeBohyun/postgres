# Copyright (c) 2026, PostgreSQL Global Development Group

# Check target settings for migrated slots, HANDOFF failure cleanup, RELINK
# operations, native replay, and transfer failures with one installation.
use strict;
use warnings FATAL => 'all';

use File::Copy qw(copy move);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

chdir ${PostgreSQL::Test::Utils::tmp_check};

sub upgrade_command
{
	my ($old, $new, $run_initdb) = @_;
	$run_initdb //= 1;
	my @command = (
		'pg_upgrade', '--no-sync', '--wal-upgrade',
		'--copy',
		'--old-datadir' => $old->data_dir,
		'--new-datadir' => $new->data_dir,
		'--old-bindir' => $old->config_data('--bindir'),
		'--new-bindir' => $new->config_data('--bindir'),
		'--socketdir' => $new->host,
		'--old-port' => $old->port,
		'--new-port' => $new->port,);
	splice @command, 2, 0, '--initdb' if $run_initdb;
	return \@command;
}

sub read_control
{
	my ($node) = @_;
	my ($out, $err);
	$node->run_log(
		[ $node->installed_command('pg_controldata'), $node->data_dir ],
		'>', \$out, '2>', \$err)
	  or die "pg_controldata failed: $err";
	return $out;
}

sub rejects_unsafe_slot_retention
{
	my ($old, $setting, $config_value, $new_options_value, $name) = @_;
	my $new = PostgreSQL::Test::Cluster->new($name);
	$new->init(allows_streaming => 1);
	$new->append_conf('postgresql.auto.conf', "$setting = '$config_value'")
	  if defined $config_value;
	my $command = upgrade_command($old, $new, 0);
	push @$command, '--new-options', "-c $setting=$new_options_value"
	  if defined $new_options_value;

	my ($out, $err);
	ok( !$new->run_log($command, '>', \$out, '2>', \$err),
		"$setting rejects unsafe migrated-slot retention");
	like(
		$out . $err,
		qr/target setting "\Q$setting\E" must be /,
		"$setting reports its required safe value");
}

sub configure_target
{
	my ($node) = @_;
	# Add the connection settings omitted by pg_upgrade's initdb invocation.
	my $host = $node->host;
	$node->append_conf('postgresql.conf', 'port = ' . $node->port);
	$node->append_conf('postgresql.conf',
		$PostgreSQL::Test::Cluster::use_tcp
		? "unix_socket_directories = ''\nlisten_addresses = '$host'"
		: "unix_socket_directories = '$host'\nlisten_addresses = ''");
}

sub read_wal
{
	my ($node) = @_;
	my $waldir = $node->data_dir . '/pg_wal';
	opendir(my $dh, $waldir) or die "opendir $waldir: $!";
	my @segments = sort grep { /^[0-9A-F]{24}$/ } readdir $dh;
	closedir $dh;
	die "no WAL segments in $waldir" unless @segments;
	my ($out, $err);
	# Accept pg_waldump output that ends at a partial final WAL page.
	$node->run_log(
		[
			$node->installed_command('pg_waldump'),
			'-p', $waldir, $segments[0], $segments[-1],
		],
		'>',
		\$out,
		'2>',
		\$err);
	die "pg_waldump did not read any records: $err" unless $out =~ /^rmgr:/m;
	append_to_file($node->basedir . '/upgrade_wal.txt', $out);
	return $out;
}

sub stage_window
{
	my ($source, $name) = @_;
	my $node = PostgreSQL::Test::Cluster->new($name);
	$node->init(allows_streaming => 1);
	my $destination = $node->data_dir . '/pg_wal';
	opendir(my $dest, $destination) or die "opendir $destination: $!";
	for my $file (grep { /^[0-9A-F]{24}$/ } readdir $dest)
	{
		unlink("$destination/$file") or die "remove skeleton WAL $file: $!";
	}
	closedir $dest;
	my $waldir = $source->data_dir . '/pg_wal';
	opendir(my $wal, $waldir) or die "opendir $waldir: $!";
	for my $file (grep { /^[0-9A-F]{24}$/ } readdir $wal)
	{
		copy("$waldir/$file", "$destination/$file")
		  or die "stage WAL $file: $!";
	}
	closedir $wal;
	$node->append_conf('postgresql.conf',
		"restore_command = 'false'\nmax_connections = 100\nmax_locks_per_transaction = 128\n"
	);
	$node->append_conf('pg_upgrade.signal', '');
	$node->append_conf('recovery.signal', '');
	return $node;
}

my $retention_old = PostgreSQL::Test::Cluster->new('retention_old');
$retention_old->init(allows_streaming => 1);
$retention_old->start;
$retention_old->safe_psql('postgres',
	"SELECT pg_create_physical_replication_slot('retention_slot', true)");
$retention_old->stop;
rejects_unsafe_slot_retention($retention_old, 'max_slot_wal_keep_size',
	'64MB', undef, 'finite_slot_retention');
rejects_unsafe_slot_retention($retention_old, 'idle_replication_slot_timeout',
	'1min', undef, 'idle_slot_timeout');
rejects_unsafe_slot_retention($retention_old, 'max_slot_wal_keep_size',
	'64MB', '-1', 'new_options_cannot_mask_persistent_retention');

$retention_old->start;
ok( !-e $retention_old->data_dir . '/pg_upgrade_handoff.pending'
	  && $retention_old->safe_psql('postgres', 'SELECT 1') eq '1',
	'retention prechecks leave HANDOFF unarmed and the source usable');
$retention_old->backup('retention_standby_base');
my $retention_standby = PostgreSQL::Test::Cluster->new('retention_standby');
$retention_standby->init_from_backup($retention_old,
	'retention_standby_base', has_streaming => 1);
$retention_standby->append_conf('postgresql.auto.conf',
	"primary_slot_name = 'retention_slot'");
$retention_standby->start;
$retention_old->wait_for_replay_catchup($retention_standby);
$retention_old->stop;

my $minimal_wal = PostgreSQL::Test::Cluster->new('minimal_wal');
$minimal_wal->init(allows_streaming => 1);
$minimal_wal->append_conf('postgresql.auto.conf',
	"wal_level = minimal\nmax_wal_senders = 0");
my ($minimal_wal_out, $minimal_wal_err);
ok( !$minimal_wal->run_log(
		upgrade_command($retention_old, $minimal_wal, 0), '>',
		\$minimal_wal_out, '2>',
		\$minimal_wal_err),
	'physical-slot migration rejects target wal_level=minimal');
like(
	$minimal_wal_out . $minimal_wal_err,
	qr/"wal_level" must be "replica" or "logical"/,
	'physical-slot migration reports the target wal_level requirement');
$retention_standby->stop;

my $capacity_old = PostgreSQL::Test::Cluster->new('capacity_old');
my $capacity_new = PostgreSQL::Test::Cluster->new('capacity_new');
$capacity_old->init(allows_streaming => 1);
$capacity_old->append_conf('postgresql.conf', 'wal_level = logical');
$capacity_old->start;
$capacity_old->safe_psql('postgres',
	"SELECT pg_create_logical_replication_slot('capacity_logical', 'test_decoding')"
);
$capacity_old->stop;
$capacity_new->init(allows_streaming => 1);
$capacity_new->append_conf('postgresql.conf', 'max_replication_slots = 1');
$capacity_new->append_conf('postgresql.conf',
	"output_plugin_libraries = 'test_decoding'");
$capacity_new->start;
$capacity_new->safe_psql('postgres',
	"SELECT pg_create_physical_replication_slot('capacity_existing')");
$capacity_new->stop;
my ($capacity_out, $capacity_err);
ok( !$capacity_new->run_log(
		upgrade_command($capacity_old, $capacity_new, 0),
		'>', \$capacity_out, '2>', \$capacity_err),
	'combined migrated and existing slots cannot exceed target capacity');
like(
	$capacity_out . $capacity_err,
	qr/target setting "max_replication_slots" must be at least 2 during normal startup/,
	'capacity failure reports the persistent slot requirement');

SKIP:
{
	skip 'HANDOFF failure injection requires a cassert build', 6
	  unless check_pg_config(qr/^#define USE_ASSERT_CHECKING 1/);
	my $guard_old = PostgreSQL::Test::Cluster->new('guard_old');
	my $guard_failed_new = PostgreSQL::Test::Cluster->new('guard_failed_new');
	$guard_old->init(allows_streaming => 1);
	$guard_old->start;
	$guard_old->stop;
	my ($guard_out, $guard_err);
	{
		local $ENV{PG_UPGRADE_TEST_FAIL_HANDOFF_REVALIDATION} = 1;
		ok( !$guard_failed_new->run_log(
				upgrade_command($guard_old, $guard_failed_new),
				'>', \$guard_out, '2>', \$guard_err),
			'post-publication revalidation failure aborts pg_upgrade');
	}
	like(
		$guard_out . $guard_err,
		qr/division by zero/,
		'post-publication query failure reaches connection cleanup');
	ok( !-e $guard_old->data_dir . '/postmaster.pid'
		  && !glob($guard_old->data_dir . '/pg_upgrade_handoff.*'),
		'query failure stops the source and removes the HANDOFF request');
	# Test retry after the source recovers from the injected stop.
	$guard_old->start;
	$guard_old->stop;
	my $guard_retry_new = PostgreSQL::Test::Cluster->new('guard_retry_new');
	command_ok(
		upgrade_command($guard_old, $guard_retry_new),
		'upgrade retries after guarded query failure');

	my $late_slot_old = PostgreSQL::Test::Cluster->new('late_slot_old');
	my $late_slot_new = PostgreSQL::Test::Cluster->new('late_slot_new');
	$late_slot_old->init(allows_streaming => 1);
	$late_slot_old->start;
	$late_slot_old->stop;
	my ($late_slot_out, $late_slot_err);
	{
		local $ENV{PG_UPGRADE_TEST_ADD_PHYSICAL_SLOT_AFTER_DUMP} = 1;
		ok( !$late_slot_new->run_log(
				upgrade_command($late_slot_old, $late_slot_new),
				'>', \$late_slot_out, '2>', \$late_slot_err),
			'pg_upgrade revalidation rejects physical slot creation');
	}
	like(
		$late_slot_out . $late_slot_err,
		qr/physical replication slot "late_physical_slot" was added during the upgrade/,
		'late physical slot creation reports the changed slot set');
}

my $old = PostgreSQL::Test::Cluster->new('old');
my $new = PostgreSQL::Test::Cluster->new('new');
$old->init(allows_streaming => 1);
$old->start;
# Give old and new template0 different OIDs for DELETE and CREATE operations.
$old->safe_psql(
	'postgres', q{
	ALTER DATABASE template0 RENAME TO original_template0;
	CREATE DATABASE template0 WITH TEMPLATE original_template0
	  IS_TEMPLATE true ALLOW_CONNECTIONS false;
	ALTER DATABASE original_template0 IS_TEMPLATE false;
	DROP DATABASE original_template0;
});
$old->safe_psql(
	'postgres', q{
	CREATE TABLE inherited (id int, payload text);
	INSERT INTO inherited
	  SELECT g % 7, md5(g::text) FROM generate_series(1, 20) g;
	CREATE SEQUENCE restored_sequence;
	SELECT setval('restored_sequence', 427, true);
	CREATE TABLE empty_table (id int);
	CREATE UNLOGGED TABLE unlogged_table (id int);
	INSERT INTO unlogged_table VALUES (42);
	CREATE DATABASE extra_db;
});
$old->safe_psql('extra_db',
	'CREATE TABLE inherited_extra (id int); INSERT INTO inherited_extra VALUES (73)'
);

# Create an invalid concurrent index that pg_dump omits.
my ($index_out, $index_err);
my $index_status = $old->psql(
	'postgres',
	'CREATE UNIQUE INDEX CONCURRENTLY omitted_index ON inherited (id)',
	stdout => \$index_out,
	stderr => \$index_err);
die 'fixture did not create an invalid index'
  unless $index_status != 0
  && $old->safe_psql(
	'postgres',
	"SELECT NOT indisvalid FROM pg_index WHERE indexrelid = 'omitted_index'::regclass"
  ) eq 't';

my $fingerprint =
  'SELECT count(*), sum(id), sum(length(payload)) FROM inherited';
my $expected = $old->safe_psql('postgres', $fingerprint);
my $old_template0_oid = $old->safe_psql('postgres',
	"SELECT oid FROM pg_database WHERE datname = 'template0'");
$old->safe_psql('postgres',
	"SELECT pg_create_physical_replication_slot('upgrade_slot', true)");
$old->backup('slot_standby_base');
my $slot_standby = PostgreSQL::Test::Cluster->new('slot_standby');
$slot_standby->init_from_backup($old, 'slot_standby_base',
	has_streaming => 1);
$slot_standby->append_conf('postgresql.auto.conf',
	"primary_slot_name = 'upgrade_slot'");
$slot_standby->start;
$old->wait_for_replay_catchup($slot_standby);
$old->stop;

my ($upgrade_out, $upgrade_err);
{
	local $ENV{PG_UPGRADE_TEST_CHECKPOINT_BEFORE_COMMIT} = 1;
	ok( $new->run_log(
			upgrade_command($old, $new), '>',
			\$upgrade_out, '2>',
			\$upgrade_err),
		'copy upgrade validates declared relations and captures their files');
}
diag($upgrade_out, $upgrade_err) if $upgrade_out !~ /Upgrade Complete/;
like(
	read_control($new),
	qr/wal-upgrade window finalized:\s+yes/,
	'successful upgrade is finalized');
$slot_standby->stop;

my $window = read_wal($new);
SKIP:
{
	skip 'checkpoint injection requires a cassert build', 1
	  unless check_pg_config(qr/^#define USE_ASSERT_CHECKING 1/);
	like(
		$window,
		qr/PG_UPGRADE_COMPLETE\b.*?CHECKPOINT_ONLINE\b.*?\bCOMMIT\b/s,
		'checkpoint WAL separates provisional COMPLETE from transaction COMMIT'
	);
}
my @markers = $window =~ /desc: (PG_UPGRADE_START|PG_UPGRADE_COMPLETE)\b/g;
is_deeply(
	\@markers,
	[ 'PG_UPGRADE_START', 'PG_UPGRADE_COMPLETE' ],
	'one START/COMPLETE upgrade window');
like(
	$window,
	qr/; DIRECTORY DELETE key \d+\/\Q$old_template0_oid\E\/0 /,
	'RELINK declares the retired database directory');

my $upgrade_replay = stage_window($new, 'upgrade_replay');
$upgrade_replay->start;
$upgrade_replay->restart;
is($upgrade_replay->safe_psql('postgres', $fingerprint),
	$expected,
	'native replay reconstructs copied contents and survives restart');
$upgrade_replay->stop;

SKIP:
{
	skip 'disconnect injection requires a cassert build', 3
	  unless check_pg_config(qr/^#define USE_ASSERT_CHECKING 1/);
	my $failure_old = PostgreSQL::Test::Cluster->new('failure_old');
	my $failure_standby = PostgreSQL::Test::Cluster->new('failure_standby');
	$failure_old->init_from_backup($old, 'slot_standby_base');
	$failure_old->start;
	# Recreate the physical slot omitted from the base backup.
	$failure_old->safe_psql('postgres',
		"SELECT pg_create_physical_replication_slot('upgrade_slot', true)");
	$failure_old->backup('failure_standby_base');
	$failure_standby->init_from_backup($failure_old, 'failure_standby_base',
		has_streaming => 1);
	$failure_standby->append_conf('postgresql.auto.conf',
		"primary_slot_name = 'upgrade_slot'");
	$failure_standby->start;
	$failure_old->wait_for_replay_catchup($failure_standby);
	$failure_old->poll_query_until('postgres',
		q{SELECT active FROM pg_replication_slots WHERE slot_name = 'upgrade_slot'}
	  )
	  or die
	  'failure fixture did not stream through the retained physical slot';
	$failure_old->stop;
	my $uncommitted = PostgreSQL::Test::Cluster->new('uncommitted');
	{
		local $ENV{PG_UPGRADE_TEST_DISCONNECT_BEFORE_COMMIT} = 1;
		ok( !$uncommitted->run_log(
				upgrade_command($failure_old, $uncommitted)),
			'emitting connection disconnects after COMPLETE without COMMIT');
	}
	like(
		read_wal($uncommitted),
		qr/desc: PG_UPGRADE_COMPLETE\b/,
		'provisional COMPLETE was flushed before disconnect');
	my $rejected = stage_window($uncommitted, 'uncommitted_replay');
	my $started = $rejected->start(fail_ok => 1);
	ok( !$started
		  && slurp_file($rejected->logfile) =~
		  /pg_upgrade WAL is incomplete: found START without committed COMPLETE/,
		'native recovery refuses the uncommitted upgrade window');
	$rejected->stop if $started;
	$failure_standby->stop;
}

configure_target($new);
$new->start;
my $new_template0_oid = $new->safe_psql('postgres',
	"SELECT oid FROM pg_database WHERE datname = 'template0'");
isnt($new_template0_oid, $old_template0_oid,
	'template0 is recreated under its target identity');
is($new->safe_psql('extra_db', 'SELECT id FROM inherited_extra'),
	'73', 'second database retains its copied contents');
my $unlogged_path = $new->safe_psql('postgres',
	"SELECT pg_relation_filepath('unlogged_table')");
is( $new->safe_psql(
		'postgres', qq{
	SELECT f.*, nextval('restored_sequence'),
	       (SELECT count(*) FROM empty_table),
	       (SELECT id FROM unlogged_table),
	       to_regclass('omitted_index') IS NULL
	  FROM ($fingerprint) f}),
	"$expected|428|0|42|t",
	'upgraded relations preserve representative storage operations');
ok( -f $new->data_dir . '/' . $unlogged_path . '_init',
	'unlogged relation retains its init fork');
$new->safe_psql('postgres', 'CHECKPOINT');
$new->restart;
is($new->safe_psql('postgres', $fingerprint),
	$expected, 'copied contents survive checkpoint and restart');
$new->stop;

my $broken_old = PostgreSQL::Test::Cluster->new('broken_old');
my $broken_new = PostgreSQL::Test::Cluster->new('broken_new');
$broken_old->init;
$broken_old->start;
$broken_old->safe_psql('postgres',
	'CREATE TABLE required_main (id int); INSERT INTO required_main VALUES (1)'
);
my $missing_path = $broken_old->safe_psql('postgres',
	"SELECT pg_relation_filepath('required_main')");
$broken_old->stop;
move(
	$broken_old->data_dir . '/' . $missing_path,
	$broken_old->basedir . '/held_required_main'
) or die "move required source fork: $!";

my ($failure_out, $failure_err);
ok( !$broken_new->run_log(
		upgrade_command($broken_old, $broken_new),
		'>', \$failure_out, '2>', \$failure_err),
	'missing source main fork rejects the upgrade');
like(
	$failure_out,
	qr/error while copying relation "public\.required_main"(?:: could not open file | \()"[^"\n]*\Q$missing_path\E"/,
	'transfer reports the missing source main fork');
my $failed_control = read_control($broken_new);
like(
	$failed_control,
	qr/wal-upgrade window finalized:\s+no/,
	'rejected attempt is not finalized');
my $failed_wal = read_wal($broken_new);
unlike(
	$failed_wal,
	qr/PG_UPGRADE_(?:START|COMPLETE)/,
	'transfer failure emits neither upgrade START nor COMPLETE');

done_testing();
