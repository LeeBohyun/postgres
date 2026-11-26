# Copyright (c) 2026, PostgreSQL Global Development Group

# Replay current first-start upgrade WAL and verify durable completion.

use strict;
use warnings FATAL => 'all';

use File::Copy qw(copy);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

chdir ${PostgreSQL::Test::Utils::tmp_check};

sub upgrade_command
{
	my ($old, $new, $mode) = @_;
	$mode //= 'copy';
	return [
		'pg_upgrade', '--no-sync', '--initdb', '--wal-upgrade',
		'--initdb-options=-c wal_keep_size=1GB -c max_connections=10 -c autovacuum=off',
		"--$mode",
		'--old-datadir' => $old->data_dir,
		'--new-datadir' => $new->data_dir,
		'--old-bindir' => $old->config_data('--bindir'),
		'--new-bindir' => $new->config_data('--bindir'),
		'--socketdir' => $new->host,
		'--old-port' => $old->port,
		'--new-port' => $new->port,
	];
}

sub configure_primary
{
	my ($node) = @_;
	$node->append_conf('postgresql.conf', 'port = ' . $node->port);
	$node->append_conf('postgresql.conf',
		"unix_socket_directories = '" . $node->host . "'");
	$node->append_conf('postgresql.conf', "listen_addresses = '127.0.0.1'");
	$node->append_conf('postgresql.conf', "wal_keep_size = '1GB'");
	$node->append_conf('pg_hba.conf',
		"host replication all 127.0.0.1/32 trust\n");
}

sub make_upgrade_standby
{
	my ($name, $source, $old_datadir) = @_;
	my $node = PostgreSQL::Test::Cluster->new($name);

	$node->init(allows_streaming => 1, extra => ['--no-data-checksums']);
	# pg_upgrade runs the source with the default connection limit.
	$node->append_conf('postgresql.conf', 'max_connections = 100');
	$node->append_conf('postgresql.conf',
		"primary_conninfo = '" . $source->connstr . "'");
	$node->append_conf('postgresql.conf', "primary_slot_name = ''");
	$node->append_conf('postgresql.conf',
		'wal_receiver_create_temp_slot = off');
	$node->append_conf('postgresql.conf',
		"pg_upgrade_standby_old_datadir = '$old_datadir'")
	  if defined $old_datadir;
	$node->append_conf('pg_upgrade.signal', '');
	$node->set_standby_mode;
	return $node;
}

sub control_is_finalized
{
	my ($node) = @_;
	my ($output) = run_command(
		[ $node->installed_command('pg_controldata'), '-D', $node->data_dir ]
	);
	return $output =~ /Backup end location:\s+0\/00000000/;
}

sub read_wal
{
	my ($node) = @_;
	my $waldir = $node->data_dir . '/pg_wal';
	opendir(my $dir, $waldir) or die "opendir $waldir: $!";
	my @segments = sort grep { /^[0-9A-F]{24}$/ } readdir $dir;
	closedir($dir);
	my ($out, $err);
	$node->run_log(
		[
			$node->installed_command('pg_waldump'),
			'-p', $waldir, $segments[0], $segments[-1]
		],
		'>',
		\$out,
		'2>',
		\$err);
	die "could not inspect upgrade WAL: $err"
	  unless $out =~ /PG_UPGRADE_START/;
	return $out;
}

sub prepare_upgrade_with_standby
{
	my ($old, $new, $name, $mode) = @_;
	$mode //= 'copy';
	my $slot = "${name}_slot";
	$old->start;
	$old->safe_psql('postgres',
		"SELECT pg_create_physical_replication_slot('$slot', true)");
	$old->backup($name);
	my $retained = PostgreSQL::Test::Cluster->new($name,
		defined($ENV{oldinstall}) ? (install_path => $ENV{oldinstall}) : ());
	$retained->init_from_backup($old, $name, has_streaming => 1);
	$retained->append_conf('postgresql.auto.conf',
		"primary_slot_name = '$slot'");
	$retained->start;
	$old->wait_for_catchup($retained->name);
	$old->stop;
	command_ok(upgrade_command($old, $new, $mode),
		"prepare $name upgrade with a retained old standby");
	$retained->poll_query_until('postgres',
		q{SELECT pg_get_wal_replay_pause_state() = 'paused'})
	  or die 'retained standby did not reach HANDOFF';
	$retained->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
	$retained->poll_query_until('postgres',
		q{SELECT pg_get_wal_replay_pause_state() = 'not paused'})
	  or die 'retained standby did not cancel HANDOFF';
	$retained->stop;
	# The last link-mode fixture disables the source. Other fixtures reuse it.
	if ($mode ne 'link')
	{
		$old->start;
		$old->safe_psql('postgres',
			"SELECT pg_drop_replication_slot('$slot')");
		$old->stop;
	}
	return $retained;
}

sub make_archive_recovery
{
	my ($name, $source, $retained) = @_;
	$retained->backup_fs_cold($name);
	my $node = PostgreSQL::Test::Cluster->new($name);
	$node->init_from_backup($retained, $name);
	unlink($node->data_dir . '/standby.signal');
	$node->append_conf('postgresql.auto.conf',
		"primary_conninfo = ''\nrestore_command = 'false'\narchive_mode = off"
	);
	$node->append_conf('recovery.signal', '');
	$node->append_conf('pg_upgrade.signal', '');
	my $waldir = $source->data_dir . '/pg_wal';
	opendir(my $dir, $waldir) or die "opendir $waldir: $!";

	for
	  my $file (grep { /^[0-9A-F]{24}$|^[0-9A-F]{8}\.history$/ } readdir $dir)
	{
		copy("$waldir/$file", $node->data_dir . "/pg_wal/$file")
		  or die "stage $file: $!";
	}
	closedir($dir);
	return $node;
}

my @old_options =
  defined($ENV{oldinstall})
  ? (install_path => $ENV{oldinstall})
  : ();
my $old = PostgreSQL::Test::Cluster->new('old', @old_options);
my $new = PostgreSQL::Test::Cluster->new('new');

$old->init(allows_streaming => 1, force_initdb => defined($ENV{oldinstall}));
$old->adjust_conf('postgresql.conf', 'autovacuum_worker_slots', undef)
  if $old->pg_version < 18;
$old->start;
$old->safe_psql(
	'postgres', q{
	CREATE TABLE replay_data (id int primary key, payload text);
	INSERT INTO replay_data
	  SELECT g, repeat(md5(g::text), 8) FROM generate_series(1, 500) g;
	CREATE UNLOGGED TABLE replay_unlogged (id int primary key);
	INSERT INTO replay_unlogged SELECT g FROM generate_series(1, 50) g;
	CREATE SEQUENCE post_upgrade_sequence;
});
my $fingerprint =
  'SELECT count(*), sum(id), sum(length(payload)) FROM replay_data';
my $expected = $old->safe_psql('postgres', $fingerprint);
$old->stop;

my $retained = prepare_upgrade_with_standby($old, $new, 'retained');

configure_primary($new);
$new->start;
my $wal = read_wal($new);
my @upgrade_xids = $wal =~ /^rmgr: PgUpgrade.*?tx:\s*(\d+)/mg;
ok( @upgrade_xids && !grep({ $_ != 0 } @upgrade_xids),
	'native first startup emits upgrade records with XID zero');
unlike(
	$wal,
	qr/PG_UPGRADE_START(?:(?!CHECKPOINT_SHUTDOWN).)*\bCOMMIT\b/s,
	'native upgrade has no transaction COMMIT');
is($new->safe_psql('postgres', $fingerprint),
	$expected, 'upgraded primary contains the expected data');

my $standby = make_upgrade_standby('standby', $new, $retained->data_dir);
$standby->start;
$standby->poll_query_until('postgres',
	'SELECT count(*) = 500 FROM replay_data')
  or die 'standby did not replay the upgrade WAL';
$new->wait_for_catchup($standby, 'replay', $new->lsn('insert'));
is( $standby->safe_psql('postgres', 'SHOW data_checksums'),
	$new->safe_psql('postgres', 'SHOW data_checksums'),
	'replay adopts the primary checksum setting');

is($standby->safe_psql('postgres', $fingerprint),
	$expected, 'standby reconstructs relation data from the upgrade WAL');
ok( control_is_finalized($standby)
	  && !-f $standby->data_dir . '/pg_upgrade.signal',
	'completion restartpoint finalizes upgrade replay');

$new->safe_psql(
	'postgres', q{
  CREATE TABLE post_upgrade_data (id int primary key, payload text);
  INSERT INTO post_upgrade_data VALUES (1, 'after upgrade');
});
$new->wait_for_catchup($standby, 'replay', $new->lsn('insert'));
is( $standby->safe_psql('postgres', 'SELECT payload FROM post_upgrade_data'),
	'after upgrade',
	'standby replays ordinary heap WAL after upgrade');

$standby->restart;
is( $standby->safe_psql(
		'postgres', "SELECT pg_is_in_recovery(), f.* FROM ($fingerprint) f"),
	"t|$expected",
	'finalized replay survives restart');
is( $standby->safe_psql('postgres', 'SELECT payload FROM post_upgrade_data'),
	'after upgrade',
	'post-upgrade heap WAL survives restart');
$standby->promote;
is($standby->safe_psql('postgres', 'SELECT count(*) FROM replay_unlogged'),
	'0', 'promotion resets the unlogged relation');

$standby->stop;
# Also verify later sequence WAL during archive recovery.
$new->safe_psql('postgres', "SELECT setval('post_upgrade_sequence', 77)");
$new->stop;

my $archive = make_archive_recovery('native_archive', $new, $retained);
$archive->start;
# Hot-standby connections can be available before archive recovery finishes.
$archive->poll_query_until('postgres', 'SELECT NOT pg_is_in_recovery()')
  or die 'archive recovery did not finish';
is( $archive->safe_psql(
		'postgres',
		'SELECT pg_is_in_recovery(), (SELECT count(*) FROM replay_data), last_value, (SELECT payload FROM post_upgrade_data) FROM post_upgrade_sequence'
	),
	'f|500|77|after upgrade',
	'archive replay crosses the upgrade and recovers later heap and sequence WAL'
);
$archive->stop;

my $missing_old = make_upgrade_standby('missing_old', $new, undef);
my $started = $missing_old->start(fail_ok => 1);
ok( !$started
	  && slurp_file($missing_old->logfile) =~
	  /streaming pg_upgrade skeleton requires "pg_upgrade_standby_old_datadir"/,
	'streaming replay requires the retained old data directory');

subtest 'native COMPLETE without its checkpoint' => sub {
	plan skip_all => 'requires an assertion-enabled build'
	  unless $new->config_data('--configure') =~ /--enable-cassert/;
	my $partial = PostgreSQL::Test::Cluster->new('partial');
	my $partial_retained =
	  prepare_upgrade_with_standby($old, $partial, 'partial_retained');
	configure_primary($partial);
	{
		local $ENV{PG_UPGRADE_TEST_CRASH_AFTER_COMPLETE} = 1;
		ok(!$partial->start(fail_ok => 1), 'emission stops after COMPLETE');
	}
	like(read_wal($partial), qr/PG_UPGRADE_COMPLETE/,
		'partial stream contains COMPLETE');
	my $incomplete = make_archive_recovery('incomplete_archive', $partial,
		$partial_retained);
	ok( !$incomplete->start(fail_ok => 1),
		'COMPLETE alone cannot finish recovery');
	like(
		slurp_file($incomplete->logfile),
		qr/cannot finish recovery before pg_upgrade is finalized/,
		'recovery still requires the completion restartpoint');
	ok(-f $incomplete->data_dir . '/pg_upgrade.signal',
		'failed replay retains its signal');
	ok( !control_is_finalized($incomplete),
		'failed replay retains its durable marker');
	ok(!$incomplete->start(fail_ok => 1), 'partial replay cannot be resumed');
	like(
		slurp_file($incomplete->logfile),
		qr/pg_upgrade WAL window was only partially applied/,
		'restart rejects the partial target before ordinary recovery');
	done_testing();
};

subtest 'native INHERIT with a local copy override' => sub {
	my $linked = PostgreSQL::Test::Cluster->new('linked');
	my $link_retained =
	  prepare_upgrade_with_standby($old, $linked, 'link_retained', 'link');
	configure_primary($linked);
	$linked->start;
	my $replica =
	  make_upgrade_standby('link_replica', $linked, $link_retained->data_dir);
	$replica->append_conf('postgresql.conf',
		"pg_upgrade_standby_transfer_mode = 'copy'");
	$replica->start;
	is($replica->safe_psql('postgres', $fingerprint),
		$expected,
		'XID-zero reference-mode replay inherits the expected user data');
	$replica->restart;
	is($replica->safe_psql('postgres', $fingerprint),
		$expected, 'reference-mode finalization survives restart');
	$replica->stop;
	$linked->stop;
	done_testing();
};

done_testing();
