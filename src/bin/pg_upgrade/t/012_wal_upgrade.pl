# Copyright (c) 2026, PostgreSQL Global Development Group

# Check upgrade-WAL emission, relink operations, primary finalization, and
# validation of the source relation storage.  Recovery is tested separately.
use strict;
use warnings FATAL => 'all';

use File::Copy qw(move);
use IPC::Run;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

chdir ${PostgreSQL::Test::Utils::tmp_check};

sub upgrade_command
{
	my ($old, $new, $mode, $do_sync) = @_;
	$mode //= 'copy';
	return [
		'pg_upgrade', ($do_sync ? () : '--no-sync'), '--initdb',
		'--wal-upgrade',
		'--initdb-options=-c wal_keep_size=1GB -c wal_level=logical -c io_method=worker -c allow_in_place_tablespaces=on',
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

sub configure_new_cluster
{
	my ($node) = @_;
	my $host = $node->host;
	$node->append_conf('postgresql.conf', 'port = ' . $node->port);
	$node->append_conf('postgresql.conf',
		$PostgreSQL::Test::Cluster::use_tcp
		? "unix_socket_directories = ''\nlisten_addresses = '$host'"
		: "unix_socket_directories = '$host'\nlisten_addresses = ''");
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

sub read_wal
{
	my ($node) = @_;
	my $waldir = $node->data_dir . '/pg_wal';
	opendir(my $dh, $waldir) or die "opendir $waldir: $!";
	my @segments = sort grep { /^[0-9A-F]{24}$/ } readdir $dh;
	closedir $dh;
	die "no WAL segments in $waldir" unless @segments;

	my ($out, $err);
	# pg_waldump can stop at the incomplete final WAL page after printing records.
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
	return $out;
}

my $old = PostgreSQL::Test::Cluster->new('old');
$old->init(allows_streaming => 'logical');
$old->append_conf('postgresql.conf', 'allow_in_place_tablespaces = on');
$old->start;

# Give the old template0 a different identity from the new cluster's template0.
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
	  SELECT g, md5(g::text) FROM generate_series(1, 20) g;
	CREATE SEQUENCE restored_sequence;
	SELECT setval('restored_sequence', 427, true);
	CREATE UNLOGGED TABLE unlogged_table (id int);
	INSERT INTO unlogged_table VALUES (42);
	CREATE DATABASE extra_db;
	CREATE TABLESPACE extra_space LOCATION '';
	CREATE TABLE extra_space_table (id int) TABLESPACE extra_space;
	INSERT INTO extra_space_table VALUES (81);
});
$old->safe_psql('extra_db',
	'CREATE TABLE inherited_extra (id int); INSERT INTO inherited_extra VALUES (73)'
);
$old->safe_psql('postgres', 'VACUUM FREEZE inherited');

my $fingerprint =
  'SELECT count(*), sum(id), sum(length(payload)) FROM inherited';
my $expected = $old->safe_psql('postgres', $fingerprint);
my $database_oid = $old->safe_psql('postgres',
	"SELECT oid FROM pg_database WHERE datname = 'postgres'");
my $inherited_filenumber =
  $old->safe_psql('postgres', "SELECT pg_relation_filenode('inherited')");
my $unlogged_filenumber = $old->safe_psql('postgres',
	"SELECT pg_relation_filenode('unlogged_table')");
my $old_template0_oid = $old->safe_psql('postgres',
	"SELECT oid FROM pg_database WHERE datname = 'template0'");
$old->stop;

# Preparation publishes input without emitting WAL. Each failed first startup
# keeps its input and requires a fresh target for another attempt.
if (check_pg_config('#define USE_ASSERT_CHECKING 1'))
{
	my @cases = (
		[ 'unknown', sub { "UNKNOWN\n" }, qr/must begin with START/ ],
		[
			'overflow',
			sub { $_[0] =~ s/^START \d+/START 18446744073709551616/r },
			qr/invalid unsigned integer/
		],
		[
			'arguments',
			sub { $_[0] =~ s/\n/ extra\n/r },
			qr/too many.*arguments/
		],
		[
			'overlong',
			sub { ('x' x 2048) . "\n" },
			qr/overlong upgrade command/
		],
		[
			'count',
			sub { $_[0] =~ s/^RELINK \d+ \d+/RELINK 3 0/mr },
			qr/invalid RELINK count/, 1
		],
		[
			'entries',
			sub { ($_[0] =~ /^(START[^\n]*\n)/)[0] . "RELINK 3 1\n" },
			qr/end before COMPLETE/, 1
		],
		[
			'truncated',
			sub { ($_[0] =~ /^(START[^\n]*\n)/)[0] },
			qr/end before COMPLETE/, 1
		],
		[
			'operation',
			sub { $_[0] =~ s/^\w+ (\d+) (\d+) 0 \d+$/CREATE $1 $2 0 0/mr },
			qr/invalid RELINK operation/, 1
		],
		[
			'directory',
			sub { $_[0] =~ s/^(\w+) (\d+) (\d+) 0 \d+$/$1 $2 $3 1 0/mr },
			qr/requires a directory first/, 1
		],
		[
			'inplace',
			sub { $_[0] =~ s/^(\w+) (\d+) (\d+) 0 \d+$/$1 $2 $3 0 8/mr },
			qr/INPLACE only on user tablespaces/, 1
		],
		[
			'path',
			sub {
				$_[0] =~
				  s@^COPY_FILE .+$@'COPY_FILE ' . ('a/' x 512) . 'PG_VERSION'@mer;
			},
			qr/requires a relative PG_VERSION/,
			1
		],
		[
			'trailing',
			sub { $_[0] . "COPY_FILE PG_VERSION\n" },
			qr/trailing data after COMPLETE/,
			1
		],);
	for my $case (@cases)
	{
		my ($name, $mutate, $error, $emission_started) = @$case;
		my $new = PostgreSQL::Test::Cluster->new("invalid_$name");
		my $path = $new->data_dir . '/pg_upgrade_relink';
		command_ok(upgrade_command($old, $new), "$name target is prepared");
		my $commands = $mutate->(slurp_file($path));
		open(my $file, '>:raw', $path) or die "open $path: $!";
		print $file $commands or die "write $path: $!";
		close($file) or die "close $path: $!";
		configure_new_cluster($new);
		ok(!$new->start(fail_ok => 1),
			"$name input is rejected at first startup");
		like(slurp_file($new->logfile),
			$error, "$name input reports its error");
		my $wal = read_wal($new);
		unlike($wal, qr/PG_UPGRADE_COMPLETE/,
			"$name input emits no COMPLETE");
		my $backup_end =
		  $emission_started ? '7FFFFFFF/FFFFFFFF' : '0/00000000';
		like(
			read_control($new),
			qr/Backup end location:\s+\Q$backup_end\E/,
			"$name input reserves the target only after emission starts");
		ok(-f $path, "$name input is retained after failure");
	}
}

# The same fixture covers copying and inheriting relation storage.
for my $mode (qw(copy link))
{
	# Give each independent upgrade a fresh, caught-up logical slot.
	$old->start;
	$old->safe_psql('postgres',
		"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'upgrade_slot'"
	);
	$old->safe_psql('postgres',
		"SELECT pg_create_logical_replication_slot('upgrade_slot', 'test_decoding')"
	);
	$old->safe_psql('postgres',
		"SELECT count(*) FROM pg_logical_slot_get_changes('upgrade_slot', NULL, NULL)"
	);
	$old->stop;
	my $new = PostgreSQL::Test::Cluster->new("new_$mode");
	# Exercise preparation sync in copy mode and retain no-sync link coverage.
	command_ok(upgrade_command($old, $new, $mode, $mode eq 'copy'),
		"$mode upgrade prepares first startup");
	ok( -f $new->data_dir . '/pg_upgrade_relink',
		'prepared commands are published');
	my $commands = slurp_file($new->data_dir . '/pg_upgrade_relink');
	my $operation = $mode eq 'link' ? 'INHERIT' : 'RECREATE';
	like(
		$commands,
		qr/^$operation 1663 $database_oid $inherited_filenumber 0$/m,
		"$mode uses the upstream mapping for transferred storage");
	like(
		$commands,
		qr/^COPY_RELFILE 1663 $database_oid $inherited_filenumber 2$/m,
		"$mode captures the visibility map for later heap WAL");
	like(
		$commands,
		qr/^RECREATE 1663 $database_oid $unlogged_filenumber 0$/m,
		'unlogged storage is recreated');
	like(
		$commands,
		qr/^COPY_RELFILE 1663 $database_oid $unlogged_filenumber 3$/m,
		'unlogged storage captures its init fork');
	unlike(
		$commands,
		qr/^COPY_RELFILE 1663 $database_oid $unlogged_filenumber 0$/m,
		'unlogged storage does not capture its main fork');
	unlike(read_wal($new), qr/PG_UPGRADE_START/,
		'preparation emits no upgrade START');
	configure_new_cluster($new);
	$new->start;
	ok( !-e $new->data_dir . '/pg_upgrade_relink',
		'completed startup removes the commands');
	is($new->safe_psql('postgres', 'SHOW io_method'),
		'worker', 'first startup preserves the configured I/O method');
	like(
		read_control($new),
		qr/Backup end location:\s+0\/00000000/,
		'successful native startup clears the upgrade marker');

	my $wal = read_wal($new);
	my @markers = $wal =~ /desc: (PG_UPGRADE_START|PG_UPGRADE_COMPLETE)\b/g;
	is_deeply(
		\@markers,
		[ 'PG_UPGRADE_START', 'PG_UPGRADE_COMPLETE' ],
		'upgrade emits one START and one COMPLETE');
	unlike(
		$wal,
		qr/PG_UPGRADE_START(?:(?!PG_UPGRADE_COMPLETE).)*CHECKPOINT_/s,
		'no background checkpoint interrupts emission');
	like(
		$wal,
		qr/PG_UPGRADE_COMPLETE.*CHECKPOINT_SHUTDOWN/s,
		'controlled completion checkpoint follows COMPLETE');
	my @upgrade_xids = $wal =~ /^rmgr: PgUpgrade.*?tx:\s*(\d+)/mg;
	ok( @upgrade_xids && !grep({ $_ != 0 } @upgrade_xids),
		'native upgrade records allocate no transaction XID');
	like($wal, qr/desc: UPGRADE_RELINK\b/, 'upgrade emits RELINK records');

	my $new_template0_oid = $new->safe_psql('postgres',
		"SELECT oid FROM pg_database WHERE datname = 'template0'");
	isnt($new_template0_oid, $old_template0_oid,
		'template0 is recreated under its new identity');
	is($new->safe_psql('postgres', $fingerprint),
		$expected, 'upgraded primary retains relation contents');
	is($new->safe_psql('extra_db', 'SELECT id FROM inherited_extra'),
		'73', 'upgraded primary retains a second database');
	is($new->safe_psql('postgres', 'SELECT id FROM extra_space_table'),
		'81', 'upgraded primary retains a second tablespace');
	is( $new->safe_psql(
			'postgres',
			"SELECT nextval('restored_sequence'), id FROM unlogged_table"),
		'428|42',
		'upgraded primary retains sequence and unlogged data');
	$new->safe_psql('postgres',
		"SELECT pg_logical_emit_message(true, 'upgrade_slot', 'after upgrade', true)"
	);
	like(
		$new->safe_psql(
			'postgres',
			"SELECT data FROM pg_logical_slot_get_changes('upgrade_slot', NULL, NULL)"
		),
		qr/message: transactional: 1 .*content:after upgrade/,
		'migrated slot decodes after the final WAL reset');
	$new->stop;
	$new->start;
	my @starts = read_wal($new) =~ /PG_UPGRADE_START/g;
	is(scalar @starts, 1, 'ordinary restart does not emit the upgrade again');
	$new->safe_psql('postgres',
		"SELECT pg_logical_emit_message(true, 'upgrade_slot', 'after restart', true)"
	);
	like(
		$new->safe_psql(
			'postgres',
			"SELECT data FROM pg_logical_slot_get_changes('upgrade_slot', NULL, NULL)"
		),
		qr/message: transactional: 1 .*content:after restart/,
		'migrated slot decodes after restart');
	$new->stop;
}

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
) or die "move required old-cluster storage: $!";

my ($failure_out, $failure_err);
ok( !$broken_new->run_log(
		upgrade_command($broken_old, $broken_new),
		'>', \$failure_out, '2>', \$failure_err),
	'missing source relation storage rejects the upgrade');
like(
	$failure_out . $failure_err,
	qr/error while copying relation "public\.required_main"/,
	'missing source storage identifies the relation');
like(
	read_control($broken_new),
	qr/Backup end location:\s+0\/00000000/,
	'rejected preparation leaves the upgrade marker unset');
unlike(
	read_wal($broken_new),
	qr/PG_UPGRADE_(?:START|COMPLETE)/,
	'validation failure emits neither START nor COMPLETE');

my $archive_old = PostgreSQL::Test::Cluster->new('archive_old');
my $archive_new = PostgreSQL::Test::Cluster->new('archive_new');
$archive_old->init(allows_streaming => 1);
$archive_old->append_conf('postgresql.conf',
	"archive_mode = on\narchive_command = 'false'\n");
$archive_old->start;
$archive_old->safe_psql('postgres',
	'CREATE TABLE archived_data (id int); INSERT INTO archived_data VALUES (1)'
);
$archive_old->stop;

command_ok(
	upgrade_command($archive_old, $archive_new),
	'upgrade preserves an active archive command');

my $archive_control = read_control($archive_old);
my ($old_checkpoint_segment) =
  $archive_control =~ /Latest checkpoint's REDO WAL file:\s+([0-9A-F]{24})/;
die 'old cluster has no checkpoint WAL file'
  unless defined $old_checkpoint_segment;

my $auto_conf = slurp_file($archive_new->data_dir . '/postgresql.auto.conf');
like($auto_conf, qr/^archive_mode = 'on'$/m, 'new cluster enables archiving');
like(
	$auto_conf,
	qr/^archive_command = 'false'$/m,
	'new cluster uses the old archive command');
my $staged_wal = $archive_new->data_dir . "/pg_wal/$old_checkpoint_segment";
my $staged_status = $archive_new->data_dir
  . "/pg_wal/archive_status/$old_checkpoint_segment.ready";
ok(-f $staged_wal && -f $staged_status,
	'old checkpoint WAL is staged for the new-cluster archiver');

done_testing();
