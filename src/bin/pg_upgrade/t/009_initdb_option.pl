# Copyright (c) 2026, PostgreSQL Global Development Group

# Exercise --initdb without a target data directory.  The success path verifies
# that pg_upgrade creates a compatible cluster with the old data and settings.
# Error paths cover an existing cluster, a missing initdb executable, and
# --check.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Choose nondefault settings to verify that --initdb derives the target
# configuration from old control data and template0 metadata.
my $oldnode = PostgreSQL::Test::Cluster->new('old_node');
$oldnode->init(
	extra => [
		'--no-data-checksums',
		'--wal-segsize' => '2',
		'--locale' => 'C',
	]);
$oldnode->start;
$oldnode->safe_psql('postgres',
		"CREATE TABLE t (id int primary key, note text); "
	  . "INSERT INTO t SELECT g, 'row ' || g FROM generate_series(1, 100) g; "
	  . "CREATE DATABASE extra_db;");
my $rows_before = $oldnode->safe_psql('postgres', 'SELECT count(*) FROM t');
is($rows_before, '100', 'old cluster has expected rows before upgrade');

# Capture the old settings for the post-upgrade comparison.
my $old_checksums = $oldnode->safe_psql('postgres', 'SHOW data_checksums');
my $old_wal_segsize =
  $oldnode->safe_psql('postgres', 'SHOW wal_segment_size');
my $old_encoding = $oldnode->safe_psql('postgres',
	"SELECT pg_encoding_to_char(encoding) FROM pg_database WHERE datname = 'template0'"
);
my $old_collate = $oldnode->safe_psql('postgres',
	"SELECT datcollate FROM pg_database WHERE datname = 'template0'");
my $old_ctype = $oldnode->safe_psql('postgres',
	"SELECT datctype FROM pg_database WHERE datname = 'template0'");
my $old_provider = $oldnode->safe_psql('postgres',
	"SELECT datlocprovider FROM pg_database WHERE datname = 'template0'");
$oldnode->stop;

# Allocate the new node's test-harness state without creating its data
# directory.
my $newnode = PostgreSQL::Test::Cluster->new('new_node');

my $oldbindir = $oldnode->config_data('--bindir');
my $newbindir = $newnode->config_data('--bindir');

ok(!-d $newnode->data_dir,
	'new cluster data directory does not exist before --initdb');

# pg_upgrade writes its output files in the current directory.
chdir ${PostgreSQL::Test::Utils::tmp_check};

command_ok(
	[
		'pg_upgrade', '--no-sync',
		'--old-datadir' => $oldnode->data_dir,
		'--new-datadir' => $newnode->data_dir,
		'--old-bindir' => $oldbindir,
		'--new-bindir' => $newbindir,
		'--socketdir' => $newnode->host,
		'--old-port' => $oldnode->port,
		'--new-port' => $newnode->port,
		'--initdb',
	],
	'run of pg_upgrade --initdb creates and upgrades the new cluster');

# --initdb must create the target data directory.
ok(-f $newnode->data_dir . '/PG_VERSION',
	'new cluster data directory created by --initdb');

# Supply the connection settings normally written by the skipped init() call.
# Match the harness's TCP and Unix-socket behavior so the cluster can start on
# every supported platform.
my $host = $newnode->host;
$newnode->append_conf('postgresql.conf', "port = " . $newnode->port);
if ($PostgreSQL::Test::Cluster::use_tcp)
{
	$newnode->append_conf('postgresql.conf', "unix_socket_directories = ''");
	$newnode->append_conf('postgresql.conf', "listen_addresses = '$host'");
}
else
{
	$newnode->append_conf('postgresql.conf',
		"unix_socket_directories = '$host'");
	$newnode->append_conf('postgresql.conf', "listen_addresses = ''");
}

$newnode->start;

# Verify that the generated cluster contains the upgraded data.
my $rows_after = $newnode->safe_psql('postgres', 'SELECT count(*) FROM t');
is($rows_after, '100', 'user data survived --initdb upgrade');

my $has_extra = $newnode->safe_psql('postgres',
	"SELECT count(*) FROM pg_database WHERE datname = 'extra_db'");
is($has_extra, '1', 'user database carried over by --initdb upgrade');

my $newver = $newnode->safe_psql('postgres',
	"SELECT current_setting('server_version_num')::int / 10000");
ok($newver >= 18, "new cluster reports target major version ($newver)");

# The target must match the old checksum, WAL segment, encoding, and locale
# settings.
my $new_checksums = $newnode->safe_psql('postgres', 'SHOW data_checksums');
is($new_checksums, $old_checksums,
	"data_checksums propagated by --initdb ($new_checksums)");

my $new_wal_segsize =
  $newnode->safe_psql('postgres', 'SHOW wal_segment_size');
is($new_wal_segsize, $old_wal_segsize,
	"wal_segment_size propagated by --initdb ($new_wal_segsize)");

my $new_encoding = $newnode->safe_psql('postgres',
	"SELECT pg_encoding_to_char(encoding) FROM pg_database WHERE datname = 'template0'"
);
is($new_encoding, $old_encoding,
	"template0 encoding propagated by --initdb ($new_encoding)");

my $new_collate = $newnode->safe_psql('postgres',
	"SELECT datcollate FROM pg_database WHERE datname = 'template0'");
is($new_collate, $old_collate,
	"template0 collation propagated by --initdb ($new_collate)");

my $new_ctype = $newnode->safe_psql('postgres',
	"SELECT datctype FROM pg_database WHERE datname = 'template0'");
is($new_ctype, $old_ctype,
	"template0 ctype propagated by --initdb ($new_ctype)");

my $new_provider = $newnode->safe_psql('postgres',
	"SELECT datlocprovider FROM pg_database WHERE datname = 'template0'");
is($new_provider, $old_provider,
	"template0 locale provider propagated by --initdb ($new_provider)");

$newnode->stop;

# An existing cluster must be rejected by pg_upgrade before initdb can overwrite
# it.  The fatal message is written to stdout.
command_checks_all(
	[
		'pg_upgrade', '--no-sync',
		'--old-datadir' => $oldnode->data_dir,
		'--new-datadir' => $newnode->data_dir,
		'--old-bindir' => $oldbindir,
		'--new-bindir' => $newbindir,
		'--socketdir' => $newnode->host,
		'--old-port' => $oldnode->port,
		'--new-port' => $newnode->port,
		'--initdb',
	],
	1,
	[qr/already contains a database system/],
	[qr/^$/],
	'--initdb refuses to overwrite an existing cluster (PG_VERSION check)');

# An empty new bindir and a nonexistent target directory isolate the missing
# initdb validation.
my $empty_bindir = PostgreSQL::Test::Utils::tempdir;
command_checks_all(
	[
		'pg_upgrade', '--no-sync',
		'--old-datadir' => $oldnode->data_dir,
		'--new-datadir' => $newnode->data_dir . '_nonexistent',
		'--old-bindir' => $oldbindir,
		'--new-bindir' => $empty_bindir,
		'--socketdir' => $newnode->host,
		'--old-port' => $oldnode->port,
		'--new-port' => $newnode->port,
		'--initdb',
	],
	1,
	[qr/could not find "initdb"/],
	[qr/^$/],
	'--initdb fails early when initdb is missing from the new bindir');

# --check is read-only, so it must reject --initdb during option parsing.
command_checks_all(
	[
		'pg_upgrade', '--no-sync',
		'--old-datadir' => $oldnode->data_dir,
		'--new-datadir' => $newnode->data_dir . '_nonexistent',
		'--old-bindir' => $oldbindir,
		'--new-bindir' => $newbindir,
		'--socketdir' => $newnode->host,
		'--old-port' => $oldnode->port,
		'--new-port' => $newnode->port,
		'--initdb',
		'--check',
	],
	1,
	[qr/options -c\/--check and --initdb cannot be used together/],
	[qr/^$/],
	'--initdb and --check cannot be used together');

done_testing();
