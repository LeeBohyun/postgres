# Copyright (c) 2022-2025, PostgreSQL Global Development Group

# Test the --initdb option of pg_upgrade: pg_upgrade creates the new cluster
# itself via initdb, instead of requiring the user to have run initdb first.

use strict;
use warnings FATAL => 'all';

use File::Path qw(rmtree);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Initialize and populate the old cluster.
my $oldnode = PostgreSQL::Test::Cluster->new('old_node');
$oldnode->init;
$oldnode->start;
$oldnode->safe_psql('postgres',
	    "CREATE TABLE t (id int primary key, note text); "
	  . "INSERT INTO t SELECT g, 'row ' || g FROM generate_series(1, 100) g; "
	  . "CREATE DATABASE extra_db;");
my $rows_before =
  $oldnode->safe_psql('postgres', 'SELECT count(*) FROM t');
is($rows_before, '100', 'old cluster has expected rows before upgrade');
$oldnode->stop;

# Create the new node object but do NOT init() it: pg_upgrade --initdb is
# responsible for creating the data directory.  Only new() runs, which
# allocates the port/host/basedir the framework needs.
my $newnode = PostgreSQL::Test::Cluster->new('new_node');

my $oldbindir = $oldnode->config_data('--bindir');
my $newbindir = $newnode->config_data('--bindir');

# Sanity: the new data directory must not exist yet.
ok(!-d $newnode->data_dir,
	'new cluster data directory does not exist before --initdb');

# Run pg_upgrade with --initdb.  We must run in a writable directory because
# pg_upgrade writes output files relative to the current directory.
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

# The new data directory should now exist and be a v18+ cluster.
ok(-f $newnode->data_dir . '/PG_VERSION',
	'new cluster data directory created by --initdb');

# The framework's init() would normally write port/socket settings into
# postgresql.conf; since we skipped it, append them now so we can start the
# upgraded cluster through the test harness.
my $conf = $newnode->data_dir . '/postgresql.conf';
open(my $fh, '>>', $conf) or die "could not open $conf: $!";
print $fh "\n# added by test to start the --initdb-created cluster\n";
print $fh "port = " . $newnode->port . "\n";
print $fh "listen_addresses = ''\n";
print $fh "unix_socket_directories = '" . $newnode->host . "'\n";
close($fh);

$newnode->start;

# Verify the user data survived the upgrade.
my $rows_after = $newnode->safe_psql('postgres', 'SELECT count(*) FROM t');
is($rows_after, '100', 'user data survived --initdb upgrade');

# Verify the extra database carried over too.
my $has_extra = $newnode->safe_psql('postgres',
	"SELECT count(*) FROM pg_database WHERE datname = 'extra_db'");
is($has_extra, '1', 'user database carried over by --initdb upgrade');

# Verify the new cluster is a newer major version than the old one.
my $newver = $newnode->safe_psql('postgres',
	"SELECT current_setting('server_version_num')::int / 10000");
ok($newver >= 18, "new cluster reports target major version ($newver)");

$newnode->stop;

# --initdb must refuse to clobber an already-populated data directory.
command_fails(
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
	'--initdb refuses to overwrite an existing cluster');

done_testing();
