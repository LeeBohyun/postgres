# Copyright (c) 2026, PostgreSQL Global Development Group

# Check primary-side upgrade WAL emission and startup. Standby and archive
# recovery are exercised separately.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use FindBin;
use lib $FindBin::RealBin;
use WalUpgradeTest qw(require_wal_upgrade_source);

sub configure_new_cluster
{
	my ($node) = @_;
	my $listen = $PostgreSQL::Test::Cluster::use_tcp ? $node->host : '';
	my $socketdir = $PostgreSQL::Test::Cluster::use_tcp ? '' : $node->host;
	$node->append_conf('postgresql.conf',
		'port = ' . $node->port
		  . "\nlisten_addresses = '$listen'\nunix_socket_directories = '$socketdir'");
}

my $old = PostgreSQL::Test::Cluster->new('old_emission',
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
$old->stop;

my $new = PostgreSQL::Test::Cluster->new('new_emission');
my @command = (
	'pg_upgrade', '--no-sync', '--initdb', '--wal-upgrade', '--copy',
	'--initdb-options=-c wal_keep_size=128MB',
	'--old-datadir' => $old->data_dir,
	'--new-datadir' => $new->data_dir,
	'--old-bindir' => $old->config_data('--bindir'),
	'--new-bindir' => $new->config_data('--bindir'),
	'--socketdir' => $new->host,
	'--old-port' => $old->port,
	'--new-port' => $new->port);

chdir ${PostgreSQL::Test::Utils::tmp_check};
command_ok([ @command, '--check' ],
	'--check completes without emitting an upgrade transaction');
ok(!-f $new->data_dir . '/PG_VERSION',
	'--check removes the initialized new cluster');
command_ok(\@command, 'primary upgrades and emits a validated WAL transaction');
ok(!-f $new->data_dir . '/pg_upgrade_relink',
	'committed upgrade removes the relink input');

my ($control) = run_command([
	$new->installed_command('pg_controldata'), $new->data_dir ]);
like($control, qr/wal-upgrade transaction finalized:\s+yes/,
	'completion checkpoint finalizes the upgraded primary');

my $waldir = $new->data_dir . '/pg_wal';
my @segments = sort grep { /^[0-9A-F]{24}$/ } slurp_dir($waldir);
die 'upgrade produced no WAL segments' unless @segments;
my $wal = '';
# Inspect segments separately because replay-capable builds can also retain
# the old-version checkpoint segment beside the new-version WAL.
for my $segment (@segments)
{
	my ($records) = run_command([
		$new->installed_command('pg_waldump'), '--rmgr=PgUpgrade',
		'--path' => $waldir, $segment, $segment ]);
	$wal .= $records . "\n";
}
for my $record (qw(PG_UPGRADE_START UPGRADE_RELINK UPGRADE_RAWFILE PG_UPGRADE_COMPLETE))
{
	like($wal, qr/\Q$record\E/, "upgrade WAL contains $record");
}
my @starts = $wal =~ /PG_UPGRADE_START/g;
my @completes = $wal =~ /PG_UPGRADE_COMPLETE/g;
is(scalar @starts, 1, 'upgrade emits exactly one START');
is(scalar @completes, 1, 'upgrade emits exactly one COMPLETE');
like($wal, qr/PG_UPGRADE_START.*PG_UPGRADE_COMPLETE/s,
	'COMPLETE follows START');

configure_new_cluster($new);
$new->start;
is($new->safe_psql('postgres', q{
	SELECT count(*), md5(string_agg(id::text || payload, '' ORDER BY id))
	FROM upgrade_data;
}), $fingerprint, 'upgrade preserves heap and toasted values');
is($new->safe_psql('postgres',
	'SET enable_seqscan = off; SELECT count(*) FROM upgrade_data WHERE id BETWEEN 1 AND 100'),
	'100', 'upgraded primary-key index returns the expected rows');
is($new->safe_psql('postgres', 'TABLE upgrade_unlogged'), '42',
	'primary preserves unlogged data');
is($new->safe_psql('postgres', "SELECT nextval('upgrade_sequence')"),
	'1235', 'primary preserves sequence state');
$new->safe_psql('postgres',
	"INSERT INTO upgrade_data VALUES (101, 'after upgrade')");
$new->restart;
is($new->safe_psql('postgres', 'SELECT count(*) FROM upgrade_data'),
	'101', 'upgraded primary accepts writes and survives restart');
$new->stop;

SKIP:
{
	skip 'disconnect injection requires a cassert build', 4
	  unless check_pg_config(qr/^#define USE_ASSERT_CHECKING 1/);
	my $incomplete = PostgreSQL::Test::Cluster->new('incomplete_emission');
	my ($out, $err);
	{
		local $ENV{PG_UPGRADE_TEST_DISCONNECT_BEFORE_COMMIT} = 1;
		ok(!$incomplete->run_log([
			@command, '--new-datadir' => $incomplete->data_dir,
			'--new-port' => $incomplete->port ], '>', \$out, '2>', \$err),
			'emission failure before COMMIT rejects the upgrade');
	}
	like($out . $err, qr/test disconnect after COMPLETE before COMMIT/,
		'failure occurs after COMPLETE without committing the upgrade');
	configure_new_cluster($incomplete);
	my $started = $incomplete->start(fail_ok => 1);
	ok(!$started, 'ordinary startup rejects an incomplete upgrade target');
	like(slurp_file($incomplete->logfile),
		qr/the pg_upgrade transaction was only partially applied/,
		'startup reports the incomplete upgrade');
	$incomplete->stop if $started;
}

# --copy leaves the stopped source usable.
$old->start;
is($old->safe_psql('postgres', 'SELECT count(*) FROM upgrade_data'),
	'100', 'copy-mode source retains its original rows');
$old->stop;

done_testing();
