# Copyright (c) 2026, PostgreSQL Global Development Group

# Printed source settings must initialize a target that passes ordinary --check
# and upgrade, preserving source rows and configuration added by the caller.

use strict;
use warnings FATAL => 'all';

use Cwd qw(getcwd);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Text::ParseWords qw(shellwords);

my $oldnode = PostgreSQL::Test::Cluster->new('print_old_node');
# A nondefault WAL size and disabled checksums must override initdb's defaults.
$oldnode->init(
	extra => [
		'--wal-segsize=2', '--no-data-checksums',
		'--encoding=UTF8', '--locale=C'
	]);
$oldnode->start;
$oldnode->safe_psql('postgres',
	'CREATE TABLE t (id int); INSERT INTO t VALUES (1), (2), (3)');
my $newnode = PostgreSQL::Test::Cluster->new('print_new_node');
my @print_command = (
	'pg_upgrade', '--print-initdb-options',
	'--old-datadir' => $oldnode->data_dir,
	'--old-bindir' => $oldnode->config_data('--bindir'),
	'--old-port' => $oldnode->port,
	'--socketdir' => $oldnode->host,);
my $cwd = getcwd();
chdir $oldnode->basedir;

# A live source must remain running, with only reusable options on stdout.
my $start_time =
  $oldnode->safe_psql('postgres', 'SELECT pg_postmaster_start_time()');
my ($live_options, $stderr);
ok( run_log(
		[ @print_command, '--verbose' ],
		'>' => \$live_options,
		'2>' => \$stderr),
	'print settings from a live source');
like($live_options, qr/\A[^\r\n]+\r?\n\z/, 'stdout contains one line');
like($stderr, qr/Running in verbose mode/, 'diagnostics go to stderr');
ok(!-d $newnode->data_dir, 'printing does not create a target');
is($oldnode->safe_psql('postgres', 'SELECT pg_postmaster_start_time()'),
	$start_time, 'printing does not restart the source');
my @options = shellwords($live_options);
is_deeply(
	[ sort @options ],
	[
		sort qw(--wal-segsize=2 --no-data-checksums --encoding=UTF8
		  --locale-provider=libc --lc-collate=C --lc-ctype=C)
	],
	'output contains the source initialization settings');

# Inject a locale string to check shell quoting without requiring an OS locale.
SKIP:
{
	skip 'POSIX shell quoting test', 2 if $windows_os;
	my $locale = q{C's locale $libdir; $(echo ignored)};
	$oldnode->safe_psql('postgres',
			q{UPDATE pg_database SET datcollate = $locale$}
		  . $locale
		  . q{$locale$ WHERE datname = 'template0'});
	my $arguments;
	ok( run_log(
			[
				'sh',
				'-c',
				q{options=$("$@") || exit; eval "set -- $options"; printf '%s\0' "$@"},
				'sh',
				@print_command,
			],
			'>' => \$arguments),
		'reuse printed options in a shell');
	is_deeply(
		[ split /\0/, $arguments ],
		[ map { /^--lc-collate=/ ? "--lc-collate=$locale" : $_ } @options ],
		'shell reuse preserves argument boundaries and literal locale text');
	$oldnode->safe_psql('postgres',
		"UPDATE pg_database SET datcollate = 'C' WHERE datname = 'template0'"
	);
}

# A stopped source must produce the same settings and be stopped again.
$oldnode->stop;
my $stopped_options;
ok(run_log(\@print_command, '>' => \$stopped_options, '2>' => \$stderr),
	'print settings from a stopped source');
is($stopped_options, $live_options, 'live and stopped settings agree');
ok(!-f $oldnode->data_dir . '/postmaster.pid', 'temporary source is stopped');
ok(!-d 'pg_upgrade_output.d', 'successful printing removes its logs');

# Failed discovery must leave stdout empty and stop its temporary source too.
command_checks_all(
	[ @print_command, '--username' => 'missing_print_options_role' ],
	1,
	[qr/\A\z/],
	[qr/role .*missing_print_options_role.* does not exist/],
	'failed discovery does not print options');
ok( !-f $oldnode->data_dir . '/postmaster.pid',
	'failed discovery stops its temporary source');
command_checks_all(
	[ 'pg_upgrade', '--print-initdb-options', '--check' ],
	1,
	[qr/\A\z/],
	[qr/options --print-initdb-options and --check cannot be used together/],
	'printing rejects --check');

# The caller can initialize and configure the target before a normal upgrade.
$newnode->init(extra => \@options);
$newnode->append_conf('postgresql.conf',
	'custom.before_upgrade = configured');
my @upgrade_command = (
	'pg_upgrade', '--no-sync',
	'--old-datadir' => $oldnode->data_dir,
	'--new-datadir' => $newnode->data_dir,
	'--old-bindir' => $oldnode->config_data('--bindir'),
	'--old-port' => $oldnode->port,
	'--new-port' => $newnode->port,
	'--socketdir' => $newnode->host,);
command_ok([ @upgrade_command, '--check' ], 'check the initialized target');
command_ok(\@upgrade_command, 'upgrade the initialized target');
$newnode->start;
is( $newnode->safe_psql(
		'postgres', 'SELECT array_agg(id ORDER BY id) FROM t'),
	'{1,2,3}',
	'upgrade preserves source rows');
is($newnode->safe_psql('postgres', 'SHOW custom.before_upgrade'),
	'configured', 'upgrade preserves caller configuration');
$newnode->stop;
chdir $cwd;

done_testing();
