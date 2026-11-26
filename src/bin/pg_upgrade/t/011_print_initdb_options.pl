# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that a wrapper can read source initialization settings, initialize and
# configure the target separately, and complete an ordinary upgrade.
# Set oldinstall to exercise a source from an older PostgreSQL installation.

use strict;
use warnings FATAL => 'all';

use Cwd    qw(getcwd);
use Encode qw(encode);
use File::Spec;
use JSON::PP qw(decode_json);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Text::ParseWords qw(shellwords);

# Keep temporary sockets short, and make cleanup observable independently of
# the working directory and any socket-directory settings in the environment.
my $print_tmpdir = PostgreSQL::Test::Utils::tempdir_short();
local $ENV{TMPDIR} = $print_tmpdir;
local $ENV{PGSOCKETDIR};
delete $ENV{PGSOCKETDIR};

my $oldnode =
  PostgreSQL::Test::Cluster->new('old_node',
	install_path => $ENV{oldinstall});
# Use 2 MB WAL segments to verify discovery of a source value that differs
# from initdb's 16 MB default. Keep checksums disabled so the printer must emit
# --no-data-checksums for a target initdb that enables them by default.
# Sources before PG18 already default to disabled checksums.
my @old_initdb_options = ('--wal-segsize=2', '--encoding=UTF8', '--locale=C');
push @old_initdb_options, '--no-data-checksums' if $oldnode->pg_version >= 18;
$oldnode->init(extra => \@old_initdb_options);
$oldnode->start;

# oldinstall can have different ICU support from the target build. Ignore the
# bootstrap unicode entry when checking the fresh source's ICU collations.
my $test_icu =
	 $oldnode->pg_version >= 15
  && $ENV{with_icu} eq 'yes'
  && $oldnode->safe_psql(
	'postgres',
	"SELECT EXISTS (SELECT 1 FROM pg_collation
                    WHERE collprovider = 'i' AND collname <> 'unicode')"
  ) eq 't';

$oldnode->safe_psql('postgres',
	'CREATE TABLE t (id int PRIMARY KEY); INSERT INTO t VALUES (1), (2), (3)'
);

my $newnode = PostgreSQL::Test::Cluster->new('new_node');
my $oldbindir = $oldnode->config_data('--bindir');

sub print_command
{
	my ($node, @extra) = @_;
	return [
		'pg_upgrade', '--print-initdb-options',
		'--old-datadir' => $node->data_dir,
		'--old-bindir' => $oldbindir,
		'--old-port' => $node->port,
		@extra,
	];
}

chdir ${PostgreSQL::Test::Utils::tmp_check};

# Inspect a live source without restarting it or creating a target. Verbose
# diagnostics must go to stderr so stdout contains only one line of options.
my $start_time =
  $oldnode->safe_psql('postgres', 'SELECT pg_postmaster_start_time()');
my ($live_options, $stderr);
ok( run_log(
		print_command(
			$oldnode, '--verbose', '--new-datadir' => $newnode->data_dir),
		'>' => \$live_options,
		'2>' => \$stderr),
	'print initialization options from a running source');
like(
	$stderr,
	qr/Running in verbose mode/,
	'verbose diagnostics are written to stderr');
like($live_options, qr/\A[^\r\n]+\r?\n\z/,
	'stdout contains exactly one line of options');
ok(!-d $newnode->data_dir, 'printing does not create the target directory');
is($oldnode->safe_psql('postgres', 'SELECT pg_postmaster_start_time()'),
	$start_time, 'printing does not restart the running source');

my @options = shellwords($live_options);
is_deeply(
	[ sort @options ],
	[
		sort qw(--wal-segsize=2 --no-data-checksums --encoding=UTF8
		  --locale-provider=libc --lc-collate=C --lc-ctype=C)
	],
	'output contains source settings without target paths or execution flags'
);

# JSON supplies complete arguments for wrappers that invoke initdb directly.
# Choosing the shell format explicitly must preserve the default output.
my ($json_options, $json_stderr);
ok( run_log(
		print_command($oldnode, '--print-initdb-options=json'),
		'>' => \$json_options,
		'2>' => \$json_stderr),
	'print initialization options as JSON');
is_deeply(decode_json($json_options),
	\@options, 'JSON and shell formats describe the same complete arguments');
like($json_options, qr/\A[^\r\n]+\r?\n\z/, 'JSON output occupies one line');
command_checks_all(
	print_command($oldnode, '--print-initdb-options=shell'),
	0, [qr/\A\Q$live_options\E\z/],
	[], 'explicit shell format matches default');

# Inject template0 metadata to test quoting without requiring such a locale.
# Other databases keep usable locale settings for the test connections.
SKIP:
{
	skip 'POSIX shell quoting test', 4 if $windows_os;
	my $locale = q{C's locale $libdir; $(echo ignored)};
	$oldnode->safe_psql('postgres',
			q{UPDATE pg_database SET datcollate = $locale$}
		  . $locale
		  . q{$locale$ WHERE datname = 'template0'});
	my ($quoted_options, $quoted_stderr);
	ok( run_log(
			print_command($oldnode),
			'>' => \$quoted_options,
			'2>' => \$quoted_stderr),
		'print locale text containing shell metacharacters');
	is_deeply(
		[ grep { /^--lc-collate=/ } shellwords($quoted_options) ],
		["--lc-collate=$locale"],
		'quoting preserves spaces, quotes, and literal shell metacharacters');
	my $shell_arguments;
	ok( run_log(
			[
				'sh', '-c', q{eval "set -- $1" || exit; printf '%s\0' "$@"},
				'sh', $quoted_options,
			],
			'>' => \$shell_arguments),
		'shell evaluates the printed initialization options');
	is_deeply(
		[ split /\0/, $shell_arguments ],
		[ map { /^--lc-collate=/ ? "--lc-collate=$locale" : $_ } @options ],
		'shell reuse preserves argument boundaries and literal metacharacters'
	);
	$oldnode->safe_psql('postgres',
		"UPDATE pg_database SET datcollate = 'C' WHERE datname = 'template0'"
	);
}

# JSON must preserve control characters, quotes, backslashes and UTF-8 even
# when PGCLIENTENCODING requests another encoding. Only template0 is changed,
# so the source remains connectable without requiring this invented locale.
my $json_locale = "C's \\\" locale\b\f\n\r\t\x01 \xc3\xa9";
$oldnode->safe_psql('postgres',
		q{UPDATE pg_database SET datcollate = $locale$}
	  . $json_locale
	  . q{$locale$ WHERE datname = 'template0'});
{
	local $ENV{PGCLIENTENCODING} = 'LATIN1';
	my ($output, $errors);
	ok( run_log(
			print_command($oldnode, '--print-initdb-options=json'),
			'>' => \$output,
			'2>' => \$errors),
		'print JSON independently of the requested client encoding');
	is_deeply(
		[
			map  { encode('UTF-8', $_) }
			grep { /^--lc-collate=/ } @{ decode_json($output) }
		],
		["--lc-collate=$json_locale"],
		'JSON preserves exact UTF-8 locale text and every control character');
}
$oldnode->safe_psql('postgres',
	"UPDATE pg_database SET datcollate = 'C' WHERE datname = 'template0'");

# A wrapper's maintenance database may differ from template0. A quoted name
# exercises libpq connection-string escaping. Disable
# template1 connections to ensure even the temporary-server probe uses it.
my $database = q{print 'source"\name; --};
my $sql_database = $database;
$sql_database =~ s/"/""/g;
$oldnode->safe_psql('postgres',
	qq{CREATE DATABASE "$sql_database" TEMPLATE template0 ENCODING 'LATIN1'});
my ($database_options, $database_stderr);
ok( run_log(
		print_command(
			$oldnode, '--print-initdb-options=json',
			'--initdb-database' => $database),
		'>' => \$database_options,
		'2>' => \$database_stderr),
	'print settings from a selected database with a quoted name');
is_deeply(
	decode_json($database_options),
	[ map { $_ eq '--encoding=UTF8' ? '--encoding=LATIN1' : $_ } @options ],
	'only the selected database supplies the printed encoding and locale');

# A database name supplied as source-encoded bytes must still identify that
# database after the printer changes its client encoding to UTF-8 for JSON.
SKIP:
{
	skip 'Unix byte arguments required', 2 if $windows_os;
	my $nonascii_database = "caf\xe9";
	$oldnode->safe_psql($database,
		q{CREATE DATABASE U&"caf\00E9" TEMPLATE template0 ENCODING 'LATIN1'});
	my ($output, $errors);
	ok( run_log(
			print_command(
				$oldnode,
				'--print-initdb-options=json',
				'--initdb-database' => $nonascii_database),
			'>' => \$output,
			'2>' => \$errors),
		'print JSON from a database with a non-ASCII source-encoded name');
	is_deeply(
		decode_json($output),
		decode_json($database_options),
		'JSON encoding conversion preserves selected database identity');
	$oldnode->safe_psql($database, q{DROP DATABASE U&"caf\00E9"});
}

$oldnode->safe_psql('postgres',
	'ALTER DATABASE template1 ALLOW_CONNECTIONS false');
$oldnode->stop;
command_checks_all(
	print_command(
		$oldnode, '--print-initdb-options=json',
		'--initdb-database' => $database),
	0,
	[qr/\A\Q$database_options\E\z/],
	[],
	'selected database also permits inspection with template1 disabled');
ok(!-f $oldnode->data_dir . '/postmaster.pid',
	'selected-database inspection stops its temporary server');
$oldnode->start;
$oldnode->safe_psql('postgres',
	'ALTER DATABASE template1 ALLOW_CONNECTIONS true');
$oldnode->safe_psql('postgres', qq{DROP DATABASE "$sql_database"});

# Inspect an initially stopped source without specifying a target. Printing must
# return the same options as for a live source and stop the server it starts
# temporarily. Successful inspection must remove its diagnostic logs.
$oldnode->stop;
my ($stopped_options, $stopped_stderr);
{
	local $ENV{PGDATANEW};
	delete $ENV{PGDATANEW};
	ok( run_log(
			print_command($oldnode),
			'>' => \$stopped_options,
			'2>' => \$stopped_stderr),
		'print initialization options without a new data directory');
}
is($stopped_options, $live_options,
	'running and stopped sources produce the same options');
ok( !-f $oldnode->data_dir . '/postmaster.pid',
	'printing leaves an initially stopped source stopped');
ok(!-d 'pg_upgrade_output.d', 'successful printing removes diagnostic logs');
is_deeply([ grep { $_ ne '.' && $_ ne '..' } slurp_dir($print_tmpdir) ],
	[], 'successful printing removes its private workspace');

# Neither logs nor the default socket may require a writable CWD. Keep source
# paths absolute here; the following case checks relative path resolution.
SKIP:
{
	skip 'Unix directory permissions required', 2 if $windows_os;
	my $cwd = getcwd();
	my $readonly = $oldnode->basedir . '/readonly';
	mkdir($readonly, 0500) or die "mkdir $readonly: $!";
	chdir $readonly or die "chdir $readonly: $!";
	command_checks_all(print_command($oldnode), 0,
		[qr/\A\Q$live_options\E\z/],
		[],
		'print from a read-only working directory with the default socket');
	chdir $cwd or die "chdir $cwd: $!";
	chmod(0700, $readonly) or die "chmod $readonly: $!";
}
{
	local $ENV{TMPDIR} = File::Spec->abs2rel($print_tmpdir, getcwd());
	command_checks_all(
		print_command(
			$oldnode,
			'--old-bindir' => File::Spec->abs2rel($oldbindir, getcwd()),
			'--old-datadir' =>
			  File::Spec->abs2rel($oldnode->data_dir, getcwd())),
		0,
		[qr/\A\Q$live_options\E\z/],
		[],
		'relative source paths and TMPDIR retain their original meaning');
}

# The default socket path must preserve spaces and apostrophes in TMPDIR.
# libpq treats commas in a host as separators, so such a TMPDIR requires an
# explicit socket directory; both supported ways to select one must work.
SKIP:
{
	skip 'Unix sockets required', 6 if $windows_os;
	my $tmpdir = $print_tmpdir . q{/space 'dir};
	mkdir($tmpdir) or die "mkdir $tmpdir: $!";
	local $ENV{TMPDIR} = $tmpdir;
	command_checks_all(print_command($oldnode), 0,
		[qr/\A\Q$live_options\E\z/],
		[], 'TMPDIR metacharacters preserve the default socket directory');
	rmdir($tmpdir) or die "rmdir $tmpdir: $!";
	$tmpdir .= ',sub';
	mkdir($tmpdir) or die "mkdir $tmpdir: $!";
	$ENV{TMPDIR} = $tmpdir;
	command_checks_all(
		print_command($oldnode, '--socketdir' => $oldnode->host), 0,
		[qr/\A\Q$live_options\E\z/], [],
		'explicit socket directory overrides the temporary directory');
	{
		local $ENV{PGSOCKETDIR} = $oldnode->host;
		command_checks_all(print_command($oldnode), 0,
			[qr/\A\Q$live_options\E\z/],
			[],
			'PGSOCKETDIR overrides the default temporary socket directory');
	}
	rmdir($tmpdir) or die "rmdir $tmpdir: $!";
}

# --retain reports an existing private directory; successful printing without
# it above must not leave one. Errors later in this test retain their logs too.
my ($retained_options, $retained_stderr);
ok( run_log(
		print_command($oldnode, '--retain'),
		'>' => \$retained_options,
		'2>' => \$retained_stderr),
	'print options while retaining diagnostic logs');
is($retained_options, $live_options, '--retain leaves stdout unchanged');
my ($workspace) =
  $retained_stderr =~ /Temporary files are retained in "([^"]+)"/;
ok( defined($workspace) && -d "$workspace/pg_upgrade_output.d",
	'--retain reports the retained diagnostic directory');

# Discovering source settings does not require the target installation.
command_checks_all(
	print_command($oldnode, '--new-bindir' => $newnode->basedir),
	0, [qr/\A\Q$live_options\E\z/],
	[], 'printing does not require target binaries');

# A supplied target path must not be examined, even when it is not a directory.
my $unused_target = $newnode->basedir . '/unused-target';
append_to_file($unused_target, "unchanged\n");
my ($ignored_target_options, $ignored_target_stderr);
ok( run_log(
		print_command($oldnode, '--new-datadir' => $unused_target),
		'>' => \$ignored_target_options,
		'2>' => \$ignored_target_stderr),
	'printing ignores a supplied target path');
is($ignored_target_options, $live_options,
	'target path does not affect source-derived options');
is(slurp_file($unused_target), "unchanged\n", 'supplied target is unchanged');

# Consume the printed options as a wrapper would, retaining control over
# authentication, paths, and configuration before the upgrade.
$newnode->init(extra => \@options);
$newnode->append_conf('postgresql.conf',
	'custom.before_upgrade = configured');
command_ok(
	[
		'pg_upgrade', '--no-sync',
		'--old-datadir' => $oldnode->data_dir,
		'--new-datadir' => $newnode->data_dir,
		'--old-bindir' => $oldbindir,
		'--old-port' => $oldnode->port,
		'--new-port' => $newnode->port,
		'--socketdir' => $newnode->host,
	],
	'ordinary upgrade accepts a target initialized with the printed options');
$newnode->start;
is( $newnode->safe_psql(
		'postgres', 'SELECT array_agg(id ORDER BY id) FROM t'),
	'{1,2,3}',
	'ordinary upgrade preserves the source rows');
is($newnode->safe_psql('postgres', 'SHOW custom.before_upgrade'),
	'configured', 'configuration added before upgrade is preserved');
$newnode->stop;

# Failed source discovery must not produce an argument list or stop a source
# that was already running.
$oldnode->start;
command_checks_all(
	print_command($oldnode, '--username' => 'missing_print_options_role'),
	1,
	[qr/\A\z/],
	[qr/role .*missing_print_options_role.* does not exist/],
	'source connection failure leaves stdout empty');
is($oldnode->safe_psql('postgres', 'SELECT count(*) FROM t'),
	'3', 'source remains available after failed discovery');
$oldnode->safe_psql('postgres', 'CREATE ROLE print_options_reader LOGIN');
$oldnode->safe_psql('template1', 'REVOKE SELECT ON pg_database FROM PUBLIC');
command_checks_all(
	print_command($oldnode, '--username' => 'print_options_reader'),
	1,
	[qr/\A\z/],
	[qr/permission denied for table pg_database/],
	'source query failure leaves stdout empty');
$oldnode->safe_psql('template1', 'GRANT SELECT ON pg_database TO PUBLIC');
$oldnode->safe_psql('postgres', 'DROP ROLE print_options_reader');
$oldnode->stop;

# A failed connection after starting the source must stop that server before
# reporting the retained workspace, while keeping stdout empty for wrappers.
my ($failed_options, $failed_stderr);
ok( !run_log(
		print_command($oldnode, '--initdb-database=missing_print_options_db'),
		'>' => \$failed_options,
		'2>' => \$failed_stderr),
	'fail inspection of a missing selected database');
is($failed_options, '', 'failed database inspection leaves stdout empty');
like(
	$failed_stderr,
	qr/database .*missing_print_options_db.* does not exist/,
	'failure identifies the missing database');
($workspace) = $failed_stderr =~ /Temporary files are retained in "([^"]+)"/;
ok(defined($workspace) && -d "$workspace/pg_upgrade_output.d",
	'failure reports retained diagnostics');
ok( !-f $oldnode->data_dir . '/postmaster.pid',
	'failed inspection stops the server it started');

for my $option ('--initdb', '--check', '--initdb-options=')
{
	command_checks_all(
		[ 'pg_upgrade', '--print-initdb-options', $option ],
		1,
		[qr/\A\z/],
		[
			qr/options --print-initdb-options and .* cannot be used together|--initdb-options requires --initdb/
		],
		"printing rejects $option");
}

for my $format ('', 'invalid')
{
	command_checks_all(
		[ 'pg_upgrade', "--print-initdb-options=$format" ],
		1,
		[qr/\A\z/],
		[qr/invalid argument for option --print-initdb-options/],
		"reject invalid output format '$format'");
}
command_checks_all(
	[ 'pg_upgrade', '--initdb-database=postgres' ], 1,
	[qr/--initdb-database requires --print-initdb-options/], [qr/\A\z/],
	'database selection requires printing');
command_checks_all(
	[ 'pg_upgrade', '--print-initdb-options', '--initdb-database=' ],
	1,
	[qr/\A\z/],
	[qr/empty database name/],
	'reject an empty database selection');

# Check that builtin and, when available, ICU options can initialize a separate
# target with enabled checksums and the source WAL segment size.
my @providers = ();
push @providers, [ 'builtin', '--builtin-locale=C.UTF-8' ]
  if $oldnode->pg_version >= 17;
push @providers, [ 'icu', '--icu-locale=en-US' ]
  if $test_icu;
for my $provider (@providers)
{
	my ($name, $locale) = @$provider;
	my $source =
	  PostgreSQL::Test::Cluster->new("${name}_source",
		install_path => $ENV{oldinstall});
	my $rules = q{&a < '$' < c < b} . " &c < \xc3\xa9";
	my $test_rules = $name eq 'icu' && $source->pg_version >= 16;
	$source->init(
		extra => [
			'--data-checksums', '--wal-segsize=1',
			'--encoding=UTF8', '--locale=C',
			"--locale-provider=$name", $locale,
			($test_rules ? "--icu-rules=$rules" : ()),
		]);
	my ($output, $errors);
	{
		# Non-ASCII rules must retain their database encoding even if the
		# caller's libpq environment requests a different client encoding.
		local $ENV{PGCLIENTENCODING} = 'LATIN1';
		ok( run_log(
				print_command($source),
				'>' => \$output,
				'2>' => \$errors),
			"print $name initialization options independently of PGCLIENTENCODING"
		);
	}
	my @args;
	if ($windows_os)
	{
		# shellwords parses POSIX quoting, not cmd.exe's. Use JSON for argv.
		ok( run_log(
				print_command($source, '--print-initdb-options=json'),
				'>' => \$output,
				'2>' => \$errors),
			"print $name arguments for direct initialization on Windows");
		@args = map { encode('UTF-8', $_) } @{ decode_json($output) };
	}
	else
	{
		@args = shellwords($output);
	}
	ok( grep({ $_ eq '--data-checksums' } @args),
		'output explicitly enables source checksums');
	ok(grep({ $_ eq $locale } @args), "output preserves the $name locale");
	my $target = PostgreSQL::Test::Cluster->new("${name}_target");
	$target->init(extra => \@args);
	$target->start;
	is($target->safe_psql('postgres', 'SHOW wal_segment_size'),
		'1MB', "$name initialization preserves WAL segment size");
	$target->stop;

	next unless $test_rules;
	is_deeply(
		[ grep { /^--icu-rules=/ } @args ],
		["--icu-rules=$rules"],
		'printed arguments preserve single-line ICU rules and literal metacharacters'
	);

	# The shell output format rejects multiline rules. JSON must preserve long
	# multiline rules exactly, and the resulting arguments must initialize a
	# real cluster. Automatic --initdb must keep its existing SQL copy path.
	$rules .= "\r\n\t&d < f" . (' ' x 9000);
	$source->start;
	$source->safe_psql('postgres',
			q{UPDATE pg_database SET daticurules = $rules$}
		  . $rules
		  . q{$rules$ WHERE datname = 'template0'});
	$source->stop;
	command_checks_all(
		print_command($source),
		1,
		[qr/\A\z/],
		[qr/Use --print-initdb-options=json to preserve this value/],
		'shell output rejects multiline rules with a JSON hint');
	my ($rules_output, $rules_stderr);
	ok( run_log(
			print_command($source, '--print-initdb-options=json'),
			'>' => \$rules_output,
			'2>' => \$rules_stderr),
		'print long multiline ICU rules as JSON');
	my @rules_args =
	  map { encode('UTF-8', $_) } @{ decode_json($rules_output) };
	is_deeply(
		[ grep { /^--icu-rules=/ } @rules_args ],
		["--icu-rules=$rules"],
		'JSON preserves newlines, Unicode, metacharacters and long ICU rules'
	);
	my $rules_target = PostgreSQL::Test::Cluster->new('icu_rules_target');
	$rules_target->init(extra => \@rules_args);
	$rules_target->start;
	is( $rules_target->safe_psql(
			'postgres',
			"SELECT daticurules FROM pg_database WHERE datname = 'template0'"
		),
		$rules,
		'initdb accepts the JSON arguments and preserves the exact rules');
	$rules_target->stop;

	my $automatic = PostgreSQL::Test::Cluster->new('icu_rules_automatic');
	command_ok(
		[
			'pg_upgrade', '--no-sync', '--initdb',
			'--old-datadir' => $source->data_dir,
			'--new-datadir' => $automatic->data_dir,
			'--old-bindir' => $oldbindir,
			'--old-port' => $source->port,
			'--new-port' => $automatic->port,
			'--socketdir' => $automatic->host,
		],
		'automatic initdb still upgrades a source with long multiline ICU rules'
	);
	# pg_upgrade invoked initdb directly, so add the test harness's connection
	# settings that Cluster::init() normally writes before starting the node.
	my $listen = $PostgreSQL::Test::Cluster::use_tcp ? $automatic->host : '';
	my $socketdir =
	  $PostgreSQL::Test::Cluster::use_tcp ? '' : $automatic->host;
	$automatic->append_conf('postgresql.conf',
			'port = '
		  . $automatic->port
		  . "\nlisten_addresses = '$listen'\nunix_socket_directories = '$socketdir'"
	);
	$automatic->start;
	is( $automatic->safe_psql(
			'postgres',
			"SELECT daticurules FROM pg_database WHERE datname = 'template0'"
		),
		$rules,
		'automatic initdb preserves canonical template0 rules through SQL');
	$automatic->stop;
}

done_testing();
