# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that --print-initdb-options emits shell-quoted source settings
# that a caller can reuse to initialize and configure an upgrade target.
# The target must pass ordinary --check and upgrade, preserving source
# rows and configuration added before upgrade.
#
# Set oldinstall to inspect and upgrade a source from an older PostgreSQL
# installation.

use strict;
use warnings FATAL => 'all';

use Cwd qw(getcwd);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Text::ParseWords qw(shellwords);

{
	my $oldnode =
	  PostgreSQL::Test::Cluster->new('print_old_node',
		install_path => $ENV{oldinstall});
	# Use 2 MB WAL segments to verify discovery of a source value that differs
	# from initdb's 16 MB default. Keep checksums disabled so the printer must emit
	# --no-data-checksums for a target initdb that enables them by default.
	# Sources before PG18 already default to disabled checksums.
	my @old_initdb_options =
	  ('--wal-segsize=2', '--encoding=UTF8', '--locale=C');
	push @old_initdb_options, '--no-data-checksums'
	  if $oldnode->pg_version >= 18;
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

	my $newnode = PostgreSQL::Test::Cluster->new('print_new_node');
	my $oldbindir = $oldnode->config_data('--bindir');

	sub print_command
	{
		my ($node, @extra) = @_;
		return [
			'pg_upgrade', '--print-initdb-options',
			'--old-datadir' => $node->data_dir,
			'--old-bindir' => $oldbindir,
			'--old-port' => $node->port,
			'--socketdir' => $node->host,
			@extra,
		];
	}

	my $cwd = getcwd();
	chdir $oldnode->basedir;

	# Inspect a live source without restarting it or creating a target. Verbose
	# diagnostics must go to stderr so stdout contains only one line of options.
	my $start_time =
	  $oldnode->safe_psql('postgres', 'SELECT pg_postmaster_start_time()');
	my ($live_options, $stderr);
	ok( run_log(
			print_command(
				$oldnode, '--verbose',
				'--new-datadir' => $newnode->data_dir),
			'>' => \$live_options,
			'2>' => \$stderr),
		'print initialization options from a running source');
	like(
		$stderr,
		qr/Running in verbose mode/,
		'verbose diagnostics are written to stderr');
	like($live_options, qr/\A[^\r\n]+\r?\n\z/,
		'stdout contains exactly one line of options');
	ok(!-d $newnode->data_dir,
		'printing does not create the target directory');
	is( $oldnode->safe_psql('postgres', 'SELECT pg_postmaster_start_time()'),
		$start_time,
		'printing does not restart the running source');

	my @options = shellwords($live_options);
	is_deeply(
		[ sort @options ],
		[
			sort qw(--wal-segsize=2 --no-data-checksums --encoding=UTF8
			  --locale-provider=libc --lc-collate=C --lc-ctype=C)
		],
		'output contains source settings without target paths or execution flags'
	);

	# Inject template0 metadata to test quoting without requiring such a locale.
	# Shell reuse must preserve argument boundaries and literal metacharacters.
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
			'quoting preserves spaces, quotes, and literal shell metacharacters'
		);
		my $shell_arguments;
		ok( run_log(
				[
					'sh', '-c',
					q{eval "set -- $1" || exit; printf '%s\0' "$@"},
					'sh', $quoted_options,
				],
				'>' => \$shell_arguments),
			'shell evaluates the printed initialization options');
		is_deeply(
			[ split /\0/, $shell_arguments ],
			[
				map { /^--lc-collate=/ ? "--lc-collate=$locale" : $_ }
				  @options
			],
			'shell reuse preserves argument boundaries and literal metacharacters'
		);
		$oldnode->safe_psql('postgres',
			"UPDATE pg_database SET datcollate = 'C' WHERE datname = 'template0'"
		);
	}

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
	ok(!-d 'pg_upgrade_output.d',
		'successful printing removes diagnostic logs');

	# Discovering source settings does not require the target installation.
	command_checks_all(
		print_command($oldnode, '--new-bindir' => $newnode->basedir), 0,
		[qr/\A\Q$live_options\E\z/], [],
		'printing does not require target binaries');

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
	is(slurp_file($unused_target),
		"unchanged\n", 'supplied target is unchanged');

	# Consume the printed options as a wrapper would, retaining control over
	# authentication, paths, and configuration before the upgrade. The upgrade
	# must preserve the source rows and the configuration added by the caller.
	$newnode->init(extra => \@options);
	$newnode->append_conf('postgresql.conf',
		'custom.before_upgrade = configured');
	my $upgrade_command = [
		'pg_upgrade', '--no-sync',
		'--old-datadir' => $oldnode->data_dir,
		'--new-datadir' => $newnode->data_dir,
		'--old-bindir' => $oldbindir,
		'--old-port' => $oldnode->port,
		'--new-port' => $newnode->port,
		'--socketdir' => $newnode->host,
	];
	# A wrapper can check compatibility without losing its configured target.
	command_ok([ @$upgrade_command, '--check' ],
		'ordinary --check accepts the separately initialized target');
	ok(-f $newnode->data_dir . '/PG_VERSION',
		'ordinary --check preserves the configured target');
	command_ok($upgrade_command,
		'ordinary upgrade accepts a target initialized with the printed options'
	);
	$newnode->start;
	is( $newnode->safe_psql(
			'postgres', 'SELECT array_agg(id ORDER BY id) FROM t'),
		'{1,2,3}',
		'ordinary upgrade preserves the source rows');
	is($newnode->safe_psql('postgres', 'SHOW custom.before_upgrade'),
		'configured', 'configuration added before upgrade is preserved');
	$newnode->stop;

	# Reject binary/data mismatches before control-file inspection or startup,
	# leaving stdout empty. Alter only a fresh, never-started fixture.
	my $mismatch = PostgreSQL::Test::Cluster->new('print_version_mismatch',
		install_path => $ENV{oldinstall});
	$mismatch->init;
	unlink $mismatch->data_dir . '/PG_VERSION' or die "unlink: $!";
	append_to_file($mismatch->data_dir . '/PG_VERSION', "10\n");
	command_checks_all(
		print_command($mismatch),
		1,
		[qr/\A\z/],
		[
			qr/Old cluster data and binary directories are from different major versions/
		],
		'printing rejects mismatched source binaries before discovery');
	ok( !-f $mismatch->data_dir . '/postmaster.opts',
		'mismatched source has never been started');

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
	$oldnode->safe_psql('template1',
		'REVOKE SELECT ON pg_database FROM PUBLIC');
	command_checks_all(
		print_command($oldnode, '--username' => 'print_options_reader'),
		1,
		[qr/\A\z/],
		[qr/permission denied for table pg_database/],
		'source query failure leaves stdout empty');
	$oldnode->safe_psql('template1', 'GRANT SELECT ON pg_database TO PUBLIC');
	$oldnode->safe_psql('postgres', 'DROP ROLE print_options_reader');
	$oldnode->stop;

	# A failed connection must also stop a source started only for inspection.
	command_checks_all(
		print_command($oldnode, '--username' => 'missing_print_options_role'),
		1,
		[qr/\A\z/],
		[qr/role .*missing_print_options_role.* does not exist/],
		'failed stopped-source inspection leaves stdout empty');
	ok( !-f $oldnode->data_dir . '/postmaster.pid',
		'failed inspection stops its temporary source');

	# Printing does not check compatibility, so reject --check in either argument
	# order without producing an option list.
	for my $options (
		[ '--print-initdb-options', '--check' ],
		[ '--check', '--print-initdb-options' ])
	{
		command_checks_all(
			[ 'pg_upgrade', @$options ],
			1,
			[qr/\A\z/],
			[
				qr/options --print-initdb-options and --check cannot be used together/
			],
			'printing rejects --check in either argument order');
	}

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
		  PostgreSQL::Test::Cluster->new("print_${name}_source",
			install_path => $ENV{oldinstall});
		$source->init(
			extra => [
				'--data-checksums', '--wal-segsize=1',
				'--encoding=UTF8', '--locale=C',
				"--locale-provider=$name", $locale,
			]);
		my ($output, $errors);
		ok( run_log(
				print_command($source),
				'>' => \$output,
				'2>' => \$errors),
			"print $name initialization options");
		my @args = shellwords($output);
		ok( grep({ $_ eq '--data-checksums' } @args),
			'output explicitly enables source checksums');
		ok(grep({ $_ eq $locale } @args),
			"output preserves the $name locale");
		my $target = PostgreSQL::Test::Cluster->new("print_${name}_target");
		$target->init(extra => \@args);
		$target->start;
		is($target->safe_psql('postgres', 'SHOW wal_segment_size'),
			'1MB', "$name initialization preserves WAL segment size");
		$target->stop;
	}

	chdir $cwd;
}

done_testing();
