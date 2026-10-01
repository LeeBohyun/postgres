# Copyright (c) 2025-2026, PostgreSQL Global Development Group

# Recover post-upgrade writes from an old base backup and WAL archive without
# taking a new base backup. Reject incomplete upgrade windows and missing
# completion checkpoints.
#
# Set oldinstall for cross-version coverage. Otherwise both clusters use this
# installation.

use strict;
use warnings FATAL => 'all';

use File::Path qw(rmtree);
use File::Copy qw(copy);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# pg_upgrade writes its output files in the current directory.
chdir ${PostgreSQL::Test::Utils::tmp_check};

sub upgrade_finalized
{
	my ($node, $datadir) = @_;
	my $bindir = $node->config_data('--bindir');
	my ($stdout, $stderr) =
	  run_command([ "$bindir/pg_controldata", '-D', $datadir ]);
	return ($stdout =~ /wal-upgrade window finalized:\s+yes/) ? 1 : 0;
}

my $lsn_pattern = qr/[0-9A-F]+\/[0-9A-F]+/i;

sub normalize_lsn
{
	my ($lsn) = @_;
	my ($hi, $lo) = $lsn =~ /\A([0-9A-F]+)\/([0-9A-F]+)\z/i;
	die "invalid LSN: $lsn" unless defined $lo;
	return sprintf('%X/%08X', hex($hi), hex($lo));
}

# Read record bounds from both pg_waldump formats used by cross-version tests.
sub parse_waldump_record
{
	my ($output) = @_;
	my ($lsn, $end, $prev) = $output =~ m{
		\blsn:[ \t]*($lsn_pattern),[ \t]+
		(?:end:[ \t]*($lsn_pattern),?[ \t]+)?
		prev[ \t]+($lsn_pattern)
	}x;
	die "could not parse pg_waldump record:\n$output" unless defined $prev;
	return (normalize_lsn($lsn), defined $end ? normalize_lsn($end) : undef,
		normalize_lsn($prev));
}

sub read_control
{
	my ($node) = @_;
	my ($stdout, $stderr);
	my $ok = $node->run_log(
		[ $node->installed_command('pg_controldata'), $node->data_dir ],
		'>', \$stdout, '2>', \$stderr);
	die "pg_controldata failed: $stderr" unless $ok;
	return $stdout;
}

sub control_lsn
{
	my ($output, $label) = @_;
	my ($lsn) = $output =~ /^\Q$label\E:[ \t]*($lsn_pattern)[ \t\r]*$/m;
	die "could not find $label in pg_controldata output:\n$output"
	  unless defined $lsn;
	return normalize_lsn($lsn);
}

sub last_wal_segment
{
	my ($waldir, $first_segment) = @_;
	my $timeline = substr($first_segment, 0, 8);
	opendir(my $dh, $waldir) or die "opendir $waldir: $!";
	my @segments =
	  sort grep { /^\Q$timeline\E[0-9A-F]{16}$/ } readdir $dh;
	closedir $dh;
	die "no WAL segments found in $waldir" unless @segments;
	return $segments[-1];
}

# Find the shutdown checkpoint bounds used by the two recovery phases.
sub shutdown_checkpoint_bounds
{
	my ($node) = @_;
	my $control = read_control($node);
	my $start = control_lsn($control, "Latest checkpoint's REDO location");
	my ($wal_segment) =
	  $control =~ /^Latest checkpoint's REDO WAL file:[ \t]*([0-9A-F]{24})/m;
	die "could not find checkpoint WAL file:\n$control"
	  unless defined $wal_segment;
	my $waldump = $node->installed_command('pg_waldump');
	my $waldir = $node->data_dir . '/pg_wal';
	my $last_segment = last_wal_segment($waldir, $wal_segment);
	my ($stdout, $stderr);
	my $ok = $node->run_log(
		[
			$waldump, '-p', $waldir, '-s',
			$start, '-n', '1', $wal_segment,
			$last_segment
		],
		'>',
		\$stdout,
		'2>',
		\$stderr);
	die "could not read old shutdown checkpoint: $stderr" unless $ok;

	my ($record) = $stdout =~ /^(rmgr:.*CHECKPOINT_SHUTDOWN.*)$/m;
	die "REDO record is not a shutdown checkpoint:\n$stdout"
	  unless defined $record;
	my ($seen_start, $end) = parse_waldump_record($record);
	die
	  "pg_control REDO location $start does not match checkpoint $seen_start"
	  unless $seen_start eq $start;

	# Derive the record end for older cross-version test output.
	if (!defined $end)
	{
		($stdout, $stderr) = ('', '');
		$ok = $node->run_log(
			[
				$waldump, '-p', $waldir, '-s',
				$start, '-n', '2', $wal_segment,
				$last_segment
			],
			'>',
			\$stdout,
			'2>',
			\$stderr);
		die "WAL follows the old shutdown checkpoint" if $ok;
		($end) = $stderr =~ /invalid record length at[ \t]+($lsn_pattern)/i;
		die "could not derive shutdown checkpoint end:\n$stderr"
		  unless defined $end;
		$end = normalize_lsn($end);
	}
	return ($start, $end);
}

# Build the --wal-upgrade command used by the primary-backup scenario.
sub upgrade_cmd
{
	my ($old, $new, @extra) = @_;
	return [
		'pg_upgrade', '--no-sync',
		'--old-datadir' => $old->data_dir,
		'--new-datadir' => $new->data_dir,
		'--old-bindir' => $old->config_data('--bindir'),
		'--new-bindir' => $new->config_data('--bindir'),
		'--socketdir' => $new->host,
		'--old-port' => $old->port,
		'--new-port' => $new->port,
		'--initdb',
		'--wal-upgrade',
		@extra,
	];
}

# Supply connection settings normally written by init(), which --initdb skips.
sub add_conn_conf
{
	my ($new) = @_;
	my $conf = $new->data_dir . '/postgresql.conf';
	open(my $fh, '>>', $conf) or die "could not open $conf: $!";
	print $fh "\n# added by test to start the --initdb-created cluster\n";
	print $fh "port = " . $new->port . "\n";
	print $fh "listen_addresses = ''\n";
	print $fh "unix_socket_directories = '" . $new->host . "'\n";
	close($fh);
}

# Delete the upgraded cluster and recover from its old base backup and archive.
# The old binary replays through the final old checkpoint.  The new binary
# then recovers the selected timeline's upgrade, rows, and DDL.
{
	# Use one archive for pre-upgrade WAL, the upgrade window, the post-upgrade
	# tail, and both recovery phases.
	my $archive = "${PostgreSQL::Test::Utils::tmp_check}/pitr_archive";
	rmtree($archive);
	mkdir($archive) or die "could not create $archive: $!";
	# Keep partial archive copies invisible to recovery.
	my $arch_cmd =
	  "test ! -f \"$archive/%f\" && cp \"%p\" \"$archive/%f.tmp\" && mv \"$archive/%f.tmp\" \"$archive/%f\"";
	my $restore_cmd = "cp \"$archive/%f\" \"%p\"";

	# Cross-version recovery must read this non-default size from staged WAL.
	my $old =
	  PostgreSQL::Test::Cluster->new('old_pitr',
		install_path => $ENV{oldinstall});
	if (defined($ENV{oldinstall}))
	{
		$old->init(
			extra => [ '-k', '--wal-segsize=1' ],
			allows_streaming => 1);
	}
	else
	{
		$old->init(extra => ['--wal-segsize=1'], allows_streaming => 1);
	}
	$old->append_conf('postgresql.conf',
		"archive_mode = on\narchive_command = '$arch_cmd'\n");
	$old->start;

	# Write data that must be present before the window is replayed.
	$old->safe_psql(
		'postgres', qq{
		CREATE TABLE t (id int primary key, note text);
		INSERT INTO t SELECT g, 'pre ' || g FROM generate_series(1, 500) g;
	});
	$old->safe_psql('postgres', 'VACUUM t');
	# Archive writes on timeline 1, then promote an earlier backup to timeline 2.
	# The upgrade must archive under timeline 2 without reusing timeline-1 names.
	$old->stop;
	$old->backup_fs_cold('before_fork');
	$old->start;
	for my $i (1 .. 12)
	{
		$old->safe_psql('postgres',
			"SELECT pg_logical_emit_message(false, 'archive_test', '$i'); SELECT pg_switch_wal()"
		);
	}
	$old->stop;
	my $original = $old;
	$old = PostgreSQL::Test::Cluster->new('old_pitr_fork',
		install_path => $ENV{oldinstall});
	$old->init_from_backup($original, 'before_fork');
	$old->set_standby_mode;
	$old->start;
	$old->promote;
	die 'restored source did not promote onto timeline 2'
	  unless $old->safe_psql('postgres',
		'SELECT timeline_id FROM pg_control_checkpoint()') eq '2';

	# Retain this as the only base backup used by the recovery tests.
	$old->backup('pitr_base');

	$old->stop;

	# Upgrade and emit the window into the same archive stream.
	my $new = PostgreSQL::Test::Cluster->new('new_pitr');
	# Link-mode replay transfers retained segments from the restored old backup.
	command_ok(upgrade_cmd($old, $new, '--link'),
		'pitr: pg_upgrade --wal-upgrade --initdb --link succeeds');
	# Derive the old-major recovery target used by the two-phase replay test.
	my $old_control = $old->data_dir . '/global/pg_control';
	copy("$old_control.old", $old_control)
	  or die "read disabled old control file: $!";
	my ($phase1_target_lsn) = shutdown_checkpoint_bounds($old);
	unlink($old_control) or die "restore disabled old control file: $!";

	# Preserve the transferred archive settings while adding harness connections.
	add_conn_conf($new);

	# Inspect the archive before starting the upgraded primary.  pg_upgrade must
	# have archived the window and its completion checkpoint before returning.
	my $bindir = $new->config_data('--bindir');
	my ($start_seg, $complete_seg) = ('', '');
	my $complete_lsn;
	my ($completion_checkpoint_seg, $completion_checkpoint_lsn);
	my $completion_checkpoint_archived = 0;
	opendir(my $ad, $archive) or die "opendir $archive: $!";
	for my $f (sort grep { /^[0-9A-F]{24}$/ } readdir $ad)
	{
		my ($out) = run_command([ "$bindir/pg_waldump", "$archive/$f" ]);
		for my $record (split /\n/, $out)
		{
			$start_seg = $f if $record =~ /PG_UPGRADE_START/;
			if ($record =~ /PG_UPGRADE_COMPLETE/)
			{
				$complete_seg = $f;
				($complete_lsn) = parse_waldump_record($record);
			}
			if (   $complete_seg ne ''
				&& $record =~ /CHECKPOINT_SHUTDOWN/
				&& !$completion_checkpoint_archived)
			{
				$completion_checkpoint_archived = 1;
				$completion_checkpoint_seg = $f;
				($completion_checkpoint_lsn) = parse_waldump_record($record);
			}
		}
	}
	closedir $ad;
	ok( $start_seg ne ''
		  && $complete_seg ne ''
		  && substr($start_seg, 0, 8) eq '00000002'
		  && $completion_checkpoint_archived,
		'pitr: upgrade window and completion checkpoint are archived on timeline 2'
	);

	# Start the upgraded primary and continue writing to the shared archive.
	$new->start;

	# Promote to timeline 3 before writing new rows and DDL.  Recovery must find
	# the upgrade on timeline 2 even when timeline-3 WAL is staged beside it.
	$new->stop;
	mkdir $new->backup_dir unless -d $new->backup_dir;
	$new->backup_fs_cold('after_upgrade');
	my $pre_promotion = $new;
	$new = PostgreSQL::Test::Cluster->new('post_upgrade_promoted');
	$new->init_from_backup($pre_promotion, 'after_upgrade');
	$new->set_standby_mode;
	$new->start;
	$new->promote;
	die 'post-upgrade writer did not promote onto timeline 3'
	  unless $new->safe_psql('postgres',
		'SELECT timeline_id FROM pg_control_checkpoint()') eq '3';

	# Add DML and DDL that exist only after the upgrade.
	$new->safe_psql('postgres',
		"INSERT INTO t SELECT g, 'post ' || g FROM generate_series(501, 800) g;"
	);
	$new->safe_psql('postgres',
		'CREATE TABLE only_on_new (x int); INSERT INTO only_on_new VALUES (42);'
	);

	# Make the post-upgrade WAL available to the recovery cases.
	$new->safe_psql('postgres', 'CHECKPOINT');
	my $last_archived = $new->safe_psql('postgres',
		'SELECT pg_walfile_name(pg_current_wal_insert_lsn())');
	$new->safe_psql('postgres', 'SELECT pg_switch_wal()');
	$new->poll_query_until('postgres',
		"SELECT last_archived_wal >= '$last_archived' FROM pg_stat_archiver")
	  or die "post-upgrade WAL $last_archived was not archived";
	die "archive does not contain $last_archived"
	  unless -f "$archive/$last_archived";

	# Remove the upgraded cluster before any new base backup is available.
	$new->stop('immediate');
	my $new_datadir = $new->data_dir;
	rmtree($new_datadir);
	die 'could not remove upgraded cluster storage' if -d $new_datadir;

	# Verify that old-major recovery persists the final old checkpoint before
	# new-major replay.
	my $old_bindir = $old->config_data('--bindir');
	my $restore = PostgreSQL::Test::Cluster->new('restore_pitr');
	$restore->init_from_backup($old, 'pitr_base');

	$restore->append_conf('postgresql.conf',
			"restore_command = '$restore_cmd'\n"
		  . "recovery_target_lsn = '$phase1_target_lsn'\n"
		  . "recovery_target_inclusive = on\n"
		  . "recovery_target_action = 'shutdown'\n");
	open(my $s1, '>', $restore->data_dir . '/recovery.signal') or die $!;
	close($s1);

	# Wait for the old-major recovery phase to stop at its target.
	my $p1log = "${PostgreSQL::Test::Utils::tmp_check}/pitr_phase1.log";
	PostgreSQL::Test::Utils::system_log(
		"$old_bindir/pg_ctl", '--wait',
		'--pgdata' => $restore->data_dir,
		'--log' => $p1log,
		'--options' => "--cluster-name=restore_pitr_p1 -c hot_standby=off",
		'start');
	my $phase1_deadline = time + $PostgreSQL::Test::Utils::timeout_default;
	while (-f $restore->data_dir . '/postmaster.pid')
	{
		die 'phase-one recovery failed to stop' if time >= $phase1_deadline;
		select(undef, undef, undef, 0.1);
	}
	like(
		slurp_file($p1log),
		qr/shutdown at recovery target/,
		'pitr phase 1: old binary stopped at the final old checkpoint (no promote)'
	);

	# Give each recovery case a fresh copy of the stopped old-version restore.
	$restore->backup_fs_cold('before_window');
	opendir(my $ad2, $archive) or die $!;
	my @archived = sort grep { /^[0-9A-F]{24}$/ } readdir $ad2;
	closedir $ad2;

	my $prepare = sub {
		my ($name, $timeline, $stage_tail, $restore_command) = @_;
		my $node = PostgreSQL::Test::Cluster->new("pitr_$name");
		$node->init_from_backup($restore, 'before_window');
		my $rwal = $node->data_dir . '/pg_wal';
		for my $f (@archived)
		{
			next if $f lt $start_seg;
			next if !$stage_tail && $f gt $completion_checkpoint_seg;
			copy("$archive/$f", "$rwal/$f") or die "stage $f: $!";
		}
		for my $f (glob "$archive/*.history")
		{
			(my $base = $f) =~ s{.*/}{};
			copy($f, "$rwal/$base") or die "stage $base: $!";
		}
		my $conf = $node->data_dir . '/postgresql.conf';
		my $txt = slurp_file($conf);
		$txt =~ s/^recovery_target.*\n//mg;
		$txt =~ s/^restore_command.*\n//mg;
		open(my $cw, '>', $conf) or die $!;
		print $cw $txt;
		print $cw "recovery_target_timeline = '$timeline'\n";
		print $cw "restore_command = '$restore_command'\n";
		# Verify that an unusable primary_conninfo does not change archive
		# recovery into streaming.
		print $cw "primary_conninfo = 'host=127.0.0.1 port=1'\n";
		# Use recovery limits compatible with the archived primary's settings.
		print $cw "max_connections = 100\nmax_worker_processes = 8\n";
		print $cw "max_wal_senders = 10\nmax_prepared_transactions = 0\n";
		print $cw "max_locks_per_transaction = 128\n";
		close($cw);
		for my $signal ('recovery.signal', 'pg_upgrade.signal')
		{
			open(my $fh, '>', $node->data_dir . "/$signal") or die $!;
			close($fh);
		}
		return $node;
	};

	# Latest-timeline recovery includes timeline-3 writes. Targeting timeline 2
	# excludes them.
	for my $case ([ 'latest', 'latest', 0, 800 ],
		[ 'explicit_parent', '2', 1, 500 ])
	{
		my ($name, $timeline, $tail, $rows) = @$case;
		my $node = $prepare->($name, $timeline, $tail, $restore_cmd);
		$node->start;
		$node->poll_query_until('postgres', 'SELECT NOT pg_is_in_recovery()')
		  or die "$name: recovery did not finish";
		is( join(
				'|',
				$node->safe_psql('postgres', 'SHOW wal_segment_size'),
				$node->safe_psql('postgres', 'SELECT count(*) FROM t'),
				$node->safe_psql(
					'postgres',
					"SELECT to_regclass('only_on_new') IS NOT NULL")),
			$rows == 800 ? '1MB|800|t' : '1MB|500|f',
			"pitr $name: recovery selects the expected timeline and data");
		ok( upgrade_finalized($node, $node->data_dir) && ($rows != 800
				|| $node->safe_psql('postgres', 'SELECT x FROM only_on_new')
				eq '42'),
			"pitr $name: committed window is finalized");
		if ($name eq 'latest')
		{
			$node->restart;
			is($node->safe_psql('postgres', 'SELECT count(*) FROM t'),
				'800', 'pitr latest: finalized recovery survives restart');
		}
		$node->stop;
		$node->clean_node;
	}

	# Remove later records for the incomplete-window recovery tests.
	my $erase_tail = sub {
		my ($path, $lsn) = @_;
		my (undef, $lo) = split '/', $lsn;
		my $size = -s $path;
		my $offset = hex($lo) % $size;
		open(my $fh, '+<', $path) or die "open $path: $!";
		binmode $fh;
		seek($fh, $offset, 0) or die "seek $path: $!";
		print $fh "\0" x ($size - $offset);
		close($fh);
	};

	# Recovery rejects both an incomplete window and a committed window without
	# its completion checkpoint.
	for my $name (qw(missing_complete missing_completion_checkpoint))
	{
		my $node = $prepare->($name, '2', 0, 'false');
		my $rwal = $node->data_dir . '/pg_wal';
		my $message;
		if ($name eq 'missing_complete')
		{
			$erase_tail->("$rwal/$complete_seg", $complete_lsn);
			$message = qr/pg_upgrade WAL is incomplete/;
		}
		elsif ($name eq 'missing_completion_checkpoint')
		{
			# Keep the matching COMMIT but withhold its completion checkpoint.
			$erase_tail->(
				"$rwal/$completion_checkpoint_seg",
				$completion_checkpoint_lsn);
			$message =
			  qr/cannot finish recovery before pg_upgrade is finalized/;
		}
		run_log(
			[
				$node->installed_command('pg_ctl'),
				'-w', '-D', $node->data_dir, '-l', $node->logfile, 'start'
			]);
		ok( !-f $node->data_dir . '/postmaster.pid'
			  && slurp_file($node->logfile) =~
			  /(?:FATAL|PANIC):[^\n]*$message/,
			"pitr $name: incomplete recovery refuses to serve");
		if (-f $node->data_dir . '/postmaster.pid')
		{
			run_log(
				[
					$node->installed_command('pg_ctl'),
					'-w', '-D', $node->data_dir, '-m', 'immediate', 'stop'
				]);
		}
		$node->clean_node;
	}

	$old->clean_node;
	$restore->clean_node;
}

done_testing();
