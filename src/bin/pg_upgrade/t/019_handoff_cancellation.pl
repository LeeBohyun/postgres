# Copyright (c) 2026, PostgreSQL Global Development Group

# Cancellation must survive restart and apply only to its original HANDOFF.
# Use matching source binaries, including when oldinstall names an older build.

use strict;
use warnings FATAL => 'all';

use File::Copy qw(copy);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

chdir $PostgreSQL::Test::Utils::tmp_check;

my $stopped_startup;

END
{
	kill 'CONT', $stopped_startup if defined $stopped_startup;
}

sub cancel_with_later_pause
{
	my ($node, $description) = @_;
  SKIP:
	{
		skip 'requires SIGSTOP and SIGCONT', 3 if $windows_os;
		my $session = $node->background_psql('postgres');
		my $waiter = $session->query_safe('SELECT pg_backend_pid()');
		my $startup = $node->safe_psql('postgres',
			q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'startup'}
		);
		die 'could not identify the test standby startup process'
		  unless $startup =~ /^\d+$/ && $startup > 1;
		my $idle_query = qq{
SELECT wait_event IN ('RecoveryPause', 'RecoveryRetrieveRetryInterval', 'RecoveryWalStream')
FROM pg_stat_activity WHERE pid = $startup
};
		# Stop only at an idle wait, so the test does not suspend a lock holder.
		for (1 .. 20)
		{
			die 'test startup did not reach an idle wait'
			  unless $node->poll_query_until('postgres', $idle_query);
			kill 'STOP', $startup or die "stop test startup: $!";
			$stopped_startup = $startup;
			last if $node->safe_psql('postgres', $idle_query) eq 't';
			kill 'CONT', $startup or die "continue test startup: $!";
			$stopped_startup = undef;
		}
		die 'could not stop test startup at an idle wait'
		  unless defined $stopped_startup;
		$session->query_until(
			qr/^resume_started\r?$/m, qq{
\\echo resume_started
SELECT pg_wal_replay_resume();
\\echo resume_finished
});
		ok( $node->poll_query_until(
				'postgres',
				"SELECT wait_event = 'RecoveryPause' FROM pg_stat_activity WHERE pid = $waiter"
			),
			"$description: resume waits for durable acknowledgement");
		$node->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
		kill 'CONT', $startup or die "continue test startup: $!";
		$stopped_startup = undef;
		like($session->query_until(qr/^resume_finished\r?$/m, ''),
			qr/^resume_finished\r?$/m,
			"$description: cancellation is acknowledged");
		$session->quit;
		is( $node->safe_psql(
				'postgres',
				q{SELECT pg_get_wal_replay_pause_state() <> 'not paused'}),
			't',
			"$description: a newer manual pause is preserved");
	}
}

my $primary = PostgreSQL::Test::Cluster->new('cancel_primary');
$primary->init(allows_streaming => 1);
$primary->append_conf(
	'postgresql.conf', q{
fsync = on
autovacuum = off
listen_addresses = '127.0.0.1'
wal_receiver_status_interval = 0
wal_sender_shutdown_timeout = '1s'
});
$primary->append_conf('pg_hba.conf',
	'host replication cancel_repl 127.0.0.1/32 trust');
$primary->start;
$primary->safe_psql(
	'postgres', q{
CREATE ROLE cancel_repl LOGIN REPLICATION;
CREATE TABLE cancel_data AS SELECT 42 AS value;
CREATE SEQUENCE cancel_sequence;
SELECT pg_create_physical_replication_slot('cancel_slot', true);
});
$primary->backup('cancel_base');

my $standby = PostgreSQL::Test::Cluster->new('cancel_standby');
$standby->init_from_backup($primary, 'cancel_base', has_streaming => 1);
$standby->append_conf('postgresql.auto.conf',
		"primary_conninfo = 'host=127.0.0.1 port="
	  . $primary->port
	  . " user=cancel_repl application_name=cancel_standby'\n"
	  . "primary_slot_name = 'cancel_slot'\n");
$standby->append_conf('postgresql.conf', "statement_timeout = '10s'");
$standby->start;
$primary->wait_for_catchup($standby);
$primary->stop;

sub prepare_upgrade
{
	my ($name) = @_;
	my $new = PostgreSQL::Test::Cluster->new($name);

	command_ok(
		[
			'pg_upgrade', '--initdb', '--wal-upgrade', '--copy',
			'--old-datadir' => $primary->data_dir,
			'--new-datadir' => $new->data_dir,
			'--old-bindir' => $primary->config_data('--bindir'),
			'--new-bindir' => $new->config_data('--bindir'),
			'--socketdir' => $primary->host,
			'--old-port' => $primary->port,
			'--new-port' => $new->port,
		],
		"$name: prepare the primary with HANDOFF");
	ok( $standby->poll_query_until(
			'postgres', q{SELECT pg_get_wal_replay_pause_state() = 'paused'}),
		"$name: standby reaches HANDOFF");
}

prepare_upgrade('cancel_attempt_1');

# A failed durable write must not acknowledge cancellation or lose the pause.
my $cancellation = $standby->data_dir . '/pg_upgrade_handoff.cancelled';
my $temporary = "$cancellation.tmp";
mkdir $temporary or die "create cancellation obstruction: $!";
my $offset = -s $standby->logfile;
my ($result) = $standby->psql('postgres', 'SELECT pg_wal_replay_resume()');
isnt($result, 0, 'failed persistence does not acknowledge cancellation');
$standby->wait_for_log(
	qr/could not create pg_upgrade handoff cancellation file/, $offset);
$standby->wait_for_log(qr/database system is shut down/, $offset);
ok(!-f $cancellation, 'failed persistence does not publish cancellation');
$standby->stop('immediate', fail_ok => 1);
rmdir $temporary or die "remove cancellation obstruction: $!";
$standby->start;
ok( $standby->poll_query_until(
		'postgres', q{SELECT pg_get_wal_replay_pause_state() = 'paused'}),
	'failed cancellation restores HANDOFF after restart');

cancel_with_later_pause($standby, 'completed HANDOFF');
$standby->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
is( $standby->safe_psql('postgres', 'SELECT pg_get_wal_replay_pause_state()'),
	'not paused',
	'successful resume completes cancellation');
ok(-f $cancellation,
	'resume publishes durable cancellation before returning');

# There is no later WAL to hide a cancelled HANDOFF after a crash.
$standby->stop('immediate');
$standby->start;
is( $standby->safe_psql('postgres', 'SELECT pg_get_wal_replay_pause_state()'),
	'not paused',
	'cancellation survives an immediate crash after resume');
$standby->restart;
is( $standby->safe_psql('postgres', 'SELECT pg_get_wal_replay_pause_state()'),
	'not paused',
	'cancellation survives a normal restart');
$standby->safe_psql(
	'postgres', q{
SELECT pg_create_physical_replication_slot('after_cancel', true);
SELECT pg_drop_replication_slot('after_cancel');
});
pass('cancellation also releases physical slot freezing');

# Corrupt the identity without updating its checksum. Startup must fail closed.
$standby->stop;
copy($cancellation, "$cancellation.saved") or die "save cancellation: $!";
open my $file, '+<:raw', $cancellation or die "open cancellation: $!";
seek($file, 8, 0) or die "seek cancellation identity: $!";
read($file, my $byte, 1) == 1 or die "read cancellation identity: $!";
seek($file, 8, 0) or die "seek cancellation identity: $!";
print {$file} chr(ord($byte) ^ 1) or die "corrupt cancellation: $!";
close $file or die "close cancellation: $!";
$offset = -s $standby->logfile;
# pg_ctl can report a clean stop during startup as success. Check the failure
# and stopped state directly instead of relying on that exit status.
$standby->run_log(
	[
		'pg_ctl', '--wait',
		'--pgdata' => $standby->data_dir,
		'--log' => $standby->logfile,
		'start'
	]);
$standby->wait_for_log(qr/invalid pg_upgrade handoff cancellation file/,
	$offset);
$standby->wait_for_log(qr/database system is shut down/, $offset);
$standby->_update_pid(-1);
ok( !-f $standby->data_dir . '/postmaster.pid',
	'corrupt cancellation prevents startup');
copy("$cancellation.saved", $cancellation) or die "restore cancellation: $!";
unlink "$cancellation.saved" or die "remove saved cancellation: $!";

# An offline mode switch must honor the cancellation as well.
$standby->backup_fs_cold('cancelled');
my $promoted_copy = PostgreSQL::Test::Cluster->new('cancel_promoted_copy');
$promoted_copy->init_from_backup($standby, 'cancelled');
unlink $promoted_copy->data_dir . '/standby.signal'
  or die "remove copied standby signal: $!";
$promoted_copy->start;
is($promoted_copy->safe_psql('postgres', 'SELECT pg_is_in_recovery()'),
	'f', 'a cancelled standby can restart in primary mode');
is($promoted_copy->safe_psql('postgres', 'TABLE cancel_data'),
	'42', 'mode switch preserves the retained data');
$promoted_copy->stop;

# Keep the old cancellation file: it must not cancel a different attempt.
$standby->start;
$primary->start;
$primary->wait_for_catchup($standby);
$standby->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
ok( $standby->poll_query_until(
		'postgres', q{SELECT pg_get_wal_replay_pause_state() = 'paused'}),
	'ordinary recovery can still be paused after HANDOFF cancellation');
$primary->safe_psql('postgres', q{SELECT setval('cancel_sequence', 19)});
my $ordinary_lsn = $primary->lsn('insert');
ok( $standby->poll_query_until(
		'postgres',
		"SELECT pg_last_wal_receive_lsn() >= '$ordinary_lsn'::pg_lsn"),
	'ordinary paused recovery continues receiving WAL');
is( $standby->safe_psql('postgres', 'SELECT last_value FROM cancel_sequence'),
	'1',
	'ordinary pause prevents replay of later sequence WAL');
$standby->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
ok( $standby->poll_query_until(
		'postgres',
		"SELECT pg_last_wal_replay_lsn() >= '$ordinary_lsn'::pg_lsn"),
	'ordinary resume continues WAL replay');
is( $standby->safe_psql('postgres', 'SELECT last_value FROM cancel_sequence'),
	'19',
	'ordinary resume exposes the replayed sequence change');
$primary->stop;
prepare_upgrade('cancel_attempt_2');

# A new cancellation replaces the earlier attempt's durable identity.
$standby->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
$standby->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
$standby->stop('immediate');
$standby->start;
is( $standby->safe_psql('postgres', 'SELECT pg_get_wal_replay_pause_state()'),
	'not paused',
	'the second cancellation survives crash restart');
$standby->promote;
is($standby->safe_psql('postgres', 'SELECT pg_is_in_recovery()'),
	'f', 'promotion is allowed after durable cancellation');
$standby->restart;
is($standby->safe_psql('postgres', 'TABLE cancel_data'),
	'42',
	'the promoted server restarts normally with stale cancellation state');
$standby->stop;

subtest 'cancellation before the final checkpoint arrives' => sub {
	my $source = PostgreSQL::Test::Cluster->new('pending_primary');
	$source->init(allows_streaming => 1);
	$source->append_conf(
		'postgresql.conf', q{
fsync = on
autovacuum = off
wal_keep_size = '256MB'
});
	$source->start;
	$source->safe_psql('postgres',
		'CREATE TABLE pending_data AS SELECT 17 AS value');
	$source->backup('pending_base');
	$source->stop;
	my $pending = PostgreSQL::Test::Cluster->new('pending_standby');
	$pending->init_from_backup($source, 'pending_base');
	$pending->append_conf(
		'postgresql.auto.conf', q{
primary_conninfo = ''
restore_command = 'false'
wal_retrieve_retry_interval = '100ms'
checkpoint_timeout = '1h'
statement_timeout = '10s'
});
	$pending->set_standby_mode;

	my $prepare = sub {
		my ($attempt) = @_;
		my $new = PostgreSQL::Test::Cluster->new("pending_new_$attempt");
		command_ok(
			[
				'pg_upgrade', '--initdb', '--wal-upgrade', '--copy',
				'--old-datadir' => $source->data_dir,
				'--new-datadir' => $new->data_dir,
				'--old-bindir' => $source->config_data('--bindir'),
				'--new-bindir' => $new->config_data('--bindir'),
				'--socketdir' => $source->host,
				'--old-port' => $source->port,
				'--new-port' => $new->port,
			],
			"prepare pending attempt $attempt");

		my ($control) = run_command(
			[
				$source->installed_command('pg_controldata'), '-D',
				$source->data_dir
			]);
		my ($checkpoint) = $control =~ /Latest checkpoint location:\s+(\S+)/;
		my ($segment) =
		  $control =~ /Latest checkpoint's REDO WAL file:\s+(\S+)/;
		my ($segment_size) = $control =~ /Bytes per WAL segment:\s+(\d+)/;
		die 'could not read pending checkpoint metadata'
		  unless defined $checkpoint
		  && defined $segment
		  && defined $segment_size;

		my $source_wal = $source->data_dir . '/pg_wal';
		opendir my $dir, $source_wal or die "open source WAL: $!";
		for my $name (grep { /^[0-9A-F]{24}$|^[0-9A-F]{8}\.history$/ }
			readdir $dir)
		{
			copy("$source_wal/$name", $pending->data_dir . "/pg_wal/$name")
			  or die "stage pending WAL: $!";
		}
		closedir $dir or die "close source WAL: $!";

		# Preserve the complete HANDOFF record but withhold the next record.
		# Keep a full-size file so WAL page reads can still decode HANDOFF.
		my (undef, $low) = split '/', $checkpoint;
		my $checkpoint_offset = hex($low) % $segment_size;
		my $path = $pending->data_dir . "/pg_wal/$segment";
		my $contents = slurp_file($path);
		substr($contents, $checkpoint_offset) =
		  "\0" x (length($contents) - $checkpoint_offset);
		open my $wal, '>:raw', $path or die "open pending WAL: $!";
		print {$wal} $contents or die "withhold checkpoint: $!";
		close $wal or die "close pending WAL: $!";
		return ($checkpoint, $segment);
	};

	my ($first_checkpoint) = $prepare->(1);
	$pending->start;
	$pending->wait_for_log(qr/reached pg_upgrade handoff on standby/);
	cancel_with_later_pause($pending, 'pending HANDOFF');
	$pending->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
	pass('pending HANDOFF cancellation completes without the final checkpoint'
	);
	$pending->stop('immediate');
	$offset = -s $pending->logfile;
	$pending->start;
	$pending->wait_for_log(qr/skipping a cancelled pg_upgrade handoff/,
		$offset);
	is( $pending->safe_psql(
			'postgres', 'SELECT pg_get_wal_replay_pause_state()'),
		'not paused',
		'pending cancellation survives crash replay');
	$pending->stop('immediate');

	# Retry on a descendant timeline, while the standby's durable restartpoint
	# still precedes the first cancellation.
	$pending->backup_fs_cold('pending_promoted');
	my $promoted = PostgreSQL::Test::Cluster->new('pending_promoted');
	$promoted->init_from_backup($pending, 'pending_promoted');
	unlink $promoted->data_dir . '/standby.signal'
	  or die "remove pending promotion signal: $!";
	$promoted->start;
	$promoted->stop;
	$source = $promoted;
	my ($second_checkpoint, $second_segment) = $prepare->(2);
	$offset = -s $pending->logfile;
	$pending->start;
	$pending->wait_for_log(qr/reached pg_upgrade handoff on standby/,
		$offset);
	is( $pending->safe_psql(
			'postgres',
			"SELECT checkpoint_lsn < '$first_checkpoint'::pg_lsn FROM pg_control_checkpoint()"
		),
		't',
		'both attempts precede a newer durable restartpoint');
	$pending->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
	$pending->stop('immediate');
	$pending->start;
	ok( $pending->poll_query_until(
			'postgres',
			"SELECT pg_last_wal_replay_lsn() >= '$second_checkpoint'::pg_lsn"
		),
		'latest cancellation also covers an earlier cancelled HANDOFF on replay'
	);
	$pending->safe_psql(
		'postgres', q{
SELECT pg_create_physical_replication_slot('pending_cancelled', true);
SELECT pg_drop_replication_slot('pending_cancelled');
});
	pass(
		'pending cancellation releases physical slot freezing after restart');
	$pending->stop('immediate');

	copy(
		$source->data_dir . "/pg_wal/$second_segment",
		$pending->data_dir . "/pg_wal/$second_segment"
	) or die "deliver the withheld checkpoint: $!";
	$pending->start;
	ok( $pending->poll_query_until(
			'postgres',
			"SELECT pg_last_wal_replay_lsn() > '$second_checkpoint'::pg_lsn"),
		'the final checkpoint replays after cancelled HANDOFF');
	is( $pending->safe_psql(
			'postgres', 'SELECT pg_get_wal_replay_pause_state()'),
		'not paused',
		'delivering the checkpoint does not revive HANDOFF');
	$pending->promote;
	is($pending->safe_psql('postgres', 'TABLE pending_data'),
		'17',
		'a cancelled pending HANDOFF permits promotion with intact data');
	$pending->stop;
};

done_testing();
