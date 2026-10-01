# Copyright (c) 2025-2026, PostgreSQL Global Development Group

# Exercise copy and link upgrades with one streaming standby, then exercise a
# copy upgrade with a two-level cascade. Check reconstruction, restart, and
# partial-replay rejection.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use File::Path qw(rmtree);
use FindBin;
use lib $FindBin::RealBin;
use WalUpgradeTest qw(
  make_old_standby_chain
  make_upgrade_standby
  wait_for_old_replay
);

# pg_upgrade writes its output files in the current directory.
chdir ${PostgreSQL::Test::Utils::tmp_check};

# Append harness connection and replication settings to the upgraded primary.
sub configure_upgraded_primary
{
	my ($node, $postgresql_conf) = @_;
	my $conf = $node->data_dir . '/postgresql.conf';

	open(my $fh, '>>', $conf) or die "could not open $conf: $!";
	print $fh "\n# added by test\n";
	print $fh "port = " . $node->port . "\n";
	print $fh "listen_addresses = '"
	  . ($PostgreSQL::Test::Utils::windows_os ? '127.0.0.1' : '') . "'\n";
	print $fh "unix_socket_directories = '" . $node->host . "'\n";
	print $fh $postgresql_conf;
	close($fh);

	$node->append_conf('pg_hba.conf',
			"local replication all trust\n"
		  . "host replication all 127.0.0.1/32 trust\n"
		  . "host replication all ::1/128 trust\n");
}

# Keep primary and standby resource limits equal during replay.
my $base_replication_conf = q{
wal_level = replica
max_wal_senders = 10
max_replication_slots = 10
hot_standby = on
};
my $new_conf = $base_replication_conf . "max_connections = 100\n";
my $old_conf = $base_replication_conf;

my $new = PostgreSQL::Test::Cluster->new('new');

# Populate the old primary with relation types used by the replay checks.
my $old =
  PostgreSQL::Test::Cluster->new('old', install_path => $ENV{oldinstall});
if (defined($ENV{oldinstall}))
{
	# Exercise the checksum-enabled path when using an older installation.
	$old->init(allows_streaming => 1, extra => ['-k']);
}
else
{
	$old->init(allows_streaming => 1);
}
$old->append_conf('postgresql.conf', $old_conf);
$old->append_conf('postgresql.conf', "listen_addresses = '127.0.0.1'");
$old->append_conf('pg_hba.conf',
	"host replication upgrade_repl 127.0.0.1/32 trust\n");
$old->start;

# Promote the source before upgrading.  The upgrade checkpoint must remain on
# timeline 2, and a new standby must find it there.
$old->backup('before_promotion');
my $promoted_old = PostgreSQL::Test::Cluster->new('promoted_old',
	install_path => $ENV{oldinstall});
$promoted_old->init_from_backup($old, 'before_promotion', has_streaming => 1);
$promoted_old->start;
wait_for_old_replay($old, $promoted_old);
$old->stop;
$promoted_old->promote;
$old = $promoted_old;

$old->safe_psql(
	'postgres', qq{
	CREATE ROLE upgrade_repl LOGIN REPLICATION;
	CREATE TABLE t (id int primary key, v text);
	INSERT INTO t SELECT g, 'v' || g FROM generate_series(1, 2000) g;
	CREATE INDEX ON t (v);
	-- Exercise unlogged-relation init-file recreation during replay.
	CREATE UNLOGGED TABLE u (id int primary key, v text);
	INSERT INTO u SELECT g, 'u' || g FROM generate_series(1, 500) g;
	CREATE TABLE toasted (id int, big text);
	INSERT INTO toasted
	  SELECT g, repeat('abcdef0123456789', 3000) FROM generate_series(1, 300) g;
	-- Include large-object content in standby convergence coverage.
	SELECT lo_from_bytea(0, decode(repeat(md5(g::text), 50), 'hex'))
	  FROM generate_series(1, 40) g;
});
# Include large-object content in the convergence check.
my $fp_query = q{SELECT count(*), sum(hashtext(v)::bigint),
	(SELECT count(*) || ':' || coalesce(sum(length(data))::text, '0')
	   FROM pg_largeobject) FROM t};
my $want = $old->safe_psql('postgres', $fp_query);

# Verify that --wal-upgrade migrates an existing physical slot under the same
# name.
my $migrated_slot = 'my_standby_slot';
$old->safe_psql('postgres',
	"SELECT pg_create_physical_replication_slot('$migrated_slot', true)");

# The retained source must use old-version binaries with HANDOFF support.
$old->backup('handoff_base');
my ($oldsby) = make_old_standby_chain(
	source => $old,
	backup_name => 'handoff_base',
	install_path => $ENV{oldinstall},
	postgresql_conf => $old_conf,
	replication_user => 'upgrade_repl',
	standbys => [
		{
			name => 'old_standby',
			slot => $migrated_slot,
		}
	]);

wait_for_old_replay($old, $oldsby);
is( $oldsby->safe_psql(
		'postgres', 'SELECT pg_is_in_recovery(), count(*) FROM t'),
	't|2000',
	'handoff: old standby is serving the pre-upgrade data');

# Start HANDOFF log matching at the upgrade boundary.
my $logstart = -s $oldsby->logfile;

# Stop the source before pg_upgrade restarts it and shuts it down with HANDOFF.
$old->stop;

command_ok(
	[
		'pg_upgrade',
		'--old-datadir' => $old->data_dir,
		'--new-datadir' => $new->data_dir,
		'--old-bindir' => $old->config_data('--bindir'),
		'--new-bindir' => $new->config_data('--bindir'),
		'--socketdir' => $new->host,
		'--new-port' => $new->port,
		'--initdb',
		'--wal-upgrade',
		'--copy',
	],
	'primary: pg_upgrade emits the final handoff and succeeds');

my ($upgrade_cd) = run_command(
	[
		$new->config_data('--bindir') . '/pg_controldata', '-D',
		$new->data_dir
	]);
my ($upgrade_checkpoint) =
  $upgrade_cd =~ /Latest checkpoint location:\s+(\S+)/;
die 'primary has no post-upgrade shutdown checkpoint'
  unless defined $upgrade_checkpoint;

$oldsby->wait_for_log(
	qr/reached the final old-major shutdown checkpoint; pausing recovery for pg_upgrade/,
	$logstart);
ok( $oldsby->poll_query_until(
		'postgres', "SELECT pg_get_wal_replay_pause_state() = 'paused'"),
	'handoff: old standby persisted the final checkpoint and paused');

is( $oldsby->safe_psql(
		'postgres', 'SELECT pg_is_in_recovery(), count(*) FROM t'),
	't|2000',
	'handoff: paused standby remains read-only with old data');

$oldsby->stop;

configure_upgraded_primary($new, $new_conf);

$new->start;

# The upgraded primary must start read-write with unchanged data.
is($new->safe_psql('postgres', 'SELECT pg_is_in_recovery()'),
	'f', 'primary: serving read-write, not in recovery');
is($new->safe_psql('postgres', $fp_query),
	$want, 'primary: data preserved after upgrade');
is( $new->safe_psql(
		'postgres', 'SELECT timeline_id FROM pg_control_checkpoint()'),
	'2',
	'primary: upgrade continues the promoted source timeline');

# Detect catalog pages that still reference discarded WAL.
$new->safe_psql('postgres', 'CHECKPOINT');

# Point the retained standby at the new primary and restart it.  It must
# restore its HANDOFF pause and still serve the old data.
$oldsby->append_conf('postgresql.conf',
	"primary_conninfo = '" . $new->connstr . "'\n");
$logstart = -s $oldsby->logfile;
$oldsby->start;
$oldsby->wait_for_log(
	qr/restored pg_upgrade pause from the final old-major shutdown checkpoint/,
	$logstart);
ok( $oldsby->poll_query_until(
		'postgres', "SELECT pg_get_wal_replay_pause_state() = 'paused'"),
	'handoff: old standby restores the pause after restart');

$oldsby->stop;

is( $new->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_replication_slots "
		  . "WHERE slot_type = 'physical' AND slot_name = '$migrated_slot'"),
	'1',
	"primary: physical slot \"$migrated_slot\" migrated across the upgrade");

# Test an exclusive recovery target at the completion checkpoint and a retry
# from partially replayed data.
{
	my $partial = make_upgrade_standby(
		name => 'partial',
		source => $new,
		old_datadir => $oldsby->data_dir,
		postgresql_conf => $new_conf,
		extra_conf => "recovery_target_lsn = '$upgrade_checkpoint'\n"
		  . "recovery_target_inclusive = off\n");
	my $pdir = $partial->data_dir;

	my $started = $partial->start(fail_ok => 1);
	ok( !$started
		  && slurp_file($partial->logfile) =~
		  /requested recovery stop point is inside a pg_upgrade window/,
		'atomic: recovery target inside the upgrade window is rejected');

	my ($partial_cd) = run_command(
		[
			$partial->config_data('--bindir') . '/pg_controldata',
			'-D', $pdir
		]);
	like(
		$partial_cd,
		qr/wal-upgrade window finalized:\s+no/,
		'atomic: partial standby remains unfinalized');

	$partial->_update_pid(0);
	$started = $partial->start(fail_ok => 1);
	ok( !$started
		  && slurp_file($partial->logfile) =~
		  /pg_upgrade window was only partially applied/,
		'atomic: partial standby refuses a second start');
	$partial->_update_pid(0);

	rmtree($pdir);
}

# Start an initdb-created standby with the upgrade signals and retained source.
# Its data must survive restart without replaying the upgrade window again.
{
	my $olddir = $oldsby->data_dir;
	my $standby = make_upgrade_standby(
		name => 'standby',
		source => $new,
		old_datadir => $olddir,
		postgresql_conf => $new_conf,
		allows_streaming => 1,
		extra_conf => "wal_keep_size = '1GB'\n");
	my $sdir = $standby->data_dir;

	$standby->start;
	$standby->poll_query_until('postgres', 'SELECT count(*) = 2000 FROM t')
	  or die "standby did not converge to the upgraded data in time";
	$new->wait_for_catchup($standby, 'replay', $new->lsn('insert'));

	is( $standby->safe_psql('postgres', $fp_query),
		$want,
		'standby: converged to the upgraded primary data from the WAL window'
	);

	my $sby_bindir = $standby->config_data('--bindir');
	my ($cd_out) = run_command([ "$sby_bindir/pg_controldata", '-D', $sdir ]);
	ok( !-f "$sdir/pg_upgrade.signal"
		  && $cd_out =~ /wal-upgrade window finalized:\s+yes/
		  && $standby->safe_psql(
			'postgres',
			"SELECT checkpoint_lsn >= '$upgrade_checkpoint'::pg_lsn "
			  . 'FROM pg_control_checkpoint()') eq 't',
		'standby: completion restartpoint finalizes upgrade replay');

	$standby->stop('immediate');
	$standby->start;
	is( $standby->safe_psql(
			'postgres', "SELECT pg_is_in_recovery(), f.* FROM ($fp_query) f"),
		"t|$want",
		'standby: finalized replay survives restart');
	$standby->stop;
}

# Start a streaming upgrade standby with pg_upgrade.signal but without
# pg_upgrade_standby_old_datadir. Startup fails before serving.
{
	my $norelink = make_upgrade_standby(
		name => 'norelink',
		source => $new,
		postgresql_conf => $new_conf);
	my $started = $norelink->start(fail_ok => 1);
	ok( !$started
		  && slurp_file($norelink->logfile) =~
		  /streaming --wal-upgrade skeleton requires "pg_upgrade_standby_old_datadir"/,
		'negative: standby requires its retained old data directory');

	# Record the failed startup before continuing to the link-mode case.
	$norelink->_update_pid(0);
}

$new->stop;

# Replay link-mode FILE INHERIT records from a stopped old standby. Mirror mode
# creates hard links in the new-major skeleton.
{
	my $link_new = PostgreSQL::Test::Cluster->new('link_new');

	my $link_old = PostgreSQL::Test::Cluster->new('link_old',
		install_path => $ENV{oldinstall});
	if (defined($ENV{oldinstall}))
	{
		$link_old->init(allows_streaming => 1, extra => ['-k']);
	}
	else
	{
		$link_old->init(allows_streaming => 1);
	}
	$link_old->append_conf('postgresql.conf', $old_conf);
	$link_old->append_conf('postgresql.conf',
		"listen_addresses = '127.0.0.1'");
	$link_old->append_conf('pg_hba.conf',
		"host replication link_repl 127.0.0.1/32 trust\n");
	$link_old->start;
	$link_old->safe_psql(
		'postgres', q{
		CREATE ROLE link_repl LOGIN REPLICATION;
		CREATE TABLE link_t (id int primary key, v text);
		INSERT INTO link_t
		  SELECT g, repeat(md5(g::text), 8) FROM generate_series(1, 256) g;
		SELECT pg_create_physical_replication_slot('link_slot', true);
	});
	my $link_fingerprint =
	  'SELECT count(*), sum(id), sum(hashtext(v)::bigint) FROM link_t';
	my $link_want = $link_old->safe_psql('postgres', $link_fingerprint);

	$link_old->backup('link_handoff_base');
	my ($link_old_standby) = make_old_standby_chain(
		source => $link_old,
		backup_name => 'link_handoff_base',
		install_path => $ENV{oldinstall},
		postgresql_conf => $old_conf,
		replication_user => 'link_repl',
		standbys => [
			{
				name => 'link_old_standby',
				slot => 'link_slot',
			}
		]);
	wait_for_old_replay($link_old, $link_old_standby);
	die 'link standby did not receive the source relation'
	  unless $link_old_standby->safe_psql('postgres', $link_fingerprint) eq
	  $link_want;
	my $link_old_relpath = $link_old_standby->safe_psql('postgres',
		"SELECT pg_relation_filepath('link_t')");
	my $link_source_file = $link_old_standby->data_dir . "/$link_old_relpath";

	my $link_logstart = -s $link_old_standby->logfile;
	$link_old->stop;
	command_ok(
		[
			'pg_upgrade',
			'--old-datadir' => $link_old->data_dir,
			'--new-datadir' => $link_new->data_dir,
			'--old-bindir' => $link_old->config_data('--bindir'),
			'--new-bindir' => $link_new->config_data('--bindir'),
			'--socketdir' => $link_new->host,
			'--new-port' => $link_new->port,
			'--initdb',
			'--wal-upgrade',
			'--link',
		],
		'link: primary upgrade emits an inherited-file window');

	$link_old_standby->wait_for_log(
		qr/reached the final old-major shutdown checkpoint; pausing recovery for pg_upgrade/,
		$link_logstart);
	ok( $link_old_standby->poll_query_until(
			'postgres', "SELECT pg_get_wal_replay_pause_state() = 'paused'"),
		'link: old standby pauses at the HANDOFF checkpoint');
	$link_old_standby->stop;
	die 'link standby did not retain the inherited relation file'
	  unless -f $link_source_file;

	configure_upgraded_primary($link_new, $new_conf);
	$link_new->start;
	is($link_new->safe_psql('postgres', $link_fingerprint),
		$link_want, 'link: upgraded primary preserves the source data');

	my $link_standby = make_upgrade_standby(
		name => 'link_standby',
		source => $link_new,
		old_datadir => $link_old_standby->data_dir,
		postgresql_conf => $new_conf,
		allows_streaming => 1);
	$link_standby->start;
	$link_standby->poll_query_until('postgres',
		'SELECT count(*) = 256 FROM link_t')
	  or die "link standby did not converge to the upgraded data in time";
	$link_new->wait_for_catchup($link_standby, 'replay',
		$link_new->lsn('insert'));
	is($link_standby->safe_psql('postgres', $link_fingerprint),
		$link_want, 'link: new standby replays the inherited-file window');

	my $link_new_relpath = $link_standby->safe_psql('postgres',
		"SELECT pg_relation_filepath('link_t')");
	my $link_target_file = $link_standby->data_dir . "/$link_new_relpath";
	die 'link replay did not place the inherited relation'
	  unless -f $link_target_file;
	my @source_stat = stat($link_source_file);
	my @target_stat = stat($link_target_file);
  SKIP:
	{
		skip 'inode identity is not portable on Windows', 1
		  if $PostgreSQL::Test::Utils::windows_os;
		is_deeply(
			[ @target_stat[ 0, 1 ] ],
			[ @source_stat[ 0, 1 ] ],
			'link: source and target have the same device and inode');
	}

	$link_standby->stop;
	$link_new->stop;
}

require WalUpgradeCascade;

done_testing();
