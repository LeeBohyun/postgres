# Copyright (c) 2025-2026, PostgreSQL Global Development Group

# Tests for "pg_upgrade --wal-upgrade" delivering the upgrade to a physical
# standby by STREAMING the upgrade window from the live, upgraded primary.
#
# Under --wal-upgrade the whole upgrade is captured as WAL (the CN..COMPLETE
# "window"), and a migrated physical replication slot pins that window in the
# primary's pg_wal/ so it survives the upgrade and stays streamable.  A new-version
# skeleton that points primary_conninfo at the upgraded primary DERIVES the window
# anchor (CN) LOCALLY from its retained old data directory (reproducing
# pg_resetwal's byte-contiguous placement), taking only the system identifier from
# the primary's standard IDENTIFY_SYSTEM, arms its control file at CN, and streams
# the window forward -- becoming a hot standby that serves the upgraded data.  No
# operator "prepare" step, no hand-copied WAL, and no bespoke replication command
# are required.
#
# ---------------------------------------------------------------------------
# ASSUMPTIONS AND INVARIANTS the standby path relies on (why this works):
#
#  1. Relfilenode/OID identity (holds for ALL transfer modes).  pg_upgrade
#     preserves every user relation's relfilenumber and its database/tablespace
#     OIDs; the transfer step uses that same number for the file on both sides
#     and the mode (copy/clone/copy_file_range/link/swap) changes only HOW the
#     file is moved, never its name.  So a user relation has the SAME on-disk
#     path in the old cluster and the upgraded new cluster.  This identity is
#     what lets the standby place a user file at the identical relative path
#     from the old datadir -- no catalog lookup -- however the mode places it
#     (copy/clone/copy_file_range/link/swap; see invariant 3).
#
#  2. Schema-only window.  The streamed window carries only what pg_upgrade
#     rewrote: system catalogs, SLRU, the directory skeleton, pg_filenode.map /
#     PG_VERSION -- NOT user relations (which pg_upgrade transfers unchanged).
#     The window is therefore schema-sized, not data-sized.  Instead the window
#     includes an XLOG_UPGRADE_RELINK manifest naming exactly the user files it
#     omitted (emitted from the same capture walk, so the two cannot drift).
#
#  3. User relations come from the standby's OWN retained old datadir.  A
#     streaming standby is a new-version skeleton -- either a fresh initdb or a
#     bare data dir whose new-version pg_control/PG_VERSION are synthesized at
#     startup (SynthesizeUpgradeStreamControlFile; initdb is optional) -- plus an
#     empty "pg_upgrade.signal" sentinel; its retained pre-upgrade data directory
#     is named by the pg_upgrade_standby_old_datadir GUC in postgresql.conf.  Redo of the
#     RELINK manifest processes it one relation-file segment at a time, placing
#     each from the old datadir at the identical relative path in the skeleton.
#     By default (pg_upgrade_standby_transfer_mode = mirror) it reproduces the primary's
#     transfer mode, but the standby MAY choose a different mode via that GUC:
#       - copy: an independent whole-file copy (copy_file()); 2x space.
#       - clone: a reflink / copy-on-write clone (copyfile(COPYFILE_CLONE_FORCE)
#         on macOS, ioctl(FICLONE) on Linux) -- near-zero extra space, exactly as
#         pg_upgrade --clone does on the primary.
#       - copy_file_range: copy_file_range(), a server-side/reflink copy.
#         The redo reproduces each mode's RESULT with the same primitive
#         pg_upgrade uses; if a reflink mode's filesystem can't reflink, redo
#         FATALs rather than silently fall back to a 2x copy.  In all three the
#         old file is only read, so the old datadir stays a bootable rollback
#         target.
#       - link: a per-file hardlink (link()), sharing the old inode.
#       - swap: a rename(), MOVING the file out of the old datadir into the
#         skeleton -- upstream --swap renames whole database directories, and a
#         per-file rename here reproduces that (no entry left in the old datadir,
#         unlike --link).
#         Both link and swap are fast and space-free and intentionally sacrifice
#         the old datadir -- the tradeoff the operator accepted by choosing
#         --link/--swap on the primary (which disables its own old cluster).
#     The primary can't know the standby's old-datadir path, so it is supplied
#     locally via pg_upgrade_standby_old_datadir; nothing new is streamed for it.
#
#  4. Recovery anchors at CN.  Recovery starts at the end-of-upgrade checkpoint
#     (CN); the XID/OID/multixact counters were transplanted into pg_control
#     before CN, so the CN checkpoint record carries them.  The skeleton fetches
#     CN (sysid + LSN + redo + TLI) from the primary over the replication
#     connection and arms its control file at CN before replay.
#
#  5. WAL page formats differ across majors.  The new-format window is
#     unreadable by an old-version standby; conversely a fresh skeleton must be
#     the NEW (in-tree) version.  This is why an existing old standby cannot just
#     follow the upgrade: it pauses at the handoff (below) -- staying up to serve
#     reads -- and is replaced by a new-version skeleton that streams the window.
#
# This test drives that path and, at each step, asserts the state transitions
# land where the state machine says they should:
#
#   handoff:   an EXISTING old-version standby streaming from the old primary,
#              on --wal-upgrade-signal-handoff, replays XLOG_UPGRADE_HANDOFF and
#              PAUSES recovery at the boundary (did not promote or shut down),
#              staying up to serve read-only queries while a new-version skeleton
#              is provisioned.  It is then stopped and its retained datadir
#              becomes the relink source for the skeleton below.  Same-version
#              only: the handoff record exists solely in the patched binary.
#   abort:     the handoff pause is reversible -- if the upgrade is abandoned, the
#              old primary is restarted read-write and the standby resumes replay
#              past the (no-op) handoff record and keeps following it.
#   primary:   old (vN)  --pg_upgrade--> auto-served new primary (not in
#              recovery), retention slot present, window pinned in pg_wal/.
#   standby:   a fresh new-version skeleton + primary_conninfo +
#              pg_upgrade_standby_old_datadir + an empty pg_upgrade.signal sentinel
#              --start--> auto-armed from the primary, STREAMED the window, and
#              the RELINK redo copied the user relations in; came up in recovery
#              (pg_is_in_recovery = t), converged to the primary's data, and the
#              copies are inode-independent (old datadir left intact).
#   no-initdb: the same, but from a BARE data dir (config + sentinel only, never
#              initdb'd) -- checkDataDir() synthesizes pg_control + PG_VERSION from
#              this binary's constants, so no initdb is required to stream.
#   negative:  streaming the window into a skeleton with pg_upgrade_standby_old_datadir
#              unset leaves the user relations ABSENT -- the window alone does not
#              reconstruct user data, proving the relink step is load-bearing.
#              And a fresh cluster with no primary and no window is just an
#              ordinary empty cluster (no silent alternative delivery path).
#
# Cross-version: when $ENV{oldinstall} is set the old primary is built with an
# older major's binaries; otherwise the test runs same-version.  Either way the
# STANDBY skeleton is always the new (in-tree) version, since streaming a window
# into an older-version standby is not the supported direction.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# pg_upgrade writes output files relative to the current directory.
chdir ${PostgreSQL::Test::Utils::tmp_check};

# Config that lets a node act as a streaming-replication primary.
# Use values >= what PostgreSQL::Test::Cluster's allows_streaming default sets
# (max_wal_senders = 10), so a hot standby replaying this primary's WAL does not
# abort with "insufficient parameter settings".
my $primary_conf = q{
wal_level = replica
max_wal_senders = 10
max_replication_slots = 10
hot_standby = on
};

#
# 1. Old primary with data -> upgrade -> auto-served new primary.
#
my $old =
  PostgreSQL::Test::Cluster->new('old', install_path => $ENV{oldinstall});
if (defined($ENV{oldinstall}))
{
	# Checksums default on from v18; pass -k on older installs so a checksum-on
	# new cluster can be upgraded.
	$old->init(allows_streaming => 1, extra => ['-k']);
}
else
{
	$old->init(allows_streaming => 1);
}
$old->append_conf('postgresql.conf', $primary_conf);
$old->start;
$old->safe_psql(
	'postgres', qq{
	CREATE TABLE t (id int primary key, v text);
	INSERT INTO t SELECT g, 'v' || g FROM generate_series(1, 2000) g;
	CREATE INDEX ON t (v);
	CREATE TABLE toasted (id int, big text);
	INSERT INTO toasted
	  SELECT g, repeat('abcdef0123456789', 3000) FROM generate_series(1, 300) g;
	-- Large objects live in pg_largeobject[_metadata], which pg_upgrade
	-- transfers verbatim as user data.  Under --wal-upgrade those catalogs are
	-- excluded from the window and named in the RELINK manifest instead, so the
	-- standby links them from its own retained old datadir like any user
	-- relation; seed some to exercise that path.
	SELECT lo_from_bytea(0, decode(repeat(md5(g::text), 50), 'hex'))
	  FROM generate_series(1, 40) g;
});
# Fold the large-object content into the fingerprint so a mis-delivered
# pg_largeobject on the standby (from WAL vs. relink) is caught by convergence.
my $fp_query = q{SELECT count(*), sum(hashtext(v)::bigint),
	(SELECT count(*) || ':' || coalesce(sum(length(data))::text, '0')
	   FROM pg_largeobject) FROM t};
my $want = $old->safe_psql('postgres', $fp_query);

# Create a NAMED physical replication slot on the old primary.  Under
# --wal-upgrade pg_upgrade migrates physical slots (stock pg_upgrade migrates
# only logical ones), so this slot must reappear on the upgraded primary under
# the SAME name -- preserving the standby's slot identity across the upgrade.
# (Only exercised same-version; a stock old major has no --wal-upgrade
# machinery, but the slot itself is ordinary and would migrate the same way.)
my $migrated_slot = 'my_standby_slot';
if (!defined($ENV{oldinstall}))
{
	$old->safe_psql('postgres',
		"SELECT pg_create_physical_replication_slot('$migrated_slot', true)");
}

#
# 1a. Handoff: an EXISTING old-version standby pauses at the upgrade point.
#
# --wal-upgrade-signal-handoff writes an XLOG_UPGRADE_HANDOFF record into the
# LIVE old primary's WAL and fast-stops the primary at that point.  A streaming
# old standby replays the record and PAUSES recovery at the boundary (it does not
# shut down or promote): it cannot follow the upgrade in the old WAL format, so it
# stays up serving read-only queries while a fresh new-version standby (section 2)
# is provisioned to take over by streaming the window.  The pause is reversible
# (see section 2b for the aborted-upgrade case).
#
# The handoff record (emitted by the old primary's own ShutdownXLOG) exists
# only in this patched binary, so this can only be exercised same-version; a
# stock old major cannot emit it.  Skip it (but still stop the primary) when
# running cross-version.
# The paused old standby is then stopped, and its retained datadir becomes the
# RELINK SOURCE for a fresh new-version skeleton in section 2 (it is never
# adopted/started in place -- the skeleton is a separate initdb'd cluster that
# links user relations from it), so declare it at outer scope (only populated on
# the same-version path, where the handoff can run).
my $oldsby;

if (!defined($ENV{oldinstall}))
{
	# A real old-version standby streaming from the live old primary.
	$old->backup('handoff_base');
	$oldsby = PostgreSQL::Test::Cluster->new('old_standby');
	$oldsby->init_from_backup($old, 'handoff_base', has_streaming => 1);
	$oldsby->append_conf('postgresql.conf', $primary_conf);
	$oldsby->start;

	# It is a hot standby that has converged to the primary's data.
	$old->wait_for_catchup($oldsby, 'replay', $old->lsn('insert'));
	is($oldsby->safe_psql('postgres', 'SELECT pg_is_in_recovery()'),
		't', 'handoff: old standby is a hot standby before the handoff');
	is($oldsby->safe_psql('postgres', 'SELECT count(*) FROM t'),
		'2000', 'handoff: old standby has the pre-upgrade data');

	# Record the current end of the standby's log so wait_for_log() below only
	# matches the handoff message emitted from this point on.
	my $logstart = -s $oldsby->logfile;

	# Signal the handoff: writes the trigger into the running old primary's WAL
	# and fast-stops the primary at that point.
	command_ok(
		[
			'pg_upgrade', '--no-sync',
			'--wal-upgrade-signal-handoff',
			'--old-datadir' => $old->data_dir,
			'--old-bindir' => $old->config_data('--bindir'),
			'--socketdir' => $old->host,
			'--old-port' => $old->port,
		],
		'handoff: signal-handoff writes the trigger and stops the old primary');

	# pg_upgrade shut the old primary down itself; sync the framework's state.
	$old->_update_pid(0);

	# The old standby replays the handoff and PAUSES recovery at the boundary
	# (rather than shutting down): it cannot follow the upgrade in the old WAL
	# format, but it stays up serving read-only queries while a fresh
	# new-version skeleton is provisioned.  Wait for the pause message.
	$oldsby->wait_for_log(
		qr/reached pg_upgrade handoff on standby; pausing recovery at the upgrade boundary/,
		$logstart);
	ok(1, 'handoff: old standby replayed the handoff and paused at the boundary');

	# It paused, it did not shut down or promote: the postmaster is still up and
	# still in recovery, and it still answers read-only queries at the
	# pre-upgrade snapshot.  This is the read-only-availability guarantee: the
	# standby keeps serving through the upgrade instead of going dark.
	ok(-f $oldsby->data_dir . '/postmaster.pid',
		'handoff: old standby is still running after the handoff (paused, not stopped)');
	is($oldsby->safe_psql('postgres', 'SELECT pg_is_in_recovery()'),
		't', 'handoff: paused standby is still in recovery (did not promote)');
	is($oldsby->safe_psql('postgres', 'SELECT count(*) FROM t'),
		'2000', 'handoff: paused standby still serves the pre-upgrade data read-only');

	# Retire the paused standby in favor of the new skeleton built in section 2.
	# A clean stop leaves its data directory a valid, bootable vN cluster; the
	# skeleton's RELINK redo (default copy mode) only READS these files, so the
	# old datadir stays intact both as the relink source and as a rollback
	# target.
	$oldsby->stop;
}
else
{
	$old->stop;
}

# The new primary is created by --initdb; do NOT init() it here.
my $new = PostgreSQL::Test::Cluster->new('new');

command_ok(
	[
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
	],
	'primary: pg_upgrade --wal-upgrade --initdb succeeds');

# The --initdb-created new cluster skipped init(); append the settings the test
# harness needs to start and stream from it.
my $conf = $new->data_dir . '/postgresql.conf';
open(my $fh, '>>', $conf) or die "could not open $conf: $!";
print $fh "\n# added by test\n";
print $fh "port = " . $new->port . "\n";
print $fh "listen_addresses = '"
  . ($PostgreSQL::Test::Utils::windows_os ? '127.0.0.1' : '') . "'\n";
print $fh "unix_socket_directories = '" . $new->host . "'\n";
print $fh $primary_conf;
close($fh);

# Trust replication + normal connections locally so the standby can connect.
$new->append_conf('pg_hba.conf',
	"local replication all trust\nhost replication all 127.0.0.1/32 trust\nhost replication all ::1/128 trust\n"
);

$new->start;

# The upgraded primary comes up read-write (not in recovery) with its data.
is($new->safe_psql('postgres', 'SELECT pg_is_in_recovery()'),
	'f', 'primary: serving read-write, not in recovery');
is($new->safe_psql('postgres', $fp_query),
	$want, 'primary: data preserved after upgrade');

# The retention slot that pins the upgrade window is present on the live
# primary, so the window is streamable to a standby.
my $nslots = $new->safe_psql('postgres',
	"SELECT count(*) FROM pg_replication_slots WHERE slot_type = 'physical'");
ok($nslots >= 1, "primary: retention slot present ($nslots physical slot(s))");

# Physical-slot migration and reuse: the named slot created on the old primary
# must have been recreated on the upgraded primary under the same name, and --
# because a migrated slot already pins the window -- the dedicated
# UPGRADE_WINDOW_SLOT ("pg_upgrade_window") must NOT have been created.  So the
# migrated slot is reused as the window pin (only checked same-version, where
# the old cluster actually had a physical slot).
if (!defined($ENV{oldinstall}))
{
	is( $new->safe_psql(
			'postgres',
			"SELECT count(*) FROM pg_replication_slots "
			  . "WHERE slot_type = 'physical' AND slot_name = '$migrated_slot'"),
		'1',
		"primary: physical slot \"$migrated_slot\" migrated across the upgrade");

	is( $new->safe_psql(
			'postgres',
			"SELECT count(*) FROM pg_replication_slots "
			  . "WHERE slot_name = 'pg_upgrade_window'"),
		'0',
		"primary: dedicated retention slot not created when a physical slot is reused");
}

#
# 2. Fresh skeleton streams the window; the RELINK manifest links user data.
#
# The upgrade window carries only the files pg_upgrade touched (system catalogs,
# SLRU, skeleton) -- NOT user relations, so it is schema-sized, not data-sized.
# Instead the window includes an XLOG_UPGRADE_RELINK manifest naming the user
# relation files pg_upgrade transferred; on redo the standby places each from its
# own retained old datadir into the fresh skeleton.  With pg_upgrade_standby_transfer_mode
# left at the default (mirror) it reproduces the primary's mode; the primary here
# used --copy, so each is an independent copy and the old datadir stays an intact
# bootable cluster -- the inode check below asserts exactly that.  The old
# datadir's path is given by the pg_upgrade_standby_old_datadir GUC.  So a standby is a
# fresh new-version initdb skeleton + a streaming connection + those config
# settings + an empty pg_upgrade.signal sentinel; it never runs pg_upgrade.
#
# The retained old datadir here is the stood-down old standby from section 1a,
# so this only runs same-version, where that standby exists.  (The mechanism is
# version-independent; only the same-version test rig for the old datadir is.)
SKIP:
{
	skip 'relink-manifest test reuses the same-version stood-down old standby', 10
	  if defined($ENV{oldinstall}) || !defined($oldsby);

	my $olddir = $oldsby->data_dir;

	# The old datadir still has the user relation files from before the upgrade;
	# they are the relink source and never travel through the window.
	my @userrels = glob("$olddir/base/*/[0-9]*");
	ok(scalar(@userrels) > 0,
		'standby: retained old datadir has the user relation files (relink source)');

	# A FRESH new-version skeleton -- initdb'd, empty of user data -- is the
	# standby target.  It points primary_conninfo at the upgraded primary,
	# names the retained old datadir in the pg_upgrade_standby_old_datadir GUC, and
	# stages an EMPTY pg_upgrade.signal sentinel; startup infers the streaming
	# mode from primary_conninfo, streams the window, and its RELINK redo copies
	# the user relations in from that old datadir.  pg_upgrade_standby_transfer_mode is
	# left unset ("mirror"), so the standby reproduces the primary's --copy.
	my $standby = PostgreSQL::Test::Cluster->new('standby');
	$standby->init;
	$standby->append_conf('postgresql.conf', $primary_conf);
	my $sdir = $standby->data_dir;
	$standby->append_conf('postgresql.conf',
		"primary_conninfo = '" . $new->connstr . "'\n");
	$standby->append_conf('postgresql.conf',
		"pg_upgrade_standby_old_datadir = '$olddir'\n");
	open(my $ss, '>', "$sdir/pg_upgrade.signal") or die $!;    # empty sentinel
	close($ss);
	$standby->set_standby_mode;    # standby.signal

	# Before streaming, the skeleton has no user table 't' data of its own.
	$standby->start;

	# It auto-armed and streamed the window.
	like(slurp_file($standby->logfile),
		qr/auto-armed streaming standby from locally derived anchor/,
		'standby: auto-armed from the primary over replication');

	# It converges: the window supplied the system catalogs and the RELINK
	# manifest linked the user relations from the old datadir, so the user table
	# (and its large objects) match the primary.
	$standby->poll_query_until('postgres', 'SELECT count(*) = 2000 FROM t')
	  or die "standby did not converge to the upgraded data in time";
	$new->wait_for_catchup($standby, 'replay', $new->lsn('insert'));

	is( $standby->safe_psql('postgres', $fp_query),
		$want,
		'standby: converged to the upgraded primary data (window + relink manifest)');

	# The user relation files are now present in the skeleton.
	my @linked = glob("$sdir/base/*/[0-9]*");
	ok(scalar(@linked) > 0,
		'standby: user relation files copied into the skeleton by RELINK redo');

	# They are COPIES, not hardlinks: a skeleton user file and its old-datadir
	# source have different inodes, so the old datadir is untouched and stays a
	# bootable cluster to roll back to (upstream's copy-transfer guarantee).
	{
		my $rel = $linked[0];
		$rel =~ s{^\Q$sdir\E/}{};    # e.g. base/5/16384
		my $ino_new = (stat("$sdir/$rel"))[1];
		my $ino_old = (stat("$olddir/$rel"))[1];
		isnt($ino_new, $ino_old,
			'standby: user file is an independent copy, not a hardlink (old datadir intact)');
	}

	# Restart with the pg_upgrade.signal sentinel STILL staged (the operator need
	# not remove it): a converged standby must come up as an ordinary hot standby
	# and must NOT re-fetch the anchor / re-arm at the old CN.  The durable
	# pg_control upgrade_finalized flag makes first-startup arming no-op.
	ok(-f "$sdir/pg_upgrade.signal",
		'standby: sentinel still present after convergence (not auto-removed)');
	my $sby_bindir = $standby->config_data('--bindir');
	my ($cd_out) = run_command([ "$sby_bindir/pg_controldata", '-D', $sdir ]);
	like($cd_out, qr/wal-upgrade window finalized:\s+yes/,
		'standby: durable upgrade_finalized flag set after convergence');
	my $log_before_restart = -s $standby->logfile;
	$standby->restart;
	is($standby->safe_psql('postgres', 'SELECT pg_is_in_recovery()'),
		't', 'standby: restarts as an ordinary hot standby');
	is($standby->safe_psql('postgres', $fp_query),
		$want, 'standby: data still matches the primary after restart');
	# The finalized guard must have short-circuited arming: no new "auto-armed"
	# line since the restart (re-arming would rewind recovery behind the standby's
	# replay position, and fail once the primary drops the retention slot).
	my $log_after = slurp_file($standby->logfile, $log_before_restart);
	unlike($log_after, qr/auto-armed streaming standby from locally derived anchor/,
		'standby: converged restart does NOT re-arm at the old CN (finalized guard)');

	$standby->stop;
}

#
# 2b. Transfer-mode override: the standby need not mirror the primary's mode.
#
# The primary upgraded with --copy, but this standby sets
# pg_upgrade_standby_transfer_mode = link.  The RELINK redo must then HARDLINK each user
# relation from the old datadir instead of copying it, so a skeleton user file
# and its old-datadir source share one inode -- the exact inverse of section 2's
# copy assertion.  This proves the mode comes from the standby's own config, not
# the manifest.  Reuses the stood-down old standby datadir, so same-version only.
#
SKIP:
{
	skip 'transfer-mode override test reuses the same-version stood-down old standby', 3
	  if defined($ENV{oldinstall}) || !defined($oldsby);

	my $olddir = $oldsby->data_dir;

	my $linksby = PostgreSQL::Test::Cluster->new('linksby');
	$linksby->init;
	$linksby->append_conf('postgresql.conf', $primary_conf);
	my $ldir = $linksby->data_dir;
	$linksby->append_conf('postgresql.conf',
		"primary_conninfo = '" . $new->connstr . "'\n");
	$linksby->append_conf('postgresql.conf',
		"pg_upgrade_standby_old_datadir = '$olddir'\n");
	# Override: place user relations by hardlink even though the primary copied.
	$linksby->append_conf('postgresql.conf',
		"pg_upgrade_standby_transfer_mode = 'link'\n");
	open(my $ls, '>', "$ldir/pg_upgrade.signal") or die $!;    # empty sentinel
	close($ls);
	$linksby->set_standby_mode;
	$linksby->start;

	$linksby->poll_query_until('postgres', 'SELECT count(*) = 2000 FROM t')
	  or die "link-mode standby did not converge to the upgraded data in time";
	$new->wait_for_catchup($linksby, 'replay', $new->lsn('insert'));
	is($linksby->safe_psql('postgres', $fp_query),
		$want, 'override: link-mode standby converged to the upgraded data');

	# At least one relation file in the skeleton must share an inode with its
	# counterpart in the old datadir -- i.e. be a hardlink.  User relations (the
	# ones in the RELINK manifest) are hardlinked under the link override;
	# system catalogs, which arrive as fresh RELFILE images in the window, never
	# share an inode.  Section 2 (copy) has ZERO shared inodes, so a nonzero
	# count here proves pg_upgrade_standby_transfer_mode overrode the primary's --copy.
	my @relfiles = glob("$ldir/base/*/[0-9]*");
	ok(scalar(@relfiles) > 0,
		'override: relation files present in the link-mode skeleton');
	my $shared = 0;
	for my $f (@relfiles)
	{
		(my $rel = $f) =~ s{^\Q$ldir\E/}{};
		my $ino_new = (stat("$ldir/$rel"))[1];
		my $ino_old = (stat("$olddir/$rel"))[1];
		$shared++ if defined $ino_old && $ino_new == $ino_old;
	}
	ok($shared > 0,
		"override: $shared user relation file(s) hardlinked to the old datadir (mode overridden to link)");

	$linksby->stop;
}

#
# 2c. No initdb: a BARE skeleton (empty data dir, never initdb'd) streams the
#     window just like an initdb'd one.
#
# The postmaster needs a valid new-version pg_control before it can load it and
# start recovery -- but that file is synthesizable from this binary's own
# constants, so a real initdb is not required.  Stage a data directory with only
# the standby-local config (postgresql.conf, pg_hba.conf), an empty
# pg_upgrade.signal sentinel, and standby.signal -- NO PG_VERSION, NO pg_control,
# NO base/.  At first startup checkDataDir() synthesizes pg_control + PG_VERSION +
# the subdir skeleton (logs "no initdb was required"), the anchor is derived
# locally, the window streams, and the RELINK redo links user relations from the
# retained old datadir.  Same convergence as section 2, without initdb.
#
# Driven with pg_ctl/psql directly (not $node->init, which runs initdb): a bare
# skeleton is exactly what has no initdb.
#
SKIP:
{
	skip 'no-initdb bare-skeleton test reuses the same-version stood-down old standby', 4
	  if defined($ENV{oldinstall}) || !defined($oldsby);

	my $olddir = $oldsby->data_dir;

	# A separate PostgreSQL::Test::Cluster gives us its allocated port, host
	# (socket dir), data_dir path, and logfile -- but we do NOT call ->init, so
	# no initdb runs and the data directory starts out empty.
	my $bare = PostgreSQL::Test::Cluster->new('bareskel');
	my $bdir = $bare->data_dir;
	mkdir $bdir or die "could not create $bdir: $!";
	chmod 0700, $bdir;
	# ->init normally makes these; ->start/->backup teardown expect them to exist.
	mkdir $bare->backup_dir;
	mkdir $bare->archive_dir;

	# Stage only the standby-local config initdb would otherwise write, plus the
	# streaming settings and the empty sentinel.  Everything else (pg_control,
	# PG_VERSION, the directory skeleton) is synthesized at first startup.
	my $sockdir = $bare->host;
	open(my $bc, '>', "$bdir/postgresql.conf") or die $!;
	print $bc "port = " . $bare->port . "\n";
	print $bc "listen_addresses = '"
	  . ($PostgreSQL::Test::Utils::windows_os ? '127.0.0.1' : '') . "'\n";
	print $bc "unix_socket_directories = '$sockdir'\n";
	print $bc "hot_standby = on\n";
	print $bc "primary_conninfo = '" . $new->connstr . "'\n";
	print $bc "pg_upgrade_standby_old_datadir = '$olddir'\n";
	close($bc);
	open(my $bh, '>', "$bdir/pg_hba.conf") or die $!;
	print $bh "local all all trust\n";
	print $bh "host all all 127.0.0.1/32 trust\n";
	print $bh "host all all ::1/128 trust\n";
	close($bh);
	open(my $bs, '>', "$bdir/pg_upgrade.signal") or die $!;    # empty sentinel
	close($bs);
	open(my $bst, '>', "$bdir/standby.signal") or die $!;
	close($bst);

	# It really is bare -- initdb's outputs are absent before startup.
	ok(!-f "$bdir/PG_VERSION" && !-f "$bdir/global/pg_control",
		'no-initdb: skeleton has no PG_VERSION / pg_control before startup (never initdb\'d)');

	$bare->start;

	# checkDataDir() synthesized the control file rather than finding one.
	like(slurp_file($bare->logfile),
		qr/synthesized a fresh pg_control and PG_VERSION/,
		'no-initdb: pg_control + PG_VERSION synthesized at startup (no initdb was required)');

	# Same convergence as the initdb'd standby: window streamed, user relations
	# relinked, data matches the primary.
	$bare->poll_query_until('postgres', 'SELECT count(*) = 2000 FROM t')
	  or die "no-initdb standby did not converge to the upgraded data in time";
	$new->wait_for_catchup($bare, 'replay', $new->lsn('insert'));
	is($bare->safe_psql('postgres', $fp_query),
		$want, 'no-initdb: bare skeleton converged to the upgraded primary data');
	is($bare->safe_psql('postgres', 'SELECT pg_is_in_recovery()'),
		't', 'no-initdb: bare skeleton is a hot standby (in recovery)');

	$bare->stop;
}

#
# 2a. Negative control: the RELINK manifest is load-bearing, and a missing
#     old-datadir path is a hard error, not a silent partial recovery.
#
# Stream the SAME schema-only window into a fresh skeleton with the
# pg_upgrade.signal sentinel present (so it arms and streams) but
# pg_upgrade_standby_old_datadir UNSET.  The RELINK redo would have no source, so the
# user relations -- which were never in the window -- could only come up ABSENT.
# Rather than silently bring up a hot standby missing all its user data, the redo
# must FATAL at the manifest.  This proves both that the relink step is
# load-bearing AND that its misconfiguration fails loudly.  Runs only
# same-version, where $new streams.
#
SKIP:
{
	skip 'negative relink test needs the live streaming primary', 2
	  if defined($ENV{oldinstall});

	my $norelink = PostgreSQL::Test::Cluster->new('norelink');
	$norelink->init;
	$norelink->append_conf('postgresql.conf', $primary_conf);
	my $ndir = $norelink->data_dir;
	# primary_conninfo + an EMPTY pg_upgrade.signal, but NO pg_upgrade_standby_old_datadir.
	# With LOCAL CN derivation the retained old datadir is needed UP FRONT (to
	# derive CN from its control file before streaming), so the skeleton FATALs at
	# startup rather than later at relink redo.
	$norelink->append_conf('postgresql.conf',
		"primary_conninfo = '" . $new->connstr . "'\n");
	$norelink->set_standby_mode;
	open(my $ns, '>', "$ndir/pg_upgrade.signal") or die $!;
	close($ns);

	# Start fails: the startup process FATALs immediately, so do not use ->start,
	# which die()s on a non-running server; launch via pg_ctl and check the
	# outcome ourselves.
	my $logstart = -s $norelink->logfile;
	$norelink->start(fail_ok => 1);

	# Startup FATALs because CN cannot be derived without the retained old
	# datadir, refusing to bring the standby up with no anchor / user data.
	$norelink->wait_for_log(
		qr/streaming --wal-upgrade skeleton requires "pg_upgrade_standby_old_datadir" to derive the upgrade anchor/,
		$logstart);
	ok(1,
		'negative: startup FATALs when the streaming skeleton has no old datadir to derive CN (no silent partial recovery)');

	# The server must NOT be serving: a standby missing all user data never opens.
	my ($rc, $stdout, $stderr) =
	  $norelink->psql('postgres', 'SELECT 1', timeout => 10);
	isnt($rc, 0,
		'negative: standby does not come up when the relink source is missing');

	# The startup process FATALed, taking the postmaster down after pg_ctl had
	# already reported "started", so the framework still thinks this node is
	# running.  Sync its state to stopped so END-time teardown does not try (and
	# fail) to stop an already-dead server.
	$norelink->_update_pid(0);
}

#
# 2b. Aborted upgrade: the paused standby resumes and keeps following the old
#     primary.  The handoff pause is reversible, so a --wal-upgrade that is
#     signaled but then abandoned does not strand the standby -- the operator
#     restarts the old primary read-write and resumes replay, and the standby
#     streams the primary's new old-format WAL as before.
#
# This uses its own same-version primary/standby pair: the main $old cluster was
# consumed by the real upgrade above, and the handoff record only exists in the
# patched (same-version) binary.
#
SKIP:
{
	skip 'abort/resume test needs the same-version handoff record', 4
	  if defined($ENV{oldinstall});

	my $apri = PostgreSQL::Test::Cluster->new('abort_primary');
	$apri->init(allows_streaming => 1);
	$apri->append_conf('postgresql.conf', $primary_conf);
	$apri->start;
	$apri->safe_psql('postgres',
		'CREATE TABLE t (id int primary key); INSERT INTO t SELECT generate_series(1, 100)');

	$apri->backup('abort_base');
	my $asby = PostgreSQL::Test::Cluster->new('abort_standby');
	$asby->init_from_backup($apri, 'abort_base', has_streaming => 1);
	$asby->append_conf('postgresql.conf', $primary_conf);
	$asby->start;
	$apri->wait_for_catchup($asby, 'replay', $apri->lsn('insert'));

	my $logstart = -s $asby->logfile;

	# Signal the handoff (writes the trigger and stops the primary) but do NOT
	# run pg_upgrade -- this models an upgrade that was started then abandoned.
	command_ok(
		[
			'pg_upgrade', '--no-sync',
			'--wal-upgrade-signal-handoff',
			'--old-datadir' => $apri->data_dir,
			'--old-bindir' => $apri->config_data('--bindir'),
			'--socketdir' => $apri->host,
			'--old-port' => $apri->port,
		],
		'abort: signal-handoff writes the trigger and stops the primary');
	$apri->_update_pid(0);

	# The standby pauses at the boundary (it does not shut down).
	$asby->wait_for_log(
		qr/reached pg_upgrade handoff on standby; pausing recovery at the upgrade boundary/,
		$logstart);
	is($asby->safe_psql('postgres', 'SELECT pg_is_in_recovery()'),
		't', 'abort: standby paused and still in recovery');

	# Abort: restart the old primary read-write (crash recovery treats the
	# handoff as a no-op) and generate more old-format WAL.
	$apri->start;
	is($apri->safe_psql('postgres', 'SELECT pg_is_in_recovery()'),
		'f', 'abort: old primary restarted read-write');
	$apri->safe_psql('postgres', 'INSERT INTO t SELECT generate_series(101, 200)');

	# Resume the standby: replay continues past the (no-op) handoff record and
	# the standby follows the old primary's new WAL.
	$asby->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
	$apri->wait_for_catchup($asby, 'replay', $apri->lsn('insert'));
	is($asby->safe_psql('postgres', 'SELECT count(*) FROM t'),
		'200', 'abort: resumed standby caught up to the old primary past the handoff');

	$asby->stop;
	$apri->stop;
}

#
# 3. Negative: a fresh new-version skeleton with NO primary and NO local window
#    starts as an ordinary cluster -- no silent alternative delivery path.
#
my $lonely = PostgreSQL::Test::Cluster->new('lonely');
$lonely->init(allows_streaming => 1);
$lonely->append_conf('postgresql.conf', $primary_conf);
$lonely->start;
is($lonely->safe_psql('postgres', 'SELECT pg_is_in_recovery()'),
	'f', 'negative: plain new cluster with no primary is a normal live cluster');
is($lonely->safe_psql('postgres', "SELECT count(*) FROM pg_tables WHERE tablename = 't'"),
	'0', 'negative: no upgrade window was silently applied');
$lonely->stop;

$new->stop;

done_testing();
