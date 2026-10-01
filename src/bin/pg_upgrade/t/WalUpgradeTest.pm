# Copyright (c) 2026, PostgreSQL Global Development Group

package WalUpgradeTest;

use strict;
use warnings FATAL => 'all';

use Exporter 'import';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

our @EXPORT_OK = qw(
  make_old_standby_chain
  make_upgrade_standby
  slot_is_inactive_at
  upgrade_finalized
  wait_for_old_replay
  wait_for_physical_slot
  wait_for_slotless_streaming
);

sub wait_for_physical_slot
{
	my ($parent, $slot, $child) = @_;
	return $parent->poll_query_until(
		'postgres', qq{
		SELECT count(*) = 1
		FROM pg_replication_slots s
		JOIN pg_stat_replication r ON r.pid = s.active_pid
		WHERE s.slot_name = '$slot'
		  AND s.slot_type = 'physical'
		  AND NOT s.temporary
		  AND r.application_name = '$child'
		  AND r.state = 'streaming'
	});
}

sub wait_for_old_replay
{
	my ($primary, $standby) = @_;
	my $target = $primary->lsn('insert');
	$standby->poll_query_until('postgres',
		"SELECT COALESCE(pg_last_wal_replay_lsn() >= '$target'::pg_lsn, false)"
	) or die "old standby did not replay through $target";
}

# Create the old-major standby topology used by HANDOFF tests from one base
# backup. Each standby uses a persistent slot on its upstream server.
sub make_old_standby_chain
{
	my (%args) = @_;
	my $source = $args{source};
	my $parent = $source;
	my @standbys;

	for my $spec (@{ $args{standbys} })
	{
		if ($spec->{create_slot})
		{
			$parent->safe_psql('postgres',
				"SELECT pg_create_physical_replication_slot('$spec->{slot}', true)"
			);
		}

		my @node_options;
		push @node_options, install_path => $args{install_path}
		  if defined $args{install_path};
		my $standby =
		  PostgreSQL::Test::Cluster->new($spec->{name}, @node_options);
		$standby->init_from_backup($source, $args{backup_name},
			has_streaming => 1);
		$standby->append_conf('postgresql.conf', $args{postgresql_conf})
		  if defined $args{postgresql_conf};
		$standby->append_conf('pg_hba.conf', $args{replication_hba})
		  if $spec->{accepts_replication}
		  && defined $args{replication_hba};
		$standby->append_conf('postgresql.auto.conf',
				"primary_conninfo = 'host=127.0.0.1 port="
			  . $parent->port
			  . " user=$args{replication_user} application_name=$spec->{name}'\n"
			  . "primary_slot_name = '$spec->{slot}'\n");
		$args{configure}->($standby, $spec, $parent)
		  if defined $args{configure};
		$standby->start;
		$args{after_start}->($parent, $standby, $spec)
		  if defined $args{after_start};

		push @standbys, $standby;
		$parent = $standby;
	}

	return @standbys;
}

sub wait_for_slotless_streaming
{
	my ($parent, $child) = @_;
	return $parent->poll_query_until(
		'postgres', qq{
		SELECT count(*) = 1
		FROM pg_stat_replication r
		WHERE r.application_name = '$child'
		  AND r.state = 'streaming'
		  AND NOT EXISTS
		      (SELECT FROM pg_replication_slots s WHERE s.active_pid = r.pid)
	});
}

sub slot_is_inactive_at
{
	my ($node, $slot, $lsn) = @_;
	return $node->safe_psql(
		'postgres', qq{
		SELECT count(*) = 1
		FROM pg_replication_slots
		WHERE slot_name = '$slot'
		  AND slot_type = 'physical'
		  AND NOT temporary
		  AND NOT active
		  AND restart_lsn = '$lsn'::pg_lsn
		  AND wal_status IS DISTINCT FROM 'lost'
		  AND invalidation_reason IS NULL
	});
}

sub upgrade_finalized
{
	my ($node) = @_;
	my ($output) = run_command(
		[ $node->installed_command('pg_controldata'), '-D', $node->data_dir ]
	);
	return $output =~ /wal-upgrade window finalized:\s+yes/;
}

# Create the new-major standby used to test slotless upgrade replay. The
# optional retained old datadir supplies its replay start and inherited files.
sub make_upgrade_standby
{
	my (%args) = @_;
	my $source = $args{source};
	my $node = PostgreSQL::Test::Cluster->new($args{name});
	my @init_options;

	push @init_options, allows_streaming => 1 if $args{allows_streaming};
	$node->init(@init_options);
	$node->append_conf('postgresql.conf', $args{postgresql_conf});

	my $primary_conninfo = $args{primary_conninfo};
	if (!defined $primary_conninfo)
	{
		$primary_conninfo =
		  defined $args{replication_user}
		  ? "host=127.0.0.1 port="
		  . $source->port
		  . " user=$args{replication_user} application_name=$args{name}"
		  : $source->connstr;
	}

	my $recovery_conf = "primary_conninfo = '$primary_conninfo'\n";
	$recovery_conf .= "primary_slot_name = ''\n";
	$recovery_conf .= "wal_receiver_create_temp_slot = off\n";
	$recovery_conf .=
	  "pg_upgrade_standby_old_datadir = '$args{old_datadir}'\n"
	  if defined $args{old_datadir};
	$recovery_conf .= $args{extra_conf} if defined $args{extra_conf};
	$node->append_conf('postgresql.conf', $recovery_conf);
	$node->append_conf('pg_hba.conf', $args{replication_hba})
	  if defined $args{replication_hba};
	$node->append_conf('pg_upgrade.signal', '');
	$node->set_standby_mode;
	return $node;
}

1;
