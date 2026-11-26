# Copyright (c) 2026, PostgreSQL Global Development Group

package WalUpgradeTest;

use strict;
use warnings FATAL => 'all';

use Exporter 'import';
use PostgreSQL::Test::Cluster;

our @EXPORT_OK = qw(
  make_old_standby_chain
  wait_for_old_replay
  wait_for_physical_slot
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

1;
