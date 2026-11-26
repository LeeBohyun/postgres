# Copyright (c) 2026, PostgreSQL Global Development Group

# Failed ordinary shutdown must still get an atexit fallback. If both fail,
# --initdb must preserve a target that is still running.

use strict;
use warnings FATAL => 'all';

use Cwd qw(abs_path);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an executable pg_ctl wrapper' if $windows_os;
chdir $PostgreSQL::Test::Utils::tmp_check;

my $wrapper_bin = PostgreSQL::Test::Utils::tempdir('stop_bin');
my $probe = PostgreSQL::Test::Cluster->new('stop_probe');
my $real_bindir = $probe->config_data('--bindir');
opendir(my $binaries, $real_bindir) or die "opendir $real_bindir: $!";
for my $name (grep { $_ ne '.' && $_ ne '..' && $_ ne 'pg_ctl' }
	readdir $binaries)
{
	symlink("$real_bindir/$name", "$wrapper_bin/$name")
	  or die "symlink $name: $!";
}
closedir($binaries);
my $wrapper = "$wrapper_bin/pg_ctl";
open(my $script, '>', $wrapper) or die "open $wrapper: $!";
print $script "#!$^X\n", <<'WRAPPER';
use strict;
use warnings;
my $datadir;
for (my $i = 0; $i < @ARGV - 1; $i++)
{
  $datadir = $ARGV[$i + 1] if $ARGV[$i] eq '-D';
}
if (grep({ $_ eq 'stop' } @ARGV)
    && defined($datadir)
    && $datadir eq $ENV{PG_UPGRADE_TEST_STOP_DATADIR})
{
  my $attempt = 0;
  if (open(my $count, '<', $ENV{PG_UPGRADE_TEST_STOP_STATE}))
  {
    $attempt = <$count>;
    close($count) or die "close counter: $!";
  }
  open(my $count, '>', $ENV{PG_UPGRADE_TEST_STOP_STATE}) or die "counter: $!";
  print $count ++$attempt;
  close($count) or die "close counter: $!";
  exit 1 if $attempt <= $ENV{PG_UPGRADE_TEST_FAIL_STOPS};
}
exec $ENV{PG_UPGRADE_TEST_REAL_PG_CTL}, @ARGV or die "exec pg_ctl: $!";
WRAPPER
close($script) or die "close $wrapper: $!";
chmod(0755, $wrapper) or die "chmod $wrapper: $!";

for my $which ('source', 'target')
{
	for my $failures (1, 2)
	{
		subtest "$which shutdown with $failures injected failures" => sub {
			my $old =
			  PostgreSQL::Test::Cluster->new("${which}_${failures}_old");
			my $new =
			  PostgreSQL::Test::Cluster->new("${which}_${failures}_new");
			$old->init;
			my $failed = $which eq 'source' ? $old : $new;
			my $counter = "$wrapper_bin/${which}_${failures}.count";
			{
				local $ENV{PG_UPGRADE_TEST_REAL_PG_CTL} =
				  "$real_bindir/pg_ctl";
				local $ENV{PG_UPGRADE_TEST_STOP_DATADIR} = $failed->data_dir;
				local $ENV{PG_UPGRADE_TEST_STOP_STATE} = $counter;
				local $ENV{PG_UPGRADE_TEST_FAIL_STOPS} = $failures;
				command_fails(
					[
						'pg_upgrade', '--initdb', '--check', '--no-sync',
						'--old-datadir' => $old->data_dir,
						'--new-datadir' => $new->data_dir,
						'--old-bindir' => $wrapper_bin,
						'--new-bindir' => $wrapper_bin,
						'--socketdir' => $old->host,
						'--old-port' => $old->port,
						'--new-port' => $new->port,
					],
					'failed normal shutdown fails the check');
			}
			is(slurp_file($counter), '2',
				'atexit attempts fallback shutdown');
			if ($failures == 1)
			{
				ok(!-f $failed->data_dir . '/postmaster.pid',
					'fallback stops the server');
				ok( !-f $new->data_dir . '/PG_VERSION',
					'stopped target is cleaned up');
			}
			else
			{
				ok( -f $failed->data_dir . '/postmaster.pid',
					'failed fallback leaves the server running');
				ok( -f $failed->data_dir . '/PG_VERSION',
					'running cluster data is retained');
				$failed->_update_pid(-1);
				$failed->stop;
			}
			ok(-f $old->data_dir . '/PG_VERSION', 'source data is retained');
			done_testing();
		};
	}
}

done_testing();
