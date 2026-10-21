#!/usr/bin/env python3
"""Run standalone manual/automatic comparisons across Linux transfer modes.

Reuse pg-upgrade-initdb-parity.py. Each lane gets a stopped, independent source
cluster, including its tablespace, so link/swap cannot affect later comparisons.
Use a filesystem supporting reflinks to exercise successful --clone transfers.
--prune removes test-owned cluster data only after a comparison passes and all
servers have stopped. Observations and diagnostic logs are retained.
"""

import argparse
import fnmatch
import gzip
import hashlib
import importlib.util
import os
from pathlib import Path
import re
import shutil
import stat
import sys


sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location(
    'parity', Path(__file__).with_name('pg-upgrade-initdb-parity.py'))
parity = importlib.util.module_from_spec(spec)
spec.loader.exec_module(parity)
TRANSFERS = ('copy', 'clone', 'link', 'swap', 'copy-file-range')


def extra_cases():
    return [
        {'name': 'memory-settings', 'chunks': [['-c', 'max_connections=24',
         '-c', 'shared_buffers=16MB', '-c', 'work_mem=2MB',
         '-c', 'maintenance_work_mem=16MB']],
         'custom': ['shared_buffers', 'work_mem', 'maintenance_work_mem']},
        {'name': 'worker-settings', 'chunks': [['-c', 'max_worker_processes=4',
         '-c', 'max_parallel_workers=2', '-c', 'max_parallel_maintenance_workers=1']],
         'custom': ['max_worker_processes', 'max_parallel_workers',
                    'max_parallel_maintenance_workers']},
        {'name': 'wal-segsize-4mb', 'chunks': [['--wal-segsize=4']],
         'expected': 'upgrade-failure'},
        {'name': 'durability-settings', 'chunks': [['-c', 'fsync=off',
         '-c', 'synchronous_commit=off', '-c', 'full_page_writes=off']],
         'custom': ['fsync', 'synchronous_commit', 'full_page_writes']},
        {'name': 'text-search-simple', 'chunks': [['--text-search-config=simple']],
         'custom': ['default_text_search_config']},
        {'name': 'message-settings', 'chunks': [['-c', 'log_min_messages=warning',
         '-c', 'client_min_messages=warning']],
         'custom': ['log_min_messages', 'client_min_messages']},
        {'name': 'no-clean-success', 'chunks': [['--no-clean']]},
        {'name': 'checksum-options-last-wins',
         'chunks': [['--data-checksums'], ['--no-data-checksums']]},
        {'name': 'empty-shared-preload-libraries',
         'chunks': [['-c', 'shared_preload_libraries=']],
         'custom': ['shared_preload_libraries']},
        {'name': 'logging-collector', 'chunks': [['-c', 'logging_collector=on',
         '-c', 'log_directory=server_logs', '-c', 'log_filename=postgresql.log']],
         'custom': ['logging_collector', 'log_directory', 'log_filename']},
        {'name': 'combined-group-wal-parents-hba-memory',
         'chunks': [['--allow-group-access', '-c', 'max_connections=24',
                    '-c', 'shared_buffers=16MB']], 'wal': 'parents',
         'generated': 'hba_file=pg_hba.conf', 'custom': ['shared_buffers']},
        {'name': 'combined-scram-group-existing-wal-hba', 'auth': 'correct',
         'wal': 'existing', 'chunks': [['--allow-group-access']],
         'generated': 'hba_file=pg_hba.conf'},
        {'name': 'combined-data-directory-config-file-wal',
         'chunks': [['--allow-group-access', '-c', 'huge_pages=off']],
         'wal': 'external', 'data_directory': True, 'config_file': True},
        {'name': 'local-auth-reject', 'chunks': [['--auth-local=reject']],
         'expected': 'upgrade-failure'},
        {'name': 'source-owner-target-owner', 'source_group': False},
        {'name': 'source-owner-target-group', 'source_group': False,
         'chunks': [['--allow-group-access']]},
        {'name': 'existing-0700', 'target_mode': 0o700},
        {'name': 'group-existing-0777', 'target_mode': 0o777,
         'chunks': [['--allow-group-access']]},
        {'name': 'setgid-without-retain', 'parent_mode': 0o2750, 'retain': False,
         'chunks': [['--allow-group-access']]},
        {'name': 'wal-existing-0500', 'wal': 'existing', 'wal_mode': 0o500},
        {'name': 'group-wal-existing-0550', 'wal': 'existing', 'wal_mode': 0o550,
         'chunks': [['--allow-group-access']]},
        {'name': 'wal-existing-setgid', 'wal': 'existing', 'wal_mode': 0o2750,
         'chunks': [['--allow-group-access']]},
        {'name': 'invalid-initdb-wal', 'wal': 'existing',
         'chunks': [['-c', 'no_such_guc=1']], 'expected': 'initdb-failure'},
        {'name': 'invalid-initdb-wal-no-clean', 'wal': 'existing',
         'chunks': [['--no-clean', '-c', 'no_such_guc=1']],
         'expected': 'initdb-failure', 'no_clean': True},
        {'name': 'sync-default', 'sync': True},
        {'name': 'syncfs', 'sync': True, 'upgrade': ['--sync-method=syncfs']},
        {'name': 'group-syncfs-wal', 'sync': True, 'wal': 'existing',
         'chunks': [['--allow-group-access']], 'upgrade': ['--sync-method=syncfs']},
        {'name': 'parallel-jobs', 'upgrade': ['--jobs=2']},
        {'name': 'parallel-group-wal', 'wal': 'parents',
         'chunks': [['--allow-group-access']], 'upgrade': ['--jobs=2']},
        {'name': 'no-statistics', 'upgrade': ['--no-statistics']},
        {'name': 'char-signed', 'upgrade': ['--set-char-signedness=signed']},
        {'name': 'char-unsigned', 'upgrade': ['--set-char-signedness=unsigned']},
        {'name': 'relative-datadirs', 'relative_paths': True},
        {'name': 'md5-password', 'auth': 'correct', 'auth_method': 'md5'},
        {'name': 'md5-missing-password', 'auth': 'missing', 'auth_method': 'md5',
         'expected': 'upgrade-failure'},
        {'name': 'checksums-on', 'old_options': ['--data-checksums']},
        {'name': 'checksums-on-group', 'old_options': ['--data-checksums'],
         'chunks': [['--allow-group-access']]},
        {'name': 'wal-segsize-matched-4mb', 'old_options': ['--wal-segsize=4']},
        {'name': 'libc-utf8', 'old_options': ['--lc-collate=C.utf8', '--lc-ctype=C.utf8']},
        {'name': 'builtin-locale', 'old_options': ['--locale-provider=builtin',
         '--builtin-locale=C.UTF-8']},
        {'name': 'icu-locale', 'old_options': ['--locale-provider=icu',
         '--icu-locale=en-US']},
        {'name': 'tablespace', 'tablespace': True},
        {'name': 'tablespace-parallel-group', 'tablespace': True,
         'chunks': [['--allow-group-access']], 'upgrade': ['--jobs=2']},
        {'name': 'encoding-latin1', 'old_options': ['--encoding=LATIN1']},
        {'name': 'encoding-sql-ascii', 'old_options': ['--encoding=SQL_ASCII']},
        {'name': 'libc-en-us', 'old_options': ['--lc-collate=en_US.utf8',
                                            '--lc-ctype=en_US.utf8']},
        {'name': 'locale-categories', 'chunks': [['--lc-messages=C.utf8',
         '--lc-monetary=en_US.utf8', '--lc-numeric=en_US.utf8', '--lc-time=en_US.utf8']]},
        {'name': 'icu-rules', 'chunks': [['--locale-provider=icu',
         '--icu-locale=en-US', '--icu-rules=&a<b']],
         'upgrade_expected': 'upgrade-failure'},
        {'name': 'icu-rules-matched', 'old_options': ['--locale-provider=icu',
         '--icu-locale=en-US', '--icu-rules=&a<b'], 'chunks': [['--icu-rules=&a<b']]},
        {'name': 'debug-output', 'chunks': [['--debug']]},
        {'name': 'no-instructions', 'chunks': [['--no-instructions']]},
        {'name': 'discard-caches', 'chunks': [['--discard-caches']]},
        {'name': 'no-sync-data-files', 'chunks': [['--no-sync-data-files']]},
        {'name': 'initdb-syncfs', 'chunks': [['--sync-method=syncfs']]},
        {'name': 'no-locale', 'chunks': [['--no-locale']]},
        {'name': 'invalid-wal-segsize', 'chunks': [['--wal-segsize=3']],
         'expected': 'initdb-failure'},
        {'name': 'invalid-icu-rules', 'chunks': [['--locale-provider=icu',
         '--icu-locale=en-US', '--icu-rules=invalid &']], 'expected': 'initdb-failure'},
        {'name': 'username-mismatch', 'chunks': [['--username=different_new_user']],
         'expected': 'upgrade-failure'},
    ]


def prune_data(path):
    """Remove owned scratch data after shutdown, including read-only targets."""
    if not path.exists():
        return
    assert not list(path.rglob('postmaster.pid')), path
    for parent, _, _ in os.walk(path):
        Path(parent).chmod(0o700)
    shutil.rmtree(path)


class LinuxRunner(parity.Runner):
    def __init__(self, args):
        super().__init__(args)
        self.templates = {}
        self.current = None
        self.seeding = None
        self.source_root = (args.source_root or self.output).resolve()
        if self.source_root != self.output:
            self.source_root.mkdir(parents=True, exist_ok=False)
        self.asserts = '--enable-cassert' in self.run(
            'build-config', [self.bin / 'pg_config', '--configure'], required=True).stdout
        source_version = self.run(
            'source-version', [self.oldbin / 'postgres', '--version'], required=True).stdout
        self.source_major = int(re.search(r'PostgreSQL\) (\d+)', source_version)[1])

    def run(self, label, argv, *args, **kwargs):
        if label == 'old-initdb':
            argv = list(argv)
            if not self.seeding.get('source_group', True):
                argv.remove('--allow-group-access')
            argv += self.seeding.get('old_options', [])
        if label in ('automatic', 'manual-upgrade') and self.current['case'].get('relative_paths'):
            argv = list(argv)
            cwd = args[0] if args else kwargs['cwd']
            for flag in ('-d', '-D'):
                index = argv.index(flag) + 1
                argv[index] = os.path.relpath(argv[index], cwd)
        result = super().run(label, argv, *args, **kwargs)
        if label in ('automatic', 'manual-upgrade') and self.current['case'].get('transfer_rejection'):
            message = {
                'clone': 'could not clone file between old and new data directories',
                'copy-file-range': 'could not copy file range between old and new data directories',
                'link': 'could not create hard link between old and new data directories',
                'swap': 'could not create hard link between old and new data directories',
            }[self.current['case']['transfer']]
            assert result.returncode and message in result.stdout, result.stdout
        return result

    def relation_paths(self, case=None, env=None):
        result = {}
        for database, names in [('postgres', "'parity_data','parity_data_pkey','parity_note','ts_data'"),
                                ('parity_extra', "'extra'")]:
            query = ("SELECT relname || '|' || pg_relation_filepath(oid) FROM pg_class "
                     f"WHERE relname IN ({names}) ORDER BY relname")
            for row in self.sql(query, self.bin, database, env).splitlines():
                name, relative = row.split('|', 1)
                result[f'{database}/{name}'] = relative
        case = case or self.current['case']
        assert len(result) == (5 if case.get('tablespace') else 4), result
        return result

    def start(self, data, bindir, env=None, options=()):
        super().start(data, bindir, env, options)
        if not self.current or data != self.current['target']:
            return
        relations = {}
        for name, relative in self.relation_paths(env=env).items():
            path = data / relative
            info = path.stat()
            before = self.current['source'][name]
            linked = (info.st_dev, info.st_ino) == tuple(before['identity'])
            transfer = self.current['case']['transfer']
            assert linked == (transfer in ('link', 'swap')), (name, transfer, linked)
            relations[name] = parity.metadata(path)
        self.current['relations'] = relations
        control = self.run('final-control', [self.bin / 'pg_controldata', data], required=True)
        self.current['control'] = {
            key: value.strip() for line in control.stdout.splitlines()
            for key, separator, value in [line.partition(':')]
            if separator and key in parity.CONTROL_FIELDS}
        for option in self.current['case'].get('upgrade', []):
            if option.startswith('--set-char-signedness='):
                assert self.current['control']['Default char data signedness'] == option.split('=', 1)[1]
        if self.current['case'].get('tablespace'):
            assert self.sql('SELECT answer FROM ts_data', self.bin, env=env) == '84'

    def template(self, case):
        key = (case.get('source_group', True), tuple(case.get('old_options', [])),
               case.get('tablespace', False))
        if key not in self.templates:
            self.seeding = case
            self.old = self.source_root / 'templates' / str(len(self.templates))
            self.old.parent.mkdir(exist_ok=True)
            super().initialize_old()
            self.start(self.old, self.oldbin)
            try:
                if case.get('tablespace'):
                    location = self.old.with_name(self.old.name + '-tablespace')
                    location.mkdir()
                    escaped = str(location).replace("'", "''")
                    self.sql(f"CREATE TABLESPACE parity_ts LOCATION '{escaped}'", self.oldbin)
                    self.sql('CREATE TABLE ts_data TABLESPACE parity_ts AS SELECT 84 AS answer', self.oldbin)
                relations = self.relation_paths(case)
            finally:
                self.stop(self.old, self.oldbin)
            self.templates[key] = (self.old, relations)
            self.seeding = None
        return self.templates[key]

    def lane(self, case, mode, automatic):
        template, relations = self.template(case)
        lane_name = 'automatic' if automatic else 'manual'
        source = self.source_root / 'sources' / f'{case["name"]}-{mode}-{lane_name}'
        source.parent.mkdir(exist_ok=True)
        shutil.copytree(template, source, symlinks=True)
        for link in (source / 'pg_tblspc').iterdir():
            destination = source.with_name(source.name + '-tablespace')
            shutil.copytree(link.resolve(), destination, symlinks=True)
            link.unlink()
            link.symlink_to(destination, target_is_directory=True)
        self.old = source
        lane = self.output / f'{case["name"]}-{mode}' / lane_name
        target = lane / 'existing/parent1/parent2/data'
        self.current = {'case': case, 'target': target, 'source': {}, 'relations': None}
        for name, relative in relations.items():
            path = source / relative
            info = path.stat()
            self.current['source'][name] = {'relative': relative,
                'identity': [info.st_dev, info.st_ino],
                'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
        try:
            outcome = super().lane(case, mode, automatic)
            if mode == 'check':
                for name, before in self.current['source'].items():
                    path = source / before['relative']
                    assert hashlib.sha256(path.read_bytes()).hexdigest() == before['sha256'], name
                assert (source / 'global/pg_control').is_file()
                assert not (source / 'global/pg_control.old').exists()
            if outcome['logical'] is not None:
                outcome['logical']['relation_permissions'] = self.current['relations']
                outcome['logical']['control'] = self.current['control']
            parity.write_json(lane / 'outcome.json', outcome)
            return outcome
        finally:
            self.current = None

    def compare(self, case, mode):
        super().compare(case, mode)
        if self.args.prune and self.results[-1]['passed']:
            for name in ('manual', 'automatic'):
                lane = self.output / f'{case["name"]}-{mode}' / name
                # Keep diagnostic files, snapshots and command logs.
                target = lane / 'existing/parent1/parent2/data'
                logs = target / 'pg_upgrade_output.d'
                if logs.exists():
                    target.chmod(0o700)
                    shutil.move(str(logs), lane / 'retained-logs')
                for data in (lane / 'existing', lane / 'external wal', lane / 'new wal parent'):
                    prune_data(data)
                source = self.source_root / 'sources' / f'{case["name"]}-{mode}-{name}'
                prune_data(source)
                prune_data(source.with_name(source.name + '-tablespace'))
                for path in lane.rglob('*'):
                    if path.is_file() and (path.suffix == '.json' or path.stat().st_size > 16384):
                        compressed = path.with_name(path.name + '.gz')
                        with path.open('rb') as original, gzip.open(compressed, 'wb', compresslevel=1) as output:
                            shutil.copyfileobj(original, output)
                        compressed.chmod(stat.S_IMODE(path.stat().st_mode))
                        path.unlink()

    def close(self):
        clean = super().close()
        remaining = list(self.source_root.rglob('postmaster.pid'))
        parity.write_json(self.output / 'remaining-source-postmasters.json', [str(path) for path in remaining])
        clean = clean and not remaining
        if self.args.prune and clean and all(result['passed'] for result in self.results):
            for template, _ in self.templates.values():
                prune_data(template)
                prune_data(template.with_name(template.name + '-tablespace'))
        return clean


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bindir', required=True, type=Path)
    parser.add_argument('--old-bindir', type=Path)
    parser.add_argument('--source-root', type=Path,
                        help='put source clusters on a different filesystem')
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--transfer', choices=(*TRANSFERS, 'all'), default='all')
    parser.add_argument('--case', action='append', default=[])
    parser.add_argument('--mode', choices=('both', 'upgrade', 'check'), default='both')
    parser.add_argument('--prune', action='store_true')
    parser.add_argument('--expect-transfer-failure', action='store_true',
                        help='verify native rejection on unsupported filesystems')
    args = parser.parse_args()
    if not sys.platform.startswith('linux') or os.geteuid() == 0:
        parser.error('run as an unprivileged Linux user')
    selected = [case for case in parity.cases() + extra_cases()
                if not args.case or any(fnmatch.fnmatchcase(case['name'], pattern) for pattern in args.case)]
    if not selected:
        parser.error('no matching configurations')
    transfers = TRANSFERS if args.transfer == 'all' else (args.transfer,)
    runner = LinuxRunner(args)
    print(f'Reports: {runner.output}', flush=True)
    parity.write_json(runner.output / 'coverage.json', {
        'cases': selected, 'transfers': transfers, 'operation': args.mode,
        'bindir': str(runner.bin), 'old_bindir': str(runner.oldbin)})
    try:
        for transfer in transfers:
            for original in selected:
                case = original | {'name': original['name'] + '-' + transfer,
                                   'transfer': transfer,
                                   'upgrade': [*original.get('upgrade', []), '--' + transfer]}
                if case.get('tablespace') and runner.oldbin == runner.bin:
                    # Native pg_upgrade rejects same-catalog upgrades with tablespaces.
                    case['expected'] = 'upgrade-failure'
                if original['name'] == 'discard-caches' and not runner.asserts:
                    # Release builds restrict debug_discard_caches to zero.
                    case['expected'] = 'initdb-failure'
                if original['name'] in ('char-signed', 'char-unsigned') and runner.source_major >= 18:
                    # Native pg_upgrade permits this override only for pre-PG18 sources.
                    case['expected'] = 'upgrade-failure'
                if args.expect_transfer_failure and case.get('expected', 'success') == 'success':
                    case['expected'] = 'upgrade-failure'
                    case['transfer_rejection'] = True
                for mode in (('upgrade', 'check') if args.mode == 'both' else (args.mode,)):
                    mode_case = case | {'expected': case.get(mode + '_expected',
                                                            case.get('expected', 'success'))}
                    runner.compare(mode_case, mode)
                    if not runner.results[-1]['passed']:
                        return 1
    finally:
        clean = runner.close()
    passed = sum(result['passed'] for result in runner.results)
    print(f'{passed}/{len(runner.results)} comparisons passed, no running servers: {clean}', flush=True)
    return 0 if clean and passed == len(runner.results) else 1


if __name__ == '__main__':
    sys.exit(main())
