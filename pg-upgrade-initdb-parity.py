#!/usr/bin/env python3
"""Standalone POSIX parity test, separate from the core TAP suite.

  python3 pg-upgrade-initdb-parity.py --bindir /path/to/install/bin
  python3 pg-upgrade-initdb-parity.py --bindir /path/to/install/bin --transfer all

Compare native initdb followed by pg_upgrade with pg_upgrade --initdb. Wrappers
observe initialization and the first new-server startup without changing their
arguments or results. Compare configuration, control settings, file permissions,
database locales and ICU rules, and upgraded SQL data, rather than random cluster
IDs or physical WAL bytes. Use --case 'icu-rules-*' for the ICU rule cases.
Those cases require ICU support in both PostgreSQL builds.
Automatic checks intentionally delete their cluster. Manual checks retain it.
Automatic upgrades also delete their cluster if compatibility checks fail.
Reports and command logs remain in --output. Any mismatch makes the test fail.
Use --case GLOB and --mode check|upgrade to run a smaller selection.

On Linux, --transfer enables the expanded configuration and filesystem checks.
Each lane gets an independent source cluster and tablespace so link/swap cannot
affect later comparisons. --transfer all covers copy, clone, link, swap, and
copy-file-range. Successful clone transfers require a filesystem with reflinks.
--prune removes test-owned data only after passing comparisons and shutdown,
while preserving observations and diagnostic logs.
"""

import argparse
import fnmatch
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
from uuid import uuid4


TRANSFERS = ('copy', 'clone', 'link', 'swap', 'copy-file-range')
USER = 'initdb_parity'
PASSWORD = 'local-parity-test-password'
BASE_OPTIONS = ['--wal-segsize=1', '--no-data-checksums', '--encoding=UTF8',
                '--locale-provider=libc', '--lc-collate=C', '--lc-ctype=C']
CONFIG_FILES = ['postgresql.conf', 'postgresql.auto.conf', 'pg_hba.conf',
                'pg_ident.conf', 'pg_hosts.conf']
CONTROL_FIELDS = ['pg_control version number', 'Catalog version number',
                  'Database cluster state', 'Database block size',
                  'WAL block size', 'Bytes per WAL segment',
                  'Data page checksum version', 'Default char data signedness']


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def metadata(path):
    if not path.exists() and not path.is_symlink():
        return None
    info = path.lstat()
    return {'mode': oct(stat.S_IMODE(info.st_mode)), 'uid': info.st_uid,
            'gid': info.st_gid,
            'kind': ('link' if path.is_symlink() else
                     'dir' if path.is_dir() else 'file')}


def permissions(target):
    return {str(i): metadata(p) for i, p in enumerate(
        [target, target.parent, target.parent.parent, target.parent.parent.parent])}


def observe(data, bindir, target):
    lane = target.parent.parent.parent.parent

    def normalize(value):
        return value.replace(str(target), '<NEW>').replace(str(lane), '<LANE>')

    tree = {}
    if data.exists():
        for parent, dirs, files in os.walk(data):
            dirs[:] = sorted(d for d in dirs if d != 'pg_upgrade_output.d')
            for name in sorted(dirs + files):
                if name == 'postmaster.opts':
                    continue
                path = Path(parent) / name
                item = metadata(path)
                if path.is_symlink():
                    item['destination'] = normalize(os.readlink(path))
                tree[str(path.relative_to(data))] = item
    wal = None
    wal_link = data / 'pg_wal'
    if wal_link.is_symlink():
        destination = wal_link.resolve()
        wal_tree = {}
        for parent, dirs, files in os.walk(destination):
            for name in sorted(dirs + files):
                path = Path(parent) / name
                wal_tree[str(path.relative_to(destination))] = metadata(path)
        wal = {'root': metadata(destination), 'tree': wal_tree}
        # A requested WAL beneath NEW is at a different relative depth inside
        # staging. Compare its link and contents separately, not that depth.
        if destination.is_relative_to(data):
            relative = destination.relative_to(data)
            prefix = str(relative)
            tree = {key: value for key, value in tree.items()
                    if key != prefix and not key.startswith(prefix + '/')}
            for parent in relative.parents:
                prefix = str(parent)
                if prefix != '.' and not any(key.startswith(prefix + '/') for key in tree):
                    tree.pop(prefix, None)
    control = {}
    if (data / 'global/pg_control').exists():
        result = subprocess.run([str(bindir / 'pg_controldata'), str(data)],
                                capture_output=True, text=True, timeout=30)
        if result.returncode:
            raise RuntimeError(result.stderr)
        for line in result.stdout.splitlines():
            key, separator, value = line.partition(':')
            if separator and key in CONTROL_FIELDS:
                control[key] = value.strip()
    configuration = {name: normalize((data / name).read_text())
                     for name in CONFIG_FILES if (data / name).exists()}
    settings = {}
    if (data / 'PG_VERSION').exists():
        names = ['max_connections', 'huge_pages']
        names += json.loads(os.environ.get('PARITY_CUSTOM_SETTINGS', '[]'))
        for name in names:
            result = subprocess.run(
                [str(bindir / 'postgres'), '-D', str(data), '-C', name],
                capture_output=True, text=True, timeout=30)
            settings[name] = {'rc': result.returncode,
                              'value': normalize(result.stdout.strip())}
    return {'data_mode': metadata(data), 'tree': tree, 'wal': wal, 'control': control,
            'configuration': configuration, 'settings': settings,
            'target_permissions': permissions(target)}


def startup_files(data, bindir, options, target):
    resolved = {}
    for name in ('config_file', 'hba_file', 'ident_file', 'hosts_file'):
        result = subprocess.run([str(bindir / 'postgres'), '-D', str(data),
                                 *options, '-C', name],
                                capture_output=True, text=True, timeout=30)
        path = Path(result.stdout.strip())
        if not path.is_absolute():
            path = data / path
        resolved[name] = {'rc': result.returncode, 'available': result.returncode == 0 and path.is_file()}
        if resolved[name]['available']:
            # Generated configuration may contain the final absolute path.
            contents = path.read_bytes().replace(str(target).encode(), b'<NEW>')
            contents = contents.replace(str(target.parent.parent.parent.parent).encode(), b'<LANE>')
            resolved[name]['sha256'] = hashlib.sha256(contents).hexdigest()
            resolved[name]['permissions'] = metadata(path)
    return resolved


def log_permissions(target):
    root = target / 'pg_upgrade_output.d'
    sessions = sorted(root.glob('*'))
    result = {'root': metadata(root), 'sessions': len(sessions)}
    if sessions:
        session = sessions[-1]
        # Internal cleanup metadata has no manual-initdb counterpart.
        logs = sorted(path for path in (session / 'log').glob('*')
                      if path.name != 'initdb_state')
        result['entries'] = {str(path.relative_to(session)): metadata(path)
                             for path in [session, session / 'log', session / 'dump',
                                          *logs]}
    return result


def wrapper(kind, bindir, args):
    """Delegate unchanged; record errors separately without masking native rc."""
    target = Path(os.environ['PARITY_TARGET'])
    report = Path(os.environ['PARITY_REPORT'])
    if kind == 'pg_ctl' and 'start' in args and '-D' in args:
        data = Path(args[args.index('-D') + 1]).resolve()
        if (data == target or target in data.parents) and not (report / 'before-server.json').exists():
            try:
                snapshot = observe(data, bindir, target)
                options = shlex.split(args[args.index('-o') + 1]) if '-o' in args else []
                snapshot['startup_files'] = startup_files(data, bindir, options, target)
                snapshot['logs'] = log_permissions(target)
                write_json(report / 'before-server.json', snapshot)
            except Exception as error:
                write_json(report / 'before-server.json', {'capture_error': str(error)})
    result = subprocess.run([str(bindir / kind), *args])
    if kind == 'initdb' and '-D' in args and '--sync-only' not in args:
        data = Path(args[args.index('-D') + 1]).resolve()
        snapshot = {'rc': result.returncode, 'argv': args, 'datadir': str(data)}
        try:
            snapshot['state'] = observe(data, bindir, target)
        except Exception as error:
            snapshot['capture_error'] = str(error)
        write_json(report / 'after-initdb.json', snapshot)
    return result.returncode


def cases():
    # Pairwise representatives, including intentional failures. This is not a
    # Cartesian expansion of every option. No known mismatch is marked xfail.
    return [
        {'name': 'default'},
        {'name': 'group', 'chunks': [['--allow-group-access']]},
        {'name': 'existing-0500', 'target_mode': 0o500},
        {'name': 'group-existing-0550', 'target_mode': 0o550,
         'chunks': [['--allow-group-access']]},
        {'name': 'setgid-parent', 'parent_mode': 0o2750},
        {'name': 'group-setgid-parent', 'parent_mode': 0o2750,
         'chunks': [['--allow-group-access']]},
        {'name': 'existing-setgid-target', 'parent_mode': 0o2750,
         'target_mode': 0o2700},
        {'name': 'group-existing-setgid-target', 'parent_mode': 0o2750,
         'target_mode': 0o2750, 'chunks': [['--allow-group-access']]},
        {'name': 'without-retain', 'retain': False},
        {'name': 'group-without-retain', 'retain': False,
         'chunks': [['--allow-group-access']]},
        {'name': 'sync-fsync', 'sync': True, 'upgrade': ['--sync-method=fsync']},
        {'name': 'no-sync-fsync', 'upgrade': ['--sync-method=fsync']},
        {'name': 'quoted-repeated-options', 'chunks': [
            ['-c', 'max_connections=20', '-c', 'custom.parity=first'],
            ['-c', 'max_connections=25', '--set=custom.parity=space, apostrophe\'s $libdir',
             '-c', 'custom.empty=', '-c', r'custom.path=C:\new\path']],
         'custom': ['custom.parity', 'custom.empty', 'custom.path']},
        {'name': 'server-options', 'upgrade': ['--new-options=-F']},
        {'name': 'invalid-server-option',
         'upgrade': ['--new-options=-c no_such_guc=1'], 'expected': 'upgrade-failure'},
        {'name': 'wal-new', 'wal': 'external'},
        {'name': 'wal-new-parents', 'wal': 'parents'},
        {'name': 'wal-existing', 'wal': 'existing'},
        {'name': 'group-wal-existing', 'wal': 'existing',
         'chunks': [['--allow-group-access']]},
        {'name': 'wal-staging-collision', 'wal': 'collision'},
        {'name': 'wal-second-staging-collision', 'wal': 'second-collision'},
        {'name': 'wal-staging-alias', 'wal': 'alias'},
        {'name': 'wal-bundled-short', 'wal': 'bundled'},
        {'name': 'wal-abbreviated-long', 'wal': 'abbreviated'},
        {'name': 'wal-repeated-short', 'wal': 'repeated'},
        {'name': 'absolute-hba', 'generated': 'hba_file=pg_hba.conf'},
        {'name': 'absolute-ident', 'generated': 'ident_file=pg_ident.conf'},
        {'name': 'absolute-hosts', 'generated': 'hosts_file=pg_hosts.conf'},
        {'name': 'external-hba', 'external_hba': True},
        {'name': 'absolute-config-file', 'config_file': True},
        {'name': 'external-config-file', 'config_file': 'external'},
        {'name': 'server-data-directory', 'server_data_directory': True},
        {'name': 'absolute-data-directory', 'data_directory': True},
        {'name': 'scram-password', 'auth': 'correct'},
        {'name': 'scram-no-password', 'auth': 'missing', 'expected': 'upgrade-failure'},
        {'name': 'scram-wrong-password', 'auth': 'wrong', 'expected': 'upgrade-failure'},
        {'name': 'checksum-mismatch', 'chunks': [['--data-checksums']],
         'expected': 'upgrade-failure'},
        {'name': 'invalid-initdb-option', 'target_mode': 0o500,
         'chunks': [['-c', 'no_such_guc=1']],
         'expected': 'initdb-failure'},
        {'name': 'invalid-initdb-no-clean',
         'chunks': [['--no-clean', '-c', 'no_such_guc=1']],
         'expected': 'initdb-failure', 'no_clean': True},
    ] + [
        {'name': 'icu-rules-' + name, 'icu_rules': rules,
         'old_options': ['--locale-provider=icu', '--icu-locale=en-US',
                         *(['--icu-rules=' + rules] if rules is not None else [])],
         'chunks': [['--icu-rules=&a<b']] if rules is None else []}
        for name, rules in (
            ('copied', "&a < '$' < c < b"),
            ('multiline', "&a < '$' < c < b\n&c < d"),
            ('long', '&a < c < b' + ' ' * 9000),
            ('multiline-long', "&a < '$' < c < b\n&c < d" + ' ' * 9000),
            ('cleared', None))
    ]


class Runner:
    def __init__(self, args):
        self.args = args
        self.bin = args.bindir.resolve()
        self.oldbin = (args.old_bindir or args.bindir).resolve()
        self.output = args.output.resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        self.sock = Path(tempfile.mkdtemp(prefix='pgup-parity-', dir='/tmp')).resolve()
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            self.port = str(sock.getsockname()[1])
        self.commands, self.results = [], []
        self.env = {key: value for key, value in os.environ.items()
                    if not key.startswith('PG') and not key.startswith('PARITY_')}
        self.env.update(LC_ALL='C', PGPASSFILE=str(self.output / 'no-pgpass'),
                        PGCONNECT_TIMEOUT='5', PGCTLTIMEOUT='20')
        self.wrapped = self.output / 'wrapped-bin'
        self.wrapped.mkdir()
        for executable in self.bin.iterdir():
            destination = self.wrapped / executable.name
            if executable.name in ('initdb', 'pg_ctl'):
                destination.write_text(
                    f'#!{sys.executable}\nimport runpy, sys\n'
                    f'sys.argv = {[str(Path(__file__).resolve()), "--wrapper", executable.name, str(self.bin)]!r} + sys.argv[1:]\n'
                    f'runpy.run_path({str(Path(__file__).resolve())!r}, run_name="__main__")\n')
                destination.chmod(0o755)
            else:
                destination.symlink_to(executable)
        self.old = self.output / 'old'

    def run(self, label, argv, cwd=None, env=None, required=False):
        command = [str(arg) for arg in argv]
        result = subprocess.run(command, cwd=cwd or self.output,
                                env=self.env | (env or {}), text=True,
                                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                timeout=180)
        log = self.output / f'{len(self.commands):04d}-{label}.log'
        log.write_text(result.stdout)
        self.commands.append({'argv': command, 'rc': result.returncode,
                              'cwd': str(cwd or self.output), 'log': log.name})
        write_json(self.output / 'commands.json', self.commands)
        if required and result.returncode:
            raise RuntimeError(f'{label}: see {log}')
        return result

    def server_options(self):
        return ['-p', self.port, '-c', "listen_addresses=",
                '-c', f'unix_socket_directories={self.sock}']

    def start(self, data, bindir, env=None, options=()):
        self.run('start', [bindir / 'pg_ctl', '-D', data, '-l', self.output / 'server.log',
                          '-o', shlex.join([*options, *self.server_options()]),
                          '-w', 'start'], env=env, required=True)

    def stop(self, data, bindir):
        if (data / 'postmaster.pid').exists():
            self.run('stop', [bindir / 'pg_ctl', '-D', data, '-m', 'fast',
                             '-w', 'stop'], required=True)

    def sql(self, query, bindir, database='postgres', env=None):
        probe_env = {'PGOPTIONS': '-c client_min_messages=error', **(env or {})}
        result = self.run('sql', [bindir / 'psql', '-XAt', '-h', self.sock,
                                 '-p', self.port, '-U', USER, '-d', database,
                                 '-v', 'ON_ERROR_STOP=1', '-c', query],
                          env=probe_env, required=True)
        return result.stdout.strip()

    def initialize_old(self, options=()):
        self.run('old-initdb', [self.oldbin / 'initdb', '-D', self.old, '-N',
                               '-U', USER, '-A', 'trust', '--allow-group-access',
                               '--locale=C', *BASE_OPTIONS, *options], required=True)
        try:
            self.start(self.old, self.oldbin)
            self.sql("CREATE TABLE parity_data(id int PRIMARY KEY, note text); "
                     "INSERT INTO parity_data VALUES (1, 'one'), (2, 'two'), (3, NULL); "
                     "CREATE INDEX parity_note ON parity_data(note);", self.oldbin)
            self.sql('CREATE DATABASE parity_extra', self.oldbin)
            self.sql('CREATE TABLE extra AS SELECT 42 AS answer', self.oldbin, 'parity_extra')
            self.expected_rows = self.sql(
                'SELECT json_agg(t ORDER BY id) FROM parity_data t', self.oldbin)
        finally:
            self.stop(self.old, self.oldbin)

    def lane(self, case, mode, automatic):
        lane = self.output / f'{case["name"]}-{mode}' / ('automatic' if automatic else 'manual')
        lane.mkdir(parents=True)
        existing = lane / 'existing'
        existing.mkdir(mode=0o710)
        if 'parent_mode' in case:
            group = next((gid for gid in os.getgroups() if gid != os.getgid()),
                         os.getgid())
            os.chown(existing, -1, group)
        existing.chmod(case.get('parent_mode', 0o710))
        target = existing / 'parent1' / 'parent2' / 'data'
        if 'target_mode' in case:
            target.mkdir(parents=True)
            target.chmod(case['target_mode'])
        baseline = permissions(target)
        cwd = lane / 'work'
        cwd.mkdir()
        env = {'PARITY_TARGET': str(target), 'PARITY_REPORT': str(lane),
               'PARITY_CUSTOM_SETTINGS': json.dumps(case.get('custom', []))}
        if case.get('auth'):
            password_file = lane / 'password'
            password_file.write_text(PASSWORD + '\n')
            password_file.chmod(0o600)
            chunks = [['-A', case.get('auth_method', 'scram-sha-256'),
                       '--pwfile', str(password_file)]]
            if case['auth'] != 'missing':
                env['PGPASSWORD'] = PASSWORD if case['auth'] == 'correct' else 'wrong-test-password'
        else:
            chunks = [['-A', 'trust']]
        chunks += [list(chunk) for chunk in case.get('chunks', [])]
        upgrade = list(case.get('upgrade', []))
        wal = None
        if case.get('wal'):
            wal = (target / 'temporary_data_dir' / 'wal with spaces'
                   if case['wal'] in ('collision', 'bundled', 'abbreviated') else lane / 'external wal')
            if case['wal'] == 'second-collision':
                chunks += [['-X', str(target / 'temporary_data_dir' / 'unused wal')]]
                wal = target / 'temporary_data_dir_1' / 'wal with spaces'
            if case['wal'] == 'parents':
                wal = lane / 'new wal parent' / 'nested' / 'wal with spaces'
            if case['wal'] == 'alias':
                alias = lane / 'staging alias'
                alias.symlink_to(target, target_is_directory=True)
                wal = alias / 'temporary_data_dir' / 'wal with spaces'
            if case['wal'] == 'existing':
                wal.mkdir(mode=0o750)
                wal.chmod(case.get('wal_mode', 0o750))
            if case['wal'] == 'repeated':
                chunks += [['-X', str(lane / 'unused wal')], [f'-X{wal}']]
            elif case['wal'] == 'bundled':
                chunks += [[f'-NX{wal}']]
            elif case['wal'] == 'abbreviated':
                chunks += [[f'--wald={wal}']]
            else:
                chunks += [[f'--waldir={wal}']]
        wal_before = metadata(wal) if wal else None
        wal_parent_before = metadata(wal.parent) if wal else None
        if case.get('generated'):
            setting, filename = case['generated'].split('=')
            chunks += [['-c', f'{setting}={target / filename}']]
        if case.get('external_hba'):
            hba = lane / 'external-hba.conf'
            hba.write_text('local all all trust\n')
            chunks += [['-c', f'hba_file={hba}']]
        if case.get('config_file'):
            config_file = target / 'postgresql.conf'
            if case['config_file'] == 'external':
                config_file = lane / 'external-postgresql.conf'
                config_file.write_bytes((self.old / 'postgresql.conf').read_bytes())
            upgrade += ['--new-options=' + shlex.join(['-c', f'config_file={config_file}'])]
        if case.get('data_directory'):
            chunks += [['-c', f'data_directory={target}']]
        if case.get('server_data_directory'):
            upgrade += ['--new-options=' + shlex.join(['-c', f'data_directory={target}'])]
        args = [self.bin / 'pg_upgrade', '-d', self.old, '-D', target,
                '-b', self.oldbin, '-B', self.wrapped, '-s', self.sock,
                '-p', self.port, '-P', self.port, '-U', USER]
        if not case.get('sync'):
            args += ['--no-sync']
        if mode == 'check':
            args += ['--check']
        if case.get('retain', True):
            args += ['--retain']
        args += upgrade
        errors = []

        def check(condition, message):
            if not condition:
                errors.append(message)

        try:
            if automatic:
                args += ['--initdb']
                args += ['--initdb-options=' + shlex.join(chunk) for chunk in chunks]
                result = self.run('automatic', args, cwd, env)
            else:
                result = self.run('manual-initdb', [self.wrapped / 'initdb', '-N',
                                  '-D', target, '-U', USER, *BASE_OPTIONS,
                                  *case.get('old_options', []),
                                  *(arg for chunk in chunks for arg in chunk)], cwd, env)
                if result.returncode == 0:
                    result = self.run('manual-upgrade', args, cwd, env)
            capture_file = lane / 'after-initdb.json'
            capture = json.loads(capture_file.read_text()) if capture_file.exists() else {}
            init_rc = capture.get('rc')
            expected = case.get('expected', 'success')
            check(result.returncode == (0 if expected == 'success' else 1),
                  f'expected {expected}, got exit {result.returncode}')
            check(init_rc == (1 if expected == 'initdb-failure' else 0),
                  f'unexpected native initdb exit: {init_rc}')
            check('capture_error' not in capture, f'initdb snapshot: {capture.get("capture_error")}')
            if init_rc == 1:
                initialized_path = Path(capture['datadir'])
                if case.get('no_clean'):
                    check(initialized_path.exists() and bool(list(initialized_path.iterdir())),
                          '--no-clean did not retain failed initialization')
                elif automatic:
                    check(initialized_path == target, 'automatic initdb did not use the final target')
                    check(not (target / 'PG_VERSION').exists(), 'failed native initialization retained cluster data')
                    leftovers = [p.name for p in target.iterdir()
                                 if p.name != 'pg_upgrade_output.d'] if target.exists() else []
                    check(not leftovers, f'failed native initialization left data: {leftovers}')
            before_file = lane / 'before-server.json'
            before = json.loads(before_file.read_text()) if before_file.exists() else None
            check(result.returncode != 0 or before is not None,
                  'successful operation has no new-server snapshot')
            check(not before or 'capture_error' not in before, 'server snapshot failed')
            check(not (cwd / 'pg_upgrade_output.d').exists(), 'logs appeared in working directory')
            logs = list(target.glob('pg_upgrade_output.d/*/log/pg_upgrade_utility.log'))
            keep_logs = case.get('retain', True) or result.returncode != 0
            cleanup_expected = automatic and (mode == 'check' or
                (init_rc == 0 and result.returncode != 0 and
                 'Performing Upgrade' not in result.stdout))
            if automatic or init_rc == 0:
                check(len(logs) == (1 if keep_logs else 0), 'wrong number/location of retained log sessions')
            if automatic and logs and init_rc == 0:
                check('performing post-bootstrap initialization' in logs[0].read_text(),
                      'successful initdb output missing from logs')
            if cleanup_expected:
                if baseline['0'] is not None:
                    check(metadata(target) == baseline['0'], 'existing target permissions/ownership not restored')
                if not case.get('no_clean'):
                    check(not (target / 'PG_VERSION').exists(), 'cleanup left initialized final cluster')
                    leftovers = [p.name for p in target.iterdir()
                                 if p.name != 'pg_upgrade_output.d'] if target.exists() else []
                    check(not leftovers, f'cleanup left initialized data: {leftovers}')
                if init_rc == 0 and wal:
                    check(metadata(wal) == wal_before, 'external WAL pre-run state not restored')
                    check(metadata(wal.parent) == wal_parent_before, 'external WAL parent state not restored')
                    check(not wal.exists() or not list(wal.iterdir()), 'external WAL still contains data')
                if not keep_logs and baseline['0'] is None:
                    check(not target.parent.parent.exists(), 'check left created parent directories')
            if init_rc == 0 and not cleanup_expected:
                expected_mode = capture['state']['data_mode']
                check(metadata(target) == expected_mode, 'final target differs from native initdb permissions')
                for key in ('1', '2', '3'):
                    info = metadata([target, target.parent, target.parent.parent, existing][int(key)])
                    expected_info = baseline[key] or expected_mode
                    check(info == expected_info, f'parent {key} permissions/ownership changed')
            if not automatic and mode == 'check' and init_rc == 0:
                check((target / 'PG_VERSION').exists(), 'manual check removed its initialized cluster')
            logical = None
            if result.returncode == 0 and mode == 'upgrade':
                server_args = []
                for option in upgrade:
                    if option.startswith('--new-options='):
                        server_args += shlex.split(option.split('=', 1)[1])
                self.start(target, self.bin, env, server_args)
                rows = self.sql('SELECT json_agg(t ORDER BY id) FROM parity_data t', self.bin, env=env)
                check(rows == self.expected_rows, 'upgraded table data differs from source')
                extra = self.sql('SELECT answer FROM extra', self.bin, 'parity_extra', env)
                check(extra == '42', 'second database data differs from source')
                settings = self.sql(
                    "SELECT name || '=' || setting FROM pg_settings WHERE name IN "
                    "('data_checksums', 'wal_segment_size', 'server_encoding', 'max_connections', 'huge_pages') "
                    "OR name LIKE 'custom.%' ORDER BY name", self.bin, env=env)
                databases = self.sql(
                    'SELECT json_agg(row_to_json(d) ORDER BY datname) FROM '
                    '(SELECT datname, encoding, datlocprovider, datcollate, datctype, '
                    "COALESCE(to_jsonb(d)->>'datlocale', to_jsonb(d)->>'daticulocale') AS locale, "
                    "to_jsonb(d)->>'daticurules' AS icu_rules FROM pg_database d) d",
                    self.bin, env=env)
                if 'icu_rules' in case:
                    for database in json.loads(databases):
                        check(database['icu_rules'] == case['icu_rules'],
                              f"ICU rules for {database['datname']} differ from source")
                indexes = self.sql("SELECT indexdef FROM pg_indexes WHERE tablename='parity_data' ORDER BY indexname", self.bin, env=env)
                logical = {'rows': rows, 'extra': extra, 'settings': settings,
                           'databases': databases, 'indexes': indexes}
                self.stop(target, self.bin)
            outcome = {'rc': result.returncode, 'init_rc': init_rc, 'errors': errors,
                       'initialized': capture.get('state'), 'before_server': before,
                       'logical': logical, 'final_permissions': permissions(target)}
            write_json(lane / 'outcome.json', outcome)
            return outcome
        finally:
            # Use real pg_ctl even if upgrade/check failed partway through.
            self.stop(self.old, self.oldbin)
            for candidate in [target, *target.glob('temporary_data_dir*')]:
                if candidate.is_dir():
                    self.stop(candidate, self.bin)

    def compare(self, case, mode):
        outcomes = {}
        errors = []
        for automatic in (False, True):
            name = 'automatic' if automatic else 'manual'
            try:
                outcomes[name] = self.lane(case, mode, automatic)
                errors += [f'{name}: {error}' for error in outcomes[name]['errors']]
            except Exception as error:
                errors.append(f'{name}: {type(error).__name__}: {error}')
        if len(outcomes) == 2:
            manual, automatic = outcomes['manual'], outcomes['automatic']
            for key in ('rc', 'init_rc', 'logical'):
                if manual[key] != automatic[key]:
                    errors.append(f'manual/automatic {key} differs')
            for phase in ('initialized', 'before_server'):
                left, right = manual[phase], automatic[phase]
                if left and right and manual['init_rc'] == automatic['init_rc'] == 0:
                    for key in ('data_mode', 'tree', 'wal', 'control', 'configuration', 'settings'):
                        if left.get(key) != right.get(key):
                            errors.append(f'{phase}: {key} differs')
                    if phase == 'before_server' and left.get('target_permissions') != right.get('target_permissions'):
                        errors.append('before_server: target/parent permissions differ')
                    if phase == 'before_server':
                        for key in ('startup_files', 'logs'):
                            if left.get(key) != right.get(key):
                                errors.append(f'before_server: {key} differs')
            if (mode == 'upgrade' and manual['rc'] == automatic['rc'] == 0
                    and manual['final_permissions'] != automatic['final_permissions']):
                errors.append('final target/parent permissions differ')
        result = {'case': case['name'], 'mode': mode, 'passed': not errors, 'errors': errors}
        self.results.append(result)
        write_json(self.output / 'results.json', self.results)
        print(f'{"PASS" if not errors else "FAIL"} {case["name"]}/{mode}', flush=True)
        for error in errors:
            print(f'  {error}', flush=True)

    def close(self):
        remaining = list(self.output.rglob('postmaster.pid'))
        write_json(self.output / 'remaining-postmasters.json', [str(p) for p in remaining])
        self.sock.rmdir()
        return not remaining


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


class LinuxRunner(Runner):
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
            relations[name] = metadata(path)
        self.current['relations'] = relations
        control = self.run('final-control', [self.bin / 'pg_controldata', data], required=True)
        self.current['control'] = {
            key: value.strip() for line in control.stdout.splitlines()
            for key, separator, value in [line.partition(':')]
            if separator and key in CONTROL_FIELDS}
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
            write_json(lane / 'outcome.json', outcome)
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
        write_json(self.output / 'remaining-source-postmasters.json', [str(path) for path in remaining])
        clean = clean and not remaining
        if self.args.prune and clean and all(result['passed'] for result in self.results):
            for template, _ in self.templates.values():
                prune_data(template)
                prune_data(template.with_name(template.name + '-tablespace'))
        return clean


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--bindir', required=True, type=Path)
    parser.add_argument('--old-bindir', type=Path)
    parser.add_argument('--output', type=Path,
                        default=Path(tempfile.gettempdir()) / ('pg-initdb-parity-' + uuid4().hex[:12]))
    parser.add_argument('--case', action='append', default=[], help='case glob (repeatable)')
    parser.add_argument('--mode', choices=('both', 'upgrade', 'check'), default='both')
    linux = parser.add_argument_group('expanded Linux checks')
    linux.add_argument('--transfer', choices=(*TRANSFERS, 'all'),
                       help='run expanded checks with the selected transfer modes')
    linux.add_argument('--source-root', type=Path,
                       help='put source clusters on a different filesystem (requires --transfer)')
    linux.add_argument('--prune', action='store_true',
                       help='remove passed test data after shutdown (requires --transfer)')
    linux.add_argument('--expect-transfer-failure', action='store_true',
                       help='verify native rejection on unsupported filesystems (requires --transfer)')
    args = parser.parse_args()
    if os.name != 'posix' or os.geteuid() == 0:
        parser.error('run as an unprivileged user on POSIX')
    expanded = args.transfer is not None
    if expanded and not sys.platform.startswith('linux'):
        parser.error('--transfer requires Linux')
    if not expanded and (args.source_root or args.prune or args.expect_transfer_failure):
        parser.error('--source-root, --prune and --expect-transfer-failure require --transfer')
    configurations = cases() + extra_cases() if expanded else cases()
    selected = [case for case in configurations
                if not args.case or any(fnmatch.fnmatchcase(case['name'], pattern) for pattern in args.case)]
    if not selected:
        parser.error('no matching cases')
    runner = LinuxRunner(args) if expanded else Runner(args)
    print(f'Reports: {runner.output}', flush=True)
    transfers = TRANSFERS if args.transfer == 'all' else (args.transfer,)
    if expanded:
        write_json(runner.output / 'coverage.json', {
            'cases': selected, 'transfers': transfers, 'operation': args.mode,
            'bindir': str(runner.bin), 'old_bindir': str(runner.oldbin)})
    try:
        for transfer in transfers:
            for original in selected:
                case = dict(original)
                if expanded:
                    case |= {'name': original['name'] + '-' + transfer,
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
                else:
                    source_name = 'old-' + case['name'] if case.get('old_options') else 'old'
                    runner.old = runner.output / source_name
                    if not runner.old.exists():
                        runner.initialize_old(case.get('old_options', []))
                for mode in (('upgrade', 'check') if args.mode == 'both' else (args.mode,)):
                    mode_case = case | {'expected': case.get(mode + '_expected',
                                                            case.get('expected', 'success'))}
                    runner.compare(mode_case, mode)
                    if expanded and not runner.results[-1]['passed']:
                        return 1
    finally:
        clean = runner.close()
    passed = sum(result['passed'] for result in runner.results)
    print(f'{passed}/{len(runner.results)} comparisons passed, no running servers: {clean}', flush=True)
    return 0 if clean and passed == len(runner.results) else 1


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--wrapper':
        sys.exit(wrapper(sys.argv[2], Path(sys.argv[3]), sys.argv[4:]))
    sys.exit(main())
