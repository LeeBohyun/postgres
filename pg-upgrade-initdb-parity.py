#!/usr/bin/env python3
"""Standalone POSIX parity test; deliberately separate from the core TAP suite.

  python3 pg-upgrade-initdb-parity.py --bindir /path/to/install/bin

Compare native initdb followed by pg_upgrade with pg_upgrade --initdb. Wrappers
observe initialization and the first new-server startup without changing their
arguments or results. Compare configuration, control settings, file permissions,
and upgraded SQL data, rather than random cluster IDs or physical WAL bytes.
Automatic checks intentionally delete their cluster; manual checks retain it.
Reports and command logs remain in --output; any mismatch makes the test fail.
Use --case GLOB and --mode check|upgrade to run a smaller selection.
"""

import argparse
import fnmatch
import hashlib
import json
import os
from pathlib import Path
import shlex
import socket
import stat
import subprocess
import sys
import tempfile
from uuid import uuid4


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

    def initialize_old(self):
        self.run('old-initdb', [self.oldbin / 'initdb', '-D', self.old, '-N',
                               '-U', USER, '-A', 'trust', '--allow-group-access',
                               '--locale=C', *BASE_OPTIONS], required=True)
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
            if automatic or init_rc == 0:
                check(len(logs) == (1 if keep_logs else 0), 'wrong number/location of retained log sessions')
            if automatic and logs and init_rc == 0:
                check('performing post-bootstrap initialization' in logs[0].read_text(),
                      'successful initdb output missing from logs')
            if automatic and mode == 'check':
                if baseline['0'] is not None:
                    check(metadata(target) == baseline['0'], 'existing target permissions/ownership not restored')
                if not case.get('no_clean'):
                    check(not (target / 'PG_VERSION').exists(), 'check left initialized final cluster')
                    leftovers = [p.name for p in target.iterdir()
                                 if p.name != 'pg_upgrade_output.d'] if target.exists() else []
                    check(not leftovers, f'check left temporary data: {leftovers}')
                if init_rc == 0 and wal:
                    check(metadata(wal) == wal_before, 'external WAL pre-run state not restored')
                    check(metadata(wal.parent) == wal_parent_before, 'external WAL parent state not restored')
                    check(not wal.exists() or not list(wal.iterdir()), 'external WAL still contains data')
                if not keep_logs and baseline['0'] is None:
                    check(not target.parent.parent.exists(), 'check left created parent directories')
            if init_rc == 0 and (not automatic or mode == 'upgrade'):
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
                    'SELECT datname, encoding, datlocprovider, datcollate, datctype '
                    'FROM pg_database ORDER BY datname', self.bin, env=env)
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
            if (mode == 'upgrade' and manual['init_rc'] == automatic['init_rc'] == 0
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


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--bindir', required=True, type=Path)
    parser.add_argument('--old-bindir', type=Path)
    parser.add_argument('--output', type=Path,
                        default=Path(tempfile.gettempdir()) / ('pg-initdb-parity-' + uuid4().hex[:12]))
    parser.add_argument('--case', action='append', default=[], help='case glob (repeatable)')
    parser.add_argument('--mode', choices=['both', 'upgrade', 'check'], default='both')
    args = parser.parse_args()
    if os.name != 'posix' or os.geteuid() == 0:
        parser.error('run as an unprivileged user on POSIX')
    selected = [case for case in cases()
                if not args.case or any(fnmatch.fnmatchcase(case['name'], glob) for glob in args.case)]
    if not selected:
        parser.error('no matching cases')
    runner = Runner(args)
    print(f'Reports: {runner.output}', flush=True)
    try:
        runner.initialize_old()
        for case in selected:
            for mode in (['upgrade', 'check'] if args.mode == 'both' else [args.mode]):
                runner.compare(case, mode)
    finally:
        clean = runner.close()
    passed = sum(result['passed'] for result in runner.results)
    print(f'{passed}/{len(runner.results)} comparisons passed; no running servers: {clean}', flush=True)
    return 0 if clean and all(result['passed'] for result in runner.results) else 1


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--wrapper':
        sys.exit(wrapper(sys.argv[2], Path(sys.argv[3]), sys.argv[4:]))
    sys.exit(main())
