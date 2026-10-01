#!/usr/bin/env python3

"""Shared support for the manual WAL-upgrade tests."""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence

MIB = 1024 * 1024
GIB = 1024 * MIB
REPLICATION_USER = "upgrade_repl"
GENERIC_TRANSFER_MODES = ("copy", "copy-file-range", "clone", "link", "swap")
REFERENCE_TRANSFER_MODES = {"clone", "link", "swap"}
WAL_SEGMENT_RE = re.compile(r"^[0-9A-F]{24}$")
LSN_RE = r"[0-9A-F]+/[0-9A-F]+"
WAL_RECORD_RE = re.compile(
    rf"^rmgr:\s+(?P<rmgr>\S+)\s+len \(rec/tot\):\s+\d+/\s*(?P<bytes>\d+),"
    rf".*\blsn:\s*(?P<lsn>{LSN_RE}),\s+end:\s*(?P<end>{LSN_RE}),"
    r".*\bdesc:\s+(?P<record>\S+)(?P<description>.*)$"
)
RELINK_ENTRY_RE = re.compile(
    r"; (?P<entry_type>DIRECTORY|RELATION|FILE) "
    r"(?P<operation>INHERIT|RECREATE|CREATE|DELETE) "
    r"key (?P<key>\d+/\d+/\d+) fork (?P<fork>\d+) blocks (?P<blocks>\d+)"
)
FPI_RELATION_RE = re.compile(r"\brel (?P<key>\d+/\d+/\d+)\b")
WAL_STATS_ROW_RE = re.compile(
    r"^(?P<record>\S+)\s+(?P<count>\d+)\s+\([^)]*\)\s+"
    r"\d+\s+\([^)]*\)\s+\d+\s+\([^)]*\)\s+(?P<bytes>\d+)\s+\([^)]*\)$"
)


def transfer_mode_option(mode: str) -> str:
    if mode not in GENERIC_TRANSFER_MODES:
        raise ValueError(mode)
    return f"--{mode}"


@dataclass(frozen=True)
class PostgresBinaries:
    bindir: Path

    def binary(self, name: str) -> Path:
        path = self.bindir / name
        if not path.is_file():
            raise FileNotFoundError(path)
        return path

    def version(self) -> str:
        return run(
            [self.binary("postgres"), "--version"],
            log_command=False,
        ).stdout.strip()


@dataclass(frozen=True)
class Cluster:
    pgdata: Path
    binaries: PostgresBinaries
    port: int
    name: str

    def psql(
        self,
        query: str,
        *,
        database: str = "postgres",
        check: bool = True,
        log_command: bool = True,
        options: Sequence[str] = (),
    ) -> str:
        result = run(
            [
                self.binaries.binary("psql"),
                "-XAt",
                "-v",
                "ON_ERROR_STOP=1",
                "-F",
                "|",
                *options,
                "-h",
                "127.0.0.1",
                "-p",
                str(self.port),
                "-d",
                database,
                "-c",
                query,
            ],
            check=check,
            log_command=log_command,
        )
        return result.stdout.strip() if result.returncode == 0 else ""

    def initdb(self) -> None:
        run(
            [
                self.binaries.binary("initdb"),
                "-D",
                self.pgdata,
                "-A",
                "trust",
                "--no-locale",
                "--data-checksums",
            ]
        )

    def start(self, log_path: Path, *, wait: bool = True) -> None:
        run(
            [
                self.binaries.binary("pg_ctl"),
                "-w" if wait else "-W",
                "-D",
                self.pgdata,
                "-l",
                log_path,
                "start",
            ]
        )

    def stop_if_running(self) -> None:
        status = run(
            [self.binaries.binary("pg_ctl"), "status", "-D", self.pgdata],
            check=False,
            log_command=False,
        )
        if status.returncode == 0:
            run(
                [
                    self.binaries.binary("pg_ctl"),
                    "-w",
                    "-D",
                    self.pgdata,
                    "-m",
                    "fast",
                    "stop",
                ],
                check=False,
            )


@dataclass
class UpgradeWal:
    start_lsn: str
    complete_end_lsn: str
    window_bytes: int
    relink_records: int
    relink_entries: int
    relink_bytes: int
    relink_entry_counts: dict[str, int]
    rawfile_records: int
    rawfile_payload_bytes: int
    rawfile_wal_bytes: int
    rawfile_payload_by_directory: dict[str, int]
    fpi_records: int
    fpi_wal_bytes: int

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass
class RelationWalCoverage:
    inherited_relation_keys: set[str]
    fpi_relation_keys: set[str]


class PortAllocator:
    def __init__(self, first_port: int):
        self.next_port = first_port

    def get(self) -> int:
        port = self.next_port
        self.next_port += 1
        return port


def run(
    command: Sequence[str | Path],
    *,
    output: Path | None = None,
    cwd: Path | None = None,
    check: bool = True,
    log_command: bool = True,
) -> subprocess.CompletedProcess[str]:
    argv = [str(argument) for argument in command]
    if log_command:
        print(f"+ {shlex.join(argv)}", flush=True)
    if output is None:
        result = subprocess.run(
            argv,
            cwd=cwd,
            env=os.environ.copy(),
            capture_output=True,
            text=True,
            check=False,
        )
    else:
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("w") as stream:
            result = subprocess.run(
                argv,
                cwd=cwd,
                env=os.environ.copy(),
                stdout=stream,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
    if check and result.returncode != 0:
        details = output.read_text() if output is not None else result.stdout + result.stderr
        raise RuntimeError(
            f"command failed with status {result.returncode}: {shlex.join(argv)}\n{details}"
        )
    return result


def append(path: Path, contents: str) -> None:
    with path.open("a") as stream:
        stream.write(contents)


def sql_literal(value: str | Path) -> str:
    return str(value).replace("'", "''")


def configure_primary(cluster: Cluster, socket_dir: Path) -> None:
    append(
        cluster.pgdata / "postgresql.conf",
        f"""
port = {cluster.port}
listen_addresses = '127.0.0.1'
unix_socket_directories = '{sql_literal(socket_dir)}'
wal_level = replica
max_wal_senders = 10
max_replication_slots = 10
hot_standby = on
max_slot_wal_keep_size = -1
autovacuum = off
""",
    )
    append(
        cluster.pgdata / "pg_hba.conf",
        f"host replication {REPLICATION_USER} 127.0.0.1/32 trust\n",
    )


def configure_standalone(cluster: Cluster, socket_dir: Path) -> None:
    append(
        cluster.pgdata / "postgresql.conf",
        f"""
port = {cluster.port}
listen_addresses = '127.0.0.1'
unix_socket_directories = '{sql_literal(socket_dir)}'
autovacuum = off
""",
    )


def configure_old_standby(standby: Cluster, primary: Cluster, slot: str) -> None:
    append(
        standby.pgdata / "postgresql.auto.conf",
        f"""
port = {standby.port}
primary_conninfo = 'host=127.0.0.1 port={primary.port} user={REPLICATION_USER} application_name={standby.name}'
primary_slot_name = '{slot}'
""",
    )
    (standby.pgdata / "standby.signal").touch()


def configure_upgrade_standby(
    standby: Cluster,
    primary: Cluster,
    retained: Cluster,
    socket_dir: Path,
) -> None:
    append(
        standby.pgdata / "postgresql.conf",
        f"""
port = {standby.port}
listen_addresses = '127.0.0.1'
unix_socket_directories = '{sql_literal(socket_dir)}'
wal_level = replica
max_wal_senders = 10
max_replication_slots = 10
hot_standby = on
primary_conninfo = 'host=127.0.0.1 port={primary.port} user={REPLICATION_USER} application_name={standby.name}'
primary_slot_name = ''
wal_receiver_create_temp_slot = off
pg_upgrade_standby_old_datadir = '{sql_literal(retained.pgdata)}'
pg_upgrade_standby_transfer_mode = mirror
""",
    )
    (standby.pgdata / "standby.signal").touch()
    (standby.pgdata / "pg_upgrade.signal").touch()


def configure_streaming_standby(standby: Cluster, primary: Cluster) -> None:
    append(
        standby.pgdata / "postgresql.auto.conf",
        f"""
port = {standby.port}
primary_conninfo = 'host=127.0.0.1 port={primary.port} user={REPLICATION_USER} application_name={standby.name}'
primary_slot_name = ''
wal_receiver_create_temp_slot = off
""",
    )
    (standby.pgdata / "standby.signal").touch()


def wait_until(description: str, predicate: Callable[[], bool], timeout: float = 600) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.1)
    raise TimeoutError(f"timed out waiting for {description}")


def streaming_over_tcp(primary: Cluster, standby: Cluster, slot: str) -> bool:
    query = f"""
        SELECT count(*) = 1
        FROM pg_stat_replication r
        JOIN pg_replication_slots s ON s.active_pid = r.pid
        WHERE r.application_name = '{standby.name}'
          AND r.client_addr = '127.0.0.1'::inet
          AND r.state = 'streaming'
          AND s.slot_name = '{slot}'
          AND s.slot_type = 'physical'
    """
    return primary.psql(query, check=False, log_command=False) == "t"


def caught_up(primary: Cluster, standby: Cluster) -> bool:
    target = primary.psql(
        "SELECT pg_current_wal_flush_lsn()",
        check=False,
        log_command=False,
    )
    if not target:
        return False
    return (
        standby.psql(
            f"SELECT coalesce(pg_last_wal_replay_lsn() >= '{target}'::pg_lsn, false)",
            check=False,
            log_command=False,
        )
        == "t"
    )


def upgrade_finalized(cluster: Cluster) -> bool:
    if (cluster.pgdata / "pg_upgrade.signal").exists():
        return False
    result = run(
        [cluster.binaries.binary("pg_controldata"), "-D", cluster.pgdata],
        check=False,
        log_command=False,
    )
    return (
        result.returncode == 0
        and re.search(r"^wal-upgrade window finalized:\s+yes$", result.stdout, re.MULTILINE)
        is not None
    )


def copy_stopped_cluster(source: Path, target: Path, output: Path | None = None) -> str:
    if (source / "postmaster.pid").exists():
        raise RuntimeError(f"source cluster is running: {source}")
    target.parent.mkdir(parents=True, exist_ok=True)
    cp = shutil.which("cp")
    if cp is not None:
        result = run(
            [cp, "--archive", "--reflink=always", source, target],
            output=output,
            check=False,
        )
        if result.returncode == 0:
            return "reflink"
        if target.exists():
            shutil.rmtree(target)
    shutil.copytree(source, target, symlinks=True)
    return "copy"


def directory_bytes(root: Path) -> int:
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def allocated_bytes(*roots: Path) -> int:
    seen: set[tuple[int, int]] = set()
    allocated = 0
    for root in roots:
        for directory, subdirectories, filenames in os.walk(root, followlinks=False):
            paths = [Path(directory), *(Path(directory) / name for name in subdirectories)]
            paths.extend(Path(directory) / name for name in filenames)
            for path in paths:
                stat = path.lstat()
                identity = (stat.st_dev, stat.st_ino)
                if identity not in seen:
                    seen.add(identity)
                    allocated += getattr(stat, "st_blocks", (stat.st_size + 511) // 512) * 512
    return allocated


def retained_wal_bytes(pgdata: Path) -> int:
    return sum(
        path.stat().st_size
        for path in (pgdata / "pg_wal").iterdir()
        if path.is_file() and WAL_SEGMENT_RE.fullmatch(path.name)
    )


def lsn_bytes(lsn: str) -> int:
    high, low = lsn.split("/")
    return int(high, 16) * (1 << 32) + int(low, 16)


def receiver_lsn_span(standby: Cluster) -> int:
    positions = standby.psql(
        "SELECT receive_start_lsn, latest_end_lsn FROM pg_stat_wal_receiver"
    ).split("|")
    if len(positions) != 2 or not all(positions):
        raise AssertionError(positions)
    return lsn_bytes(positions[1]) - lsn_bytes(positions[0])


def _rawfile_directory(path: str) -> str | None:
    for directory in ("pg_xact", "pg_multixact/offsets", "pg_multixact/members"):
        if path.startswith(f"{directory}/"):
            return directory
    return None


def read_upgrade_wal(
    binaries: PostgresBinaries,
    pgdata: Path,
    output_dir: Path,
    label: str,
) -> tuple[UpgradeWal, RelationWalCoverage]:
    wal_dir = pgdata / "pg_wal"
    segments = sorted(
        path.name for path in wal_dir.iterdir() if WAL_SEGMENT_RE.fullmatch(path.name)
    )
    if not segments:
        raise AssertionError(f"no WAL segments in {wal_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)
    records_path = output_dir / f"upgrade_pg_waldump_{label}.txt"
    run(
        [
            binaries.binary("pg_waldump"),
            "-p",
            wal_dir,
            "-r",
            "PgUpgrade",
            segments[0],
            segments[-1],
        ],
        output=records_path,
        check=False,
    )

    start_lsn = None
    complete_end_lsn = None
    relink_records = 0
    relink_entries = 0
    relink_bytes = 0
    relink_entry_counts: dict[str, int] = {}
    rawfile_records = 0
    rawfile_payload_bytes = 0
    rawfile_wal_bytes = 0
    rawfile_payload_by_directory: dict[str, int] = {}
    inherited_relation_keys: set[str] = set()
    for line in records_path.read_text().splitlines():
        match = WAL_RECORD_RE.match(line)
        if match is None:
            continue
        record = match.group("record")
        description = match.group("description")
        record_bytes = int(match.group("bytes"))
        if record == "PG_UPGRADE_START":
            start_lsn = match.group("lsn")
        elif record == "PG_UPGRADE_COMPLETE":
            complete_end_lsn = match.group("end")
        elif record == "UPGRADE_RELINK":
            entries = re.search(r"\bentries\s+(\d+)", description)
            if entries is None:
                raise AssertionError(description)
            relink_records += 1
            relink_entries += int(entries.group(1))
            relink_bytes += record_bytes
            for entry in RELINK_ENTRY_RE.finditer(description):
                key = f"{entry.group('entry_type').lower()}_{entry.group('operation').lower()}"
                relink_entry_counts[key] = relink_entry_counts.get(key, 0) + 1
                if key == "relation_inherit":
                    inherited_relation_keys.add(entry.group("key"))
        elif record == "UPGRADE_RAWFILE":
            rawfile = re.search(r'\brawfile "([^"]+)";.*\bbytes\s+(\d+)', description)
            if rawfile is None:
                raise AssertionError(description)
            path = rawfile.group(1)
            payload_bytes = int(rawfile.group(2))
            rawfile_records += 1
            rawfile_payload_bytes += payload_bytes
            rawfile_wal_bytes += record_bytes
            directory = _rawfile_directory(path)
            if directory is not None:
                rawfile_payload_by_directory[directory] = (
                    rawfile_payload_by_directory.get(directory, 0) + payload_bytes
                )

    if start_lsn is None or complete_end_lsn is None:
        raise AssertionError(records_path.read_text())
    if sum(relink_entry_counts.values()) != relink_entries:
        raise AssertionError((relink_entries, relink_entry_counts))

    stats_path = output_dir / f"upgrade_pg_waldump_stats_{label}.txt"
    run(
        [
            binaries.binary("pg_waldump"),
            "-p",
            wal_dir,
            "-s",
            start_lsn,
            "-e",
            complete_end_lsn,
            "--stats=record",
        ],
        output=stats_path,
    )
    fpi_records = 0
    fpi_wal_bytes = 0
    for line in stats_path.read_text().splitlines():
        stats = WAL_STATS_ROW_RE.match(line)
        if stats is not None and stats.group("record") == "XLOG/FPI":
            fpi_records = int(stats.group("count"))
            fpi_wal_bytes = int(stats.group("bytes"))
            break
    if fpi_records == 0:
        raise AssertionError(stats_path.read_text())

    fpi_path = output_dir / f"upgrade_pg_waldump_fpi_{label}.txt"
    run(
        [
            binaries.binary("pg_waldump"),
            "-p",
            wal_dir,
            "-r",
            "XLOG",
            "--fullpage",
            "-s",
            start_lsn,
            "-e",
            complete_end_lsn,
        ],
        output=fpi_path,
    )
    fpi_relation_keys: set[str] = set()
    for line in fpi_path.read_text().splitlines():
        fpi_relation_keys.update(
            relation.group("key") for relation in FPI_RELATION_RE.finditer(line)
        )

    return (
        UpgradeWal(
            start_lsn=start_lsn,
            complete_end_lsn=complete_end_lsn,
            window_bytes=lsn_bytes(complete_end_lsn) - lsn_bytes(start_lsn),
            relink_records=relink_records,
            relink_entries=relink_entries,
            relink_bytes=relink_bytes,
            relink_entry_counts=relink_entry_counts,
            rawfile_records=rawfile_records,
            rawfile_payload_bytes=rawfile_payload_bytes,
            rawfile_wal_bytes=rawfile_wal_bytes,
            rawfile_payload_by_directory=rawfile_payload_by_directory,
            fpi_records=fpi_records,
            fpi_wal_bytes=fpi_wal_bytes,
        ),
        RelationWalCoverage(
            inherited_relation_keys=inherited_relation_keys,
            fpi_relation_keys=fpi_relation_keys,
        ),
    )


def ensure_empty_work_dir(path: Path) -> Path:
    path = path.resolve()
    path.mkdir(parents=True, exist_ok=True)
    if any(path.iterdir()):
        raise RuntimeError(f"work directory is not empty: {path}")
    return path


def check_free_space(path: Path, minimum_gib: int) -> None:
    free = shutil.disk_usage(path).free
    if free < minimum_gib * GIB:
        raise RuntimeError(f"test needs {minimum_gib} GiB free, found {free / GIB:.1f} GiB")


def check_binaries(binaries: PostgresBinaries, names: Iterable[str]) -> None:
    for name in names:
        binaries.binary(name)


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
