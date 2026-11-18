#!/usr/bin/env python3

"""Exercise WAL-upgrade completion validation with valid and damaged state."""

from __future__ import annotations

import argparse
import os
import re
import shutil
import struct
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from common import (
    Cluster,
    PortAllocator,
    PostgresBinaries,
    append,
    check_binaries,
    check_free_space,
    configure_standalone,
    copy_stopped_cluster,
    ensure_empty_work_dir,
    read_upgrade_wal,
    run,
    transfer_mode_option,
    write_json,
)

BYTE_ORDER = "<" if sys.byteorder == "little" else ">"
HEADER_SIZE = 112
HEADER_PREFIX = struct.Struct(f"{BYTE_ORDER}QIIQQ")
DATABASE = struct.Struct(f"{BYTE_ORDER}IIIB3s")
UINT32 = struct.Struct(f"{BYTE_ORDER}I")
UINT64 = struct.Struct(f"{BYTE_ORDER}Q")
ENTRY = struct.Struct(f"{BYTE_ORDER}IIBBH")

HEADER_CHECKSUM_OFFSET = 12
HEADER_NEW_SYSTEM_IDENTIFIER_OFFSET = 16
HEADER_NEW_SECTION_OFFSET = 24
START_NEW_MAJOR_OFFSET = 36
START_BLOCK_SIZE_OFFSET = 80

BLCKSZ = 8192


@dataclass
class RelinkDatabase:
    description: DatabaseDescription
    entries: list[RelinkEntry]


@dataclass
class RelinkManifest:
    header: bytearray
    old_databases: list[RelinkDatabase]
    new_databases: list[RelinkDatabase]

    @classmethod
    def read(cls, path: Path) -> RelinkManifest:
        contents = path.read_bytes()
        if len(contents) < HEADER_SIZE:
            raise AssertionError("relink file is too short")
        _, _, _, _, new_offset = HEADER_PREFIX.unpack_from(contents)
        if not HEADER_SIZE <= new_offset <= len(contents):
            raise AssertionError("invalid new-section offset")
        return cls(
            bytearray(contents[:HEADER_SIZE]),
            read_databases(contents, HEADER_SIZE, new_offset),
            read_databases(contents, new_offset, len(contents)),
        )

    def write(
        self,
        path: Path,
        *,
        repair_checksum: bool = True,
        repair_new_offset: bool = True,
    ) -> None:
        old_contents = write_databases(self.old_databases)
        new_contents = write_databases(self.new_databases)
        if repair_new_offset:
            UINT64.pack_into(
                self.header,
                HEADER_NEW_SECTION_OFFSET,
                HEADER_SIZE + len(old_contents),
            )
        if repair_checksum:
            UINT32.pack_into(self.header, HEADER_CHECKSUM_OFFSET, 0)
        contents = self.header + old_contents + new_contents
        if repair_checksum:
            UINT32.pack_into(contents, HEADER_CHECKSUM_OFFSET, crc32c(contents))
            self.header[:] = contents[:HEADER_SIZE]
        path.write_bytes(contents)


@dataclass(frozen=True)
class DatabaseDescription:
    database_oid: int
    default_tablespace: int
    relation_count: int
    flags: int
    reserved: bytes


@dataclass(frozen=True)
class RelinkEntry:
    tablespace_oid: int
    filenumber: int
    persistence: int
    flags: int
    reserved: int


@dataclass(frozen=True)
class ValidationCase:
    name: str
    succeeds: bool
    error: str | None = None
    transfer_mode: str = "copy"
    allow_unsupported: bool = False


@dataclass
class CaseContext:
    name: str
    new_pgdata: Path
    manifest_path: Path
    socket_dir: Path
    new_cluster: Cluster
    output_dir: Path

    def manifest(self) -> RelinkManifest:
        return RelinkManifest.read(self.manifest_path)

    def new_cluster_query(self, query: str) -> str:
        return run(
            [
                self.new_cluster.binaries.binary("psql"),
                "-XAt",
                "-v",
                "ON_ERROR_STOP=1",
                "-h",
                self.socket_dir,
                "-p",
                str(self.new_cluster.port),
                "-d",
                "postgres",
                "-c",
                query,
            ],
            log_command=False,
        ).stdout.strip()

    def relation_path(self, relation: str, fork: str = "") -> Path:
        relative = self.new_cluster_query(f"SELECT pg_relation_filepath('{relation}')")
        if not relative:
            raise AssertionError(f"no path for {relation}")
        suffix = f"_{fork}" if fork else ""
        return self.new_pgdata / f"{relative}{suffix}"


def crc32c(contents: bytes | bytearray) -> int:
    crc = 0xFFFFFFFF
    for byte in contents:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82F63B78 if crc & 1 else 0)
    return crc ^ 0xFFFFFFFF


def read_databases(contents: bytes, start: int, end: int) -> list[RelinkDatabase]:
    databases: list[RelinkDatabase] = []
    offset = start
    while offset < end:
        if end - offset < DATABASE.size:
            raise AssertionError("truncated database description")
        description = DatabaseDescription(*DATABASE.unpack_from(contents, offset))
        offset += DATABASE.size
        entry_count = description.relation_count
        entry_bytes = entry_count * ENTRY.size
        if entry_bytes > end - offset:
            raise AssertionError("database entries exceed their section")
        entries = [
            RelinkEntry(*ENTRY.unpack_from(contents, offset + index * ENTRY.size))
            for index in range(entry_count)
        ]
        offset += entry_bytes
        databases.append(RelinkDatabase(description, entries))
    return databases


def write_databases(databases: list[RelinkDatabase]) -> bytearray:
    contents = bytearray()
    for database in databases:
        description = database.description
        contents.extend(
            DATABASE.pack(
                description.database_oid,
                description.default_tablespace,
                description.relation_count,
                description.flags,
                description.reserved,
            )
        )
        for entry in database.entries:
            contents.extend(
                ENTRY.pack(
                    entry.tablespace_oid,
                    entry.filenumber,
                    entry.persistence,
                    entry.flags,
                    entry.reserved,
                )
            )
    return contents


def replace_database(description: DatabaseDescription, **changes: object) -> DatabaseDescription:
    values = {
        "database_oid": description.database_oid,
        "default_tablespace": description.default_tablespace,
        "relation_count": description.relation_count,
        "flags": description.flags,
        "reserved": description.reserved,
    }
    values.update(changes)
    return DatabaseDescription(**values)


def replace_entry(value: RelinkEntry, **changes: object) -> RelinkEntry:
    values = {
        "tablespace_oid": value.tablespace_oid,
        "filenumber": value.filenumber,
        "persistence": value.persistence,
        "flags": value.flags,
        "reserved": value.reserved,
    }
    values.update(changes)
    return RelinkEntry(**values)


def relation_entry(database: RelinkDatabase, position: int = 0) -> tuple[int, RelinkEntry]:
    return position, database.entries[position]


def database_with_relations(databases: list[RelinkDatabase]) -> RelinkDatabase:
    for database in databases:
        if database.description.relation_count >= 2:
            return database
    raise AssertionError("fixture has no database with two relations")


def write_manifest(manifest: RelinkManifest, context: CaseContext, **options: bool) -> None:
    manifest.write(context.manifest_path, **options)


def apply_manifest_mutation(context: CaseContext) -> bool:
    name = context.name
    manifest = context.manifest()

    if name in {
        "baseline_copy",
        "baseline_copy_file_range",
        "baseline_link",
        "baseline_clone",
        "baseline_swap",
    }:
        write_manifest(manifest, context)
        return True
    if name == "bad_header_magic":
        manifest.header[0] ^= 0xFF
    elif name == "wrong_new_identity":
        system_identifier = UINT64.unpack_from(
            manifest.header, HEADER_NEW_SYSTEM_IDENTIFIER_OFFSET
        )[0]
        UINT64.pack_into(
            manifest.header,
            HEADER_NEW_SYSTEM_IDENTIFIER_OFFSET,
            system_identifier + 1,
        )
    elif name == "bad_file_checksum":
        checksum = UINT32.unpack_from(manifest.header, HEADER_CHECKSUM_OFFSET)[0]
        UINT32.pack_into(manifest.header, HEADER_CHECKSUM_OFFSET, checksum ^ 1)
        write_manifest(manifest, context, repair_checksum=False)
        return True
    elif name == "bad_new_section_offset":
        UINT64.pack_into(manifest.header, HEADER_NEW_SECTION_OFFSET, HEADER_SIZE - 1)
        write_manifest(manifest, context, repair_new_offset=False)
        return True
    elif name == "wrong_new_major":
        new_major = UINT32.unpack_from(manifest.header, START_NEW_MAJOR_OFFSET)[0]
        UINT32.pack_into(manifest.header, START_NEW_MAJOR_OFFSET, new_major + 10000)
    elif name == "wrong_block_size":
        block_size = UINT32.unpack_from(manifest.header, START_BLOCK_SIZE_OFFSET)[0]
        UINT32.pack_into(manifest.header, START_BLOCK_SIZE_OFFSET, block_size * 2)
    elif name == "database_reserved_byte":
        database = manifest.old_databases[1]
        database.description = replace_database(database.description, reserved=b"\x01\x00\x00")
    elif name == "database_unknown_flag":
        database = manifest.old_databases[1]
        database.description = replace_database(
            database.description, flags=database.description.flags | 0x80
        )
    elif name == "database_out_of_order":
        databases = [
            database for database in manifest.old_databases if database.description.database_oid
        ]
        databases[1].description = replace_database(
            databases[1].description,
            database_oid=databases[0].description.database_oid,
        )
    elif name == "declared_observation_missing":
        database = database_with_relations(manifest.old_databases)
        database.description = replace_database(
            database.description,
            relation_count=database.description.relation_count + 1,
        )
    elif name == "excess_observation":
        database = database_with_relations(manifest.old_databases)
        database.description = replace_database(
            database.description,
            relation_count=database.description.relation_count - 1,
        )
    elif name == "zero_default_tablespace":
        database = manifest.old_databases[1]
        database.description = replace_database(
            database.description,
            default_tablespace=0,
        )
    elif name == "old_relation_has_metadata":
        database = database_with_relations(manifest.old_databases)
        index, value = relation_entry(database)
        database.entries[index] = replace_entry(value, persistence=ord("p"))
    elif name == "new_relation_zero_filenumber":
        database = database_with_relations(manifest.new_databases)
        index, value = relation_entry(database)
        database.entries[index] = replace_entry(value, filenumber=0)
    elif name == "new_relation_bad_persistence":
        database = database_with_relations(manifest.new_databases)
        index, value = relation_entry(database)
        database.entries[index] = replace_entry(value, persistence=ord("t"))
    elif name == "new_relation_unknown_flag":
        database = database_with_relations(manifest.new_databases)
        index, value = relation_entry(database)
        database.entries[index] = replace_entry(value, flags=value.flags | 0x80)
    elif name == "new_relation_reserved_byte":
        database = database_with_relations(manifest.new_databases)
        index, value = relation_entry(database)
        database.entries[index] = replace_entry(value, reserved=1)
    elif name == "duplicate_relation_key":
        database = database_with_relations(manifest.new_databases)
        _, first = relation_entry(database)
        second_index, second = relation_entry(database, 1)
        database.entries[second_index] = replace_entry(
            second,
            tablespace_oid=first.tablespace_oid,
            filenumber=first.filenumber,
        )
    else:
        return False

    write_manifest(manifest, context)
    return True


def apply_storage_mutation(context: CaseContext) -> None:
    name = context.name
    main = context.relation_path("validation_table")
    fsm = context.relation_path("validation_table", "fsm")
    vm = context.relation_path("validation_table", "vm")
    unlogged_init = context.relation_path("validation_unlogged", "init")

    if name == "missing_optional_fsm":
        fsm.unlink()
    elif name == "missing_optional_vm":
        vm.unlink()
    elif name == "empty_optional_fsm":
        fsm.write_bytes(b"")
    elif name == "empty_trailing_segment":
        Path(f"{main}.1").write_bytes(b"")
    elif name == "empty_valid_slru_segment":
        (context.new_pgdata / "pg_xact" / "FFFF").write_bytes(b"")
    elif name == "missing_required_main":
        main.unlink()
    elif name == "required_main_is_directory":
        main.unlink()
        main.mkdir()
    elif name == "required_main_is_symlink":
        held = main.with_name(f"{main.name}.held")
        main.rename(held)
        main.symlink_to(held.name)
    elif name == "unaligned_main_size":
        with main.open("ab") as stream:
            stream.write(b"x")
    elif name == "nonempty_after_short_segment":
        Path(f"{main}.1").write_bytes(bytes(BLCKSZ))
    elif name == "missing_unlogged_init":
        unlogged_init.unlink()
    elif name == "wrong_database_pg_version":
        version = main.parent / "PG_VERSION"
        version.write_bytes(b"00\n")
    elif name == "missing_filenode_map":
        (main.parent / "pg_filenode.map").unlink()
    elif name == "invalid_slru_name":
        (context.new_pgdata / "pg_xact" / "NOTHEX").write_bytes(bytes(BLCKSZ))
    elif name == "unaligned_slru_size":
        (context.new_pgdata / "pg_xact" / "FFFF").write_bytes(b"x")
    else:
        raise AssertionError(name)


CASES = (
    ValidationCase("baseline_copy", True),
    ValidationCase(
        "baseline_copy_file_range",
        True,
        transfer_mode="copy-file-range",
        allow_unsupported=True,
    ),
    ValidationCase("baseline_link", True, transfer_mode="link"),
    ValidationCase("baseline_clone", True, transfer_mode="clone", allow_unsupported=True),
    ValidationCase("baseline_swap", True, transfer_mode="swap", allow_unsupported=True),
    ValidationCase("missing_optional_fsm", True),
    ValidationCase("missing_optional_vm", True),
    ValidationCase("empty_optional_fsm", True),
    ValidationCase("empty_trailing_segment", True),
    ValidationCase("empty_valid_slru_segment", True),
    ValidationCase("bad_header_magic", False, "incompatible format"),
    ValidationCase("wrong_new_identity", False, "new cluster system identifier does not match"),
    ValidationCase("bad_file_checksum", False, "checksum mismatch"),
    ValidationCase("bad_new_section_offset", False, "invalid new-section offset"),
    ValidationCase("wrong_new_major", False, "START does not match this server"),
    ValidationCase("wrong_block_size", False, "START does not match this server"),
    ValidationCase("database_reserved_byte", False, "invalid upgrade relink file"),
    ValidationCase("database_unknown_flag", False, "invalid upgrade relink file"),
    ValidationCase("database_out_of_order", False, "invalid upgrade relink file"),
    ValidationCase("declared_observation_missing", False, "invalid upgrade relink file"),
    ValidationCase("excess_observation", False, "invalid upgrade relink file"),
    ValidationCase("zero_default_tablespace", False, "invalid upgrade relink file"),
    ValidationCase("old_relation_has_metadata", False, "invalid upgrade relink file"),
    ValidationCase("new_relation_zero_filenumber", False, "invalid upgrade relink file"),
    ValidationCase("new_relation_bad_persistence", False, "invalid upgrade relink file"),
    ValidationCase("new_relation_unknown_flag", False, "invalid upgrade relink file"),
    ValidationCase("new_relation_reserved_byte", False, "invalid upgrade relink file"),
    ValidationCase("duplicate_relation_key", False, "invalid upgrade relink file"),
    ValidationCase("missing_required_main", False, "could not stat"),
    ValidationCase("required_main_is_directory", False, "nonregular file"),
    ValidationCase("required_main_is_symlink", False, "nonregular file"),
    ValidationCase("unaligned_main_size", False, "invalid relation segment size"),
    ValidationCase("nonempty_after_short_segment", False, "nonempty segment follows"),
    ValidationCase("missing_unlogged_init", False, "could not stat"),
    ValidationCase("wrong_database_pg_version", False, "PG_VERSION contents do not match"),
    ValidationCase("missing_filenode_map", False, "could not stat"),
    ValidationCase("invalid_slru_name", False, "unrecognized segment name"),
    ValidationCase("unaligned_slru_size", False, "SLRU segment has an invalid length"),
)

QUICK_CASES = {
    "baseline_copy",
    "baseline_link",
    "missing_optional_fsm",
    "empty_trailing_segment",
    "bad_file_checksum",
    "bad_new_section_offset",
    "wrong_new_major",
    "declared_observation_missing",
    "new_relation_reserved_byte",
    "missing_required_main",
    "unaligned_main_size",
    "nonempty_after_short_segment",
    "invalid_slru_name",
}


def configure_fixture(cluster: Cluster, socket_dir: Path) -> str:
    cluster.initdb()
    configure_standalone(cluster, socket_dir)
    append(cluster.pgdata / "postgresql.conf", "max_locks_per_transaction = 256\n")
    cluster.start(cluster.pgdata.parent / "seed.log")
    cluster.psql(
        """
        CREATE TABLE validation_table
          (id integer PRIMARY KEY, payload text) WITH (fillfactor = 70);
        INSERT INTO validation_table
          SELECT id, repeat(md5(id::text), 4) FROM generate_series(1, 20000) id;
        DELETE FROM validation_table WHERE id % 3 = 0;
        CREATE TABLE validation_empty (id integer);
        CREATE UNLOGGED TABLE validation_unlogged (id integer);
        INSERT INTO validation_unlogged VALUES (42);
        CREATE SEQUENCE validation_sequence;
        """
    )
    cluster.psql("VACUUM (FREEZE, ANALYZE) validation_table")
    cluster.psql("CREATE DATABASE validation_extra")
    cluster.psql(
        "CREATE TABLE extra_table (id integer PRIMARY KEY); INSERT INTO extra_table VALUES (73)",
        database="validation_extra",
    )
    fingerprint = cluster.psql(
        "SELECT count(*), sum(id), sum(length(payload)) FROM validation_table"
    )
    main = cluster.pgdata / cluster.psql("SELECT pg_relation_filepath('validation_table')")
    for suffix in ("_fsm", "_vm"):
        if not Path(f"{main}{suffix}").is_file():
            raise AssertionError(f"fixture did not create {main}{suffix}")
    cluster.psql("CHECKPOINT")
    cluster.stop_if_running()
    return fingerprint


def upgrade_command(
    case: ValidationCase,
    old: Cluster,
    new: Cluster,
    socket_dir: Path,
    jobs: int,
) -> list[str | Path]:
    return [
        new.binaries.binary("pg_upgrade"),
        "--no-sync",
        "--initdb",
        "--retain",
        "--wal-upgrade",
        transfer_mode_option(case.transfer_mode),
        "--old-datadir",
        old.pgdata,
        "--new-datadir",
        new.pgdata,
        "--old-bindir",
        old.binaries.bindir,
        "--new-bindir",
        new.binaries.bindir,
        "--socketdir",
        socket_dir,
        "--old-port",
        str(old.port),
        "--new-port",
        str(new.port),
        f"--jobs={jobs}",
        "--new-options",
        "-c autovacuum=off -c wal_keep_size=1GB -c max_wal_size=1GB",
    ]


def wait_for_manifest(
    process: subprocess.Popen[object],
    path: Path,
    output: Path,
    new_cluster: Cluster,
    socket_dir: Path,
) -> None:
    deadline = time.monotonic() + 180
    while True:
        manifest_ready = False
        if path.is_file():
            try:
                RelinkManifest.read(path)
                manifest_ready = True
            except (AssertionError, OSError, struct.error):
                pass
        if manifest_ready:
            ready = subprocess.run(
                [
                    new_cluster.binaries.binary("psql"),
                    "-XAt",
                    "-h",
                    str(socket_dir),
                    "-p",
                    str(new_cluster.port),
                    "-d",
                    "postgres",
                    "-c",
                    "SELECT 1",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
            if ready.returncode == 0:
                return
        if process.poll() is not None:
            raise RuntimeError(output.read_text())
        if time.monotonic() >= deadline:
            raise TimeoutError(f"timed out waiting for {path}")
        time.sleep(0.01)


def is_unsupported(output: str) -> bool:
    return (
        re.search(
            r"not supported on this platform|could not .* between old and new data directories",
            output,
        )
        is not None
    )


def control_finalized(binaries: PostgresBinaries, pgdata: Path) -> bool:
    output = run(
        [binaries.binary("pg_controldata"), "-D", pgdata],
        log_command=False,
    ).stdout
    match = re.search(r"^wal-upgrade transaction finalized:\s+(yes|no)$", output, re.MULTILINE)
    if match is None:
        raise AssertionError(output)
    return match.group(1) == "yes"


def upgrade_records(binaries: PostgresBinaries, pgdata: Path, output: Path) -> str:
    segments = sorted(
        path.name
        for path in (pgdata / "pg_wal").iterdir()
        if re.fullmatch(r"[0-9A-F]{24}", path.name)
    )
    if not segments:
        return ""
    run(
        [
            binaries.binary("pg_waldump"),
            "-p",
            pgdata / "pg_wal",
            "-r",
            "PgUpgrade",
            segments[0],
            segments[-1],
        ],
        output=output,
        check=False,
    )
    return output.read_text()


def run_case(
    case: ValidationCase,
    seed: Cluster,
    old_binaries: PostgresBinaries,
    new_binaries: PostgresBinaries,
    old_port: int,
    new_port: int,
    socket_dir: Path,
    work_dir: Path,
    fingerprint: str,
    jobs: int,
    keep_clusters: bool,
) -> dict[str, object]:
    case_root = work_dir / "cases" / case.name
    case_root.mkdir(parents=True)
    old = Cluster(case_root / "old", old_binaries, old_port, f"old_{case.name}")
    new = Cluster(case_root / "new", new_binaries, new_port, f"new_{case.name}")
    copy_method = copy_stopped_cluster(
        seed.pgdata,
        old.pgdata,
        work_dir / "logs" / f"{case.name}_snapshot.log",
    )
    upgrade_cwd = case_root / "pg_upgrade_cwd"
    upgrade_cwd.mkdir()
    gate = case_root / "relink_gate"
    gate.touch()
    manifest_path = new.pgdata / "pg_upgrade_relink"
    output = work_dir / "logs" / f"{case.name}.log"
    env = os.environ.copy()
    env["PG_UPGRADE_TEST_PAUSE_AFTER_RELINK_FINALIZE"] = str(gate)
    started = time.monotonic()
    process: subprocess.Popen[object] | None = None
    try:
        with output.open("w") as stream:
            process = subprocess.Popen(
                [str(argument) for argument in upgrade_command(case, old, new, socket_dir, jobs)],
                cwd=upgrade_cwd,
                env=env,
                stdout=stream,
                stderr=subprocess.STDOUT,
            )
            try:
                wait_for_manifest(process, manifest_path, output, new, socket_dir)
            except RuntimeError:
                text = output.read_text()
                if case.allow_unsupported and is_unsupported(text):
                    return {
                        "status": "unsupported",
                        "transfer_mode": case.transfer_mode,
                        "seconds": time.monotonic() - started,
                        "old_cluster_snapshot": copy_method,
                    }
                raise

            context = CaseContext(
                case.name,
                new.pgdata,
                manifest_path,
                socket_dir,
                new,
                work_dir,
            )
            if not apply_manifest_mutation(context):
                apply_storage_mutation(context)
            artifact = work_dir / "manifests" / f"{case.name}.relink"
            shutil.copyfile(manifest_path, artifact)
            gate.unlink()
            try:
                returncode = process.wait(timeout=600)
            except subprocess.TimeoutExpired:
                process.terminate()
                process.wait(timeout=30)
                raise TimeoutError(f"{case.name} did not finish") from None

        text = output.read_text()
        elapsed = time.monotonic() - started
        finalized = control_finalized(new_binaries, new.pgdata)
        wal_text = upgrade_records(
            new_binaries,
            new.pgdata,
            work_dir / "logs" / f"{case.name}_wal.txt",
        )
        has_complete = "PG_UPGRADE_COMPLETE" in wal_text
        if case.succeeds:
            if returncode != 0:
                raise AssertionError(f"{case.name} failed\n{text}")
            if not finalized or not has_complete:
                raise AssertionError((case.name, finalized, has_complete))
            read_upgrade_wal(
                new_binaries,
                new.pgdata,
                work_dir / "wal_records" / case.name,
                case.name,
            )
            configure_standalone(new, socket_dir)
            new.start(work_dir / "logs" / f"{case.name}_new.log")
            actual = new.psql(
                "SELECT count(*), sum(id), sum(length(payload)) FROM validation_table"
            )
            extra = new.psql("SELECT id FROM extra_table", database="validation_extra")
            edge_relations = new.psql(
                "SELECT (SELECT count(*) FROM validation_empty), "
                "(SELECT sum(id) FROM validation_unlogged), "
                "(SELECT last_value FROM validation_sequence), "
                "(SELECT is_called FROM validation_sequence)"
            )
            if actual != fingerprint or extra != "73" or edge_relations != "0|42|1|f":
                raise AssertionError((case.name, fingerprint, actual, extra, edge_relations))
        else:
            if returncode == 0:
                raise AssertionError(f"{case.name} unexpectedly succeeded")
            if case.error is None or case.error not in text:
                raise AssertionError(f"{case.name} did not report {case.error!r}\n{text}")
            if finalized or has_complete:
                raise AssertionError((case.name, finalized, has_complete))
        return {
            "status": "passed",
            "expected_success": case.succeeds,
            "transfer_mode": case.transfer_mode,
            "seconds": elapsed,
            "old_cluster_snapshot": copy_method,
            "finalized": finalized,
            "complete_record": has_complete,
            "expected_error": case.error,
        }
    finally:
        if gate.exists():
            gate.unlink()
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for cluster in (new, old):
            if cluster.pgdata.exists():
                cluster.stop_if_running()
        if not keep_clusters:
            shutil.rmtree(case_root, ignore_errors=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old-bindir", type=Path, required=True)
    parser.add_argument("--new-bindir", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--profile", choices=("quick", "exhaustive"), default="quick")
    parser.add_argument("--cases", nargs="+", choices=[case.name for case in CASES])
    parser.add_argument("--port-base", type=int, default=55432)
    parser.add_argument("--minimum-free-gb", type=int, default=10)
    parser.add_argument("--jobs", type=int, default=8)
    parser.add_argument("--keep-clusters", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    work_dir = ensure_empty_work_dir(args.work_dir)
    check_free_space(work_dir, args.minimum_free_gb)
    (work_dir / "logs").mkdir()
    (work_dir / "manifests").mkdir()
    (work_dir / "wal_records").mkdir()
    (work_dir / "cases").mkdir()
    old_binaries = PostgresBinaries(args.old_bindir.resolve())
    new_binaries = PostgresBinaries(args.new_bindir.resolve())
    check_binaries(old_binaries, ("initdb", "pg_ctl", "psql"))
    check_binaries(
        new_binaries, ("pg_config", "pg_controldata", "pg_ctl", "pg_upgrade", "pg_waldump", "psql")
    )
    includedir = run(
        [new_binaries.binary("pg_config"), "--includedir"],
        log_command=False,
    ).stdout.strip()
    config = (Path(includedir) / "pg_config.h").read_text()
    if not re.search(r"^#define\s+USE_ASSERT_CHECKING\s+1\b", config, re.MULTILINE):
        raise RuntimeError("completion validation tests require a cassert new-major build")

    selected_names = set(args.cases or ())
    if not selected_names:
        selected_names = (
            {case.name for case in CASES} if args.profile == "exhaustive" else QUICK_CASES
        )
    selected = [case for case in CASES if case.name in selected_names]
    ports = PortAllocator(args.port_base)
    seed = Cluster(work_dir / "seed", old_binaries, ports.get(), "validation_seed")
    old_port = seed.port
    new_port = ports.get()
    socket_dir = Path(tempfile.mkdtemp(prefix="wal-upgrade-validation-"))
    results: dict[str, object] = {}

    try:
        fingerprint = configure_fixture(seed, socket_dir)
        for case in selected:
            print(f"\n=== {case.name} ===", flush=True)
            results[case.name] = run_case(
                case,
                seed,
                old_binaries,
                new_binaries,
                old_port,
                new_port,
                socket_dir,
                work_dir,
                fingerprint,
                args.jobs,
                args.keep_clusters,
            )
            write_json(
                work_dir / "completion_validation.json",
                {
                    "old_pg_version": old_binaries.version(),
                    "new_pg_version": new_binaries.version(),
                    "profile": args.profile,
                    "cases": results,
                },
            )
    finally:
        seed.stop_if_running()
        shutil.rmtree(socket_dir, ignore_errors=True)

    print((work_dir / "completion_validation.json").read_text())


if __name__ == "__main__":
    main()
