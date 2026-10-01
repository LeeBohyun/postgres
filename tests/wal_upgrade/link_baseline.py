#!/usr/bin/env python3

"""Compare ordinary link standby preparation with WAL-upgrade link and copy."""

from __future__ import annotations

import argparse
import re
import shutil
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from common import (
    REPLICATION_USER,
    Cluster,
    PortAllocator,
    PostgresBinaries,
    UpgradeWal,
    allocated_bytes,
    append,
    caught_up,
    check_binaries,
    check_free_space,
    configure_old_standby,
    configure_primary,
    configure_streaming_standby,
    configure_upgrade_standby,
    copy_stopped_cluster,
    directory_bytes,
    ensure_empty_work_dir,
    read_upgrade_wal,
    receiver_lsn_span,
    retained_wal_bytes,
    run,
    streaming_over_tcp,
    transfer_mode_option,
    upgrade_finalized,
    wait_until,
    write_json,
)

RSYNC_BYTES_RE = re.compile(r"^Total transferred file size:\s+([\d,]+) (?:bytes|B)$", re.MULTILINE)


@dataclass(frozen=True)
class WalWorkflow:
    transfer_mode: str
    root: Path
    primary_old: Cluster
    primary_new: Cluster
    standby_old: Cluster
    standby_new: Cluster

    def clusters(self) -> tuple[Cluster, ...]:
        return self.standby_new, self.primary_new, self.standby_old, self.primary_old


@dataclass(frozen=True)
class WalResult:
    fingerprint: str
    metrics: dict[str, int | float]
    snapshot_methods: dict[str, str]
    upgrade_wal: UpgradeWal


def new_wal_workflow(
    root: Path,
    transfer_mode: str,
    old_binaries: PostgresBinaries,
    new_binaries: PostgresBinaries,
    old_primary_port: int,
    ports: PortAllocator,
) -> WalWorkflow:
    label = transfer_mode.replace("-", "_")
    return WalWorkflow(
        transfer_mode=transfer_mode,
        root=root,
        primary_old=Cluster(
            root / "primary" / "old",
            old_binaries,
            old_primary_port,
            f"wal_{label}_old_primary",
        ),
        primary_new=Cluster(
            root / "primary" / "new",
            new_binaries,
            ports.get(),
            f"wal_{label}_new_primary",
        ),
        standby_old=Cluster(
            root / "standby" / "old",
            old_binaries,
            ports.get(),
            f"wal_{label}_old_standby",
        ),
        standby_new=Cluster(
            root / "standby" / "new",
            new_binaries,
            ports.get(),
            f"wal_{label}_new_standby",
        ),
    )


def upgrade_command(
    old: Cluster,
    new: Cluster,
    socket_dir: Path,
    *,
    wal_upgrade: bool,
    transfer_mode: str,
    wal_keep_gb: int,
    jobs: int,
) -> list[str | Path]:
    command: list[str | Path] = [
        new.binaries.binary("pg_upgrade"),
        "--initdb",
        "--retain",
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
    ]
    if wal_upgrade:
        command.extend(
            [
                "--wal-upgrade",
                transfer_mode_option(transfer_mode),
                "--new-options",
                f"-c wal_keep_size={wal_keep_gb}GB "
                f"-c max_wal_size={wal_keep_gb}GB -c wal_compression=off",
            ]
        )
    else:
        if transfer_mode != "link":
            raise AssertionError(transfer_mode)
        command.append("--link")
    return command


def rsync_standby(source_parent: Path, target_parent: Path, output: Path) -> int:
    help_result = run(["rsync", "--help"], log_command=False)
    command = [
        "rsync",
        "--archive",
        "--delete",
        "--hard-links",
        "--size-only",
        "--stats",
    ]
    if "--no-inc-recursive" in help_result.stdout:
        command.append("--no-inc-recursive")
    command.extend(["old", "new", str(target_parent)])
    result = run(command, cwd=source_parent, output=output, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"rsync failed with status {result.returncode}\n{output.read_text()}")
    match = RSYNC_BYTES_RE.search(output.read_text())
    if match is None:
        raise AssertionError(output.read_text())
    return int(match.group(1).replace(",", ""))


def run_wal_workflow(
    workflow: WalWorkflow,
    seed: Cluster,
    seed_standby: Cluster,
    slot: str,
    source_fingerprint: str,
    database_oid: str,
    relation_filenode: str,
    socket_dir: Path,
    output_dir: Path,
    wal_keep_gb: int,
    jobs: int,
) -> WalResult:
    label = workflow.transfer_mode.replace("-", "_")
    prefix = f"wal_{label}"
    snapshot_methods = {
        cluster.name: copy_stopped_cluster(
            source.pgdata,
            cluster.pgdata,
            output_dir / f"snapshot_{cluster.name}.log",
        )
        for cluster, source in (
            (workflow.primary_old, seed),
            (workflow.standby_old, seed_standby),
        )
    }

    workflow.primary_old.start(output_dir / f"{prefix}_old_primary.log")
    workflow.primary_old.psql(f"SELECT pg_create_physical_replication_slot('{slot}', true)")
    configure_old_standby(workflow.standby_old, workflow.primary_old, slot)
    workflow.standby_old.start(output_dir / f"{prefix}_old_standby.log")
    wait_until(
        f"old {workflow.transfer_mode} standby streaming",
        lambda: streaming_over_tcp(workflow.primary_old, workflow.standby_old, slot),
    )
    wait_until(
        f"old {workflow.transfer_mode} standby catchup",
        lambda: caught_up(workflow.primary_old, workflow.standby_old),
    )
    workflow.primary_old.stop_if_running()

    upgrade_cwd = workflow.root / "pg_upgrade_cwd"
    upgrade_cwd.mkdir()
    total_started = time.monotonic()
    upgrade_started = time.monotonic()
    run(
        upgrade_command(
            workflow.primary_old,
            workflow.primary_new,
            socket_dir,
            wal_upgrade=True,
            transfer_mode=workflow.transfer_mode,
            wal_keep_gb=wal_keep_gb,
            jobs=jobs,
        ),
        cwd=upgrade_cwd,
        output=output_dir / f"{prefix}_pg_upgrade.log",
    )
    upgrade_seconds = time.monotonic() - upgrade_started
    wait_until(
        f"old {workflow.transfer_mode} standby HANDOFF pause",
        lambda: workflow.standby_old.psql(
            "SELECT pg_get_wal_replay_pause_state()",
            check=False,
            log_command=False,
        )
        == "paused",
    )
    workflow.standby_old.stop_if_running()

    configure_primary(workflow.primary_new, socket_dir)
    append(
        workflow.primary_new.pgdata / "postgresql.conf",
        f"wal_keep_size = '{wal_keep_gb}GB'\n",
    )
    workflow.primary_new.start(output_dir / f"{prefix}_new_primary.log")
    prepare_started = time.monotonic()
    workflow.standby_new.initdb()
    configure_upgrade_standby(
        workflow.standby_new,
        workflow.primary_new,
        workflow.standby_old,
        socket_dir,
    )
    replay_started = time.monotonic()
    workflow.standby_new.start(output_dir / f"{prefix}_new_standby.log", wait=False)
    wait_until(
        f"{workflow.transfer_mode} standby finalization",
        lambda: upgrade_finalized(workflow.standby_new),
        timeout=1800,
    )
    wait_until(
        f"{workflow.transfer_mode} standby catchup",
        lambda: caught_up(workflow.primary_new, workflow.standby_new),
        timeout=1800,
    )
    replay_seconds = time.monotonic() - replay_started
    prepare_seconds = time.monotonic() - prepare_started
    total_seconds = time.monotonic() - total_started
    fingerprint = workflow.standby_new.psql(
        "SELECT count(*), sum(aid), sum(abalance) FROM pgbench_accounts"
    )
    if fingerprint != source_fingerprint:
        raise AssertionError((source_fingerprint, fingerprint))

    relation_paths = (
        (
            workflow.primary_old.pgdata / "base" / database_oid / relation_filenode,
            workflow.primary_new.pgdata / "base" / database_oid / relation_filenode,
        ),
        (
            workflow.standby_old.pgdata / "base" / database_oid / relation_filenode,
            workflow.standby_new.pgdata / "base" / database_oid / relation_filenode,
        ),
    )
    for old_relation, new_relation in relation_paths:
        same_file = old_relation.samefile(new_relation)
        if same_file != (workflow.transfer_mode == "link"):
            raise AssertionError((workflow.transfer_mode, old_relation, new_relation))

    stream_bytes = receiver_lsn_span(workflow.standby_new)
    workflow.standby_new.stop_if_running()
    workflow.primary_new.stop_if_running()
    standby_storage = allocated_bytes(
        workflow.standby_old.pgdata,
        workflow.standby_new.pgdata,
    )
    total_storage = allocated_bytes(
        workflow.primary_old.pgdata,
        workflow.primary_new.pgdata,
        workflow.standby_old.pgdata,
        workflow.standby_new.pgdata,
    )
    standby_wal = retained_wal_bytes(workflow.standby_new.pgdata)
    upgrade_wal, coverage = read_upgrade_wal(
        workflow.primary_new.binaries,
        workflow.primary_new.pgdata,
        output_dir / f"{prefix}_records",
        workflow.transfer_mode,
    )
    if workflow.transfer_mode == "link":
        if upgrade_wal.relink_entry_counts.get("file_inherit", 0) == 0:
            raise AssertionError(upgrade_wal.relink_entry_counts)
        if coverage.inherited_relation_keys & coverage.fpi_relation_keys:
            raise AssertionError("inherited relations were emitted as FPIs")
    else:
        if upgrade_wal.relink_entry_counts.get("file_inherit", 0) != 0:
            raise AssertionError(upgrade_wal.relink_entry_counts)
        if upgrade_wal.relink_entry_counts.get("relation_recreate", 0) == 0:
            raise AssertionError(upgrade_wal.relink_entry_counts)
        if coverage.inherited_relation_keys:
            raise AssertionError(coverage.inherited_relation_keys)

    return WalResult(
        fingerprint=fingerprint,
        snapshot_methods=snapshot_methods,
        upgrade_wal=upgrade_wal,
        metrics={
            "primary_upgrade_seconds": upgrade_seconds,
            "standby_prepare_seconds": prepare_seconds,
            "standby_replay_seconds": replay_seconds,
            "end_to_end_seconds": total_seconds,
            "upgrade_window_bytes": upgrade_wal.window_bytes,
            "stream_lsn_span_bytes": stream_bytes,
            "relink_bytes": upgrade_wal.relink_bytes,
            "rawfile_payload_bytes": upgrade_wal.rawfile_payload_bytes,
            "rawfile_record_bytes": upgrade_wal.rawfile_wal_bytes,
            "fpi_record_bytes": upgrade_wal.fpi_wal_bytes,
            "standby_final_allocated_bytes": standby_storage,
            "total_final_allocated_bytes": total_storage,
            "standby_retained_wal_bytes": standby_wal,
        },
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old-bindir", type=Path, required=True)
    parser.add_argument("--new-bindir", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--scale", type=int, default=100)
    parser.add_argument("--port-base", type=int, default=55432)
    parser.add_argument("--wal-keep-gb", type=int, default=8)
    parser.add_argument("--minimum-free-gb", type=int, default=30)
    parser.add_argument("--jobs", type=int, default=8)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    work_dir = ensure_empty_work_dir(args.work_dir)
    check_free_space(work_dir, args.minimum_free_gb)
    if shutil.which("rsync") is None:
        raise RuntimeError("rsync is required")

    old_binaries = PostgresBinaries(args.old_bindir.resolve())
    new_binaries = PostgresBinaries(args.new_bindir.resolve())
    check_binaries(old_binaries, ("initdb", "pg_basebackup", "pgbench", "pg_ctl", "psql"))
    check_binaries(
        new_binaries,
        ("initdb", "pg_controldata", "pg_ctl", "pg_upgrade", "pg_waldump", "psql"),
    )

    ports = PortAllocator(args.port_base)
    socket_dir = Path(tempfile.mkdtemp(prefix="wal-upgrade-link-baseline-"))
    seed = Cluster(work_dir / "seed", old_binaries, ports.get(), "seed")
    seed_standby = Cluster(
        work_dir / "seed_standby",
        old_binaries,
        ports.get(),
        "seed_standby",
    )
    ordinary_root = work_dir / "ordinary_link"
    ordinary_primary_old = Cluster(
        ordinary_root / "primary" / "old",
        old_binaries,
        ports.get(),
        "ordinary_old_primary",
    )
    ordinary_primary_new = Cluster(
        ordinary_root / "primary" / "new",
        new_binaries,
        ports.get(),
        "ordinary_new_primary",
    )
    ordinary_standby_old = Cluster(
        ordinary_root / "standby" / "old",
        old_binaries,
        ports.get(),
        "ordinary_old_standby",
    )
    ordinary_standby_new = Cluster(
        ordinary_root / "standby" / "new",
        new_binaries,
        ports.get(),
        "ordinary_new_standby",
    )
    wal_link = new_wal_workflow(
        work_dir / "wal_upgrade_link",
        "link",
        old_binaries,
        new_binaries,
        seed.port,
        ports,
    )
    wal_copy = new_wal_workflow(
        work_dir / "wal_upgrade_copy",
        "copy",
        old_binaries,
        new_binaries,
        seed.port,
        ports,
    )
    clusters = [
        *wal_copy.clusters(),
        *wal_link.clusters(),
        ordinary_standby_new,
        ordinary_primary_new,
        ordinary_standby_old,
        ordinary_primary_old,
        seed_standby,
        seed,
    ]

    try:
        seed.initdb()
        configure_primary(seed, socket_dir)
        seed.start(work_dir / "seed.log")
        run(
            [
                old_binaries.binary("pgbench"),
                "-i",
                "-s",
                str(args.scale),
                "-h",
                "127.0.0.1",
                "-p",
                str(seed.port),
                "postgres",
            ],
            output=work_dir / "pgbench_init.log",
        )
        seed.psql(f"CREATE ROLE {REPLICATION_USER} LOGIN REPLICATION")
        slot = "upgrade_standby"
        seed.psql(f"SELECT pg_create_physical_replication_slot('{slot}', true)")
        run(
            [
                old_binaries.binary("pg_basebackup"),
                "-D",
                seed_standby.pgdata,
                "-h",
                "127.0.0.1",
                "-p",
                str(seed.port),
                "-U",
                REPLICATION_USER,
                "-S",
                slot,
                "-X",
                "stream",
                "--checkpoint=fast",
                "--no-sync",
            ],
            output=work_dir / "seed_basebackup.log",
        )
        configure_old_standby(seed_standby, seed, slot)
        seed_standby.start(work_dir / "seed_standby.log")
        wait_until(
            "seed standby streaming",
            lambda: streaming_over_tcp(seed, seed_standby, slot),
        )
        wait_until("seed standby catchup", lambda: caught_up(seed, seed_standby))
        source_fingerprint = seed.psql(
            "SELECT count(*), sum(aid), sum(abalance) FROM pgbench_accounts"
        )
        database_oid, relation_filenode = seed.psql(
            "SELECT oid, pg_relation_filenode('pgbench_accounts') "
            "FROM pg_database WHERE datname = current_database()"
        ).split("|")
        seed.psql("CHECKPOINT")
        wait_until("seed checkpoint replay", lambda: caught_up(seed, seed_standby))
        seed_standby.stop_if_running()
        seed.psql(f"SELECT pg_drop_replication_slot('{slot}')")
        seed.stop_if_running()

        snapshot_methods = {
            cluster.name: copy_stopped_cluster(
                source.pgdata,
                cluster.pgdata,
                work_dir / f"snapshot_{cluster.name}.log",
            )
            for cluster, source in (
                (ordinary_primary_old, seed),
                (ordinary_standby_old, seed_standby),
            )
        }
        ordinary_cwd = ordinary_root / "pg_upgrade_cwd"
        ordinary_cwd.mkdir()
        ordinary_total_started = time.monotonic()
        ordinary_upgrade_started = time.monotonic()
        run(
            upgrade_command(
                ordinary_primary_old,
                ordinary_primary_new,
                socket_dir,
                wal_upgrade=False,
                transfer_mode="link",
                wal_keep_gb=args.wal_keep_gb,
                jobs=args.jobs,
            ),
            cwd=ordinary_cwd,
            output=work_dir / "ordinary_pg_upgrade.log",
        )
        ordinary_upgrade_seconds = time.monotonic() - ordinary_upgrade_started
        rsync_started = time.monotonic()
        rsync_bytes = rsync_standby(
            ordinary_primary_old.pgdata.parent,
            ordinary_standby_old.pgdata.parent,
            work_dir / "ordinary_rsync.log",
        )
        rsync_seconds = time.monotonic() - rsync_started
        configure_primary(ordinary_primary_new, socket_dir)
        configure_streaming_standby(ordinary_standby_new, ordinary_primary_new)
        ready_started = time.monotonic()
        ordinary_primary_new.start(work_dir / "ordinary_new_primary.log")
        ordinary_standby_new.start(work_dir / "ordinary_new_standby.log")
        wait_until(
            "ordinary standby catchup",
            lambda: caught_up(ordinary_primary_new, ordinary_standby_new),
        )
        ordinary_ready_seconds = time.monotonic() - ready_started
        ordinary_total_seconds = time.monotonic() - ordinary_total_started
        ordinary_fingerprint = ordinary_standby_new.psql(
            "SELECT count(*), sum(aid), sum(abalance) FROM pgbench_accounts"
        )
        if ordinary_fingerprint != source_fingerprint:
            raise AssertionError((source_fingerprint, ordinary_fingerprint))
        old_relation = ordinary_standby_old.pgdata / "base" / database_oid / relation_filenode
        new_relation = ordinary_standby_new.pgdata / "base" / database_oid / relation_filenode
        if not old_relation.samefile(new_relation):
            raise AssertionError((old_relation, new_relation))
        ordinary_standby_new.stop_if_running()
        ordinary_primary_new.stop_if_running()

        link_result = run_wal_workflow(
            wal_link,
            seed,
            seed_standby,
            slot,
            source_fingerprint,
            database_oid,
            relation_filenode,
            socket_dir,
            work_dir,
            args.wal_keep_gb,
            args.jobs,
        )
        copy_result = run_wal_workflow(
            wal_copy,
            seed,
            seed_standby,
            slot,
            source_fingerprint,
            database_oid,
            relation_filenode,
            socket_dir,
            work_dir,
            args.wal_keep_gb,
            args.jobs,
        )
        snapshot_methods.update(link_result.snapshot_methods)
        snapshot_methods.update(copy_result.snapshot_methods)
        if copy_result.upgrade_wal.window_bytes <= link_result.upgrade_wal.window_bytes:
            raise AssertionError("copy did not produce a larger upgrade window")
        if copy_result.upgrade_wal.fpi_wal_bytes <= link_result.upgrade_wal.fpi_wal_bytes:
            raise AssertionError("copy did not produce more FPI WAL")

        metrics: dict[str, int | float] = {
            "pgbench_scale": args.scale,
            "source_rows": int(source_fingerprint.split("|")[0]),
            "source_pgdata_bytes": directory_bytes(seed.pgdata),
            "ordinary_primary_upgrade_seconds": ordinary_upgrade_seconds,
            "ordinary_standby_rsync_seconds": rsync_seconds,
            "ordinary_standby_ready_seconds": ordinary_ready_seconds,
            "ordinary_end_to_end_seconds": ordinary_total_seconds,
            "ordinary_rsync_transferred_bytes": rsync_bytes,
            "ordinary_standby_final_allocated_bytes": allocated_bytes(
                ordinary_standby_old.pgdata,
                ordinary_standby_new.pgdata,
            ),
            "ordinary_total_final_allocated_bytes": allocated_bytes(
                ordinary_primary_old.pgdata,
                ordinary_primary_new.pgdata,
                ordinary_standby_old.pgdata,
                ordinary_standby_new.pgdata,
            ),
            "ordinary_standby_retained_wal_bytes": retained_wal_bytes(ordinary_standby_new.pgdata),
        }
        metrics.update({f"wal_link_{name}": value for name, value in link_result.metrics.items()})
        metrics.update({f"wal_copy_{name}": value for name, value in copy_result.metrics.items()})
        result = {
            "source_pg_version": old_binaries.version(),
            "target_pg_version": new_binaries.version(),
            "snapshot_methods": snapshot_methods,
            "source_fingerprint": source_fingerprint,
            "ordinary_standby_fingerprint": ordinary_fingerprint,
            "wal_link_standby_fingerprint": link_result.fingerprint,
            "wal_copy_standby_fingerprint": copy_result.fingerprint,
            "metrics": metrics,
        }
        write_json(work_dir / "link_baseline.json", result)
        print((work_dir / "link_baseline.json").read_text())
    finally:
        for cluster in clusters:
            if cluster.pgdata.exists():
                cluster.stop_if_running()
        shutil.rmtree(socket_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
