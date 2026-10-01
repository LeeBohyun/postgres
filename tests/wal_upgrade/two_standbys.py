#!/usr/bin/env python3

"""Run WAL-upgrade with two direct physical standbys."""

from __future__ import annotations

import argparse
import shutil
import tempfile
import time
from pathlib import Path

from common import (
    GENERIC_TRANSFER_MODES,
    REPLICATION_USER,
    Cluster,
    PortAllocator,
    PostgresBinaries,
    append,
    caught_up,
    check_binaries,
    check_free_space,
    configure_old_standby,
    configure_primary,
    configure_upgrade_standby,
    copy_stopped_cluster,
    ensure_empty_work_dir,
    run,
    streaming_over_tcp,
    transfer_mode_option,
    upgrade_finalized,
    wait_until,
    write_json,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old-bindir", type=Path, required=True)
    parser.add_argument("--new-bindir", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--mode", choices=GENERIC_TRANSFER_MODES, default="copy")
    parser.add_argument("--scale", type=int, default=100)
    parser.add_argument("--port-base", type=int, default=55432)
    parser.add_argument("--wal-keep-gb", type=int, default=8)
    parser.add_argument("--minimum-free-gb", type=int, default=15)
    parser.add_argument("--jobs", type=int, default=8)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    work_dir = ensure_empty_work_dir(args.work_dir)
    check_free_space(work_dir, args.minimum_free_gb)
    old_binaries = PostgresBinaries(args.old_bindir.resolve())
    new_binaries = PostgresBinaries(args.new_bindir.resolve())
    check_binaries(old_binaries, ("initdb", "pg_basebackup", "pgbench", "pg_ctl", "psql"))
    check_binaries(new_binaries, ("initdb", "pg_controldata", "pg_ctl", "pg_upgrade", "psql"))

    ports = PortAllocator(args.port_base)
    socket_dir = Path(tempfile.mkdtemp(prefix="wal-upgrade-two-standbys-"))
    old_primary = Cluster(work_dir / "old_primary", old_binaries, ports.get(), "old_primary")
    old_standbys = [
        Cluster(
            work_dir / f"old_standby_{number}",
            old_binaries,
            ports.get(),
            f"old_standby_{number}",
        )
        for number in (1, 2)
    ]
    new_primary = Cluster(work_dir / "new_primary", new_binaries, ports.get(), "new_primary")
    new_standbys = [
        Cluster(
            work_dir / f"new_standby_{number}",
            new_binaries,
            ports.get(),
            f"new_standby_{number}",
        )
        for number in (1, 2)
    ]
    slots = ["upgrade_standby_1", "upgrade_standby_2"]
    clusters = [*new_standbys, new_primary, *old_standbys, old_primary]
    upgrade_cwd = work_dir / "pg_upgrade_cwd"
    upgrade_cwd.mkdir()

    try:
        old_primary.initdb()
        configure_primary(old_primary, socket_dir)
        old_primary.start(work_dir / "old_primary.log")
        run(
            [
                old_binaries.binary("pgbench"),
                "-i",
                "-s",
                str(args.scale),
                "-h",
                "127.0.0.1",
                "-p",
                str(old_primary.port),
                "postgres",
            ],
            output=work_dir / "pgbench_init.log",
        )
        old_primary.psql(f"CREATE ROLE {REPLICATION_USER} LOGIN REPLICATION")
        for slot in slots:
            old_primary.psql(f"SELECT pg_create_physical_replication_slot('{slot}', true)")

        basebackup = work_dir / "old_standby_basebackup"
        run(
            [
                old_binaries.binary("pg_basebackup"),
                "-D",
                basebackup,
                "-h",
                "127.0.0.1",
                "-p",
                str(old_primary.port),
                "-U",
                REPLICATION_USER,
                "-S",
                slots[0],
                "-X",
                "stream",
                "--checkpoint=fast",
                "--no-sync",
            ],
            output=work_dir / "basebackup.log",
        )
        for standby, slot in zip(old_standbys, slots):  # noqa: B905
            copy_stopped_cluster(
                basebackup,
                standby.pgdata,
                work_dir / f"snapshot_{standby.name}.log",
            )
            configure_old_standby(standby, old_primary, slot)
            standby.start(work_dir / f"{standby.name}.log")

        for standby, slot in zip(old_standbys, slots):  # noqa: B905
            wait_until(
                f"{standby.name} streaming over TCP",
                lambda standby=standby, slot=slot: streaming_over_tcp(old_primary, standby, slot),
            )
            wait_until(
                f"{standby.name} catchup",
                lambda standby=standby: caught_up(old_primary, standby),
            )

        old_primary.stop_if_running()
        upgrade_started = time.monotonic()
        run(
            [
                new_binaries.binary("pg_upgrade"),
                "--no-sync",
                "--initdb",
                "--retain",
                "--wal-upgrade",
                transfer_mode_option(args.mode),
                "--old-datadir",
                old_primary.pgdata,
                "--new-datadir",
                new_primary.pgdata,
                "--old-bindir",
                old_binaries.bindir,
                "--new-bindir",
                new_binaries.bindir,
                "--socketdir",
                socket_dir,
                "--old-port",
                str(old_primary.port),
                "--new-port",
                str(new_primary.port),
                f"--jobs={args.jobs}",
                "--new-options",
                f"-c wal_keep_size={args.wal_keep_gb}GB "
                f"-c max_wal_size={args.wal_keep_gb}GB -c wal_compression=off",
            ],
            cwd=upgrade_cwd,
            output=work_dir / "pg_upgrade.log",
        )
        primary_upgrade_seconds = time.monotonic() - upgrade_started

        for standby in old_standbys:
            wait_until(
                f"{standby.name} HANDOFF pause",
                lambda standby=standby: standby.psql(
                    "SELECT pg_get_wal_replay_pause_state()",
                    check=False,
                    log_command=False,
                )
                == "paused",
            )
            standby.stop_if_running()

        configure_primary(new_primary, socket_dir)
        new_primary.start(work_dir / "new_primary.log")
        for standby, retained in zip(new_standbys, old_standbys):  # noqa: B905
            standby.initdb()
            configure_upgrade_standby(standby, new_primary, retained, socket_dir)

        started_at: dict[str, float] = {}
        for standby in new_standbys:
            started_at[standby.name] = time.monotonic()
            standby.start(work_dir / f"{standby.name}.log", wait=False)

        replay_seconds: dict[str, float] = {}
        pending = {standby.name: standby for standby in new_standbys}
        deadline = time.monotonic() + 1800
        while pending:
            for name, standby in list(pending.items()):
                if upgrade_finalized(standby):
                    replay_seconds[name] = time.monotonic() - started_at[name]
                    del pending[name]
            if pending:
                if time.monotonic() >= deadline:
                    raise TimeoutError("timed out waiting for " + ", ".join(sorted(pending)))
                time.sleep(0.1)

        primary_fingerprint = new_primary.psql(
            "SELECT count(*), sum(aid), sum(abalance) FROM pgbench_accounts"
        )
        expected_rows = args.scale * 100_000
        if int(primary_fingerprint.split("|")[0]) != expected_rows:
            raise AssertionError(primary_fingerprint)
        standby_fingerprints: dict[str, str] = {}
        for standby in new_standbys:
            wait_until(
                f"{standby.name} query readiness",
                lambda standby=standby: standby.psql(
                    "SELECT pg_is_in_recovery()",
                    check=False,
                    log_command=False,
                )
                == "t",
            )
            fingerprint = standby.psql(
                "SELECT count(*), sum(aid), sum(abalance) FROM pgbench_accounts"
            )
            if fingerprint != primary_fingerprint:
                raise AssertionError((primary_fingerprint, fingerprint))
            standby_fingerprints[standby.name] = fingerprint

        for standby, slot in zip(new_standbys, slots):  # noqa: B905
            append(
                standby.pgdata / "postgresql.auto.conf",
                f"primary_slot_name = '{slot}'\n",
            )
            standby.psql("SELECT pg_reload_conf()")
            wait_until(
                f"{standby.name} migrated slot",
                lambda standby=standby, slot=slot: streaming_over_tcp(new_primary, standby, slot),
            )

        result = {
            "source_pg_version": old_binaries.version(),
            "target_pg_version": new_binaries.version(),
            "transfer_mode": args.mode,
            "pgbench_scale": args.scale,
            "expected_rows": expected_rows,
            "primary_pg_upgrade_seconds": primary_upgrade_seconds,
            "standby_replay_finalize_seconds": replay_seconds,
            "primary_fingerprint": primary_fingerprint,
            "standby_fingerprints": standby_fingerprints,
            "migrated_slots_active": slots,
        }
        write_json(work_dir / "two_standbys.json", result)
        print((work_dir / "two_standbys.json").read_text())
    finally:
        for cluster in clusters:
            if cluster.pgdata.exists():
                cluster.stop_if_running()
        shutil.rmtree(socket_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
