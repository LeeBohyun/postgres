# --wal-log-upgrade manual test harnesses

Bash harnesses that exercise `pg_upgrade --wal-log-upgrade` end to end: they
build a real old cluster, upgrade it, and verify the new cluster is
reconstructed correctly on first startup from the WAL alone (the upgrade wipes
the data files to a bare skeleton, so a correct startup proves WAL replay did
all the work).

These are developer smoke tests, not part of the meson/TAP suite.  They need an
**installed** build; by default they use `<repo>/pginst/bin`, overridable with
`PGBIN`:

    meson install -C build --destdir ""   # or: --prefix <repo>/pginst
    PGBIN=/path/to/bin bash run_upgrade_test.sh

Most accept `MODE` (`--copy` [default], `--copy-file-range`, `--link`, `--swap`)
and `WORK` (work dir; default under `/tmp` because Unix-socket paths must stay
under 107 bytes).

| Script | What it checks |
|---|---|
| `run_upgrade_test.sh` | Multi-DB cluster with indexes/toast reconstructs from WAL; data files are 0 bytes on disk after upgrade |
| `run_extreme_test.sh` | Rich schema: toast, partitions, matview, sequences, large objects, btree/hash/gin/brin/expression indexes, enum/composite types, multiple DBs |
| `run_large_test.sh` | Table &gt;1 GB, exercising the 1020 MB relfile chunking |
| `run_mxact_test.sh` | Real multixacts (overlapping `FOR SHARE`); pg_multixact skipped then rebuilt |
| `run_crash_test.sh` | Crash mid-upgrade (COMPLETE suppressed via `PG_UPGRADE_TEST_SKIP_COMPLETE`) → first startup FATALs, old cluster stays usable |
| `run_compare_test.sh` | Diffs a `--wal-log-upgrade`+replay cluster against a normal pg_upgrade of identical data, page by page (LSN/checksum aware) |
