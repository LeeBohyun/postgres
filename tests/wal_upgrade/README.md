# Supplemental WAL-upgrade tests

These manual programs exercise WAL upgrade with larger and more varied
fixtures than the TAP tests.  They are not part of the PostgreSQL test suite
and are not required to apply the WAL-upgrade patch.

For an overview of the design and motivation, see
[The case for WAL-logging pg_upgrade](../../doc/high-level/wal-logging-pg-upgrade-v2.pdf).

The programs require:

- A Unix-like host with Python 3.10 or newer
- An installed PostgreSQL 20 build containing the WAL-upgrade patch
- Free TCP ports starting at `--port-base`
- `rsync` for `link_baseline.py`

Use the patched V20 installation for both `--old-bindir` and
`--new-bindir`.  The source server must understand the HANDOFF record, so an
unmodified PostgreSQL installation cannot be used as the source.  The two
options may name the same installation.

Each program requires an empty `--work-dir`.  Cluster data, command logs, WAL
dumps, and a JSON result remain there after the run.

## Transfer-mode scale and WAL volume

The quick profile checks every transfer mode with a small fixture:

```sh
work_dir=$(mktemp -d)
python3 tests/wal_upgrade/scale.py \
  --old-bindir /path/to/patched-pg20/bin \
  --new-bindir /path/to/patched-pg20/bin \
  --work-dir "$work_dir" \
  --profile quick
```

The review profile creates large catalogs, MultiXact state, relation data, and
WAL volume:

```sh
work_dir=$(mktemp -d)
python3 tests/wal_upgrade/scale.py \
  --old-bindir /path/to/patched-pg20/bin \
  --new-bindir /path/to/patched-pg20/bin \
  --work-dir "$work_dir" \
  --profile review
```

The program checks RELINK classifications, relation full-page images, RAWFILE
SLRU sizes, catalog coverage, sampled relation contents, and MultiXact use.  It
records WAL volume and runtime for every supported transfer mode in
`scale.json`.  Filesystem-dependent modes that are unavailable are recorded as
unsupported.

## Two direct standbys

```sh
work_dir=$(mktemp -d)
python3 tests/wal_upgrade/two_standbys.py \
  --old-bindir /path/to/patched-pg20/bin \
  --new-bindir /path/to/patched-pg20/bin \
  --work-dir "$work_dir" \
  --mode link \
  --scale 100
```

The program streams two source standbys through separate physical slots,
verifies their HANDOFF pauses, creates two target standbys, replays the upgrade
window, checks their data, and verifies that both migrated slots resume
streaming.  It writes measurements to `two_standbys.json`.

## Link baseline

```sh
work_dir=$(mktemp -d)
python3 tests/wal_upgrade/link_baseline.py \
  --old-bindir /path/to/patched-pg20/bin \
  --new-bindir /path/to/patched-pg20/bin \
  --work-dir "$work_dir" \
  --scale 100
```

The program runs three primary-standby workflows from one source fixture:

- Ordinary `pg_upgrade --link` followed by standby preparation with `rsync`
- WAL upgrade with `--link`
- WAL upgrade with `--copy`

It verifies standby data and relation-file placement.  The result in
`link_baseline.json` compares upgrade time, standby preparation and replay
time, WAL bytes, transferred bytes, retained WAL, and allocated storage.
