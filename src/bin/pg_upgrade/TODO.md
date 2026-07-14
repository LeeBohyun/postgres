# pg_upgrade --wal-log-upgrade — open TODO

Working and tested today (see wal_log_tests/): primary upgrade via WAL replay;
standby upgrade by REPLAYING a delivered upgrade WAL (in-band CN derivation,
converges to the primary, writable); tablespaces (in-place + capture/wipe);
empty-catalog reconstruction; crash-mid-upgrade atomicity; all 5 transfer modes;
10GB + concurrent-client stress.

The items below are deferred.

## 1. Live-streaming standby: halt at START, then relaunch to replay

The intended model: a working standby streams the primary's WAL, reaches
`XLOG_PG_UPGRADE_START`, HALTS cleanly (does not apply the window live), the
operator installs the new-version binary and restarts, and on relaunch the
standby re-anchors at the end-of-upgrade checkpoint (CN) and replays the
self-contained window.

Status:
- The re-anchor + replay-from-CN half WORKS and is tested (run_standby_test.sh):
  deliver the upgrade WAL, first startup arms the bootstrap and replays from CN.
- The halt-at-START half is CODED (pg_upgrade_redo START guard FATALs with a
  standby-specific message when StandbyMode && !in_upgrade_bootstrap) but NOT
  exercised end to end, because:
    * file-delivering the window into pg_wal/ makes the startup scan arm the
      bootstrap and apply directly -> it never halts;
    * the halt is intrinsically a LIVE-STREAMING behavior (window arrives after
      startup), and live streaming currently cannot reach START (see item 2).

UNEXPECTED FINDING (2026-07-14, needs investigation) --
run_standby_stream_half_test.sh drives the standby via ARCHIVE recovery
(recovery.signal + restore_command; window in the archive only, NOT pre-staged
in pg_wal, so the startup scan cannot pre-arm the bootstrap).  Result:
  - PerformWalUpgradeIfNeeded did NOT run/arm (0 "arming" messages),
  - the START guard did NOT FATAL (no "pg_upgrade WAL encountered"),
  - yet the upgrade window (START/DIRSKEL/RELFILE/SLRU/COMPLETE) REPLAYED via
    ordinary archive redo and the cluster converged (fingerprint matched the
    primary) and was WRITABLE.
This contradicts the guard's stated premise that the image records are "only
safe from the sanctioned bootstrap".  Two possibilities to run down:
  (a) the guard is over-conservative and archive recovery can just replay the
      window -- in which case the halt may be unnecessary for the archive path;
      OR
  (b) the records applied in a subtly-unsafe way (old-cluster page LSNs below
      the replay point) that a row-count/writable check does NOT catch -- needs
      the PHYSICAL page comparison (run_compare_test-style, LSN/checksum-aware)
      against the upgraded primary to confirm byte-correctness, not just logical.
Do NOT trust "logical fingerprint matched + writable" as proof of FPI-LSN
safety here.  Resolve (a) vs (b) before relying on the archive-recovery path.
Also note: recovery.signal is ARCHIVE recovery, NOT StandbyMode, so the
StandbyMode-specific halt message added in this work would not fire on this
path anyway.

TODO:
- Investigate the finding above (a vs b) FIRST -- it may change the whole halt
  design.
- Build the live-streaming trigger: on reaching START mid-stream, confirm the
  whole START..COMPLETE window is present, then stop cleanly (the halt) rather
  than relying on the walreceiver erroring out.
- Decide the halt mechanism: keep the recovery-process FATAL (chosen: the old
  binary is about to be swapped anyway) vs. a graceful shutdown. Do NOT attempt
  a graceful in-loop shutdown without confirming it is safe.
- Block the standby from serving ANY connection while the window replays. Today
  pgUpgradeReplayInProgress only prevents hot-standby ACTIVATION, not an
  already-active standby -- verify/extend so no client sees a half-upgraded
  cluster.
- Add an end-to-end test once live streaming can reach START.

## 1b. CROSS-VERSION standby: PG_VERSION pre-replay gate blocks it (PROVEN)

run_standby_xversion_test.sh does a REAL cross-version test (old = PG18.4, new =
20devel), which is the only way to actually prove the standby replayed the
upgrade (a same-version test can't -- the v18 basebackup already matches the
"upgraded" v-same primary whether or not anything replayed).

Result: the standby FAILS to start on the new binary:
  FATAL: database files are incompatible with server
  DETAIL: The data directory was initialized by PostgreSQL version 18, which is
          not compatible with this version 20devel.
Cause: ValidatePgVersion() reads the on-disk PG_VERSION (=18, from the v18
basebackup) at startup, BEFORE any WAL is replayed, and refuses.  So the upgrade
WAL -- whose XLOG_PG_UPGRADE_START record carries and would install the new
PG_VERSION -- never gets to run.  PG_VERSION is a pre-replay BOOTSTRAP file, like
pg_control: it gates whether replay starts, so it cannot be supplied by the WAL
itself.

This also invalidates the earlier same-version "standby works" confidence: those
tests passed only because 18-vs-18 (really 20devel-vs-20devel) sails through the
version gate and the on-disk copy already matched.  The cross-version test is the
real acceptance test and it currently fails at the version gate.

There are TWO pre-replay version gates, both v18 in the basebackup:
  1. PG_VERSION file (=18): ValidatePgVersion() FATAL.
  2. pg_control (PG_CONTROL_VERSION 1800): "cluster initialized with
     PG_CONTROL_VERSION 1800, server compiled with 1902" FATAL.
Bumping only PG_VERSION is not enough; the pg_control gate fires next.

CONCLUSION (matches the "skeleton" model): a cross-version standby CANNOT reuse
its old v18 data directory -- the new binary rejects it before replay on both
gates.  It needs a FRESH NEW-VERSION SKELETON (a v20 initdb, or at least v20
PG_VERSION + v20-initialized pg_control) into which the upgrade WAL replays from
CN.  This is exactly what run_e2e_equivalence_test.sh already does (fresh initdb
target + sysid stamp + upgrade WAL, in-band CN derivation) -- and why THAT test
works while reusing the old standby dir cannot.

So the real cross-version standby flow is:
  standby streams to START -> halts -> operator builds/points at a NEW-version
  skeleton (fresh initdb of the new binary, sysid stamped to the old cluster) ->
  deliver the upgrade WAL -> replay from CN into the skeleton -> v20 standby.
The "reuse the old data dir in place" idea does not work across versions; the
old files are the wrong version and are only unreferenced garbage after replay
anyway (design-doc Q2).

TODO for cross-version standby:
- Adapt run_standby_xversion_test.sh to the skeleton model: instead of relaunching
  the v18 standby dir on the new binary, build a fresh v20 initdb skeleton, stamp
  the old sysid, deliver the upgrade WAL, and replay -- then assert catalog
  version changed 202506291 -> 202607022, data matches, writable.  That is the
  real proof the standby upgraded via WAL.
- Binaries on the Arca dev box: old PG18.4 at
  /home/bohyun.lee/postgres/pg18-src/tmp_install/.../pg18-initdb/bin; new 20devel
  at ~/wal_test/inst/bin.

## 2. Residual WAL contiguity for live streaming

`pg_resetwal --control-only` (introduced this session) closed the gross gap
(measured ~5 segments -> 0; CN now lands right after the old cluster's WAL end).
But pure live streaming still cannot follow, for two segment-boundary reasons:

  1. The old cluster's LAST segment is usually PARTIAL; the `-l` reset starts the
     new WAL at the NEXT segment, so a caught-up standby (at the start of that
     partial segment) asks the new primary for a segment it does not have
     ("requested WAL segment ... has already been removed").  The new WAL would
     need to continue the old cluster's last partial segment, not skip to the
     next.
  2. Residual burst drift: CHECKPOINT (CN) + the burst-server restart still
     advance a couple of segments past the `-l` point.

NOTE: this only matters for the pure live-streaming path (item 1).  The
file-delivered standby path re-anchors at CN and does NOT need contiguity, so
this is not blocking the tested capability.

## 3. Decide the fate of `pg_resetwal --control-only`

We added `--control-only` to pg_resetwal (commit c8926c22eb) to close the gap in
item 2.  Since the standby model re-anchors at CN and does not strictly need
contiguity, and we prefer to avoid adding flags, decide:
  - KEEP it (smaller WAL, closer to contiguous, helps a future streaming path), or
  - REVERT it (fewer flags; the file-delivered path does not need it).
Leaning: revisit when item 1/2 (live streaming) is actually built.

## 4. From REPLICA_UPGRADE_DESIGN.md (already tracked there)

- Q6: remove/gate `revert_wal_logged_disk_writes` (a test-only device) before
  the final patch, once the equivalence tests no longer depend on the
  wiped-then-replayed cluster.
- Q7(b): external-location tablespace symlink REPLAY branch is coded but not
  exercised end to end on a same-build test (pg_upgrade refuses same-catalog +
  tablespaces); needs a real cross-version run.
- Q2: orphaned old-cluster relfiles on the standby (unreferenced garbage) --
  confirm benign / clean up.
