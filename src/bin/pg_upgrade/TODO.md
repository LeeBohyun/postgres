# pg_upgrade --wal-log-upgrade — open TODO

Working and tested today (see wal_log_tests/): primary upgrade via WAL replay;
standby upgrade by REPLAYING a delivered upgrade WAL (in-band CN derivation,
converges to the primary, writable); cross-version standby upgrade (18->20) via
the skeleton model; tablespaces (in-place + capture/wipe); empty-catalog
reconstruction; crash-mid-upgrade atomicity; all 5 transfer modes; 10GB +
concurrent-client stress; multixact members long-name SLRU segments.

## 1. Remaining wiring for the handoff trigger

The XLOG_PG_UPGRADE_HANDOFF record + the standby self-shutdown are implemented
and tested.  Still to wire for a complete operator flow:
- Decide WHO calls pg_write_pg_upgrade_handoff() on the live old primary before
  shutdown: pg_upgrade core vs. an HA/orchestration layer.  (Lean: HA layer --
  pg_upgrade never connects to the old primary as a standby-visible writer.)

CONNECTION BLOCKING -- investigated 2026-07-14, found NO reachable exposure, no
code change warranted:
  The worry was that pgUpgradeReplayInProgress only SUPPRESSES hot-standby
  activation and does not revoke an ALREADY-ACTIVE one, so an active hot standby
  might serve a half-upgraded cluster while the window replays.  Tracing the two
  real paths shows this cannot happen:
    1. File-delivered / bootstrap path (the working model):
       ArmControlFileForUpgradeRecovery() sets state = DB_IN_PRODUCTION, so
       StartupXLOG runs the window as CRASH RECOVERY, not archive/standby
       recovery.  Hot standby only activates under InArchiveRecovery; in crash
       recovery CheckRecoveryConsistency's hot-standby block never runs, so NO
       connections are accepted during the window at all.  Proven by
       run_connblock_test.sh (0 anomalies while hammering during replay).
    2. Streaming / archive standby: cannot even REACH the window -- replay stops
       at the version/format boundary (and the WAL gap) before START, so it keeps
       serving the pre-upgrade state and never applies anything half-upgraded.
       Attempted to build an "already-active standby applies the window" test
       (archive recovery + hot_standby); it could not reach the window (restored
       old segs 4-6, upgrade window is in segs A-D, segs 7-9 absent), confirming
       the boundary.  The test was therefore VACUOUS and NOT added to the repo.
       If a same-version streaming standby ever DID reach START, the START guard
       FATALs (StandbyMode && !in_upgrade_bootstrap) -> server down -> connections
       dropped, so still no half-upgraded read.
  PostgreSQL core also cannot de-activate hot standby once active (see comment in
  HotStandbyActive(), xlogrecovery.c) -- which is fine, because no reachable path
  applies the window under an active hot standby.  Nothing to fix here; keep this
  note so the concern is not re-opened without a concrete reachable path.

## 2. From REPLICA_UPGRADE_DESIGN.md (already tracked there)

- Q6: remove/gate `revert_wal_logged_disk_writes` (a test-only device) before
  the final patch, once the equivalence tests no longer depend on the
  wiped-then-replayed cluster.
- Q7(b): external-location tablespace symlink REPLAY branch is coded but not
  exercised end to end on a same-build test (pg_upgrade refuses same-catalog +
  tablespaces); needs a real cross-version run.
- Q2: orphaned old-cluster relfiles on the standby (unreferenced garbage) --
  confirm benign / clean up.

## 3. All-version upgrade permutation test

Add a test matrix that exercises --wal-log-upgrade across EVERY supported
old->new major-version pair, not just the single 18->20 pair currently proven by
run_standby_xversion_test.sh.

- For each supported OLD major (e.g. the last N majors pg_upgrade accepts as a
  source) upgrading to the current NEW major, run: primary self-upgrade via WAL
  replay (run_upgrade_test-style) AND the cross-version standby re-provision
  (run_standby_xversion_test-style), asserting catalog version changes old->new,
  data matches, and the result is writable.
- Drive it from a list of OLDBIN dirs (one per major) + the single NEWBIN; skip
  pairs whose OLDBIN is unavailable on the box, and LOG which pairs were skipped
  (no silent coverage gaps).
- Include the handoff trigger where the OLD binary supports it (forward-looking),
  and note where it cannot be exercised (stock old majors that do not emit
  XLOG_PG_UPGRADE_HANDOFF).
- Goal: catch version-specific regressions (catalog layout, SLRU format, control
  version gates) that a single pair cannot surface -- e.g. the members long-name
  SLRU truncation bug fixed this session would have been caught here.
