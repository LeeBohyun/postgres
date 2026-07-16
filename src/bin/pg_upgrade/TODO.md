# pg_upgrade --wal-log-upgrade — open TODO

Working and tested today (see wal_log_tests/): primary upgrade via WAL replay;
standby upgrade by REPLAYING a delivered upgrade WAL (in-band CN derivation,
converges to the primary, writable); cross-version standby upgrade (18->20) via
the skeleton model; ALL-version matrix 14/15/16/17/18 -> 20 (primary
self-upgrade); tablespaces (in-place + capture/wipe); empty-catalog
reconstruction; crash-mid-upgrade atomicity; all 5 transfer modes; 10GB +
concurrent-client stress; multixact members long-name SLRU segments; the
revertable lifecycle (quarantine hold, commit-is-finalize, rollback, delete-old,
signal-handoff) plus its adversarial edges (run_revertable_extreme_test.sh).

## 1. Handoff trigger wiring — RESOLVED

The XLOG_PG_UPGRADE_HANDOFF record + the standby self-shutdown are implemented
and tested.  RESOLVED: `pg_upgrade --signal-handoff -d <old> -U <user>` connects
to the LIVE old primary and calls pg_write_pg_upgrade_handoff(); the trigger
then propagates to standbys through the normal WAL path (in Neon: primary ->
safekeepers -> standby -- verified against the hadron compute config, replicas
stream from the safekeepers, so no per-standby push is needed).  This can be run
by an operator or an HA/orchestration layer.  pg_upgrade still never acts as a
standby-visible writer during the upgrade itself; --signal-handoff is a distinct
pre-upgrade action against the running primary.

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

- Q6: KEEP `revert_wal_logged_disk_writes` (decided) -- do NOT remove it.  It is
  the device that PROVES reconstruction is truly WAL-only (wipe the on-disk data,
  then require replay to rebuild it): the disk-wiped assertions in
  run_upgrade/large/stress/mxact/tablespace/manyrel/datashape all depend on it,
  and it is the honest guarantee behind the whole feature.  Production keeps the
  reconstructed files (Phase-0 wipe-policy decision); the wipe stays as the
  test-only path.  Leave it in.
- Q7(b): external-location tablespace symlink REPLAY branch is coded but not
  exercised end to end on a same-build test (pg_upgrade refuses same-catalog +
  tablespaces); needs a real cross-version run.
- Q2: orphaned old-cluster relfiles on the standby (unreferenced garbage) --
  confirm benign / clean up.

## 3. All-version upgrade permutation test — DONE

run_allversion_matrix_test.sh upgrades EVERY available old major to the current
NEW (20devel) via --wal-log-upgrade and asserts, per pair: catalog version jumps
old->new, the new cluster holds then commits, data matches, appdb data survives,
and the result is writable.  Driven from OLDBIN_DIRS (defaults to the Arca
hadron/pg_install/vNN layout); unavailable pairs are SKIPPED and logged, and the
test FAILS if no cross-version pair was available (no silent all-skip pass).

Proven on Arca (ran=5 passed=5): 14.23, 15.18, 16.14, 17.10, 18.4 -> 20devel,
each with a real catalog-version jump (e.g. 202107181 -> 202607022).

Still primary self-upgrade only.  Possible follow-ups (not blocking):
- extend each matrix pair to ALSO do the cross-version standby re-provision
  (run_standby_xversion_test-style), not just the primary self-upgrade;
- exercise the handoff trigger where the OLD binary emits
  XLOG_PG_UPGRADE_HANDOFF (stock old majors do not, so note where skipped).
