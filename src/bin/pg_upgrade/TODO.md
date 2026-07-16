# pg_upgrade --wal-log-upgrade — TODO

Working and tested today (see wal_log_tests/): primary upgrade via WAL replay;
standby upgrade by REPLAYING a delivered upgrade WAL (in-band CN derivation,
converges to the primary, writable); cross-version standby upgrade (18->20) via
the skeleton model; ALL-version matrix 14/15/16/17/18 -> 20 (primary
self-upgrade); tablespaces (in-place + capture/wipe); empty-catalog
reconstruction; crash-mid-upgrade atomicity; all 5 transfer modes; 10GB +
concurrent-client stress; multixact members long-name SLRU segments; the
revertable lifecycle (quarantine hold, commit-is-finalize, rollback, delete-old,
signal-handoff) plus its adversarial edges (crash during commit, commit-failure
C4 ordering, signal-handoff edges, data-shape extremes).

## Open

- Q7(b): external-location tablespace symlink REPLAY branch is coded but not
  exercised end to end on a same-build test (pg_upgrade refuses same-catalog +
  tablespaces); needs a real cross-version run (now feasible on Arca with the
  v14-v18 binaries).
- Q2: orphaned old-cluster relfiles on the standby (unreferenced garbage) --
  confirm benign / clean up.
- Matrix follow-ups (not blocking): extend each all-version pair to ALSO do the
  cross-version standby re-provision (run_standby_xversion_test-style), not just
  the primary self-upgrade; and exercise the handoff trigger where the OLD binary
  emits XLOG_PG_UPGRADE_HANDOFF (stock old majors do not -- note where skipped).

## Settled — do not re-open without a concrete new reason

- Handoff wiring: `pg_upgrade --signal-handoff -d <old> -U <user>` emits the
  trigger into the live primary's WAL; it propagates to standbys via the normal
  WAL path (in Neon: primary -> safekeepers -> standby, verified against the
  hadron compute config -- no per-standby push).  Runnable by operator or HA.
- All-version matrix (run_allversion_matrix_test.sh): 14/15/16/17/18 -> 20devel,
  proven on Arca (ran=5 passed=5), each a real catalog-version jump.
- Q6: KEEP `revert_wal_logged_disk_writes`.  It is the test-only device that
  proves reconstruction is WAL-only (wipe on-disk data, require replay to rebuild
  it); the disk-wiped assertions across the suite depend on it.  Production keeps
  the reconstructed files (Phase-0 wipe policy); the wipe stays test-only.
- Connection blocking: NO reachable exposure (investigated 2026-07-14).  The
  file-delivered/bootstrap path runs the window as CRASH recovery
  (state=DB_IN_PRODUCTION), so hot standby never activates and NO connections are
  accepted during replay (proven by run_connblock_test.sh).  A streaming/archive
  standby cannot even reach the window (stops at the version/format boundary +
  WAL gap before START); if one ever did, the START guard FATALs.  PG core cannot
  de-activate an already-active hot standby, but no reachable path applies the
  window under one.  Nothing to fix.
