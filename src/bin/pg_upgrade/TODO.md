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

TODO:
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
