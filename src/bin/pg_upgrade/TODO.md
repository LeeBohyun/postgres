# pg_upgrade --wal-log-upgrade — open TODO

Working and tested today (see wal_log_tests/): primary upgrade via WAL replay;
standby upgrade by REPLAYING a delivered upgrade WAL (in-band CN derivation,
converges to the primary, writable); cross-version standby upgrade (18->20) via
the skeleton model; tablespaces (in-place + capture/wipe); empty-catalog
reconstruction; crash-mid-upgrade atomicity; all 5 transfer modes; 10GB +
concurrent-client stress; multixact members long-name SLRU segments.

## Resolved this session (kept as short notes; see git history for detail)

- Live-streaming standby halt: settled.  A streaming standby cannot follow the
  upgrade burst across a major version -- WAL page magic is version-stamped
  (v18=0xD118, v20=0xD120), so no single running binary can read both the old
  tail and the new burst.  Delivery is therefore inherently OUT-OF-BAND: the
  self-contained new-version window is delivered and replayed from the
  end-of-upgrade checkpoint (CN) into a fresh new-version skeleton (proven by
  run_standby_xversion_test.sh / run_e2e_equivalence_test.sh).
  The clean-handoff signal is the OLD-FORMAT XLOG_PG_UPGRADE_HANDOFF trigger
  (SQL pg_write_pg_upgrade_handoff), emitted into the OLD primary's own WAL so a
  streaming standby reads it and shuts itself down for the swap
  (run_handoff_trigger_test.sh, run_standby_handoff_e2e_test.sh).
- "Make the WAL gap zero": settled as impossible cross-version (same
  version-stamped-page-magic reason).  Not a fixable gap; the out-of-band model
  does not need contiguity.
- pg_resetwal --control-only: REVERTED.  It existed only to keep the upgrade WAL
  contiguous for live streaming; since streaming-follow is impossible
  cross-version, the contiguity bought nothing.  The counter transplants are back
  to stock pg_resetwal (which reposition/rewrite the WAL); counters are still
  captured by the CN checkpoint recovery replays from, so the delivered upgrade
  artifact is unchanged.  (--system-identifier stamping in the "Resetting WAL
  archives" step is kept -- the burst must carry the old sysid so a re-provisioned
  standby accepts it.)

## 1. Remaining wiring for the handoff trigger

The XLOG_PG_UPGRADE_HANDOFF record + the standby self-shutdown are implemented
and tested.  Still to wire for a complete operator flow:
- Decide WHO calls pg_write_pg_upgrade_handoff() on the live old primary before
  shutdown: pg_upgrade core vs. an HA/orchestration layer.  (Lean: HA layer --
  pg_upgrade never connects to the old primary as a standby-visible writer.)
- Block the standby from serving ANY connection during the handoff window.  Today
  pgUpgradeReplayInProgress only prevents hot-standby ACTIVATION, not an
  already-active standby -- verify/extend so no client sees a half-upgraded
  cluster.  (see run_connblock_test.sh)

## 2. Hand over the system identifier WITHOUT the pg_resetwal --system-identifier flag

We still carry ONE added pg_resetwal flag: --system-identifier, used in the
"Resetting WAL archives" step so the new cluster's pg_control AND its fresh WAL
segment header (xlp_sysid) both get the OLD cluster's sysid -- required so a
re-provisioned standby accepts the delivered burst.  It needs a WAL-rewriting
reset because the sysid lives in two places (pg_control.system_identifier and
every WAL page's xlp_sysid).  We prefer to avoid flags (cf. how --upgrade-recovery
was dropped by deriving CN in-process).  Investigate a flag-free handover:
  - Derive/stamp the sysid IN-PROCESS at the new cluster's first startup, the
    same way PerformWalUpgradeIfNeeded() already derives CN and arms pg_control
    -- i.e. read the old sysid from the delivered artifact (or a value embedded
    in the START/HANDOFF record) and write it into pg_control there, rather than
    via an offline pg_resetwal flag.  The WAL pages the burst wrote already carry
    whatever sysid the emitting server had, so check whether stamping pg_control
    alone (matching the burst's xlp_sysid) is sufficient, or whether the burst
    itself should simply be emitted under the correct sysid from the start.
  - Alternative: have the skeleton provisioning stamp the sysid when it builds
    the fresh new-version datadir (it already runs initdb + places the WAL), so
    no pg_upgrade-side flag is needed at all.
  - Goal: remove --system-identifier from pg_resetwal (mirroring the --control-only
    revert and the --upgrade-recovery removal) so --wal-log-upgrade adds NO new
    pg_resetwal flags.
  - Verify the chosen approach against run_standby_xversion_test.sh /
    run_e2e_equivalence_test.sh (sysid must match the primary; standby accepts
    the WAL).

## 3. From REPLICA_UPGRADE_DESIGN.md (already tracked there)

- Q6: remove/gate `revert_wal_logged_disk_writes` (a test-only device) before
  the final patch, once the equivalence tests no longer depend on the
  wiped-then-replayed cluster.
- Q7(b): external-location tablespace symlink REPLAY branch is coded but not
  exercised end to end on a same-build test (pg_upgrade refuses same-catalog +
  tablespaces); needs a real cross-version run.
- Q2: orphaned old-cluster relfiles on the standby (unreferenced garbage) --
  confirm benign / clean up.

## 4. BUG: rich-schema RELFILE/RAWFILE redo collision (run_extreme_test.sh)

run_extreme_test.sh FAILS (pre-existing, NOT caused by the --control-only revert
-- verified: fails identically on the pre-revert binary).  On replay of a rich
schema, startup FATALs:
    redo done at ...
    FATAL: could not create file "base/5/16439": File exists
The empty-relfile branch of XLOG_UPGRADE_RELFILE_DATA redo (pgupgrade_wal.c)
calls smgrcreate(), which mdcreate()s with O_EXCL and fails because the file
already exists -- i.e. some earlier record in the burst (a RAWFILE, or another
RELFILE entry for the same relfilenode) already created base/5/16439.  Likely a
capture/redo ORDERING or DEDUP problem for certain object types present in the
extreme schema (toast/partition/matview/seq/LO/enum/composite/unlogged) that the
simpler tests do not exercise.
TODO:
- Identify which object type / which record pair collides on 16439 (dump the
  burst with pg_waldump, find the two records that both target that relfile).
- Fix the empty-relfile redo to tolerate an already-created file (smgrexists
  check is already there for the fork -- verify it covers the base relation
  case), OR fix the capture side to not emit two creators for one relfilenode.
- run_extreme_test.sh is the reproducer; it must go green.

## 5. All-version upgrade permutation test

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
