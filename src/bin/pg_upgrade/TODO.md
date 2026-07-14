# pg_upgrade --wal-log-upgrade — open TODO

Working and tested today (see wal_log_tests/): primary upgrade via WAL replay;
standby upgrade by REPLAYING a delivered upgrade WAL (in-band CN derivation,
converges to the primary, writable); tablespaces (in-place + capture/wipe);
empty-catalog reconstruction; crash-mid-upgrade atomicity; all 5 transfer modes;
10GB + concurrent-client stress.

The items below are deferred.

## 1. Live-streaming standby: OLD-FORMAT trigger record for clean handoff

STATUS (2026-07-14): IMPLEMENTED + TESTED (same-version trigger mechanism).
  * New WAL record XLOG_PG_UPGRADE_HANDOFF (0x60) in RM_PG_UPGRADE_ID, struct
    xl_pg_upgrade_handoff (pg_control.h).  Emitted by XLogWritePgUpgradeHandoff()
    (xlog.c), exposed as SQL pg_write_pg_upgrade_handoff(target_major int) -> pg_lsn
    (oid 9706), redo/desc/identify handlers added.
  * The record is written by the OLD binary on the LIVE OLD primary (via the SQL
    fn), so it is old-format and chained onto the old stream (waldump: prev points
    at the prior old record, NOT 0/0).  A streaming standby replays it and, under
    StandbyMode, FATALs with "reached pg_upgrade handoff on standby; halting for
    upgrade" + a re-provision hint.
  * PROVEN by run_handoff_trigger_test.sh: caught-up streaming standby receives
    the trigger and SHUTS ITSELF DOWN cleanly (postmaster exits on its own,
    pg_ctl reports not running, port refused, postmaster.pid removed, FATAL fires
    exactly once = no restart loop) -- the halt that was UNREACHABLE via the
    new-format START burst.  The FATAL message is "reached pg_upgrade handoff on
    standby; shutting down for pg_upgrade".
  * PROVEN end to end by run_standby_handoff_e2e_test.sh: streaming standby ->
    trigger -> self-shutdown -> pg_upgrade on the primary -> re-provision the
    halted standby from the delivered window (replay from CN) -> converged +
    writable.  This wires the TRIGGER (halt) and TRANSPORT (out-of-band replay)
    halves into one operator flow.  (Single fork build, so it proves the WIRING,
    not a cross-major catalog change; run_standby_xversion_test.sh proves the
    18->20 catalog change half.)
  * REMAINING to fully wire the operator flow: (i) have pg_upgrade / an
    orchestration step actually CALL pg_write_pg_upgrade_handoff() on the live old
    primary before shutdown (today the test calls it directly; decide whether
    pg_upgrade drives it or it stays an operator/HA-tool primitive -- pg_upgrade
    itself never connects to the old primary as a standby-visible writer, so this
    likely belongs to the HA/orchestration layer, not pg_upgrade core); (ii) block
    the standby from serving any connection during the handoff window (see below);
    (iii) chain to the out-of-band replay-from-CN provisioning (already proven).

DESIGN CORRECTION (2026-07-14): the original "standby streams into
XLOG_PG_UPGRADE_START on the new binary and halts" model is IMPOSSIBLE
cross-version.  WAL page magic is version-stamped (v18=0xD118, v20=0xD120), so:
  * a v18 standby cannot read the v20 upgrade burst (wrong magic), and
  * a v20 standby cannot read the v18 tail it is sitting in.
No single running standby can walk from the old WAL into the new upgrade WAL.
(See item 2 for the full proof.)  So the burst's v20 XLOG_PG_UPGRADE_START can
NEVER be the thing that stops a streaming standby -- it is unreadable by the
streaming (old) binary.  The v20 START record is correct as-is for its ONLY real
consumer: PerformWalUpgradeIfNeeded() on the v20 replayer, to bound the window
and derive CN.

TRIGGER vs TRANSPORT (the corrected design):
  * TRANSPORT of the upgrade is inherently OUT-OF-BAND: the self-contained v20
    window is delivered to the standby (file copy) and replayed into a fresh v20
    skeleton from CN.  This is done and proven (run_standby_xversion_test.sh,
    run_e2e_equivalence_test.sh).  Nothing streams the burst.
  * TRIGGER (this item, to build): an OLD-FORMAT WAL record emitted into the OLD
    cluster's OWN WAL stream, just before pg_upgrade shuts the old cluster down.
    Because it is old-format and chained onto the old stream, a streaming
    old-version standby READS it normally.  On seeing it, the standby performs a
    clean HANDOFF: stop applying at that known LSN, shut down cleanly, and signal
    the operator/automation "upgrade beginning -- swap to the new binary/VM and
    fetch the new-version window".  It is a CONTROL SIGNAL, not a data path: it
    initiates shutdown + new-cluster preparation, then the out-of-band transport
    above takes over.

FORWARD-LOOKING CONSTRAINT: the trigger record must be emitted by the version
being upgraded FROM, so it only helps upgrades OUT OF a version that already
ships it (e.g. helps vN->vN+1 once vN emits it; cannot retrofit a stock-v18
source).  On this fork, old and new are both the fork, so it is implementable and
testable same-fork now.

TWO START-LIKE RECORDS, complementary (do not conflate):
  | record            | format | lives in           | consumer            | job                      |
  | old TRIGGER (new) | old    | old cluster WAL    | streaming old stby  | halt + prepare handoff   |
  | v20 START (exists)| new    | upgrade burst      | v20 replayer        | bound window + derive CN |

IMPLEMENTATION PLAN:
- New WAL record emitted on the OLD (still-running-under-pg_upgrade) primary,
  before the final shutdown, via a SQL function on the old cluster (old binary
  has it because it is the fork).  Carries no upgrade data -- just a marker +
  maybe the target major version for logging.
- Redo/replay handler in the OLD binary: when a StandbyMode server replays the
  trigger, stop cleanly at that LSN with a clear "prepare for upgrade handoff"
  message (do NOT apply anything past it).  Decide mechanism: pause-and-promote
  vs. clean shutdown; a controlled shutdown at the trigger LSN is the goal so the
  operator can swap binaries and re-provision from the delivered v20 window.
- The standby must serve NO writes/connections in a half-upgraded state during
  the handoff window (extend pgUpgradeReplayInProgress semantics if needed).
- End-to-end test: old fork primary + streaming standby -> emit trigger ->
  assert the standby halts cleanly at the trigger LSN with the handoff message
  (this IS reachable, unlike the old v20-START streaming model) -> then out-of-band
  deliver + replay-from-CN as today -> converged v-new standby.

SUPERSEDED history (kept brief): earlier the halt was coded as a
pg_upgrade_redo() FATAL guard on the v20 START under StandbyMode.  That guard is
DEAD for the streaming case (the v20 START is unreadable by the streaming old
binary) -- it remains only as a defensive check for the file-delivered replayer.
The guardtrace.sh "replay stopped at a WAL gap before START" finding was a
symptom of the same version/format boundary, not a fixable contiguity gap.

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
(measured ~5 segments -> 0).

MEASURED DEFINITIVELY (2026-07-14, gapdiag.sh, clean single-INSERT run):
  old cluster last WAL:  0/017A30D0  seg 000000010000000000000001 (MID-segment)
  new cluster first WAL: 0/02000028  seg 000000010000000000000002 (boundary),
                         record = CHECKPOINT_SHUTDOWN ... prev 0/00000000
So:
  * SEGMENT-level gap is ZERO -- old ends in seg 1, new begins in seg 2,
    consecutive, nothing missing.  The task "make the gap zero" is DONE at the
    granularity pg_resetwal controls; the earlier 2->4 / 5->8 gaps were an
    artifact of extra pg_switch_wal in those runs, not the real flow.
  * What REMAINS is NOT a segment gap but a WAL-CHAIN discontinuity, and it is
    NOT closable with pg_resetwal:
      (a) the old cluster ends MID-segment (0/017A30D0); `pg_resetwal -l` can
          only position at SEGMENT-BOUNDARY granularity, so the new WAL must
          start at the next boundary (0/02000000), leaving the sub-segment tail
          0/017A30D0 -> 0/02000000 with no valid continuation; and
      (b) the new cluster's first record is a FRESH WAL START (prev 0/00000000),
          not a continuation of the old chain.  A streaming standby at 0/017A30D0
          cannot chain forward into a record whose prev is 0/0 -- the redo chain
          is broken, independent of segment adjacency.

CONSEQUENCE: pure live-streaming follow would require the new cluster's WAL to
GENUINELY CONTINUE the old chain -- start at the exact old-end LSN with prev
pointing at the old cluster's last record.  The only Postgres mechanism for
"fork the WAL at an exact LSN and continue" is a TIMELINE SWITCH (.history fork
at 0/017A30D0).  That is exactly the approach the earlier experiment DISPROVED
for two independently-reset clusters (primary lands on TLI 2, standby computes
TLI 3, "highest timeline 2 behind recovery timeline 3").  pg_resetwal's
segment-granular fresh-start positioning fundamentally cannot synthesize
chain-continuous WAL.

ROOT CAUSE, cross-version (2026-07-14, magictest -- SUPERSEDES the above for the
real use case): even a PERFECTLY chain-continuous stream is unreadable across a
major version, because WAL PAGE MAGIC is version-stamped:
    v18 pages = 0xD118   (seg header bytes 18 d1)
    v20 pages = 0xD120   (seg header bytes 20 d1)
After halting to upgrade, the standby runs on the NEW (v20) binary; its WAL
reader validates every page against XLOG_PAGE_MAGIC == 0xD120 and REJECTS the
old cluster's v18 (0xD118) pages -- the very segment it is sitting in at the
halt LSN -- BEFORE it can chain forward into the burst.  So there is no single
contiguous WAL stream simultaneously readable by the version that wrote the tail
(v18) and the version that must replay the head (v20).  The major-version WAL
format change IS a hard WAL-stream boundary.

Consequences that this settles definitively:
  * "continue the old chain in the first place" is IMPOSSIBLE cross-version --
    the old tail is physically v18-format pages the v20 replayer cannot read.
  * splicing the old cluster's last segment into the new pg_wal (the "skip
    pg_resetwal" idea) also dies here: that segment is v18-format.
  * the timeline-fork idea dies here too: a .history fork does not change page
    magic.
  * therefore LIVE-STREAMING halt-at-START is fundamentally UNREACHABLE for a
    cross-version upgrade; replay stops at the VERSION boundary, not a fixable
    gap.  (Same-version streaming could be made continuous by splicing the old
    last segment + --control-only anchoring at the old shutdown checkpoint, but
    same-version upgrade is not the use case and proves nothing -- see 1b.)

NOTE: none of this blocks the tested capability.  The file-delivered / skeleton
standby path re-anchors at CN and never tries to read across the v18->v20 WAL
boundary: it replays the self-contained v20 burst into a fresh v20 skeleton from
CN.  That is not a workaround -- given the version-stamped page magic it is the
ONLY correct model.  Proven by run_standby_xversion_test.sh and
run_e2e_equivalence_test.sh (both pass).

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
