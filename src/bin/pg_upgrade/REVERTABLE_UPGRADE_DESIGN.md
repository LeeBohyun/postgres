# Revertable Upgrade + Standby Handoff via WAL — Unified Design

> STATUS: design only. Builds on `REPLICA_UPGRADE_DESIGN.md` (the standby-side
> WAL replay path) and the two settled findings recorded there and in project
> memory: the cross-version **WAL page-magic boundary** and the **segment
> discontiguity** of the upgrade window. This doc adds the *revertability*
> (blue-green / branching) layer and folds the standby handoff into it as one
> model.

## Goal

Make a `--wal-log-upgrade` **revertable** the way a branch is: the old cluster
survives untouched alongside the newly-built one, there is a single explicit
**commit point**, and until that point a **rollback** is a cheap discard — on
both the primary and its physical standby.

Two outcomes, one mechanism:

1. **Revert (blue-green).** Old (vN) and new (vN+1) data directories coexist.
   Before the new cluster accepts its first write, the operator can either
   *commit* (cut over to new) or *roll back* (discard new, resume old).
2. **Standby handoff.** A physical standby of the old cluster converges to the
   new version by replaying the delivered upgrade window (file-delivered, not
   live-streamed — see constraints), and lands in the same
   revertable/pre-commit state as the primary, so the whole replica set commits
   or rolls back together.

## Non-negotiable constraints (already proven — do not re-litigate)

These are established by measurement and recorded in
`REPLICA_UPGRADE_DESIGN.md` and `[[wal-gap-chain-discontinuity]]`:

- **C1 — Version-stamped WAL page magic.** vN pages (`0xD1NN`) are unreadable by
  the vN+1 WAL reader. No single WAL stream is readable by both the old
  tail-writer and the new head-replayer. → *Live streaming of the upgrade window
  across the version boundary is impossible.* Delivery is file-based.
- **C2 — Segment discontiguity.** The upgraded cluster's WAL (CN + START..
  COMPLETE) begins past the old cluster's WAL end, with a hole between. A
  caught-up standby cannot stream into it ("segment already removed"). Reinforces
  C1's conclusion: file-delivered, not streamed.
- **C3 — Independent timeline switches cannot coordinate.** Two end-of-recovery
  switches each pick `findNewestTimeLine()+1` at different LSNs → the standby
  forks onto a timeline the primary isn't on. → *Timelines are usable as a
  branch LABEL, but NOT as the streaming-coordination mechanism.*
- **C4 — Revert is only sound pre-first-write.** Once the vN+1 cluster serves a
  write, that write cannot be replayed back into vN (same boundary as C1). So
  the revert window is exactly: **after COMPLETE, before first user write on
  new.**
- **C5 — The old cluster is SHUT DOWN for the whole upgrade and takes no
  writes.** pg_upgrade requires both clusters stopped; the old cluster never
  serves during or after the upgrade. Therefore old_dir is *frozen*, not merely
  isolated — there is no post-upgrade old_dir delta to reconcile. **Rollback is
  simply: discard new_dir, start the old binary on old_dir.** (This retires the
  earlier Q-R3 "quarantine duration / stale new_dir" concern entirely — it was
  based on the false premise that old_dir keeps taking writes. It does not.)

## Core model: two directories, one commit point

```
                      vN (old)                         vN+1 (new)
                 ┌──────────────┐                  ┌──────────────┐
 PRIMARY         │  old_dir     │  --wal-log-      │  new_dir     │
                 │  (untouched, │  upgrade  ──────▶│  (built from │
                 │   --copy)    │                  │   WAL/image) │
                 └──────┬───────┘                  └──────┬───────┘
                        │ streams vN                      │
                        ▼                                 ▼
 STANDBY         ┌──────────────┐   file-deliver   ┌──────────────┐
                 │  old_dir'    │   upgrade window │  new_dir'    │
                 │  (following) │  ──────────────▶ │  (replayed   │
                 │              │                  │   from CN)   │
                 └──────────────┘                  └──────────────┘

   ── COMMIT POINT ──▶  retire old_dir(') , serve from new_dir(')
   ── ROLLBACK    ──▶  discard new_dir(') , resume old_dir(')
```

Key property that makes this safe today: **`--wal-log-upgrade` in `--copy` mode
never mutates old_dir.** The new cluster is a distinct directory. `--link` and
`--swap` deliberately break this (shared inodes / moved dirs) and are therefore
**incompatible with revertable upgrade** — revert requires `--copy`.

**Standby symmetry (invariant).** The standby holds the *same two directories*
as the primary: `old_dir'` (the physical replica of old_dir, still following the
vN primary) and `new_dir'` (built by replaying the delivered upgrade window).
Every node in the replica set is blue-green in exactly the same shape. This is
what makes set-wide rollback trivial: rollback = *each node independently
discards its own `new_dir'` and keeps serving/following `old_dir'`*; no node ever
touched its old_dir, so there is nothing to repair anywhere. Commit is the mirror
image: every node cuts over from `old_dir(')` to `new_dir(')` together (Q-R2).
The standby must therefore keep `old_dir'` intact and following throughout —
it does NOT convert old_dir' in place into new_dir'; the two coexist, just as on
the primary.

## Why "separate dirs, maybe different timeline" is the right instinct

- **Separate dirs** give the rollback for free: rollback = "don't cut over,"
  commit = "cut over." Nothing to undo, because old_dir was never touched. This
  is strictly better than `pg_rewind`-after-the-fact, which pays to repair
  damage we'd have inflicted.
- **Different timeline as a *label*** (C3): stamp new_dir's history so tooling
  and standbys can tell "the vN+1 branch" from "the vN line." This does NOT try
  to make streaming resume across the switch (that's the rejected path); it's
  purely an identity/branch marker, like a git branch name.

## The commit point (new concept this design adds)

Today the flow ends at COMPLETE and the new cluster is immediately "the
cluster." Revertability requires inserting an explicit gate:

1. Upgrade runs, emits START..COMPLETE, new_dir is reconstructed and consistent
   **but quarantined** — new_dir must not accept a user write yet.
   - Mechanism candidate: bring new cluster up in a **no-write / recovery-held**
     state (analogous to a paused standby), OR leave it stopped with a
     `new.upgrade_pending` sentinel and only the operator's explicit `commit`
     starts it read-write.
2. Operator verifies (smoke test on new_dir via a side port, read-only).
3. **Commit:** clear the sentinel, allow writes on new_dir, retire old_dir
   (and, on standbys, re-point streaming at the committed new primary).
4. **Rollback:** delete new_dir, restart old binary on old_dir (which is exactly
   as it was), standbys simply keep following the vN primary.

The existing `revert_wal_logged_disk_writes` (TODO Q6) is a *test device* that
wipes new_dir's reconstructed image; it only ever touches new_dir, so old-dir
rollback is already inherently safe. For the real feature it must be
removed/gated (Q6), but it confirms the invariant we depend on: **the revert
path lives entirely in new_dir; old_dir is immutable through the whole upgrade.**

## Standby handoff, folded in

The standby reaches the same quarantined state, by the file-delivered path from
`REPLICA_UPGRADE_DESIGN.md` (§"Proposed flow", `PerformReplicaUpgradeIfNeeded`):

1. Standby (vN binary) streams vN as usual, following old_dir on the primary.
2. Replay reaches `XLOG_PG_UPGRADE_START` → stop, confirm a genuine COMPLETE is
   present (atomicity), write the pause sentinel + CN anchor, shut down clean.
   (If COMPLETE never comes — primary crashed mid-upgrade — the standby stays vN,
   mirroring the primary's own abort behavior. This IS a rollback, for free.)
3. Operator swaps in the vN+1 binary; it detects the sentinel, enters the
   replica-upgrade path, anchors at CN, replays the **file-delivered** window
   (C1/C2: not streamed) into new_dir' in NON-serving recovery (FPI-LSN safety),
   clears the sentinel → new_dir' is now quarantined, same as the primary.
4. Commit/rollback happens **set-wide**: the orchestration commits primary and
   standbys together, or rolls back together. Because each node kept its old_dir,
   a set-wide rollback is every node independently discarding its new_dir'.

The one genuinely open sub-problem remains **segment sourcing** (Open Q3): where
the standby reads the delivered window from (archive / restore_command / an
explicit copy). This design assumes file delivery and does not depend on solving
live-streaming (C1/C2 say we can't).

## What already exists vs. what this needs

| Piece | State |
|---|---|
| old_dir untouched in `--copy` | EXISTS (stock pg_upgrade property) |
| new cluster built from WAL (CN-anchored replay) | EXISTS (`PerformWalUpgradeIfNeeded`) |
| standby-side replay entry point | DESIGNED, not built (`PerformReplicaUpgradeIfNeeded`, REPLICA doc) |
| file-delivery of upgrade window | partial (file-delivered model works in tests) |
| **quarantine / commit gate on new_dir** | **NEW — this design** |
| **set-wide commit/rollback orchestration** | **NEW — partly outside Postgres (operator contract)** |
| branch-label timeline on new_dir | NEW, optional (label only, per C3) |
| revert lives only in new_dir | EXISTS as invariant (confirmed via Q6 wipe) |

## Open questions specific to this design

Q-R1. **Quarantine mechanism.** Recovery-held live server vs. stopped +
sentinel. Stopped+sentinel is least-coupled and mirrors the standby sentinel
(REPLICA Q5 leans sentinel too); a held live server allows read-only smoke tests
without a side start. Decide.

Q-R2. **Commit atomicity across the set.** The commit must be all-or-nothing
across primary + N standbys, or a partial commit leaves a split-version replica
set. This is an orchestration contract (like C4 says revert is only sound
pre-first-write, commit is only sound if it's set-wide). Define the protocol;
likely: quarantine everywhere → verify → commit primary → standbys re-point →
only then allow writes.

Q-R3. **RESOLVED (was a false problem).** The old cluster is shut down for the
whole upgrade and takes no writes (C5), so new_dir never goes stale relative to
old_dir. There is no delta to reconcile and no freeze needed. Quarantine can
last arbitrarily long: old_dir just sits there, frozen and startable, until the
operator commits (adopt new_dir) or rolls back (discard new_dir, start old
binary on old_dir). This is the classic pg_upgrade rollback story, just made
explicit and extended to the standby.

Q-R4. **Interaction with `--link`/`--swap`.** Document that revertable upgrade
requires `--copy` (C4 + shared-inode reasoning). Reject the combo at option
parse time, exactly as we now reject `--check --initdb`.

## Build order (when we proceed)

0. Resolve Q-R3 (quarantine duration / delta handling) — it gates the whole
   shape.
1. Add the **quarantine gate**: after COMPLETE, new cluster does not serve writes
   until an explicit commit (sentinel + operator command). Reject
   `--link`/`--swap` + revertable (Q-R4).
2. Add **commit** and **rollback** operations (likely `pg_upgrade --commit` /
   `--rollback`, or a small companion, operating on the sentinel + dir lifecycle).
3. Fold in the standby: `PerformReplicaUpgradeIfNeeded` lands new_dir' in the
   same quarantined state (depends on REPLICA doc build order 1–2).
4. Set-wide commit/rollback orchestration + the operator contract doc (Q-R2).
5. Optional branch-label timeline on new_dir (Q C3 — label only).
6. Tests: revert-after-COMPLETE leaves old_dir byte-identical and serving;
   commit cuts over cleanly; standby rolls back with the primary; crash-mid-
   upgrade on a standby stays vN (free rollback).

## The two missing parts (verified against code, not just docs)

The primary already has **both directories** (old_dir intact in `--copy`, new_dir
built). What is missing splits in two:

1. **The commit / switch-over decision — missing on BOTH primary and standby.**
   Today new_dir goes live the moment the operator starts it; there is no
   "hold, then decide" state and no `--commit`/`--rollback`. This is the gate.

2. **The standby's two-directory (blue-green) layout — missing entirely.**
   Verified: `PerformReplicaUpgradeIfNeeded()` does NOT exist in code (only in
   docs). The current standby path (`run_standby_handoff_e2e_test.sh`) is
   **re-provision-by-demolition**: on the HANDOFF trigger the standby shuts down
   (works), then a *fresh* target dir is `initdb`'d, wiped to a skeleton, the
   delivered upgrade window is copied in, and it replays from CN (works). That
   builds ONE new dir by discarding the old standby — it does NOT keep
   `old_dir'` and `new_dir'` side by side. For the revertable design the standby
   must instead build `new_dir'` as a SEPARATE directory while `old_dir'` stays
   intact and shut down, mirroring the primary — so a rollback is "discard
   new_dir', restart old binary on old_dir'".

   What already works and is reusable: the boundary halt (`XLOG_PG_UPGRADE_HANDOFF`,
   `pgupgrade_wal.c:568`) and the delivered-window replay-from-CN convergence
   (`PerformWalUpgradeIfNeeded` handles the new dir's first startup). What is new:
   keep old_dir' instead of demolishing it, build new_dir' alongside, and add the
   quarantine + commit/rollback gate.

## Implementation map (grounded in current code)

This section pins the design to concrete code locations, from a full read of
`pgupgrade_wal.c`, `xlog.c`, `xlogrecovery.c`, `postmaster.c`, `pg_upgrade.c`,
`relfilenumber.c`, and the timeline/archive machinery. The headline finding:
**most of what we need already exists in PostgreSQL's recovery machinery — the
new code is mostly gating, not new mechanism.**

### A. old_dir stays untouched — already true in `--copy`

- The only step that mutates the old cluster is `disable_old_cluster()`
  (`pg_upgrade.c:245-247`, renames old `pg_control` → `pg_control.old`), and it
  runs **only for `--link`/`--swap`**. Plain `--copy` never calls it, so old_dir
  already has blue-green isolation.
- `transfer_all_new_tablespaces()` (`pg_upgrade.c:249`) reads old, writes new; in
  `--copy` it copies, never moves.
- **Action:** reject `--link`/`--swap` + revertable at option parse
  (`option.c`, same spot as the new `--check --initdb` rejection). Revertable ⇒
  `--copy` only. (Q-R4)

### B. The commit token already exists: `XLOG_PG_UPGRADE_COMPLETE`

- Written at the end of the WAL-capture burst (`pg_upgrade.c:351-356`). Its
  presence/absence is *already* an atomic commit gate: first startup
  (`PerformWalUpgradeIfNeeded`, `pgupgrade_wal.c:418-421`) FATALs if START is
  present but COMPLETE is absent, leaving old_dir intact.
- The test hook `PG_UPGRADE_TEST_SKIP_COMPLETE` (`pg_upgrade.c:347-355`) already
  simulates "no commit" by withholding COMPLETE. This is the crash/abort =
  rollback path, working today.
- **So the primary already has a crude commit/abort gate.** What it lacks is a
  *hold-after-COMPLETE* state (quarantine) — today COMPLETE ⇒ next startup goes
  straight to serving.

### C. Quarantine mechanism — REUSE `recovery_target_lsn` + `recovery_target_action`

The cleanest quarantine is **not** a new control-file field; it's PostgreSQL's
existing recovery-target machinery, which already does "replay to a chosen LSN,
then hold without going live":

- Build new_dir's first startup as **targeted archive recovery**: drop a
  `recovery.signal` (`xlogrecovery.c:1058-1061` → `ArchiveRecoveryRequested=true,
  StandbyModeRequested=false`) with `recovery_target_lsn` = COMPLETE's LSN and
  `recovery_target_action = shutdown` (`xlogrecovery.c:1835-1841`).
- Effect: recovery replays CN..COMPLETE, reaches the target, and the postmaster
  **`proc_exit(3)`s without writing an end-of-recovery record or forking a
  timeline** (`xlog.c:6443-6488` is skipped). The cluster is frozen at COMPLETE,
  fully reconstructed but never served, trivially discardable. **This is the
  quarantine state, built from stock parts.**
- `recovery_target_action = pause` (`xlogrecovery.c:1843-1845`, needs
  `hot_standby=on`) is the alternative if we want new_dir queryable **read-only**
  during the decision window (smoke tests via a side port) — backed by
  `RecoveryPauseState` (`xlogrecovery.c:3056-3104`, `recoveryPausesHere` 2911).
- **Tension with the current bootstrap:** `ArmControlFileForUpgradeRecovery()`
  (`xlog.c:4658-4685`) sets `state = DB_IN_PRODUCTION`, which forces *crash*
  recovery, not archive recovery — so the recovery-target path is currently
  bypassed. The revertable path must instead arm recovery as archive-recovery-
  to-target. This is the main new wiring on the startup side: a variant of
  `PerformWalUpgradeIfNeeded` that sets up recovery-to-COMPLETE-then-hold rather
  than crash-recover-and-go-live.

### D. Commit / rollback operations

- **Commit** = restart new_dir *without* the recovery target (or
  `pg_wal_replay_resume()` if we used `pause`): recovery reaches end, writes the
  end-of-recovery record, promotes. Then retire old_dir.
- **Rollback** = delete new_dir; old_dir was never touched, restart old binary.
- **Surface:** a `pg_upgrade --commit` / `--rollback` companion, or a small
  standalone tool, operating on the sentinel + `recovery.signal`/target config.
  (Commit removes the target and restarts; rollback rm -rf new_dir.)

### E. Standby: it already halts at the boundary; reuse archive delivery

- **The standby already stops cleanly at the upgrade boundary.** Two existing
  triggers do this (more than the older design doc assumed):
  - `XLOG_PG_UPGRADE_HANDOFF` (`pgupgrade_wal.c:568-611`): a control-only record
    written into the *old* primary's WAL before shutdown. A `StandbyMode` server
    replaying it FATALs with "reached pg_upgrade handoff on standby; shutting
    down" — the swap-binary signal, in the OLD page format the standby can still
    read.
  - The `XLOG_PG_UPGRADE_START` guard (`pgupgrade_wal.c:513-527`): if a standby
    somehow reaches START without the sanctioned bootstrap, it FATALs too.
- **Delivery = archive, not streaming** (forced by C1/C2). Reuse
  `RestoreArchivedFile` + `restore_command` (`xlogarchive.c:54`): the upgrade
  window segments are handed to new_dir' recovery on demand, keyed by
  `XLogFileName(tli, segno)`. This is exactly the "file-delivered" model, and it
  is already how a standby fetches any archived segment.
- **new_dir' quarantine is identical to the primary's** (section C): the standby
  builds new_dir' via recovery.signal + restore_command + recovery_target_lsn =
  COMPLETE, action = shutdown/pause. old_dir' keeps following the vN primary via
  ordinary streaming (`walreceiver.c` path) until the set-wide commit.

### F. What is genuinely NEW code (everything else is reuse)

1. Option handling: a `--revertable` (or make it implied by a `--commit`/
   `--rollback` workflow) + reject `--link`/`--swap` (`option.c`).
2. A startup entry that arms **archive-recovery-to-COMPLETE-then-hold** instead
   of the current crash-recover-and-go-live (`pgupgrade_wal.c` +
   `xlog.c:4658` sibling of `ArmControlFileForUpgradeRecovery`).
3. Commit/rollback commands (§D).
4. Standby: turn the boundary FATAL (§E) into "mark + hold for binary swap", then
   build new_dir' with the same quarantine (depends on
   `PerformReplicaUpgradeIfNeeded`, still to build per REPLICA doc).
5. Set-wide commit orchestration + operator contract (Q-R2) — partly outside PG.

Everything under "reuse": recovery targets, `RecoveryPauseState`,
`restore_command`/`RestoreArchivedFile`, timeline history follow, the existing
COMPLETE commit token, and `--copy`'s old_dir isolation.

### H. Streaming the window into the skeleton (spike-validated, 2026-07-16)

The older sections assume the window is **file-delivered** (archive / restore_command).
A spike on Arca (fresh v20 skeleton streaming from an upgraded v18->20 primary)
established that **streaming the window into a fresh skeleton is viable** — the
PG_VERSION gate does NOT apply to a fresh vN+1 skeleton (it starts, "entering
standby mode", "ready to accept read-only connections"). So "no cp, stream it"
is achievable, subject to the pieces below.

WHAT THE SPIKE RULED OUT vs. IN:
- OUT (my test error, not a design blocker): the spike hit a walreceiver
  "system identifier differs" FATAL — but only because it started the skeleton
  in plain standby.signal streaming WITHOUT arming the upgrade bootstrap first.
  **No sysid seeding is needed.** The sysid rides IN the WAL (`xlp_sysid`); the
  bootstrap already adopts it in-process (`ArmControlFileForUpgradeRecovery`:
  `ControlFile->system_identifier = wal_sysid`, proven by
  run_standby_xversion_test with intentionally-different sysids). The real
  requirement is that arming (which adopts the sysid + anchors CN) must run in
  the streaming path too, before/around the walreceiver handshake — not an
  out-of-band sysid stamp.
- IN (genuine, spike-confirmed): the upgraded primary had ALREADY RECYCLED the
  window ("window markers in primary pg_wal: 0") because its end-of-recovery
  checkpoint reclaimed those segments. **The primary must RETAIN the window** for
  a standby to stream it — via a replication slot / wal_keep_size, and/or
  archive.  This is C2 made concrete: without retention there is nothing to
  stream.

TIMELINE COORDINATION (useful — checked against code):
The window is currently emitted on TLI 1 (`pgupgrade_wal.c:280` `priv.tli = 1`).
Walreceiver already supports FOLLOWING a timeline switch while streaming
(`WalRcvFetchTimeLineHistoryFiles`, `startpointTLI`; standby reads the `.history`
file — `xlogrecovery.c:4317`).  So emitting the window as a **new timeline forked
at CN** (write a `.history`) lets a streaming standby cross TLI 1 -> TLI 2 at the
fork point and pull the window, exactly as standbys already cross timelines
during failover/promotion.  This is the "manage the timeline like Neon branching"
idea, and it is the natural C2 solution for the streaming path: the branch point
IS the CN anchor.  (For the file-delivered path, TLI 1 is fine.)

ATOMICITY WITHOUT A PRE-SCAN (decided 2026-07-16):
The file path proves atomicity by pre-scanning pg_wal for COMPLETE and refusing
to start if absent.  **Streaming cannot pre-check COMPLETE** — the window arrives
incrementally.  So the model inverts, and this is WHY the feature is revertable:
- DETECT an upgrade by the FIRST record being XLOG_PG_UPGRADE_START (not by
  finding COMPLETE up front).  Drop the `!found_complete -> FATAL` pre-scan.
- ARM on START, then HOLD in quarantine — the cluster NEVER auto-goes-live.
- A `--status` / commit-gate check verifies COMPLETE was ACTUALLY REPLAYED before
  allowing commit.  An incomplete stream (primary died mid-window) simply never
  reaches COMPLETE -> stays quarantined -> rollback discards it.  No half-upgraded
  cluster ever serves.  Rollback-ability REPLACES the pre-scan as the atomicity
  guarantee.
- NOTE (code impact): today's "already applied?" restart check keys off
  `GetControlFileCheckPointLSN() >= complete_lsn`.  With COMPLETE no longer
  required up-front, that test must be re-based on a COMPLETE-independent signal
  (e.g. pg_control state past the window / not-quarantined), or `complete_lsn`
  being Invalid (0) would make the check always true and wrongly skip arming.

NEW CODE for the streaming standby path (beyond the file path):
1. Arming must run in the streaming path (adopt sysid + anchor CN before the
   walreceiver applies the window); rework the START-guard so a bootstrap-armed
   streaming standby APPLIES the window instead of FATAL-halting.
2. Window retention on the primary (slot / wal_keep_size / archive) + optional
   emit-on-new-timeline-forked-at-CN so the walreceiver can follow to it.
3. Gate rework: arm-on-START, drop the COMPLETE pre-scan, add the
   COMPLETE-replayed status/commit check (shared with the file path).

### G. No stale-new_dir problem (Q-R3 retired)

The old cluster is shut down for the whole upgrade (C5), so new_dir never goes
stale — there is nothing to ship old→new and no freeze to coordinate. Quarantine
is just "new_dir reconstructed but not yet adopted; old_dir frozen and startable."
Commit adopts new_dir; rollback discards it and starts the old binary. This is
already how stock pg_upgrade's rollback works (the old cluster is left intact);
we are making it an explicit, first-class state and extending it to the standby.

## Relationship to existing docs

- `REPLICA_UPGRADE_DESIGN.md` — the standby WAL-replay convergence path. This
  doc reuses its `PerformReplicaUpgradeIfNeeded` flow and its atomicity rule, and
  adds the quarantine/commit layer on top so the standby lands revertable.
- `TODO.md` Q6 — `revert_wal_logged_disk_writes` removal. Relevant because it
  proves revert lives only in new_dir; production revert must not depend on the
  test wipe.
- `[[wal-gap-chain-discontinuity]]`, `[[wal-log-upgrade-inprocess-anchor]]`
  (project memory) — C1/C2/C3 and the in-process CN anchor this builds on.

---

# Detailed design B: standby two-directory (blue-green) restructure

> Do this FIRST (per plan). It converts today's "re-provision by demolition"
> into "keep old_dir', build new_dir' alongside," so a standby rollback is a
> discard, not a rebuild.

## What the standby does today (from `run_standby_handoff_e2e_test.sh`)

Two directories appear in the test, but they are NOT a blue-green pair:

- `$STBY` — the live streaming standby (basebackup of the old primary,
  `standby.signal`). On the HANDOFF trigger it shuts down (works) and is then
  **abandoned**.
- `$TGT` — a *brand-new* `initdb` skeleton, wiped, the delivered upgrade window
  copied into `$TGT/pg_wal/`, started → first startup runs
  `PerformWalUpgradeIfNeeded()`, arms at CN, replays the window, converges
  writable.

So convergence works, but the old standby (`$STBY`) is thrown away and a fresh
dir is built. There is nothing to roll back TO once you've committed to building
`$TGT`, and the old standby data is gone.

## Target layout (mirror the primary)

The standby holds two directories at once, exactly like the primary:

```
  old_dir'  ($STBY)   vN, still a valid shut-down standby of the old primary.
                      Frozen. Startable as a vN standby again = ROLLBACK.
  new_dir'  ($NEW')   vN+1, built by replaying the delivered upgrade window
                      from CN into a fresh vN+1 skeleton. Quarantined (held,
                      not serving) until the set-wide COMMIT.
```

Rollback on the standby = `rm -rf new_dir'`, start vN binary on old_dir'
(re-attaches to the still-vN old primary — but note: after a *primary* rollback
the primary is also vN again, so the pair is consistent; after a *primary*
commit the standby must instead commit, see design A). Commit = adopt new_dir'
as the serving standby of the upgraded primary; retire old_dir'.

## The one structural change

Today the code path **reuses the target dir's identity by demolition**: wipe a
fresh initdb, drop in WAL, let first-startup rebuild. To keep old_dir' intact we
simply stop demolishing it and build new_dir' as a genuinely separate directory:

1. **Do not touch `$STBY`.** Leave the halted standby dir exactly as the HANDOFF
   trigger left it: a clean shut-down vN standby. (Today the test moves on and
   ignores it; the design makes "leave it intact and startable" an explicit
   guarantee — no wipe, no reuse.)
2. **Build new_dir' as a fresh, separate directory** (the current `$TGT` step,
   but named/owned as new_dir' and never conflated with old_dir'):
   - `initdb` a vN+1 skeleton at new_dir'.
   - Deliver the upgrade-window segments into `new_dir'/pg_wal/` (archive /
     `restore_command` / copy — the file-delivered model; C1/C2 forbid
     streaming).
   - First startup runs `PerformWalUpgradeIfNeeded()` → arms at CN → replays
     DIRSKEL/RELFILE/SLRU/RAWFILE → COMPLETE. (This already works.)
3. **Hold new_dir' in quarantine** instead of letting it go straight to writable
   (design A's recovery-target gate). Until commit, new_dir' is reconstructed but
   not serving; old_dir' is the fallback.

That is the whole restructure: **stop demolishing old_dir'; build new_dir'
beside it; hold it.** No new redo logic — `PerformWalUpgradeIfNeeded` and the
delivered-window replay are unchanged. The change is *directory lifecycle +
quarantine*, not WAL mechanics.

## Where `PerformReplicaUpgradeIfNeeded` fits (and why it may be thin)

The older `REPLICA_UPGRADE_DESIGN.md` names a dedicated
`PerformReplicaUpgradeIfNeeded()` distinct from the primary bootstrap. Given what
now exists, the standby's new_dir' first-startup can REUSE
`PerformWalUpgradeIfNeeded()` almost verbatim (it already derives CN from the
delivered WAL and replays into a skeleton — proven by the handoff test). The
genuinely standby-specific needs are narrow:

- **Segment sourcing** (Open Q3): where new_dir' reads the window from
  (`restore_command` vs. a pre-staged copy). Reuse `RestoreArchivedFile`
  (`xlogarchive.c:54`).
- **Quarantine hold** (design A): shared with the primary.
- **old_dir' preservation**: a lifecycle rule, not recovery code.

So `PerformReplicaUpgradeIfNeeded` may reduce to "reuse the primary bootstrap +
source segments from archive + honor the quarantine target," rather than a
separate replay engine. Decide during build whether it needs to be a distinct
function at all, or just a call-site difference (segment source + target).

## B build order

B1. Rename/relabel the test's `$TGT` construction as new_dir', and add an
    explicit assertion that old_dir' (`$STBY`) is left byte-intact and is still a
    startable vN standby after new_dir' is built. (Turns the implicit
    abandonment into a tested guarantee.)
B2. Factor segment delivery into new_dir'/pg_wal/ behind `restore_command`
    (archive delivery) rather than a raw `cp`, so it matches a real standby.
B3. Apply the quarantine hold (design A) to new_dir' first-startup so it does not
    auto-serve. old_dir' stays frozen.
B4. Standby rollback test: after B1–B3, `rm -rf new_dir'`, start vN binary on
    old_dir', assert it re-attaches to the (still-vN) old primary and serves.

---

# Detailed design A: the commit / switch-over decision

> Do this SECOND. It is the gate missing on BOTH primary and standby: hold the
> new cluster non-serving after COMPLETE, then an explicit operator decision
> COMMITs (adopt new) or ROLLs BACK (discard new, keep old).

## The two operations

**Commit** (adopt new_dir):
1. Release the quarantine: new_dir finishes recovery (writes its end-of-recovery
   record, promotes) and comes up writable. Mechanically: restart new_dir
   *without* the recovery target, or `pg_wal_replay_resume()` if we held via
   `pause` (`xlogfuncs.c:1067`).
2. Retire old_dir (the stock `delete_old_cluster.sh` already exists,
   `pg_upgrade.c:440`; commit can run/authorize it).
3. On standbys: promote new_dir' to the serving standby of the upgraded primary;
   retire old_dir'. Set-wide (Q-R2).

**Rollback** (discard new_dir):
1. Delete new_dir (and new_dir' on standbys). It was never adopted.
2. Start the vN binary on old_dir — untouched and shut down (C5), so this is a
   plain server start, not a repair.
3. This is stock pg_upgrade's rollback story (old cluster left intact) made into
   a first-class command.

## The quarantine hold (the state both operations act on)

After COMPLETE replays, new_dir must be reconstructed-but-not-serving. Two ways,
both from stock machinery (see implementation map §C):

- **`recovery_target_lsn = COMPLETE` + `recovery_target_action = shutdown`**
  (`xlogrecovery.c:1835`): replays to COMPLETE, then `proc_exit(3)` — never
  writes the end-of-recovery record, never forks a timeline
  (`xlog.c:6443-6488` skipped). new_dir is frozen at COMPLETE, dark, discardable.
  Commit = restart without the target.
- **`... action = pause`** (`xlogrecovery.c:1843`, needs `hot_standby=on`): holds
  at COMPLETE with a **read-only** server up, so the operator can smoke-test
  new_dir before deciding. Commit = `pg_wal_replay_resume()`.

Tension to resolve in code: the current bootstrap arms
`state = DB_IN_PRODUCTION` (`xlog.c:4666`), forcing *crash* recovery, which
bypasses recovery targets. The revertable path must arm
archive-recovery-to-target instead — a sibling of
`ArmControlFileForUpgradeRecovery` that leaves state as archive-recovery and sets
the target to COMPLETE. This is the main new startup-side wiring.

## User surface

Options on the same footing as the `--check`/`--initdb` rejection we just added:

- `--revertable` (or implied by the workflow): implies `--copy`; reject
  `--link`/`--swap` (they destroy old_dir); leave new_dir quarantined instead of
  finalizing.
- `pg_upgrade --commit -D new_dir` / `pg_upgrade --rollback -D new_dir` (or a
  small companion tool): operate on the quarantine sentinel + recovery-target
  config + dir lifecycle.

## Set-wide atomicity (Q-R2)

Commit must be all-or-nothing across primary + N standbys, or the set splits
versions. Protocol sketch: quarantine everywhere → verify (optional read-only
smoke test if `pause`) → commit primary → standbys adopt new_dir' and re-point at
the upgraded primary → only then allow writes anywhere. A partial commit is the
one dangerous state; the orchestration contract (partly outside PG) must make it
atomic or clearly recoverable. Rollback is inherently safe set-wide because every
node independently discards its own new_dir and restarts vN on its untouched
old_dir.

## A build order

A1. Add the quarantine arm: sibling of `ArmControlFileForUpgradeRecovery` that
    sets archive-recovery-to-COMPLETE-then-hold instead of crash-recover-and-go.
A2. `--revertable` option + `--link`/`--swap` rejection.
A3. `--commit` / `--rollback` (release quarantine + retire old, vs. discard new +
    start old).
A4. Wire the standby (design B's new_dir') to the same quarantine + set-wide
    commit.
A5. Tests: commit adopts new_dir and old is retired; rollback leaves old_dir
    byte-identical and serving; crash between COMPLETE and commit still holds (no
    auto-go-live); set-wide commit/rollback across a primary+standby pair.
