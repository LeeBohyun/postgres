# Revertable Upgrade — Implementation Plan

> The actionable build plan. Rationale, constraints (C1–C5), and the code
> survey live in `REVERTABLE_UPGRADE_DESIGN.md`; this file is the ordered
> checklist with file anchors and test gates. Branch: `wal_log_upgrade`.

## One-paragraph summary

Make `--wal-log-upgrade` revertable by DEFAULT (no opt-in flag), blue-green
style, on the primary AND its standby. **A new cluster never silently goes
live**: after `XLOG_PG_UPGRADE_COMPLETE` it is held **quarantined**
(reconstructed but not serving) until an explicit **commit**. The old cluster is
shut down for the whole upgrade (C5), so old_dir is frozen and startable —
rollback is just "discard new_dir, start old binary." The standby mirrors the
primary: it keeps old_dir' intact and builds new_dir' beside it, instead of
today's re-provision-by-demolition.

**Scope of "default":** default *within* `--wal-log-upgrade`. The quarantine
mechanism is the hold-recovery-at-COMPLETE gate, which only exists because
`--wal-log-upgrade` produces a START..COMPLETE window with a COMPLETE marker.
Stock (non-wal-logged) pg_upgrade has no window and nothing to quarantine on, so
it is unchanged and out of scope. There is NO `--revertable` flag; the behavior
is intrinsic to `--wal-log-upgrade`.

## Upgrade lifecycle state machine

The core of the feature is a small, explicit state carried on new_dir across the
two phases (WAL **logging** on the old side, WAL **applying** on the new side).
`commit`/`rollback` are the only transitions out of QUARANTINED.

```
   PHASE                STATE (new_dir)         set / cleared by
   ─────────────────────────────────────────────────────────────────────
   (logging, on old)    —                       pg_upgrade emits the window;
                                                 old_dir untouched
   first start of new   REPLAYING               PerformWalUpgradeIfNeeded arms
     ├─ applies CN..COMPLETE                     recovery-to-target (A1)
     └─ reaches COMPLETE                         redo of XLOG_PG_UPGRADE_COMPLETE
   held at COMPLETE      QUARANTINED             recovery stops at target;
                                                 NOT serving; restart re-holds
   operator: commit  ── DB_IN_PRODUCTION ──────►  (1) release hold → new_dir
                                                     finishes recovery, promotes,
                                                     verified live; THEN
                                                 (2) stamp old_dir superseded
   operator: rollback ─ (state discarded) ─────► rm -rf new_dir; start vN on
                                                 old_dir (frozen, C5)
```

State transitions and their invariants:

| From (new_dir DBState) | Event | To | Invariant enforced |
|---|---|---|---|
| (none) | START replayed w/o COMPLETE present | FATAL | crash mid-upgrade → old_dir intact (`pgupgrade_wal.c:418`) |
| (none) | START..COMPLETE window present | `DB_IN_ARCHIVE_RECOVERY` (replaying) | sanctioned bootstrap only (`in_upgrade_bootstrap`) |
| replaying | COMPLETE replayed, target reached | `DB_UPGRADE_QUARANTINED` | new_dir NOT opened read-write; no end-of-recovery record / TLI fork |
| `DB_UPGRADE_QUARANTINED` | restart | `DB_UPGRADE_QUARANTINED` | idempotent hold — never re-replays, never auto-goes-live |
| `DB_UPGRADE_QUARANTINED` | `commit` | `DB_IN_PRODUCTION` | only explicit; release hold → promote → **verify live** → THEN stamp old_dir |
| `DB_UPGRADE_QUARANTINED` | `rollback` | (gone) | discard new_dir; old_dir started as vN |
| `DB_IN_PRODUCTION` (committed) | — | (terminal) | ordinary live cluster; C4: no rollback after first write |

**How the state is represented. DECIDED (Phase 0): new `DBState` values in
pg_control** — not a side-file sentinel. The cluster's own control file records
the lifecycle state, so it is the single source of truth, `pg_controldata`
prints it for free, and startup (which already switches on `DBState`) re-enters
the hold on restart by reading it.

```
typedef enum DBState {
    ...
    DB_IN_PRODUCTION,
    DB_UPGRADE_QUARANTINED,   /* NEW: reconstructed at COMPLETE, held, not serving */
} DBState;
```

**Exactly ONE new state.** Commit transitions new_dir
`DB_UPGRADE_QUARANTINED → DB_IN_PRODUCTION` (recovery finishes normally): a
committed cluster is just an ordinary live cluster, so no code that checks
`state == DB_IN_PRODUCTION` needs to change. There is deliberately **no
`DB_UPGRADE_COMMITTED`** — it would leave a live production database
permanently in a non-`DB_IN_PRODUCTION` state (huge blast radius across
connection handling, replication, monitoring, tools) purely for an audit
convenience. The "a commit happened" fact that actually matters lives on
**old_dir's superseded stamp** (which `--delete-old -d old` reads); the stamp
can record commit LSN + timestamp for audit.

The QUARANTINED **hold** itself is implemented by
`recovery_target_lsn = complete_lsn` + `recovery_target_action = shutdown`
(A1). The pg_control `DBState` (not a side-file) is what makes a *restart*
re-enter the hold rather than finalize. Model the "already past?" test on the
existing `GetControlFileCheckPointLSN() >= complete_lsn` check
(`pgupgrade_wal.c:434`).

**What choosing a pg_control state obligates (on-disk format contract):**
- It touches the cross-version control format. Bump/verify against
  `PG_CONTROL_VERSION` handling; upstream reviews new `DBState` values closely.
- `pg_controldata` must print the new states (its `dbState()` string table),
  and `pg_resetwal` / any control-state reader must handle them (else
  "unrecognized status code").
- Bootstrapping tension (DESIGN.md §C): today's path forces
  `DB_IN_PRODUCTION` (crash recovery, bypasses recovery targets). The A1 arm
  must set `DB_UPGRADE_QUARANTINED` and route to archive-recovery-to-target
  instead — this is the main new startup-side wiring.
- The commit stamp on old_dir (rename `pg_control`→`pg_control.old`) is
  independent of this and unchanged; `DBState` tracks *new_dir's* lifecycle.

The standby carries the SAME state on new_dir'; old_dir' is a separate,
always-startable vN standby (never enters this machine).

## What already exists (reuse — do not rebuild)

| Capability | Location |
|---|---|
| old_dir untouched in `--copy` | `pg_upgrade.c:245` (`disable_old_cluster` only for link/swap) |
| new_dir is a separate dir (`-D`) | inherent |
| atomic commit token (START..COMPLETE) | `pg_upgrade.c:351`; gate at `pgupgrade_wal.c:418` |
| CN-anchored replay of the window | `PerformWalUpgradeIfNeeded` `pgupgrade_wal.c:365` |
| standby halts at boundary | `XLOG_PG_UPGRADE_HANDOFF` `pgupgrade_wal.c:568` |
| hold-recovery-at-LSN-then-stop | `recovery_target_lsn` + `recovery_target_action` `xlogrecovery.c:1835` |
| file delivery of segments | `RestoreArchivedFile`/`restore_command` `xlogarchive.c:54` |
| retire old cluster | `delete_old_cluster.sh` `pg_upgrade.c:440` |

## What is new

1. Quarantine arm (archive-recovery-to-COMPLETE-then-hold) — **the linchpin**.
   Always on for `--wal-log-upgrade` (no flag).
2. Reject `--link`/`--swap` with `--wal-log-upgrade` (revertable needs old_dir
   intact ⇒ `--copy` only).
3. `commit` / `rollback` (surface TBD — see Phase 0).
4. Standby: keep old_dir', build new_dir' beside it (stop demolition).
5. Set-wide commit orchestration (partly outside PG).

## Dependency graph

```
        A1 (quarantine arm)  ◄── linchpin; first code either track needs
        /            \
   A2/A3            B3 (hold new_dir')
 (--revertable,      |
  commit/rollback)  B1,B2 (keep old_dir', archive-deliver new_dir')
        \            /
         A4 (wire standby to quarantine + set-wide commit)
              |
         A5 / B4 (tests: primary + standby commit & rollback)
```

Do **B first, then A** per decision — but note **A1 is the first code to write**
regardless, because both B3 and A2–A4 depend on the quarantine hold existing.

---

## Phase 0 — Decide before coding

- [x] **Enablement: DEFAULT, no flag.** Revertable/quarantine is intrinsic to
      `--wal-log-upgrade`; a new cluster never silently goes live. No
      `--revertable` opt-in. (Decided.)
- [x] **commit/rollback surface (the operator interface). DECIDED:
      `pg_upgrade` subcommands (candidate 1), upstream-first.** No new binary; the
      lifecycle verbs live on `pg_upgrade` itself. Branch-style verb set, each a
      discrete deliberate operation (modeled on Neon branching, where cut-over and
      branch-deletion are separate acts):

      | Command | Args | Gate / behavior |
      |---|---|---|
      | `pg_upgrade --wal-log-upgrade -b -B -d -D` | full pair | builds new_dir HELD (quarantined); records `old_datadir`/`old_bindir` in a new_dir sentinel |
      | `pg_upgrade --status   -D new` | `-D new` only | reads sentinel → `REPLAYING`/`QUARANTINED`/`COMMITTED` |
      | `pg_upgrade --commit   -D new` | `-D new` only | refuse unless QUARANTINED. Makes new live; **stamps old_dir as superseded** (rename `pg_control`→`pg_control.old`, reusing `disable_old_cluster()` `pg_upgrade.c:245`). Does NOT delete old. |
      | `pg_upgrade --rollback -D new` | `-D new` only | refuse unless QUARANTINED **and** no first write (C4). Discards new_dir; prints `pg_ctl -D <old> start` (old path from sentinel). |
      | `pg_upgrade --delete-old -d old` | `-d old` only | refuse unless old_dir carries the superseded stamp (i.e. a commit happened). Then `rm -rf old_dir`. |

      Rationale for the key rulings:
      - **commit/rollback/status self-gate on the new_dir sentinel**, so
        `--wal-log-upgrade` is NOT re-passed to them — the state produced by that
        flag is the gate.
      - **commit does not auto-delete old** (matches stock pg_upgrade, which prints
        `delete_old_cluster.sh` and lets the operator delete when confident). The
        post-commit window is exactly when old_dir is the last safety net.
      - **`--delete-old` targets the old dir directly (`-d old`)** and requires the
        superseded stamp. This is why **commit must stamp old_dir**: otherwise
        `--delete-old` (which only sees old_dir) can't tell a commit happened, and
        deleting a pre-commit old_dir while new is still quarantined would leave
        ZERO usable clusters.
      - The stamp is a **one-way mark written only at commit** (the C4 point of no
        return), never during the revertable window — so the "old_dir immutable
        while rollback is possible" invariant (DESIGN.md:127) holds. Side benefit:
        a stamped old_dir won't be started as a primary, preventing split-brain.
      - Edge case: if the operator moves old_dir between build and rollback, the
        sentinel path goes stale — rollback validates and errors clearly rather
        than assuming.
- [x] **Wipe policy. DECIDED: keep files.** new_dir keeps its reconstructed
      files on disk (startable immediately at commit; WAL replay is
      belt-and-suspenders). The `revert_wal_logged_disk_writes` wipe
      (TODO Q6) is retained **only as a test-only knob for now** — it still
      runs in the WAL-log test harnesses to prove reconstruction-from-WAL, but
      production revertable does NOT wipe. Gate the wipe behind a test flag;
      do not remove it yet.
- [x] **Quarantine flavor. DECIDED: `recovery_target_action = shutdown`** (dark,
      simplest) for the first cut — recovery replays to COMPLETE, `proc_exit(3)`s
      without end-of-recovery record / timeline fork, new_dir frozen and
      discardable; commit = restart without the target. `pause` (read-only
      smoke-test window, needs `hot_standby=on`, commit via
      `pg_wal_replay_resume()`) is deferred as a later opt-in, so the linchpin
      isn't coupled to hot_standby semantics before the basic gate works.
- [ ] **`PerformReplicaUpgradeIfNeeded` — distinct function or call-site
      variant?** Likely a thin reuse of `PerformWalUpgradeIfNeeded` + archive
      segment source + quarantine. Decide during A1/B2.

---

## Phase A1 — Quarantine arm (LINCHPIN, write first)

Goal: after the window replays to COMPLETE, hold new_dir non-serving instead of
finalizing to writable.

**PROGRESS — A1 implemented (hold mechanism = Approach 2, COMPLETE-handler stop):**
- [x] Added `DB_UPGRADE_QUARANTINED` to `DBState` (appended last, existing values
      unchanged) — `pg_control.h`.
- [x] Bumped `PG_CONTROL_VERSION` 1902 → 1903 (on-disk format contract).
- [x] `pg_controldata` prints "in pg_upgrade quarantine" — `pg_controldata.c`.
- [x] `xlog.c` startup switch handles the new state (was `default: FATAL invalid
      cluster state`) with a LOG + commit/rollback hint.
- [x] `SetControlFileUpgradeQuarantined()` + `ControlFileInUpgradeQuarantine()`
      helpers — `xlog.c` / `xlog.h`.
- [x] **The hold:** `XLOG_PG_UPGRADE_COMPLETE` redo handler, when reached under
      `in_upgrade_bootstrap`, sets `DB_UPGRADE_QUARANTINED` and `proc_exit(3)`
      before end-of-recovery finalization — `pgupgrade_wal.c`. (Approach 2, chosen
      over the recovery-target machinery; matches the `shutdown` flavor.)
- [x] **Re-hold on restart:** `PerformWalUpgradeIfNeeded()` FATALs if pg_control
      is already `DB_UPGRADE_QUARANTINED` (its checkpoint is still at CN, so the
      LSN "already applied?" test alone would wrongly re-replay) — `pgupgrade_wal.c`.
- [x] Made the quarantine state DURABLE with no new files: pg_upgrade stamps
      `DB_UPGRADE_QUARANTINED` at end of the wal-log run
      (`mark_new_cluster_quarantined()`), and the shutdown restartpoints in
      `xlog.c` preserve it (they no longer clobber it to
      `DB_SHUTDOWNED_IN_RECOVERY`).  Held cluster now FATALs immediately on
      start via the `ControlFileInUpgradeQuarantine()` guard.
- [x] Built A3 primary subcommands in `revertable.c`: `--status`, `--commit`
      (start→verify live→stop→stamp old), `--rollback` (rm new, old intact),
      `--delete-old` (gated on old_dir `pg_control.old` stamp).  Commit drives
      finalization via a `pg_upgrade_commit.signal` sentinel that
      `PerformWalUpgradeIfNeeded`/COMPLETE-handler consume.
- [x] **Primary lifecycle test PASSES end-to-end**:
      `wal_log_tests/run_revertable_test.sh` — upgrade→hold, rollback restores
      old, delete-old refused pre-commit, commit adopts new (data verified) +
      stamps old, delete-old removes old post-commit.  Run and green.

- [x] **Whole wal_log_tests suite updated + green (17/18).** Every existing
      test that starts the new cluster now inserts `pg_upgrade --commit` at the
      right point (after any pre-start WAL/disk-wipe assertions, before the
      start).  Results: 17 PASS; the only non-pass is `run_standby_xversion_test`,
      which needs `OLDBIN` (a separate older-major install) and cannot run in
      this env — it bails before any revertable code and fails identically on
      baseline.
- [x] **Commit precondition relaxed for the replay path.** `--commit` refuses
      only if the cluster is already `DB_IN_PRODUCTION`; it accepts both a
      pre-stamped `DB_UPGRADE_QUARANTINED` primary AND a fresh skeleton fed the
      WAL (state `DB_SHUTDOWNED`) — the standby/replay case, exercised and
      passing in `run_standby_handoff_e2e_test`.

**NEXT (not yet done): full standby Phase B / A4.** The handoff-e2e test proves
the standby-side *commit* works, but the real Phase-B two-directory layout
(keep old_dir', build new_dir' beside it; `PerformReplicaUpgradeIfNeeded`) and
set-wide commit orchestration are still unbuilt.

- [ ] Add a sibling of `ArmControlFileForUpgradeRecovery` (`xlog.c:4658`) that,
      for the revertable path, arms **archive recovery to a target** rather than
      crash-recovery-and-go-live. Concretely, instead of
      `state = DB_IN_PRODUCTION` (`xlog.c:4666`, which bypasses recovery targets),
      set up recovery so that:
      - recovery replays CN..COMPLETE, and
      - `recovery_target_lsn = complete_lsn`, `recovery_target_action = shutdown`
        (or `pause`), so it stops at COMPLETE without writing the end-of-recovery
        record / timeline fork (`xlog.c:6443-6488` must be skipped).
- [ ] Trigger is intrinsic, not a flag: `PerformWalUpgradeIfNeeded`
      (`pgupgrade_wal.c:365`) always arms the quarantine hold for a
      `--wal-log-upgrade` window. A durable `new_dir` sentinel (e.g.
      `upgrade.pending`, decided in Phase 0) records QUARANTINED so a restart
      re-holds instead of finalizing.
- [ ] Persist "quarantined at COMPLETE" so a restart re-enters the hold, not a
      re-replay. Reuse the existing control-file "already applied?" check
      (`GetControlFileCheckPointLSN() >= complete_lsn`, `pgupgrade_wal.c:434`) as
      the model.

**Gate A1:** a `--wal-log-upgrade` upgrade leaves new_dir reconstructed but NOT
accepting connections; `pg_controldata` shows it held at COMPLETE; a restart
stays held (no re-replay, no go-live). No flag needed to get this behavior.

---

## Phase B — Standby two-directory restructure (do first per decision)

Rework `run_standby_handoff_e2e_test.sh`'s `$STBY`/`$TGT` into a real old_dir'/
new_dir' pair.

- [ ] **B1.** Stop abandoning old_dir' (`$STBY`). After the HANDOFF halt, assert
      old_dir' is byte-intact and still a startable vN standby. Build new_dir' as
      a separate dir (today's `$TGT` step), never conflated with old_dir'.
- [ ] **B2.** Deliver the upgrade window into `new_dir'/pg_wal/` via
      `restore_command`/`RestoreArchivedFile` (`xlogarchive.c:54`) rather than a
      raw `cp`, matching a real standby's archive path.
- [ ] **B3.** Apply the A1 quarantine hold to new_dir' first-startup so it does
      not auto-serve; old_dir' stays frozen.
- [ ] **B4.** Standby rollback test: `rm -rf new_dir'`, start vN binary on
      old_dir', assert it re-attaches to the still-vN old primary and serves.

**Gate B:** standby ends with two dirs — old_dir' (startable vN standby) and
new_dir' (quarantined vN+1) — and a rollback restores the vN standby cleanly.

---

## Phase A2–A5 — Commit / switch-over

- [ ] **A2.** Reject `--link`/`--swap` with `--wal-log-upgrade` at parse
      (`option.c`, same spot/style as the new `--check --initdb` rejection):
      revertable-by-default needs old_dir intact ⇒ `--copy` only. No
      `--revertable` flag is added; the quarantine behavior is intrinsic.
- [ ] **A3.** `pg_upgrade --commit -D new` / `--rollback -D new` /
      `--delete-old -d old` (subcommands on pg_upgrade; args are `-D new` only,
      except delete which is `-d old` only).

      **commit — STRICT ORDER (the stamp must be last):**
      1. Require new_dir `DB_UPGRADE_QUARANTINED` (else refuse).
      2. Release the hold: restart new_dir *without* the recovery target → it
         finishes recovery, promotes, lands `DB_IN_PRODUCTION`.
      3. **Verify** new_dir is actually live (came up, `DB_IN_PRODUCTION`).
      4. **Only then** stamp old_dir superseded (see mechanism below).
      Rationale: if the stamp were written before new_dir is confirmed live and
      new_dir then failed to promote, `--delete-old` would destroy the only good
      cluster. The stamp is the LAST step, gated on verification (C4 point of no
      return).

      **How commit marks the old cluster (it is SHUT DOWN and is vN):**
      The old cluster has no running process to message, and it is an *older*
      version — so we canNOT put a new `DBState` in old_dir's pg_control (the vN
      binary that reads it doesn't know new state values; same boundary as C1).
      Instead, **rename `old_dir/global/pg_control` → `pg_control.old`**, reusing
      stock `disable_old_cluster()` (`pg_upgrade.c:245`). This one filesystem op:
      - is writable by the vN+1 commit with no format knowledge,
      - is readable by `--delete-old` ("`pg_control.old` present, `pg_control`
        absent" ⇒ a commit completed),
      - and makes the vN postmaster **refuse to start** (no `pg_control`) —
        split-brain prevention for free.
      WIRING NOTE: stock `disable_old_cluster()` runs mid-upgrade for
      `--link`/`--swap`; for revertable `--copy` it must run at **commit**, not
      during the upgrade run — old_dir must stay startable through the whole
      quarantine window. Optionally also write `old_dir/upgrade.superseded`
      (commit LSN + timestamp) as a human/audit marker; the rename alone
      suffices for the delete guard.

      **rollback:** require QUARANTINED (and no first write, C4); `rm -rf new`;
      old_dir still has `pg_control` (never stamped) → start vN binary on it.

      **--delete-old -d old:** refuse unless old_dir shows the superseded stamp
      (`pg_control.old` present, `pg_control` absent). The stamp is a transitive
      proof that replay reached COMPLETE AND commit verified new live (because
      the stamp is written only as commit's final step). Never reads new_dir.
      Then `rm -rf old`.
- [ ] **A4.** Wire the standby (B's new_dir') to the same quarantine + a set-wide
      commit path (quarantine everywhere → commit primary → standbys adopt
      new_dir' and re-point → allow writes). Document the operator contract for
      the parts outside PG (Q-R2).
- [ ] **A5.** Tests: commit adopts new_dir + retires old; rollback leaves old_dir
      byte-identical and serving; crash between COMPLETE and commit stays held
      (no auto-go-live); set-wide commit/rollback across a primary+standby pair.

**Gate A:** on both primary and standby, an explicit commit adopts new / an
explicit rollback restores old; neither auto-fires; the set moves together.

---

## Test scripts to add/modify (`src/bin/pg_upgrade/wal_log_tests/`)

- `run_revertable_rollback_test.sh` — primary: `--revertable` → quarantine →
  rollback → old_dir byte-identical + serving. (A5)
- `run_revertable_commit_test.sh` — primary: `--revertable` → quarantine →
  commit → new_dir writable, old retired. (A5)
- modify `run_standby_handoff_e2e_test.sh` — old_dir'/new_dir' pair + standby
  rollback assertion. (B1, B4)
- `run_revertable_setwide_test.sh` — primary+standby commit and rollback move
  together. (A4/A5)

## Open questions (tracked in design doc)

- Q-R2 set-wide commit atomicity (orchestration contract, partly outside PG).
- Q3 segment sourcing detail for new_dir' (archive vs pre-staged) — B2.
- Q6 wipe removal / test-only gate — Phase 0.
- Does `PerformReplicaUpgradeIfNeeded` need to be its own function — A1/B2.

## Non-goals (settled, do not attempt)

- Live-streaming the upgrade window to a standby (C1 version page magic, C2
  segment discontiguity). Delivery is file-based.
- Coordinating two independent end-of-recovery timeline switches (C3). If a
  timeline label is used, it is a label only.
- Rolling back after new_dir has served a write (C4).
