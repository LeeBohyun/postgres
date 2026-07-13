# Replica (Physical Standby) Upgrade via WAL — Design

> STATUS: design only. No code beyond the *safety guard* described in
> "Current state" exists yet. This doc is the architecture we agreed on before
> building the constructive half (pause-mark + the dedicated replica-upgrade
> startup path).

## Goal

Let a physical standby of the OLD cluster upgrade itself to the NEW major
version **purely by replaying the primary's `--wal-log-upgrade` WAL stream** —
no separate `pg_upgrade` run, no file transfer, no `initdb` on the standby.

This is the last bullet of the PROGRESS.md goal ("Standbys can eventually follow
an upgrade purely via WAL stream").

## Why the primary's crash-recovery path does NOT work for a standby

The primary applies its own upgrade WAL through `PerformWalUpgradeIfNeeded()`,
which is a **primary-only bootstrap**:

- It keys off a `pg_wal_upgrade/` directory that only exists because the
  primary's `pg_upgrade` renamed `pg_wal/` → `pg_wal_upgrade/` at the end of the
  upgrade.
- It copies those segments into a fresh `pg_wal/` and lets `StartupXLOG()`
  crash-recover from CN (the end-of-upgrade checkpoint) through COMPLETE, into a
  wiped, non-serving data directory.

A standby has NONE of that: it has a WAL **stream/archive**, not a pre-staged
`pg_wal_upgrade/`; it is a live, serving replica; and — the crux — it is running
the **OLD major version's `postgres` binary**.

## The hard constraint: the binary must change at the boundary

The upgrade records (`RM_PG_UPGRADE_ID`: DIRSKEL / RELFILE / SLRU / RAWFILE) are
emitted by, and only meaningful to, the NEW version. After COMPLETE the cluster
IS a new-major-version cluster (new catalog layout, possibly new page/WAL
semantics). So the OLD binary cannot simply replay past `XLOG_PG_UPGRADE_START`
and keep running.

Therefore the "restart" at the boundary is not a plain `pg_ctl restart`; it is:

    stop OLD binary  →  install/point to NEW binary  →  start NEW binary

and the NEW binary resumes exactly where the OLD one paused.

## Proposed flow

```
1. Standby (OLD binary) streams/replays old-cluster WAL as usual.

2. Replay reaches XLOG_PG_UPGRADE_START:
      - STOP replay at that record (do NOT apply it).
      - Check for a CONFIRMED XLOG_PG_UPGRADE_COMPLETE in the WAL the standby
        can see (see "Atomicity" below). Do NOT proceed to swap/apply until the
        whole START..COMPLETE window is present:
          * COMPLETE present  → safe to pause, mark, and apply the window.
          * COMPLETE absent    → either replication lag (wait) or a crashed
                                  primary (never apply) — block at the boundary,
                                  stay the OLD cluster.
      - Once COMPLETE is confirmed: write a sentinel marker recording "paused at
        pg_upgrade boundary" plus the CN/REDO anchor.
      - Shut down cleanly. Log:
          "reached pg_upgrade point; install the new-version binaries and
           restart this standby to apply the upgrade."

3. Operator / orchestration swaps in the NEW-version binary.

4. NEW binary starts, detects the sentinel marker, and enters the
   REPLICA-UPGRADE path (NOT PerformWalUpgradeIfNeeded):
      - Anchor recovery at CN.
      - Source the upgrade segments from the archive / stream (not from a
        pre-existing pg_wal_upgrade/).
      - Replay DIRSKEL → RELFILE/SLRU/RAWFILE → COMPLETE, in the same
        non-serving recovery mode the primary uses (so the FPI-LSN trick is
        safe — see below).
      - Clear the marker. Come up as an upgraded standby, resume streaming.
```

Your two-line summary — "hit START → stop; on restart → apply" — is exactly
this, with step 2 being a controlled *stop-and-mark* and step 4 a *new binary*
entering a dedicated path (not the same process looping).

## Atomicity: START is not the trigger — a confirmed COMPLETE is

A subtlety in step 2 above: on a streaming standby, replay reaches
`XLOG_PG_UPGRADE_START` **before** `XLOG_PG_UPGRADE_COMPLETE` has necessarily
arrived. Acting on START alone would be wrong. The window must be applied
ATOMICALLY — all of START..COMPLETE, or none of it — mirroring the primary's
existing two-pass logic in `PerformWalUpgradeIfNeeded()` (Pass 1 parses for a
genuine COMPLETE *before touching anything*; no COMPLETE → FATAL, old cluster
left intact).

So the standby rule is: **the trigger to pause+swap+apply is a CONFIRMED
COMPLETE, not START.** Three cases when replay reaches START:

| State of the upgrade window in available WAL | Standby action |
|---|---|
| START **and** COMPLETE both present | window is whole → pause, swap binary, apply START..COMPLETE atomically |
| START present, COMPLETE **not yet** (replication lag) | WAIT — do not cross the boundary until the full window is visible |
| START present, COMPLETE **never** (primary crashed mid-upgrade) | upgrade is VOID → stay the OLD cluster, exactly as the primary's old cluster stays intact; never apply a partial window |

The last two look identical at the instant replay hits START (COMPLETE simply
absent). Distinguishing "not yet" from "never" is a timing/liveness question:
the standby cannot apply until it *has* COMPLETE, and if the primary aborted, it
never will — so the safe default is "block at the boundary, apply only when the
whole window is in hand." A partial application must be impossible.

Practically this means the pause point and the marker must NOT commit the
standby to the swap until COMPLETE is confirmed present in the WAL the standby
can see (streamed or archived). The atomic unit is the whole window; START is
merely the "stop and check for COMPLETE" signal, just like on the primary.

## wal_level: NOT an obstacle (both moments verified `replica`)

There are TWO distinct wal_level moments, and the in-tree "minimal" comments
conflate them — but both check out as `replica`:

1. **Runtime (while the upgrade server runs)** — verified `wal_level=replica`.
   Reproduced by starting the server exactly as pg_upgrade does (`-b` + the
   wal-log opts): it reports `replica`. So the `--wal-log-upgrade` records
   (DIRSKEL/RELFILE/SLRU/RAWFILE) ARE generated at replica level. Good.

2. **Persisted (what pg_control shows afterward)** — verified `replica`.
   Checked `pg_controldata` on two completed `--wal-log-upgrade` clusters (the
   dirskel manual run and the mxact test): both show `wal_level setting:
   replica`. So despite `--wal-log-upgrade` SKIPPING
   `issue_warnings_and_set_wal_level()` (pg_upgrade.c:470-471, PROGRESS.md:54),
   the final pg_control does NOT end up minimal. (Likely because the upgrade
   server itself ran at replica and its checkpoint/pg_control writes reflect
   that; the throwaway `pg_resetwal -o` value does not survive.)

### The invariant to keep (why this mattered)

StartupXLOG refuses archive recovery if pg_control says minimal (xlog.c:5431):

```c
if (ArchiveRecoveryRequested && ControlFile->wal_level == WAL_LEVEL_MINIMAL)
    ereport(FATAL, "WAL was generated with \"wal_level=minimal\", cannot continue recovering");
```

Today the persisted value is `replica`, so a standby doing archive/stream
recovery will NOT hit this. The replica path just has to PRESERVE that — any
future change to the counter-transplant / pg_resetwal sequence must keep
pg_control at `replica` (add a regression check:
`pg_controldata | grep wal_level` must be `replica` after upgrade).

NOTE / cleanup: the "runs at wal_level=minimal" comments (pg_upgrade.c ~line 187,
IMPLEMENTATION ~line 144, PROGRESS.md line 36) are STALE/misleading — both the
runtime level AND the persisted pg_control value are `replica`, as verified and
as other parts of PROGRESS.md (126/190/319) already state. They should be fixed.

## Where this lives: a dedicated "replica upgrade" path

This must be SEPARATE from `PerformWalUpgradeIfNeeded()`:

- Different trigger: sentinel marker, not `pg_wal_upgrade/`.
- Different segment source: archive/stream, not a renamed local dir.
- Different lifecycle: survives a binary swap.

Working name: `PerformReplicaUpgradeIfNeeded()` (or fold both under one
`pg_upgrade_recovery` module with two entry points sharing the CN-anchored
replay core). The redo handlers (`pg_upgrade_redo`) are shared — only the
bootstrap/entry differs.

## The safety guard that ALREADY exists (current state)

In `pg_upgrade_redo()`'s `XLOG_PG_UPGRADE_START` branch:

```c
if (!in_upgrade_bootstrap)
    ereport(FATAL,
            (errmsg("pg_upgrade WAL encountered during replay"),
             errhint("Restart this server to apply the pg_upgrade; the "
                     "upgrade cannot be replayed on a running standby.")));
```

`in_upgrade_bootstrap` is set true only by `PerformWalUpgradeIfNeeded()` (the
primary bootstrap). So today:

- On the PRIMARY's crash recovery: flag armed → images apply. (Tested, passes.)
- Anywhere else (a standby streaming the record live): flag NOT armed → FATAL.

This is the *safety-critical half*: a standby is prevented from unsafely applying
the upgrade images live. It is a recovery-process FATAL (which brings the server
down), NOT an "immediate shutdown" command, and it does not by itself set up the
correct re-entry.

### Known gap in the current guard

As written, a bare restart of a real standby would LOOP: FATAL at START →
restart → no bootstrap armed (no `pg_wal_upgrade/`, no replica path yet) → FATAL
again. The constructive half below is what turns the FATAL into a clean
pause-and-resume. Until then the guard only *protects*; it does not *complete*.

## FPI-LSN safety (must hold for the replica path)

RELFILE redo keeps each page's **old-cluster LSN verbatim** (to byte-match a
normal pg_upgrade). Writing pages stamped with an LSN *below* the current replay
point is safe in the primary's CN-anchored, non-serving crash recovery, but
would violate `minRecoveryPoint` / LSN-monotonicity invariants if done on a
LIVE standby. The replica path MUST therefore replay the upgrade window in the
same non-serving recovery mode (paused, not accepting queries), exactly like the
primary bootstrap. This is a hard requirement, not an optimization.

## Open questions / risks

1. **CN/REDO anchor on the standby.** The primary captures CN via
   `pg_control_checkpoint()` and arms it with `pg_resetwal --upgrade-recovery`.
   The standby must obtain the same anchor. Likely from the checkpoint record in
   the stream itself (the CN checkpoint is in the WAL the standby receives), but
   the mechanism needs to be worked out — this is the biggest unknown.

2. **Orphaned old-cluster files.** The standby still has the OLD cluster's files
   on disk (old system-catalog relfilenodes). The upgrade WAL *creates* the new
   files but nothing *deletes* the old ones (the primary deletes them locally in
   `revert_wal_logged_disk_writes`, which the standby never runs). Likely benign
   (the relation map points at the new relfilenodes; the old files are
   unreferenced garbage) but should be confirmed, and possibly cleaned up.

3. **Segment sourcing.** Where the replica-upgrade path reads the upgrade
   segments from (restore_command / archive vs. what already streamed into
   pg_wal/) needs a concrete answer.

4. **Binary-swap orchestration** is partly outside Postgres. Postgres can pause
   cleanly, mark, and resume into the right path; *who* installs the new binary
   and restarts is an operator/tooling contract to document, not code.

5. **Marker format.** Sentinel file (e.g. `standby.upgrade`) vs. a new pg_control
   state (`DB_UPGRADE_PENDING`). Sentinel file is least-coupled and survives the
   binary swap; pg_control state is cleaner but touches on-disk layout and both
   binaries must agree. Leaning sentinel file.

## Build order (when we proceed)

0. **Decide delivery mechanism:** live streaming vs. archive/file catch-up. NOT
   blocked by wal_level (it is already `replica`); this is purely about how the
   replica receives the upgrade segments. Archive-based catch-up likely fits the
   "feed this to a replica" framing best.
1. Turn the guard's FATAL into **pause + confirm-COMPLETE + mark**: on START,
   verify the whole window is present (atomicity), only then write the sentinel
   and stop cleanly with the operator message. If COMPLETE is absent, block
   without committing to the swap.
2. Add `PerformReplicaUpgradeIfNeeded()` (new binary): detect marker → anchor at
   CN → replay window → clear marker.
3. Resolve segment sourcing + the CN anchor mechanism (Open Q1/Q3).
4. A real standby replay test, including the **crash-mid-upgrade** case:
   verify a standby that sees START but never COMPLETE stays the OLD cluster
   (the standby analog of the existing run_crash_test.sh).
5. Address orphaned-old-file cleanup (Open Q2) if confirmed necessary.
