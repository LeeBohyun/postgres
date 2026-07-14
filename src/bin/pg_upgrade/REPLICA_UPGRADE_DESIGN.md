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

## Post-replay history divergence (measured) and how to continue replication

A standby that converges by *standalone crash recovery* of the upgrade WAL
(deliver the segments, remove `standby.signal`, start) reaches the correct data
but **forks the primary's WAL history** — so ordinary streaming cannot resume.
Measured with `run_standby_replication_test.sh` + `pg_waldump` (both nodes on
timeline 1, having replayed byte-identical upgrade WAL through COMPLETE):

```
shared:   0/0A000028  CHECKPOINT_SHUTDOWN   (end-of-upgrade-replay checkpoint,
                                             identical on both nodes)
P' only:  0/0A0000A8  PARAMETER_CHANGE      (primary's postgresql.conf gained
                                             max_wal_senders etc.)
          ...FPI_FOR_HINT burst...          (both nodes, but +54B offset on P')
P' ends:  0/0A00E928  CHECKPOINT_SHUTDOWN   (P' stays in segment 0A)
S  only:  0/0A0093B8  SWITCH                (standby forced a segment switch)
S  ends:  0/0B000028  CHECKPOINT_SHUTDOWN   (standby lands in segment 0B, AHEAD)
```

The fork is NOT in the replayed upgrade window (that is identical); it is in the
**post-replay finalization**: each node independently writes its own
end-of-recovery checkpoint, and the primary additionally writes a
PARAMETER_CHANGE from its differing config. The standby ends up *ahead* of the
primary on the same timeline, so `START_REPLICATION` fails with "requested
starting point ... is ahead of the WAL flush position".

The root cause is that the test finalizes the standby into its OWN primary: it
delivers the upgrade WAL, removes `standby.signal`, and starts — which tells the
server "you are standalone now, finish crash recovery and come up as a primary."
Finishing recovery is what writes the divergent end-of-recovery checkpoint (and
the primary independently writes PARAMETER_CHANGE from its config). The standby
briefly *became its own primary* on a parallel branch of timeline 1.

The fix is NOT to repair the divergence after the fact (e.g. `pg_rewind` back to
the shared checkpoint) — that only pays to undo damage we inflicted. The fix is
to never create the divergence: the standby must **stay a standby** through the
whole upgrade and never finalize its own history. A server in archive/standby
recovery does not write end-of-recovery checkpoints; it only mirrors the
primary's checkpoints as restartpoints. So a standby that keeps `standby.signal`
and remains in recovery would replay the upgrade window and then simply continue
following the primary's stream — receiving the primary's post-COMPLETE
PARAMETER_CHANGE and end-of-recovery checkpoint as ordinary replayed records
rather than inventing its own. No divergence, nothing to rewind.

The obstacle to doing that today is the safety guard in `pg_upgrade_redo()`:

```c
if (!in_upgrade_bootstrap)
    ereport(FATAL, "pg_upgrade WAL encountered during replay ...");
```

A standby replaying in ordinary standby mode reaches START without
`in_upgrade_bootstrap` armed and FATALs — deliberately, because applying the
RELFILE full-page images (which carry OLD page LSNs, below the replay point) on
a *serving* standby would violate minRecoveryPoint / LSN-monotonicity. So the
constructive path (`PerformReplicaUpgradeIfNeeded`, Open Q1) must:

  1. arm a *sanctioned replica bootstrap* so the standby applies the window in
     NON-serving mode (CN-anchored, hot-standby suppressed — exactly like the
     primary bootstrap; the FPI-LSN-safety section below is why this is
     mandatory), and
  2. keep recovery in standby mode ACROSS the window rather than ending it, so
     the standby never writes its own end-of-recovery checkpoint and stays on
     the primary's history.

The CN-derivation half already exists (PerformWalUpgradeIfNeeded derives CN from
the WAL). What remains is arming that bootstrap from the standby side and holding
standby mode through the window.

## FPI-LSN safety (must hold for the replica path)

RELFILE redo keeps each page's **old-cluster LSN verbatim** (to byte-match a
normal pg_upgrade). Writing pages stamped with an LSN *below* the current replay
point is safe in the primary's CN-anchored, non-serving crash recovery, but
would violate `minRecoveryPoint` / LSN-monotonicity invariants if done on a
LIVE standby. The replica path MUST therefore replay the upgrade window in the
same non-serving recovery mode (paused, not accepting queries), exactly like the
primary bootstrap. This is a hard requirement, not an optimization.

## Connection blocking during replay (IMPLEMENTED)

No client may observe a half-upgraded cluster (new catalogs partially applied)
while the upgrade WAL is replaying. Two layers enforce this:

1. **Crash-recovery consistency gate (stock).** The primary /
   spawn-fresh-cluster replay is crash recovery (`DB_IN_PRODUCTION`, no
   `standby.signal`). The postmaster rejects every connection until a consistent
   state is reached, which for crash recovery is only after redo completes. So
   the whole window is covered with no extra code. Verified: racing 400
   connection attempts against the replay yields rejections during replay and
   zero partial-state reads (`run_connblock_test.sh`).

2. **Hot-standby suppression guard (added for the replica path).** A streaming
   standby doing *archive* recovery with `hot_standby=on` could otherwise flip
   hot standby active at its consistency point — mid-upgrade. The
   `pgUpgradeReplayInProgress` flag (xlogrecovery.c) is set by `pg_upgrade_redo()`
   at `XLOG_PG_UPGRADE_START` and cleared at `XLOG_PG_UPGRADE_COMPLETE`;
   `CheckRecoveryConsistency()` will not activate hot standby while it is set. So
   even on a hot-standby replica, connections are refused until the whole
   START..COMPLETE window has replayed.

   (Layer 2 is in place and does not regress the crash-recovery path, but its
   unique effect — blocking hot-standby reads during archive recovery — can only
   be exercised directly once the full streaming-standby convergence path below
   is built.)

## Open questions / risks

1. **CN/REDO anchor on the standby.** The primary now derives CN *in-process* at
   first startup: `PerformWalUpgradeIfNeeded()` scans `pg_wal/`, takes CN to be
   the last checkpoint record preceding `XLOG_PG_UPGRADE_START`, and arms
   pg_control via `ArmControlFileForUpgradeRecovery()` (the former offline
   `pg_resetwal --upgrade-recovery` step is gone). This same derivation is what
   the standby needs: the CN checkpoint record is in the WAL stream it receives,
   so it can recover the anchor in-band with no copied pg_control and no flag.

   **Post-replay history divergence — root cause and the existing fix (traced).**
   The measured fork (standby ends ~1 segment AHEAD of the primary on the same
   timeline, so streaming can't resume) was SELF-INFLICTED by the test: it did
   `rm standby.signal` before restarting, so `ArchiveRecoveryRequested` was
   false and StartupXLOG wrote a same-timeline `CHECKPOINT_END_OF_RECOVERY`
   (xlog.c:6855). PostgreSQL already avoids this for a real standby: when
   recovery ran with `standby.signal`/`recovery.signal`
   (`ArchiveRecoveryRequested = true`), StartupXLOG UNCONDITIONALLY switches to a
   new timeline at end-of-recovery (`newTLI = findNewestTimeLine + 1`,
   xlog.c:6414) and writes a timeline-history file that other standbys follow.
   So the correct delivery is: keep the standby in ARCHIVE recovery
   (recovery.signal), let it apply the window, and let the built-in
   end-of-recovery TIMELINE SWITCH happen -- no same-timeline fork, no sentinel,
   using only existing replication machinery. Reconciling primary and upgraded
   standby is then the standard "follow a timeline switch" flow.

   **Why the standby path must be a SIBLING of the primary bootstrap, not the
   same code.** The two want DIFFERENT end-of-recovery behavior from the SAME
   START..COMPLETE WAL:
     - Primary bootstrap (`PerformWalUpgradeIfNeeded`): crash recovery from CN on
       the SAME timeline, into a wiped dir (the rebuild-from-empty property).
     - Standby (`PerformReplicaUpgradeIfNeeded`, to build): ARCHIVE recovery that
       applies the window and finalizes via the timeline SWITCH above.
   So the standby entry point must arm the redo guard (below) but leave normal
   archive-recovery finalization in place instead of the primary's CN-anchored
   crash-recovery finalization.

   **Status: a standalone standby can converge (file-delivered), but streaming
   from the upgraded primary is NOT yet closed — see the rejected approach
   below.**  Delivering the upgrade WAL to a standby with `recovery.signal`
   present (archive recovery) makes it apply the window in-band from CN and come
   up consistent; verified by `run_standby_tli_test.sh` (which also confirms a
   subsequent restart does not re-arm).  In that archive-recovery path the server
   happens to take PostgreSQL's normal end-of-recovery timeline switch — which is
   fine for a standalone converge, but is exactly what does NOT coordinate with
   the primary for streaming (below).

   One fix here is independently correct and was KEPT: "already applied?" is
   decided from the CONTROL FILE (`GetControlFileCheckPointLSN() >= COMPLETE's
   LSN`), not by scanning for a post-COMPLETE checkpoint (which lands on a
   timeline the TLI-1 scan cannot read); and the pg_wal/ directory scan bounds
   itself to TLI-1 segments so coexisting higher-TLI segments do not push the
   reader off the end.  These make restart-after-upgrade robust regardless of the
   timeline question.

   **What remains (true live-streaming path) + why the timeline-switch approach
   was REJECTED.** The file-delivered case above keeps a standby consistent on
   its own after upgrade, but it does NOT let the standby then STREAM from the
   upgraded primary.  We tried to close that by making the primary ALSO switch
   timelines at end-of-upgrade (so both ends land on TLI 2 and streaming
   resumes).  A full primary+standby streaming test disproved it:

     - primary bootstrap finalized -> TLI 2 (history: 1 @ 0/0A000000)
     - standby archive-recovery finalized -> TLI 3 (it saw the primary's TLI-2
       history in the archive, so findNewestTimeLine()+1 = 3), at a DIFFERENT
       switch LSN (0/05000000).
     - result: `FATAL: highest timeline 2 of the primary is behind recovery
       timeline 3` — the standby forked onto its own timeline and could not
       stream.

   Root cause: two INDEPENDENT end-of-recovery timeline switches cannot
   coordinate (each does `findNewestTimeLine()+1`, so they pick different numbers
   at different LSNs).  A standby must FOLLOW the primary's single switch, not
   perform its own.  The primary-timeline-switch change was therefore reverted.

   Intended model: keep primary and standby on the SAME timeline and have the
   standby FOLLOW the primary's WAL, rather than each node independently bumping
   the timeline.  The real remaining work is the streaming-follow path: a standby
   applies the primary's upgrade WAL AS IT STREAMS (same timeline), which needs
   the `pg_upgrade_redo()` START guard turned into a sanctioned apply for a
   streaming standby, WITH the standby blocked from serving any connection while
   the START..COMPLETE window replays (a stricter version of the existing
   `pgUpgradeReplayInProgress` gate, which today only prevents hot-standby
   ACTIVATION, not an already-active standby).

   **THE ACTUAL BLOCKER (measured): WAL DISCONTIGUITY, not timelines.** A live
   streaming standby cannot even reach the upgrade window.  Measured with a real
   streaming setup:
     - old cluster (and a caught-up standby) end at WAL segment 3
       (`Latest checkpoint's REDO WAL file: 0000000100000000000000 03`).
     - the upgraded new cluster's WAL -- including the CN checkpoint and the
       whole START..COMPLETE window -- BEGINS at segment 8; segments 4-7 do not
       exist.
     - the standby, caught up at segment 3, requests segment 3/4 from the
       upgraded primary and gets `requested WAL segment ...03 has already been
       removed` -- there is no contiguous path from where it is to where the
       upgrade WAL lives.

   Nothing was recycled and no checkpoint discarded the window (it is intact in
   seg 8).  The gap is created by pg_upgrade's stock "Resetting WAL archives"
   step (`pg_resetwal -l 00000001<nextxlogfile>`, pg_upgrade.c) PLUS the burst
   server's own WAL generation (counter-transplant resets, CHECKPOINT, the FPI
   burst), which together position the new cluster's WAL at a HIGHER segment than
   the old cluster's end, leaving a hole (4-7 here).  Physical streaming requires
   a contiguous (TLI, LSN) sequence; the hole makes streaming impossible even
   though the window is present.

   So the true fix for streaming-follow is to make the upgrade WAL CONTIGUOUS
   with the old cluster's WAL end -- i.e. the new cluster's WAL (CN + window)
   must start exactly at the old cluster's next segment, with no skipped
   segments through the burst -- so a caught-up standby streams seg N -> N+1
   straight into the window and ingests it like any other WAL.  The alternative
   is out-of-band delivery of the gap (the file-delivered model, which works but
   is not pure streaming).  Neither needs a timeline bump.

   **PARTIAL FIX LANDED: pg_resetwal --control-only (see commit).** The
   counter-transplant resets no longer march the WAL a segment each; measured on
   a small cluster the gap dropped from ~5 segments to 0 (CN lands immediately
   after the old cluster's WAL end).  This is committed and the primary path +
   file-delivered standby path both still pass.

   **STILL NOT sufficient for pure live streaming (measured).** Two residual
   obstacles remain, both about the exact segment boundary:
     1. The old cluster's LAST segment is usually PARTIAL (e.g. it ends at
        0/0300B720, mid-segment 3).  The `-l` reset positions the new WAL at the
        old cluster's NEXT segment (4), so the old cluster's partial segment 3 is
        not carried into the new cluster.  A caught-up standby streamed to
        0/03000000 then asks the new primary for segment 3 and gets "already
        removed" -- the new cluster starts at 4.  The new WAL must instead
        CONTINUE the old cluster's last partial segment (start at the old end
        LSN, same segment), not the next segment.
     2. Residual burst drift: even with --control-only, the CHECKPOINT (CN) and
        the burst server restart still advance the position a couple of segments
        past the -l point (observed CN at segment 6 when the new WAL began at 4).
        For strict contiguity the CN/window must begin at the old end LSN with no
        intervening advance.
   Until both are addressed, pure live streaming of the window is not possible;
   the file-delivered model (copy the upgrade segments to the standby, in-band
   CN derivation) remains the working path and is what the tests exercise.

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

6. **Remove `revert_wal_logged_disk_writes` before the final patch submission.**
   Today, after emitting the upgrade WAL, pg_upgrade wipes the on-disk data
   image (relation files, SLRU segments, and the directory tree) so first
   startup is *forced* to reconstruct the entire cluster purely from WAL replay.
   This is a **testing device**, not a correctness requirement: it proves every
   byte is captured in WAL (if any image were missing the cluster would come
   back wrong or fail to start) and it exercises the DIRSKEL redo path. In the
   real feature the data is already on disk from the normal upgrade, and replay
   would simply overwrite it — so the wipe is wasted work and, for `--link`
   mode, it deletes files that share inodes with the old cluster. Before the
   patch is proposed upstream, `revert_wal_logged_disk_writes` (and the manifest
   plumbing that feeds it) should be removed or gated behind a test-only knob, so
   production `--wal-log-upgrade` leaves the reconstructed files on disk and
   replay is idempotent over them rather than rebuilding from an empty tree.
   NOTE: keep it while the end-to-end equivalence tests
   (`run_e2e_equivalence_test.sh`, `run_compare_test.sh`) still depend on the
   wiped-then-replayed cluster to prove WAL completeness.

7. **User tablespaces are NOT handled — silent data loss (CONFIRMED BUG).**
   `--wal-log-upgrade` ignores `pg_tblspc/` in three places, so any relation in a
   user-defined tablespace is not WAL-logged and would be lost on a real
   fresh-target/standby replay. Demonstrated by `run_tablespace_test.sh` (which
   currently FAILS). The three gaps:

     (a) **Capture** — `pg_write_upgrade_relfile_data()` (xlogfuncs.c) walks only
         `global/` and `base/<dboid>/`; it never descends
         `pg_tblspc/<spcoid>/PG_*/<dboid>/`, so no RELFILE FPI is emitted for
         tablespace relations. Fix: after base/, iterate `pg_tblspc/`, resolve
         each `<spcoid>` symlink, and `capture_dir_files(..., tsoid=<spcoid>,
         dboid=<dboid>)` for every db subdir under its version directory. RELFILE
         redo already reconstructs via `rlocator.spcOid`, so once the FPI carries
         the right spcoid, smgr places it correctly — PROVIDED the symlink exists
         at replay (see (b)).
     (b) **DIRSKEL** — FIXED. `collect_upgrade_dirs()` (xlog.c) now captures
         symlinks: the XLOG_UPGRADE_DIRSKEL record carries an (linkpath, target)
         section after the directory list, and `pg_upgrade_redo()` recreates the
         target directory + symlink before the tablespace RELFILE images replay.
         Verified by `run_tblspc_symlink_test.sh`: the DIRSKEL record for an
         external-location tablespace reports "symlinks 1".
         LIMITATION: the replay-recreate branch cannot be driven end-to-end on a
         SAME-BUILD test, because pg_upgrade refuses "same catalog version +
         tablespaces" for an absolute external path (tablespace.c), so an
         external tablespace only reaches the wal-log flow in a real CROSS-version
         upgrade. The capture is proven; the symlink-redo branch is covered by
         code but not yet by an end-to-end same-build test. (in-place tablespaces
         have no symlink, so they don't exercise this branch.)
     (c) **Wipe** — `revert_wal_logged_disk_writes()` (pg_upgrade.c) skips
         `pg_tblspc/`, leaving the data on disk. This is why a SAME-NODE upgrade
         currently appears to work: the table is read from the un-wiped files,
         not reconstructed from WAL. The disk-wipe assertion in the test exposes
         this. When (a)+(b) are fixed, (c) must also wipe pg_tblspc/ so the test
         proves real WAL recovery. (If Q6's wipe removal lands first, (c) becomes
         moot for production but the test still needs to wipe to stay honest.)

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

Already done (independent of the streaming-standby path above):
  - sysid preservation (new cluster carries the old cluster's identifier)
  - connection blocking during replay (pgUpgradeReplayInProgress guard +
    crash-recovery consistency gate) — see "Connection blocking during replay"
  - end-to-end WAL-replay-vs-vanilla equivalence test (run_e2e_equivalence_test.sh)
