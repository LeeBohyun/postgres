# pg_upgrade WAL Atomicity — Progress Notes

> UPDATE (2026-07-12): the design below evolved into a "skip the disk writes"
> model.  ALL of pg_upgrade's own WAL is now generated as one burst at the very
> END of the upgrade (after a CHECKPOINT), at the coarsest granularity that fits
> a 1020MB WAL record.  The user relation files and the copied SLRU segments are
> then reverted on disk (unlink / truncate) so that first startup MUST rebuild
> them from WAL — which is what verifies recovery.  SLRU redo is now
> AUTHORITATIVE (SlruUpgradeRestoreSegment), relation-file segments larger than
> the 1020MB limit are split into chunked records (blockoff), and the upgrade
> server pins WAL (wal_keep_size/max_wal_size) so no segment from C0 through
> COMPLETE is recycled.  System catalogs are NOT skipped (they back the
> CREATE DATABASE FILE_COPY records replayed from C0).  See the IMPLEMENTATION
> file's "--wal-log-upgrade" section for the current, authoritative description.
> Verified: all transfer modes, >1GB tables (chunking), real multixacts,
> multi-segment pg_xact (>1M XIDs), empty clusters, and old-cluster survival.

## Goal

Make pg_upgrade atomic and WAL-replayable so that:
- A crash mid-upgrade leaves the new cluster in a clean, recoverable state
- The entire upgrade window is captured in WAL and replayable from scratch
- Standbys can eventually follow an upgrade purely via WAL stream (no separate file transfer)

---

## FINAL WORKING DESIGN (verified end-to-end 2026-07-10)

Status: `pg_upgrade --wal-log-upgrade --initdb` completes; the new cluster
crash-recovers from the initdb checkpoint (C0) through PG_UPGRADE_COMPLETE and
comes up with all user data AND working indexes.  Crash-mid-upgrade (START but
no COMPLETE) FATALs with a re-run hint and leaves the old cluster intact.

### pg_upgrade side (src/bin/pg_upgrade/pg_upgrade.c)

The restore-phase server runs at **wal_level=replica** (initdb's default; NOT
lowered to minimal) with `full_page_writes=on`, `fsync=on`.  (An earlier draft
of this file said "minimal" here — that was wrong; verified via pg_controldata
that both the runtime level and the persisted pg_control value are `replica`,
consistent with the "Design decisions" section below.)  After
`create_new_objects()` and `transfer_all_new_tablespaces()`, while the server is
still running:

1. `pg_write_pg_upgrade_start(old, new)`   — writes PG_VERSION marker
2. `pg_write_upgrade_relfile_data()`        — FPI of every transferred user relfile
3. `pg_write_upgrade_control_counters()`    — XID/OID/multixact transplant
4. `pg_write_pg_upgrade_complete(old, new)` — terminal marker
5. `pg_switch_wal()`                        — seal the segment
6. `stop_postmaster_immediate()`            — SIGQUIT, no checkpoint, WAL intact
7. remove `pg_upgrade_transferred_files` manifest

Then the standard tail runs (`pg_resetwal --control-only -o`, etc.), and FINALLY
(after all pg_resetwal calls, which require pg_wal/ to exist):

8. `pg_resetwal --upgrade-recovery=C0` — set checkPoint=C0, state=DB_IN_PRODUCTION
9. rename `pg_wal/` → `pg_wal_upgrade/`  — last filesystem op; no pg_resetwal after

`issue_warnings_and_set_wal_level()` is SKIPPED for --wal-log-upgrade (it would
restart the server and recycle the upgrade WAL).

### transferred-files manifest (src/bin/pg_upgrade/relfilenumber.c)

`transfer_relfile()` appends `<db_oid> <relfilenumber> <forknum> <segno>` to
`$PGDATA/pg_upgrade_transferred_files` for each transferred file.  The WAL emit
`pg_write_upgrade_relfile_data()` reads ONLY this manifest — so RELFILE_DATA
covers exactly the user relations pg_upgrade physically transferred, NEVER system
catalogs (which pg_restore rebuilds in the new-version format).

**--swap mode** does NOT call transfer_relfile() — it moves the whole old DB
directory into place and swaps only catalog files (do_swap/swap_catalog_files).
So swap_catalog_files() has its own manifest-recording loop: after the swap,
new_db_dir holds the old cluster's user relation files (every file whose
relfilenumber IS in `maps`); it records each such file (all forks/segments) into
the same manifest.  Without this, --swap replays user tables as 0 rows.

### Transfer-mode test matrix (all pass, verified 2026-07-10)

| Mode                | Result |
|---------------------|--------|
| `--copy`            | DATA MATCH |
| `--copy-file-range` | DATA MATCH |
| `--link`            | DATA MATCH (new cluster; old cluster NOT expected to survive — standard --link semantics, shares inodes) |
| `--swap`            | DATA MATCH (after adding manifest hook to swap_catalog_files) |
| `--clone`           | untested here — `/tmp` overlayfs returns "Operation not supported" at the file-transfer step (environment limit, not a code issue; clone path == copy path through my manifest hook) |

### startup side (src/backend/access/transam/pgupgrade_wal.c)

`PerformWalUpgradeIfNeeded()` (called before StartupXLOG):
- No `pg_wal_upgrade/` → return false (normal startup)
- `pg_wal_upgrade/` present, COMPLETE found → recreate fresh pg_wal/ +
  archive_status, copy all segments in, return true → crash recovery from C0
- `pg_wal_upgrade/` present, NO COMPLETE → FATAL "re-run pg_upgrade from old
  cluster" (nothing copied, old cluster intact)

`XLOG_UPGRADE_RELFILE_DATA` redo applies each page THROUGH the buffer manager
(`XLogReadBufferExtended(RBM_ZERO_AND_LOCK)` + memcpy + `PageSetLSN(end_lsn)` +
`MarkBufferDirty`).  This is essential: a direct file write loses to pg_restore's
own buffered pages (e.g. the btree metapage FPI from log_newpage) at the
end-of-recovery checkpoint.  Going through the buffer manager with the record's
(highest) LSN makes our image win.

### Two subtle bugs found and fixed during bring-up

1. **Index metapage corruption.** `CREATE INDEX` emits a metapage FPI via
   `log_newpage()` UNCONDITIONALLY (even at wal_level=minimal).  On replay that
   buffered empty metapage (btm_root=0) was flushed over our direct-write page
   at checkpoint → index scans returned nothing.  Fix: buffer-manager redo (see
   above) so our page dominates by LSN.

2. **Catalog corruption / "relation does not exist".** The emit originally
   walked all of `base/<db>/`, capturing system catalogs too.  Those were stale
   (pg_restore's rows were still in shared buffers, not flushed to the file the
   emit read) and — once buffer-manager redo made them win — clobbered the
   correct new catalogs.  Fix: the transferred-files manifest restricts
   RELFILE_DATA to user relations only.

---

## Background: what pg_upgrade does that is NOT WAL-logged

pg_upgrade operates in three server states:

1. **initdb phase** — `postgres --boot` then `postgres --single`
   - Bootstrap: `XLogInsertRecord()` silently discards all non-XLOG rmgr records
     (`xloginsert.c:497`: `if (IsBootstrapProcessingMode() && rmid != RM_XLOG_ID) return EndPos`)
   - Post-bootstrap single-user: new-relfilenode optimization skips WAL
     (`RelationNeedsWAL()` returns false when `rd_createSubid != InvalidSubTransactionId`)
   - **Conclusion**: initdb output is the physical baseline. Raising wal_level has zero effect here.

2. **pg_restore phase** — running postmaster, `wal_level=replica` (with `--wal-log-upgrade`)
   - All catalog INSERT/UPDATE/DELETE are fully WAL-logged via RM_HEAP/HEAP2
   - Index builds are WAL-logged via RM_BTREE, RM_GIN, etc.
   - pg_filenode.map writes are WAL-logged via XLOG_RELMAP_UPDATE (full map content embedded)
   - **This phase is already fully covered once wal_level=replica is active**

3. **Post-restore / out-of-band phase** — server stopped, external tools
   - `copy_subdir_files("pg_xact", ...)` — raw `cp -Rf`, no server involved
   - `copy_subdir_files("pg_multixact/offsets", ...)` — same
   - `copy_subdir_files("pg_multixact/members", ...)` — same
   - `pg_resetwal --control-only` → `update_controlfile()` → raw `write()` to `global/pg_control`
   - `transfer_relfile()` → `copyFile()` / `cloneFile()` / `linkFile()` — raw syscall
   - **None of these go through WAL**

---

## The shutdown checkpoint problem

`stop_postmaster()` uses `pg_ctl -m smart stop` which writes a shutdown checkpoint.
The shutdown checkpoint advances `checkPointLoc` to the end of WAL and sets
`pg_control` state to `DB_SHUTDOWNED`. On next startup PostgreSQL sees a clean
shutdown and skips crash recovery — the pg_restore WAL is never replayed.

**Solution**: use a dedicated `pg_wal_upgrade/` directory separate from `pg_wal/`.
After `stop_postmaster()`, before `pg_resetwal` runs:
- Rename `pg_wal/` → `pg_wal_upgrade/`  (preserves the restore WAL)
- `pg_resetwal` creates a fresh `pg_wal/` for normal operation

On next startup, a new pre-recovery check scans `pg_wal_upgrade/`:
- `XLOG_PG_UPGRADE_START` + `XLOG_PG_UPGRADE_COMPLETE` both present → replay
- `XLOG_PG_UPGRADE_START` only (crash mid-upgrade) → discard `pg_wal_upgrade/`, cluster is intact initdb baseline
- Neither → no upgrade, skip

---

## WAL buffer size (reference)

Default: `XLOGbuffers = -1` = `NBuffers / 32`, clamped 64KB–16MB.
For `shared_buffers=128MB`: `16384 / 32 = 512 pages × 8KB = 4MB`.
Test run measured: **~14MB WAL** for a fresh test cluster schema restore.

---

## Completed work

### 1. `pg_resetwal --control-only`
**Files**: `src/bin/pg_resetwal/pg_resetwal.c`, `src/bin/pg_upgrade/pg_upgrade.c`

Added `--control-only` flag to `pg_resetwal`. When set:
- Applies counter fields (nextXid, epoch, nextOid, multixact, char_signedness) to `pg_control`
- Calls `RewriteControlFileCounters()` → `update_controlfile()` only
- Skips `RewriteControlFile()` (which overwrites checkPointLoc and state)
- Skips `KillExistingXLOG()`, `KillExistingArchiveStatus()`, `KillExistingWALSummaries()`, `WriteEmptyXLOG()`
- Incompatible with `-l` and `--wal-segsize` (validated at startup)

All 9 `pg_resetwal` calls in pg_upgrade now use `--control-only`.
The explicit "Resetting WAL archives" (`-l 00000001...`) call is removed.

**Effect**: WAL written by pg_restore survives the counter transplant step.

### 2. `--wal-log-upgrade` option
**Files**: `src/bin/pg_upgrade/option.c`, `src/bin/pg_upgrade/pg_upgrade.c`, `src/bin/pg_upgrade/pg_upgrade.h`

New opt-in flag. When set:
- Starts the new cluster with `-c wal_level=replica` for the schema-restore phase
- Writes `XLOG_PG_UPGRADE_START` WAL marker before pg_restore
- Writes `XLOG_PG_UPGRADE_COMPLETE` WAL marker after pg_restore

When not set: pg_upgrade behaves exactly as before (no performance impact).

### 3. `XLOG_PG_UPGRADE_START` / `XLOG_PG_UPGRADE_COMPLETE` WAL records
**Files**: `src/include/catalog/pg_control.h`, `src/backend/access/transam/xlog.c`,
           `src/backend/access/transam/xlogfuncs.c`, `src/backend/access/rmgrdesc/xlogdesc.c`,
           `src/include/catalog/pg_proc.dat`

Two new record types in `RM_XLOG_ID` (0xC0, 0xC1):
```c
typedef struct xl_pg_upgrade {
    uint32    old_major_version;
    uint32    new_major_version;
    pg_time_t upgrade_time;
} xl_pg_upgrade;
```
Redo: no-op (informational markers).
Desc: `"old_major_version N; new_major_version M; time T"` — visible in pg_waldump.
SQL functions: `pg_write_pg_upgrade_start(int4, int4)` and `pg_write_pg_upgrade_complete(int4, int4)`,
OIDs 9700 and 9701, restricted to superusers.

### 4. `pg_upgrade_wal_bytes` counter
**Files**: `src/bin/pg_upgrade/pg_upgrade.h`, `src/bin/pg_upgrade/pg_upgrade.c`, `src/bin/pg_upgrade/check.c`

`LogOpts.pg_upgrade_wal_bytes` measures WAL bytes generated during the schema-restore
phase (from CHECKPOINT before pg_restore to reconnect after all jobs complete).
Reported in the completion banner:
```
WAL generated during schema restore: <N> bytes
```
Also emitted at `PG_VERBOSE` level during the restore phase.

### 5. `--initdb` option
**Files**: `src/bin/pg_upgrade/option.c`, `src/bin/pg_upgrade/pg_upgrade.c`

Automates creation of the new cluster. Derives initdb parameters from the old cluster's
pg_controldata output: encoding, locale-provider, locale, lc-collate, lc-ctype,
wal-segsize, data-checksums, char-signedness. See IMPLEMENTATION for full details.

---

## WAL record types added (RM_XLOG_ID, 0xC0–0xC3)

All four types share the 0xC high nibble. Because `~XLR_INFO_MASK` masks to 0xF0,
they all reduce to 0xC0 after masking. Fixed by checking `raw_info` (the unmasked
byte) in `xlog_redo()`, `xlog_desc()`, and a pre-check in `xlog_identify()` before
the masked switch statement.

| Value | Name | Redo action |
|---|---|---|
| 0xC0 | `XLOG_PG_UPGRADE_START` | Write `$PGDATA/PG_VERSION` from embedded string |
| 0xC1 | `XLOG_PG_UPGRADE_COMPLETE` | No-op (informational) |
| 0xC2 | `XLOG_UPGRADE_SLRU_DATA` | `pwrite()` packed SLRU segment data to SLRU dir |
| 0xC3 | `XLOG_UPGRADE_RELFILE_DATA` | `pwrite()` relation file segment to `base/<dboid>/` |

### `xl_pg_upgrade` (START/COMPLETE)
```c
typedef struct xl_pg_upgrade {
    uint32    old_major_version;
    uint32    new_major_version;
    pg_time_t upgrade_time;
    char      pg_version[8];   /* PG_MAJORVERSION e.g. "18\n" — written to PG_VERSION on redo */
} xl_pg_upgrade;
```
SQL functions: `pg_write_pg_upgrade_start(int4,int4)` OID 9700,
`pg_write_pg_upgrade_complete(int4,int4)` OID 9701.

### `xl_upgrade_slru_data` (SLRU)
```c
typedef struct xl_upgrade_slru_data {
    uint8   slru_type;    /* 0=pg_xact, 1=pg_multixact/offsets, 2=pg_multixact/members */
    int64   first_seg;    /* first segment number in this batch */
    int64   last_seg;     /* last segment number in this batch */
    uint32  total_bytes;  /* bytes of raw data that follow */
} xl_upgrade_slru_data;
```
Granularity: consecutive segments packed up to `XLogRecordMaxSize` (1020MB) per record.
Buffer allocated exactly per batch — no 1GB upfront allocation.
SQL function: `pg_write_upgrade_slru_data(slru_type int4) → pg_lsn`, OID 9702.

### `xl_upgrade_relfile_data` (relation files)
```c
typedef struct xl_upgrade_relfile_data {
    Oid      tablespace_oid;
    Oid      database_oid;
    uint32   relfilenumber;
    uint8    forknum;        /* 0=main, 1=FSM, 2=VM, 3=init */
    uint32   segno;          /* 1GB segment number */
    uint32   total_bytes;
} xl_upgrade_relfile_data;
```
One record per physical file segment. Redo: `pwrite()` to `base/<dboid>/<rfnum>[_fsm|_vm][.segno]`.
SQL function: `pg_write_upgrade_relfile_data() → void`, OID 9703 — walks `base/` and emits all segments.

---

## `pg_wal_upgrade/` mechanism ✅ IMPLEMENTED

After all WAL is written, pg_upgrade:
1. Calls `pg_switch_wal()` to flush WAL buffers to disk
2. Calls `stop_postmaster_immediate()` (new function, `-m immediate` / SIGQUIT) — **no shutdown checkpoint, no WAL recycling**
3. Renames `pg_wal/` → `pg_wal_upgrade/` — upgrade WAL preserved permanently
4. `pg_resetwal --control-only` creates a fresh `pg_wal/` for normal operation

The result: `pg_wal_upgrade/` contains the complete ordered upgrade WAL stream:
```
UPGRADE_SLRU_DATA (pg_xact)
UPGRADE_SLRU_DATA (pg_multixact/offsets)
UPGRADE_SLRU_DATA (pg_multixact/members)
PG_UPGRADE_START  (old=N, new=M, pg_version="18\n")
... pg_restore WAL (HEAP/BTREE/RELMAP records for catalog schema) ...
... UPGRADE_RELFILE_DATA × N  (one per relation file segment) ...
PG_UPGRADE_COMPLETE (old=N, new=M)
```

On next startup (not yet implemented — see Hole 2 below):
- `PG_UPGRADE_START` + `PG_UPGRADE_COMPLETE` both present → replay `pg_wal_upgrade/`, remove it
- `PG_UPGRADE_START` only (crash mid-upgrade) → discard `pg_wal_upgrade/`, cluster is intact initdb baseline, safe to re-run pg_upgrade
- Neither → no upgrade in progress, normal startup

---

## Already-covered gaps (no new WAL type needed)

| What | Covered by |
|---|---|
| Catalog heap pages (pg_restore) | `RM_HEAP/HEAP2` — fully logged at `wal_level=replica` |
| Index pages (pg_restore) | `RM_BTREE`, `RM_GIN`, `RM_GIST`, `RM_SPGIST`, `RM_BRIN`, `RM_HASH` |
| `pg_filenode.map` global + per-db | `XLOG_RELMAP_UPDATE` — full map content embedded |
| Per-database `PG_VERSION` files | `XLOG_DBASE_CREATE_WAL_LOG` (RM_DBASE_ID) via pg_restore |
| Top-level `$PGDATA/PG_VERSION` | Embedded in `xl_pg_upgrade.pg_version`, written on START redo |
| `pg_subtrans/` | Not copied — rebuilt by `StartupSUBTRANS()` from XID range in pg_control |
| `pg_notify/`, `pg_serial/` | Not copied — volatile, transient |
| `pg_commit_ts/` | Not copied — new cluster starts empty |

---

## Fundamental limitation

`pg_wal_upgrade/` is a **delta on top of the initdb baseline**, not a standalone
backup. Replay requires:
- The initdb-created data files (`base/`, `global/`, etc.) — the baseline
- `global/pg_control` — to locate the checkpoint LSN to start from
- `pg_wal_upgrade/` — the delta

If the entire new cluster data directory is lost, `pg_wal_upgrade/` alone cannot
reconstruct it. Re-run `pg_upgrade` from scratch.

---

## Remaining holes (not yet implemented)

### Hole 1: pg_control counter transplant NOT in WAL stream (correctness bug)

`pg_resetwal --control-only` runs **after** `pg_wal_upgrade/` is renamed, so it
writes counter values (nextXid, epoch, nextOid, multixact, char_signedness) directly
to `pg_control` but they are **not captured in `pg_wal_upgrade/`**.

On replay, the replayed cluster would have the wrong XID/OID counters — it would
use the initdb-era counters, not the old-cluster counters that pg_upgrade transplanted.

**Fix needed**: emit `XLOG_UPGRADE_CONTROL_COUNTERS` (a new 0xC4 record type) just
before `pg_switch_wal()` / `stop_postmaster_immediate()`, while the server is still
up. The record carries all counter fields. Redo calls `update_controlfile()`.

```c
/* proposed */
typedef struct xl_upgrade_control_counters {
    FullTransactionId nextXid;
    TransactionId     oldestXid;
    Oid               nextOid;
    MultiXactId       nextMulti;
    MultiXactOffset   nextMultiOffset;
    MultiXactId       oldestMulti;
    TransactionId     oldestCommitTsXid;
    TransactionId     newestCommitTsXid;
    bool              default_char_signedness;
} xl_upgrade_control_counters;
```

The counter values are known at this point from `old_cluster.controldata.*` (already
read). Emit as a SQL function `pg_write_upgrade_control_counters(...)` called just
before COMPLETE, or piggyback directly onto the COMPLETE record payload.

### Hole 2: Startup replay — partially implemented, needs redesign

**Current state**: `PerformWalUpgradeIfNeeded()` exists in
`src/backend/access/transam/pgupgrade_wal.c` and is called from
`StartupProcessMain()` before `StartupXLOG()`. It correctly:
- Detects `pg_wal_upgrade/`
- Scans for START+COMPLETE
- Replays all RM_PG_UPGRADE_ID records via `pg_upgrade_redo()` (direct pwrite)
- Removes `pg_wal_upgrade/` afterward
- Skips during binary upgrade mode (`IsBinaryUpgrade=true`)

**Blocker**: `pg_upgrade_redo()` writes files directly via `pwrite()`, bypassing
the PostgreSQL buffer manager and WAL infrastructure. After replay, the on-disk
files are correct but the in-memory state is not initialized — the relcache,
syscache, and shared memory have no knowledge of the written data. When
`StartupXLOG()` then does a clean startup from the fresh `pg_resetwal` checkpoint,
it finds a catalog that is inconsistent: `cache lookup failed for relation 1261`
(pg_authid) because the shared catalog pages written by replay are ahead of where
the checkpoint says the database should be.

**Required redesign**: Instead of writing files directly, `PerformWalUpgradeIfNeeded()`
should set up `pg_control` so that `StartupXLOG()`'s **normal crash recovery path**
replays the upgrade WAL:

1. Scan `pg_wal_upgrade/` for START+COMPLETE (existing logic — keep)
2. If both found:
   a. Copy the upgrade WAL segment files from `pg_wal_upgrade/` into `pg_wal/`
      (or symlink, or mount — they need to be accessible via the standard WAL path)
   b. Set `pg_control.checkPoint` to the LSN just BEFORE the SLRU records
      (this is the checkpoint that was active when pg_upgrade wrote the START record)
   c. Set `pg_control.minRecoveryPoint` to the COMPLETE record LSN
   d. Set `pg_control.state = DB_IN_PRODUCTION` (crash recovery mode)
   e. Update `pg_control` to disk
   f. Return — let `StartupXLOG()` run normally
3. `StartupXLOG()` sees `DB_IN_PRODUCTION`, enters crash recovery, reads from
   `pg_wal/` (which now contains the upgrade WAL), replays all records through
   normal buffer manager path (heap_redo, dbase_redo, etc.)
4. After recovery completes, add a hook to remove `pg_wal_upgrade/` and restore
   `pg_wal/` to normal (remove the copied upgrade segments)

**Alternative simpler approach**: Instead of copying WAL segments, configure
`pg_control` to replay from `pg_wal_upgrade/` directly by setting the WAL
archive path or recovery target. But the cleanest is to merge the WAL streams.

**Key insight**: The `pg_resetwal -f` that creates the fresh `pg_wal/` sets
`pg_control.checkPoint` to a location in that fresh WAL. We need to instead set
it to a location that makes `StartupXLOG()` replay through the upgrade WAL.
The checkpoint LSN recorded in `xl_pg_upgrade_control_counters` or in the
`pg_control` before the immediate stop is the right anchor point.

Entry point: `PerformWalUpgradeIfNeeded()` in
`src/backend/access/transam/pgupgrade_wal.c`, called from
`StartupProcessMain()` in `src/backend/postmaster/startup.c`.

Status: infrastructure in place, redesign of the pg_control manipulation needed.

### Hole 3: Non-default tablespace files not WAL-logged

`pg_write_upgrade_relfile_data()` currently only walks `base/` (default tablespace).
Relation files in `pg_tblspc/<oid>/PG_<ver>_<catver>/<dboid>/` are not covered.

**Fix**: extend the walk in `pg_write_upgrade_relfile_data()` to also iterate
`pg_tblspc/` symlinks and emit `XLOG_UPGRADE_RELFILE_DATA` for each non-default
tablespace. The redo handler also needs a tablespace-aware path builder (it currently
hardcodes `base/<dboid>/`).

### Hole 4: Shared relations (global/) not WAL-logged via RELFILE

Shared system catalogs (`global/<rfnum>`) are written by pg_restore through normal
heap WAL (`XLOG_HEAP_INSERT`, etc.), so they should be covered by the pg_restore
WAL already in `pg_wal_upgrade/`. However, `pg_write_upgrade_relfile_data()` does
not walk `global/` and does not emit `XLOG_UPGRADE_RELFILE_DATA` for shared files.
This means if only `pg_wal_upgrade/` is replayed from the initdb baseline without
the pg_restore WAL being complete, shared catalog pages could be missing.

**Assessment**: lower priority since pg_restore WAL covers shared catalogs for the
normal upgrade case. Only relevant if pg_restore WAL is incomplete.

### Hole 5: WAL segment truncation at immediate stop ✅ NOT A PROBLEM

`pg_switch_wal()` is called before `stop_postmaster_immediate()`. This forces the
server to seal the current WAL segment and start a new one, so `XLOG_PG_UPGRADE_COMPLETE`
always lands in a freshly-started, fully-written segment. The immediate stop cannot
corrupt a segment that has already been sealed. The trailing segment after COMPLETE
may be empty or partially written, but replay stops at COMPLETE — it never reads past
that record. pg_upgrade is purely atomic: either COMPLETE is present (full replay)
or it is not (discard, retry).

### Hole 6: Test for SLRU files > a few MB

The test suite uses a fresh small cluster with only 1–2 SLRU segments. A test with
enough transactions to span multiple MB of pg_xact (> 3M XIDs = 3 segments) would
verify the packing logic and multi-segment redo. pgbench with `-t 500000 -c 8`
generates ~4M XIDs in ~60s but requires confirming the SLRU packing actually
produces one record per contiguous batch rather than one per segment.

---

## Implementation order for next session

1. ✅ **Hole 1** — `XLOG_UPGRADE_CONTROL_COUNTERS` (0x40 in RM_PG_UPGRADE_ID):
   Implemented. Record emitted before `pg_switch_wal()`. Redo applies counters
   via `XLogApplyUpgradeControlCounters()` in xlog.c. OID 9704.

2. 🔄 **Hole 2** — Startup replay redesign:
   Infrastructure exists (`PerformWalUpgradeIfNeeded()` in pgupgrade_wal.c,
   called from StartupProcessMain()). Current direct-pwrite design is broken.
   **Next step**: redesign to copy upgrade WAL into `pg_wal/` and set
   `pg_control` to crash-recovery mode so `StartupXLOG()` replays via the
   normal buffer manager path. See Hole 2 section above for full design.

3. **Hole 3** — Non-default tablespace walk in `pg_write_upgrade_relfile_data()`.
   Extend to iterate `pg_tblspc/<oid>/PG_<ver>_<catver>/<dboid>/` and update
   the redo path builder to handle non-default tablespace paths.

4. **Hole 6** — Test with large SLRU (> 3 SLRU segments, > 1MB pg_xact).
   Verify packing produces correct batch records and redo reconstructs files exactly.

5. **Hole 4** — Shared relations (`global/`) in relfile emit — lower priority,
   covered by pg_restore WAL for the normal case.

---

## Test plan: extreme cases

The existing `t/002_pg_upgrade.pl` only tests a tiny fresh cluster. These additional
tests are needed to validate correctness and edge cases of the WAL logging work.

### T1: Large SLRU — multi-segment pg_xact

**Goal**: verify SLRU packing across multiple segments and redo correctness.

**Setup**: generate enough transactions to span ≥ 4 pg_xact segments (≥ 4M XIDs).
Each segment covers 1M XIDs (32 pages × 32K XIDs/page). pgbench with enough
transactions, or a direct loop committing individual transactions.

```sql
-- Each iteration is one transaction
DO $$ BEGIN
  FOR i IN 1..4100000 LOOP
    PERFORM txid_current();  -- won't work; need individual txns
  END LOOP;
END $$;
-- Instead: pgbench -t 600000 -c 8 generates ~4.8M XIDs
```

**Verify**:
- `pg_waldump` on `pg_wal_upgrade/` shows `UPGRADE_SLRU_DATA slru pg_xact; segs 0000..0003`
  — four segments packed into one or two records (not four separate records)
- After replay, `pg_xact/0000` through `pg_xact/0003` in new cluster match old cluster byte-for-byte
- `pg_upgrade` completes and new cluster starts without XID errors

### T2: Multi-segment relation file (>1GB table)

**Goal**: verify `XLOG_UPGRADE_RELFILE_DATA` handles 1GB segment files and the `.1`
segment extension.

**Setup**: create a table and INSERT enough data to exceed 1GB (one 1GB segment = `relfilenumber.1`).
```sql
CREATE TABLE big (data text);
INSERT INTO big SELECT repeat('x', 1000) FROM generate_series(1, 1100000);
-- ~1.1GB → creates relfilenumber and relfilenumber.1
```

**Verify**:
- `pg_waldump` shows two `UPGRADE_RELFILE_DATA` records for the same relfilenumber:
  `seg 0` (1GB) and `seg 1` (remaining bytes)
- Each record is ≤ `XLogRecordMaxSize`
- After replay, table data is intact

### T3: Crash at each upgrade phase

**Goal**: verify atomicity — crashing at any point leaves the cluster in a
recoverable or retryable state.

**Phases to inject crash**:
1. Before `XLOG_PG_UPGRADE_START` — `pg_wal_upgrade/` absent → normal startup, no replay
2. After START, before any pg_restore WAL — START only → discard, retry
3. Mid pg_restore (after some databases restored, not all) — START only → discard, retry
4. After all pg_restore, before RELFILE emit — START only → discard, retry
5. Mid RELFILE emit — START only → discard, retry
6. After COMPLETE written, before `pg_switch_wal()` — COMPLETE may not be flushed → treated as START-only
7. After `pg_switch_wal()`, before immediate stop — COMPLETE guaranteed durable → full replay
8. After rename to `pg_wal_upgrade/`, before `pg_resetwal` — full replay on next startup

**Implementation**: inject `kill -9 <pg_upgrade_pid>` at each phase using a wrapper
script or a `PG_UPGRADE_CRASH_AT` environment variable tested with `pg_fatal()` calls
at each checkpoint. After crash, verify:
- New cluster starts normally (phases 1–6: initdb baseline, no stale state)
- OR new cluster replays correctly (phases 7–8: COMPLETE present)

### T4: Non-default tablespace

**Goal**: verify Hole 3 once fixed — relfile WAL covers non-default tablespace files.

**Setup**:
```sql
CREATE TABLESPACE ts1 LOCATION '/tmp/ts1';
CREATE TABLE tbl_ts1 (i int) TABLESPACE ts1;
INSERT INTO tbl_ts1 SELECT generate_series(1, 10000);
```

**Verify**:
- `pg_waldump` shows `UPGRADE_RELFILE_DATA` records with `tablespace_oid != 1663`
  (not the default tablespace OID)
- After replay, tablespace directory and table data are intact
- Redo path correctly constructs `pg_tblspc/<oid>/PG_18_<catver>/<dboid>/<rfnum>`

### T5: pg_wal_upgrade/ replay correctness

**Goal**: verify that a cluster with `pg_wal_upgrade/` present but `pg_wal/` freshly
initialized (from `pg_resetwal`) actually replays correctly on startup.

**Setup**: run `pg_upgrade --wal-log-upgrade`, confirm `pg_wal_upgrade/` is present,
then start the new cluster and verify:
- Server log shows replay of `pg_wal_upgrade/` records
- All user tables from old cluster are accessible with correct data
- `pg_xact/` contents match old cluster (XID status preserved)
- `pg_multixact/` contents match old cluster
- XID counter (`pg_current_xid()`) is at the old cluster's value, not initdb's

### T6: Multiple databases, large schema

**Goal**: verify pg_restore WAL volume and relfile count at scale.

**Setup**: create 10 databases each with 50 tables, 5 indexes per table, various
data types (enum, composite, range) to exercise `--binary-upgrade` OID preservation.

**Verify**:
- All 10 databases restored correctly
- `UPGRADE_RELFILE_DATA` count ≈ 10 × 50 × (main + fsm + vm) = ~1500 records
- `pg_upgrade_wal_bytes` counter reflects realistic production WAL volume

### T7: WAL spanning > 1 segment (many relfiles)

**Goal**: verify `pg_wal_upgrade/` can contain multiple WAL segments and replay
reads across segment boundaries correctly.

**Setup**: enough relation files to push `UPGRADE_RELFILE_DATA` records past 16MB
(the default WAL segment size). With ~1500 files × average 50KB each = 75MB of
relfile WAL → ~5 WAL segments in `pg_wal_upgrade/`.

**Verify**:
- `pg_wal_upgrade/` shows 5+ segment files
- Replay reads all segments in order without skipping or double-reading
- Final cluster state is correct

### T8: Link mode (`--link`)

**Goal**: verify `--wal-log-upgrade` with `--link` (hardlinks instead of copy).
In link mode, `transfer_relfile()` creates hardlinks — the files are shared between
old and new cluster. `XLOG_UPGRADE_RELFILE_DATA` still reads the file content
(via the new cluster path) and WAL-logs it.

**Verify**:
- `pg_wal_upgrade/` is created and contains RELFILE records
- Old cluster cannot be started after upgrade (link mode disables it)
- New cluster starts and replays correctly

### T9: WAL record size boundary (XLogRecordMaxSize)

**Goal**: verify that a SLRU directory approaching `XLogRecordMaxSize` splits
correctly into multiple records rather than failing.

**Setup**: synthesize a scenario with ~4000 SLRU segments (would require ~4B XIDs
in pg_xact — impractical to generate; instead mock by creating segment files
directly in the SLRU directory before upgrade).

**Verify**:
- Packing logic emits multiple records when `batch_count × seg_size > XLogRecordMaxSize`
- Each record's `total_bytes ≤ XLogRecordMaxSize`
- Redo reassembles all segments correctly
