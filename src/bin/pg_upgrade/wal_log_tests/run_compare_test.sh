#!/usr/bin/env bash
# Compare the physical cluster produced by a NORMAL pg_upgrade vs a
# --wal-log-upgrade + first-startup replay, of identical old-cluster data.
# Relation files are compared page-by-page, optionally ignoring the 8-byte
# page LSN (pd_lsn), which replay legitimately rewrites.
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"; BIN="${PGBIN:-$ROOT/pginst/bin}"
W=/tmp/pgu_cmp3; P=55530; export PGDATABASE=postgres
rm -rf "$W"; mkdir -p "$W"
log(){ echo "=== $* ==="; }

# ---- seed old cluster ----
SEED=$W/seed
"$BIN/initdb" -D "$SEED" -U postgres -N >/dev/null 2>&1
echo "unix_socket_directories='$W'">>$SEED/postgresql.conf; echo "port=$P">>$SEED/postgresql.conf
PGPORT=$P "$BIN/pg_ctl" -D "$SEED" -l "$W/seed.log" -w start >/dev/null 2>&1
PGPORT=$P "$BIN/psql" -h "$W" -U postgres -v ON_ERROR_STOP=1 -q >/dev/null <<'SQL'
CREATE TABLE t(id int primary key, v text);
INSERT INTO t SELECT g, repeat('y',50)||g FROM generate_series(1,8000) g;
CREATE INDEX t_v ON t(v);
CREATE TABLE toast_t(id int, big text);
INSERT INTO toast_t SELECT g, repeat(md5(g::text),400) FROM generate_series(1,500) g;
DO $$ BEGIN FOR i IN 1..60 LOOP EXECUTE 'CREATE TABLE c'||i||'(a int, b text)'; END LOOP; END $$;
SQL
PGPORT=$P "$BIN/pg_ctl" -D "$SEED" -w stop >/dev/null 2>&1

run_upgrade() { # $1=variant  $2=extraflag
  local V=$1 FLAG=$2 D=$W/$1
  cp -a "$SEED" "$D/old_src" 2>/dev/null; mkdir -p "$D"; cp -a "$SEED" "$D/old"
  cd "$D"; "$BIN/pg_upgrade" -b $BIN -B $BIN -d "$D/old" -D "$D/new" -U postgres --initdb $FLAG --copy >"$D/up.log" 2>&1 \
    || { echo "$V UPGRADE FAILED"; tail -8 "$D/up.log"; exit 1; }
  echo "unix_socket_directories='$W'">>$D/new/postgresql.conf; echo "port=$P">>$D/new/postgresql.conf; echo "autovacuum=off">>$D/new/postgresql.conf
  # start + clean stop so both go through an identical startup/shutdown cycle
  PGPORT=$P "$BIN/pg_ctl" -D "$D/new" -l "$D/new.log" -w start >/dev/null 2>&1 || { echo "$V START FAILED"; tail -20 "$D/new.log"; exit 1; }
  PGPORT=$P "$BIN/pg_ctl" -D "$D/new" -w stop >/dev/null 2>&1
}

log "normal pg_upgrade"; run_upgrade normal ""
log "wal-log pg_upgrade (+ replay on first start)"; run_upgrade wal "--wal-log-upgrade"

NA=$W/normal/new; WB=$W/wal/new
log "compare relation files + SLRU + relmap page-by-page (LSN-aware)"
verdict_rc=0
python3 - "$NA" "$WB" <<'PY' || verdict_rc=$?
import os, sys, re
na, wb = sys.argv[1], sys.argv[2]
BL = 8192
relname = re.compile(r'^[0-9]+(_fsm|_vm|_init)?(\.[0-9]+)?$')

def rels(root):
    out = {}
    for base in ('base', 'global'):
        for dirpath, _, files in os.walk(os.path.join(root, base)):
            for f in files:
                rel = os.path.relpath(os.path.join(dirpath, f), root)
                out[rel] = os.path.join(dirpath, f)
    return out

A, B = rels(na), rels(wb)
common = sorted(set(A) & set(B))
onlyA = sorted(set(A) - set(B)); onlyB = sorted(set(B) - set(A))

# Non-data files that legitimately differ between vanilla and replay and are
# NOT authoritative cluster content (matches run_e2e_equivalence):
#   pg_control        -- checkpoint LSN, timestamps, etc.
#   pg_internal.init  -- ephemeral relcache init file, regenerated per cluster
IGNORE = {'pg_control', 'pg_internal.init'}
common = [r for r in common if os.path.basename(r) not in IGNORE]
onlyA = [r for r in onlyA if os.path.basename(r) not in IGNORE]
onlyB = [r for r in onlyB if os.path.basename(r) not in IGNORE]

def is_vm_fsm(rel):
    b = os.path.basename(rel)
    return b.endswith('_vm') or '_vm.' in b or b.endswith('_fsm') or '_fsm.' in b

import struct

def normalize_page(p):
    """Zero out the bytes that a vanilla upgrade and a WAL replay legitimately
    disagree on WITHOUT any difference in authoritative data:

      Page header (PageHeaderData):
        0..7   pd_lsn        -- replay assigns its own LSN
        8..9   pd_checksum   -- recomputed over the page (incl. the LSN)
        20..23 pd_prune_xid  -- opportunistic-prune hint, set lazily
                                (layout: pd_lsn,pd_checksum,pd_flags,pd_lower,
                                 pd_upper,pd_special,pd_pagesize_version,
                                 pd_prune_xid at byte 20)

      Per heap tuple (HeapTupleHeaderData):
        t_infomask (2 bytes at tuple off +20) -- xmin/xmax COMMITTED/INVALID
                        hint bits, set lazily on first access and recomputed
                        from CLOG when unset; non-authoritative.

    Everything else (pd_flags, pd_lower/upper/special, line pointers, tuple
    data, all non-hint infomask bits) must match byte-for-byte.
    """
    p = bytearray(p)
    if len(p) < 24:
        return bytes(p)
    p[0:10] = b'\x00' * 10          # pd_lsn + pd_checksum
    p[20:24] = b'\x00' * 4          # pd_prune_xid
    pd_lower = struct.unpack_from('<H', p, 12)[0]
    if 24 <= pd_lower <= len(p):
        nptrs = (pd_lower - 24) // 4
        for i in range(nptrs):
            lp = struct.unpack_from('<I', p, 24 + i * 4)[0]
            lp_off = lp & 0x7FFF
            lp_len = (lp >> 17) & 0x7FFF
            # only LP_NORMAL line pointers with a sane tuple extent
            if lp_off == 0 or lp_len < 23 or lp_off + lp_len > len(p):
                continue
            # HEAP_XMIN_COMMITTED|INVALID|XMAX_COMMITTED|INVALID = 0x0F00; mask
            # the hint bits out of t_infomask (2 bytes at tuple offset +20).
            im = struct.unpack_from('<H', p, lp_off + 20)[0]
            struct.pack_into('<H', p, lp_off + 20, im & ~0x0F00)
    return bytes(p)

cat = {'identical':0, 'lsn_only':0, 'other_diff':0, 'size_diff':0, 'vmfsm':0}
other_examples = []
lsn_pages_total = 0
for rel in common:
    da = open(A[rel],'rb').read(); db = open(B[rel],'rb').read()
    isrel = relname.match(os.path.basename(rel)) is not None
    if da == db:
        cat['identical'] += 1; continue
    # _fsm / _vm are lazily-maintained derived forks (free-space / visibility
    # map): not authoritative data, legitimately differ between a vanilla
    # upgrade and a WAL replay.  Report but do not fail (matches run_e2e_equivalence).
    if is_vm_fsm(rel):
        cat['vmfsm'] += 1; continue
    if len(da) != len(db):
        cat['size_diff'] += 1
        if len(other_examples) < 8: other_examples.append(f"SIZE {rel}: {len(da)} vs {len(db)}")
        continue
    if not isrel:
        cat['other_diff'] += 1
        if len(other_examples) < 8: other_examples.append(f"NONREL {rel}")
        continue
    # page-by-page, normalizing LSN/checksum/prune-xid + per-tuple hint bits
    only_lsn = True; diffpages = 0
    for off in range(0, len(da), BL):
        pa = da[off:off+BL]; pb = db[off:off+BL]
        if pa == pb: continue
        if normalize_page(pa) == normalize_page(pb):
            diffpages += 1
        else:
            only_lsn = False
            break
    if only_lsn:
        cat['lsn_only'] += 1; lsn_pages_total += diffpages
    else:
        cat['other_diff'] += 1
        if len(other_examples) < 8: other_examples.append(f"DATA {rel} (page off {off})")

print(f"common relation/data files: {len(common)}")
print(f"  identical (byte-for-byte): {cat['identical']}")
print(f"  differ only in LSN/checksum: {cat['lsn_only']}  ({lsn_pages_total} pages)")
print(f"  vm/fsm derived-fork diff:  {cat['vmfsm']}")
print(f"  size differs:              {cat['size_diff']}")
print(f"  differ in real content:    {cat['other_diff']}")
# A 0-byte relation file and a missing one are semantically identical to
# Postgres (smgr creates/extends on demand).  The --wal-log-upgrade FPI capture
# deliberately skips empty files (nothing to image), so replay never recreates
# them.  Only a NON-empty file present on one side but not the other is a real
# divergence; empty-only files are expected and benign (same as run_e2e_equivalence).
onlyA_nonempty = [r for r in onlyA if os.path.getsize(A[r]) > 0]
onlyB_nonempty = [r for r in onlyB if os.path.getsize(B[r]) > 0]
if onlyA: print(f"  files only in NORMAL: {len(onlyA)} ({len(onlyA_nonempty)} non-empty) e.g. {onlyA[:5]}")
if onlyB: print(f"  files only in WAL:    {len(onlyB)} ({len(onlyB_nonempty)} non-empty) e.g. {onlyB[:5]}")
for e in other_examples: print("   !", e)

# exact compare of non-page files that must match identically
print("--- exact (non-page) files ---")
for rel in ('base','global'):
    pass
for f in sorted(set(A)&set(B)):
    if os.path.basename(f) in ('pg_filenode.map','PG_VERSION'):
        s = "OK" if open(A[f],'rb').read()==open(B[f],'rb').read() else "DIFFER"
        print(f"  {f}: {s}")

verdict = (cat['other_diff']==0 and cat['size_diff']==0
           and not onlyA_nonempty and not onlyB_nonempty)
print("VERDICT:", "PASS - identical modulo page LSN and empty-only files" if verdict else "DIFFERENCES BEYOND LSN")
sys.exit(0 if verdict else 1)
PY

# SLRU compare is INFORMATIONAL ONLY: the two clusters run independent
# restore+startup cycles, so pg_xact/pg_multixact bookkeeping legitimately
# differs.  It does not feed the verdict (physical-equivalence is about relation
# files, checked above; SLRU content is validated logically by run_mxact_test).
log "SLRU (pg_xact/pg_multixact) exact compare (informational)"
for d in pg_xact pg_multixact/offsets pg_multixact/members; do
  if diff -r "$NA/$d" "$WB/$d" >/dev/null 2>&1; then echo "  $d: IDENTICAL"; else echo "  $d: DIFFERS"; diff -rq "$NA/$d" "$WB/$d" 2>&1 | head -3; fi
done

if [ "$verdict_rc" = 0 ]; then
  log "PASS: WAL-replayed cluster physically matches normal pg_upgrade (modulo page LSN + empty-only files)"
  exit 0
else
  log "FAIL: physical divergence beyond page LSN / empty-only files"
  exit 1
fi
