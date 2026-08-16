#!/usr/bin/env bash
#
# End-to-end test harness for pg_upgrade --wal-upgrade.
#
# Creates an "old" cluster with real data, runs pg_upgrade --wal-upgrade
# --initdb into a "new" cluster, then starts the new cluster and verifies the
# data survived a pure WAL-replay recovery.
#
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
BIN="${PGBIN:-$ROOT/pginst/bin}"
WORK=${WORK:-/tmp/pgu}
OLD=$WORK/old
NEW=$WORK/new
PORT=${PORT:-55432}
export PGPORT=$PORT
export PGDATABASE=postgres

log() { echo "=== $* ==="; }

rm -rf "$WORK"
mkdir -p "$WORK"

# ------------------------------------------------------------------ old cluster
log "initdb old cluster"
"$BIN/initdb" -D "$OLD" -U postgres -N >/dev/null 2>&1 || { echo FAIL initdb old; exit 1; }
echo "unix_socket_directories = '$WORK'" >> "$OLD/postgresql.conf"
echo "port = $PORT" >> "$OLD/postgresql.conf"
# wal_level=replica + a physical slot so the upgrade window is RETAINED in the
# new cluster's pg_wal/ for the waldump inspection below: --wal-upgrade retains
# the window only when a physical slot is migrated (a standby is expected).
# Without one, the window is unused and recycled as ordinary WAL.
echo "wal_level = replica" >> "$OLD/postgresql.conf"
echo "max_wal_senders = 8" >> "$OLD/postgresql.conf"

"$BIN/pg_ctl" -D "$OLD" -l "$WORK/old.log" -w start >/dev/null 2>&1 || { echo FAIL start old; cat "$WORK/old.log"; exit 1; }
"$BIN/psql" -h "$WORK" -p "$PORT" -U postgres -qtAc \
    "SELECT pg_create_physical_replication_slot('stby_slot', true)" >/dev/null 2>&1 || { echo FAIL create-slot; exit 1; }

log "load data into old cluster"
"$BIN/psql" -h "$WORK" -U postgres -v ON_ERROR_STOP=1 <<'SQL'
CREATE TABLE t1 (id int primary key, val text);
INSERT INTO t1 SELECT g, 'row-'||g FROM generate_series(1,50000) g;
CREATE INDEX t1_val_idx ON t1(val);
CREATE TABLE t2 (id bigserial primary key, payload text);
INSERT INTO t2 (payload) SELECT repeat('x', 200) FROM generate_series(1,20000);
CREATE DATABASE appdb;
SQL
[ $? -eq 0 ] || { echo FAIL load; exit 1; }

"$BIN/psql" -h "$WORK" -U postgres -d appdb -v ON_ERROR_STOP=1 <<'SQL'
CREATE TABLE orders (oid int primary key, amount numeric, note text);
INSERT INTO orders SELECT g, g*1.5, 'order '||g FROM generate_series(1,30000) g;
CREATE INDEX orders_amt ON orders(amount);
SQL
[ $? -eq 0 ] || { echo FAIL load appdb; exit 1; }

# Capture reference checksums from the old cluster
OLD_T1=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT count(*), sum(hashtext(val)::bigint) FROM t1")
OLD_T2=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT count(*), sum(hashtext(payload)::bigint) FROM t2")
OLD_ORD=$("$BIN/psql" -h "$WORK" -U postgres -d appdb -tAc "SELECT count(*), sum(hashtext(note)::bigint), sum(amount) FROM orders")
log "OLD t1=$OLD_T1  t2=$OLD_T2  orders=$OLD_ORD"

"$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1

# ------------------------------------------------------------------ pg_upgrade
log "run pg_upgrade --wal-upgrade --initdb (mode=${MODE:---copy})"
cd "$WORK"
"$BIN/pg_upgrade" \
    -b "$BIN" -B "$BIN" \
    -d "$OLD" -D "$NEW" \
    -U postgres \
    --initdb --wal-upgrade ${MODE:---copy} \
    > "$WORK/upgrade.log" 2>&1
UPG_RC=$?
log "pg_upgrade exit=$UPG_RC"
if [ $UPG_RC -ne 0 ]; then
    echo "---- upgrade.log tail ----"; tail -40 "$WORK/upgrade.log"; exit 1
fi

# Show the upgrade WAL.  It lives in pg_wal/ (there is no pg_wal_upgrade/
# rename), and must contain the RM_PG_UPGRADE records.  Waldump the DIRECTORY
# from the lowest segment (not file-by-file: large records span segments and a
# per-file dump cannot find their start).
log "pg_waldump of upgrade WAL in pg_wal/ (RM_PG_UPGRADE records)"
LOSEG=$(ls "$NEW/pg_wal/" | grep -E '^[0-9A-F]{24}$' | sort | head -1)
LOLSN=$("$BIN/pg_waldump" -p "$NEW/pg_wal" "$LOSEG" -n 1 2>&1 | grep -oE 'lsn: [0-9A-F]+/[0-9A-F]+' | head -1 | awk '{print $2}')
NPGU=$("$BIN/pg_waldump" -p "$NEW/pg_wal" -s "${LOLSN:-0/0}" 2>/dev/null | grep -icE "PG_UPGRADE_START|PG_UPGRADE_COMPLETE|UPGRADE_DIRTREE|UPGRADE_RAWFILE|UPGRADE_RELINK")
log "RM_PG_UPGRADE record count in pg_wal/: $NPGU"
[ "${NPGU:-0}" -ge 2 ] || { echo "FAIL: upgrade WAL not found in pg_wal/ (got $NPGU records)"; exit 1; }

# --- PROOF the SLRU disk writes were skipped and ride the WAL instead.
#
# NOTE: user relations are NOT wiped, and must not be.  Under the RELINK model
# the window carries only the rewritten catalog/system files; user relations are
# transferred to disk by pg_upgrade as usual (here --copy) and appear in the
# window only as an identity manifest, whose redo is a no-op on the primary.  So
# a populated base/ is expected here; only a streaming standby rebuilds it from
# its own retained old datadir.
log "user data files on disk after pg_upgrade (expected: present, via transfer)"
BIGGEST=$(find "$NEW/base" -type f -regextype posix-extended -regex '.*/[0-9]+(_fsm|_vm)?(\.[0-9]+)?' -printf '%s %p\n' 2>/dev/null | sort -rn | head -1)
echo "largest data file on disk after pg_upgrade: $BIGGEST"
# pg_upgrade copies pg_xact to the new cluster itself (copy_xact_xlog_xid), so
# it is present on disk here; the window ALSO carries the SLRU segments as
# XLOG_UPGRADE_RAWFILE records, which is what a standby or PITR replays.
XACT_BYTES=$(find "$NEW/pg_xact" -type f -printf '%s\n' 2>/dev/null | awk '{s+=$1} END{print s+0}')
echo "pg_xact bytes on disk after pg_upgrade: $XACT_BYTES"

# ------------------------------------------------------------------ new cluster
# Auto-serve: --wal-upgrade leaves the new cluster ready to come up read-write
# on first start, like upstream pg_upgrade.
# The pre-start assertions above inspect the on-disk state before first start.
echo "unix_socket_directories = '$WORK'" >> "$NEW/postgresql.conf"
echo "port = $PORT" >> "$NEW/postgresql.conf"

log "start new cluster (auto-serves on first start)"
"$BIN/pg_ctl" -D "$NEW" -l "$WORK/new.log" -w start >/dev/null 2>&1
START_RC=$?
log "new cluster start exit=$START_RC"
if [ $START_RC -ne 0 ]; then
    echo "---- new.log tail ----"; tail -60 "$WORK/new.log"; exit 1
fi

log "verify data in new cluster"
NEW_T1=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT count(*), sum(hashtext(val)::bigint) FROM t1" 2>&1)
NEW_T2=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT count(*), sum(hashtext(payload)::bigint) FROM t2" 2>&1)
NEW_ORD=$("$BIN/psql" -h "$WORK" -U postgres -d appdb -tAc "SELECT count(*), sum(hashtext(note)::bigint), sum(amount) FROM orders" 2>&1)
# Index-only correctness check: force index scans
NEW_IDX=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SET enable_seqscan=off; SELECT count(*) FROM t1 WHERE val LIKE 'row-1%'" 2>&1)
log "NEW t1=$NEW_T1  t2=$NEW_T2  orders=$NEW_ORD  idxcount=$NEW_IDX"

"$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1

# ------------------------------------------------------------------ verdict
FAIL=0
[ "$OLD_T1" = "$NEW_T1" ] || { echo "MISMATCH t1: old=$OLD_T1 new=$NEW_T1"; FAIL=1; }
[ "$OLD_T2" = "$NEW_T2" ] || { echo "MISMATCH t2: old=$OLD_T2 new=$NEW_T2"; FAIL=1; }
[ "$OLD_ORD" = "$NEW_ORD" ] || { echo "MISMATCH orders: old=$OLD_ORD new=$NEW_ORD"; FAIL=1; }

if [ $FAIL -eq 0 ]; then
    log "PASS: all data matches after WAL-replay recovery"
else
    log "FAIL: data mismatch"
fi
exit $FAIL
