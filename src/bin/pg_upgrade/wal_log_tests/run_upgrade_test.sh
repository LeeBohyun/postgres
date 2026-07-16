#!/usr/bin/env bash
#
# End-to-end test harness for pg_upgrade --wal-log-upgrade.
#
# Creates an "old" cluster with real data, runs pg_upgrade --wal-log-upgrade
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

"$BIN/pg_ctl" -D "$OLD" -l "$WORK/old.log" -w start >/dev/null 2>&1 || { echo FAIL start old; cat "$WORK/old.log"; exit 1; }

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
log "run pg_upgrade --wal-log-upgrade --initdb (mode=${MODE:---copy})"
cd "$WORK"
"$BIN/pg_upgrade" \
    -b "$BIN" -B "$BIN" \
    -d "$OLD" -D "$NEW" \
    -U postgres \
    --initdb --wal-log-upgrade ${MODE:---copy} \
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
NPGU=$("$BIN/pg_waldump" -p "$NEW/pg_wal" -s "${LOLSN:-0/0}" 2>/dev/null | grep -icE "PG_UPGRADE_START|PG_UPGRADE_COMPLETE|UPGRADE_RELFILE|UPGRADE_SLRU|UPGRADE_DIRSKEL")
log "RM_PG_UPGRADE record count in pg_wal/: $NPGU"
[ "${NPGU:-0}" -ge 2 ] || { echo "FAIL: upgrade WAL not found in pg_wal/ (got $NPGU records)"; exit 1; }

# --- PROOF the disk writes were skipped: user relfiles + pg_xact must be wiped.
# These are ASSERTIONS, not just prints: if the data were still on disk the
# data-match below would not prove WAL replay.
log "verify disk writes were skipped (data files wiped to baseline)"
BIGGEST=$(find "$NEW/base" -type f -regextype posix-extended -regex '.*/[0-9]+(_fsm|_vm)?(\.[0-9]+)?' -printf '%s %p\n' 2>/dev/null | sort -rn | head -1)
echo "largest data file on disk after pg_upgrade: $BIGGEST"
# A restored cluster of this size has multi-MB user tables; if the skip worked
# every base/ main-fork data file is 0 bytes (relfilenodes were unlinked).
TOTAL_BASE=$(find "$NEW/base" -type f -regextype posix-extended -regex '.*/[0-9]+(\.[0-9]+)?' -printf '%s\n' 2>/dev/null | awk '{s+=$1} END{print s+0}')
echo "total bytes in base/ main-fork data files after pg_upgrade: $TOTAL_BASE"
[ "${TOTAL_BASE:-0}" = "0" ] || { echo "FAIL: base/ data not wiped ($TOTAL_BASE bytes) — WAL-replay claim unproven"; exit 1; }
XACT_BYTES=$(find "$NEW/pg_xact" -type f -printf '%s\n' 2>/dev/null | awk '{s+=$1} END{print s+0}')
echo "pg_xact bytes on disk after pg_upgrade (should be 0 = skipped): $XACT_BYTES"
[ "${XACT_BYTES:-0}" = "0" ] || { echo "FAIL: pg_xact not wiped ($XACT_BYTES bytes) — WAL-replay claim unproven"; exit 1; }

# ------------------------------------------------------------------ new cluster
# --wal-log-upgrade holds the new cluster in quarantine.  All the pre-start
# assertions above (upgrade WAL present, disk wiped) inspect that held state.
# Commit now to adopt it, which replays the window and brings it live.
echo "unix_socket_directories = '$WORK'" >> "$NEW/postgresql.conf"
echo "port = $PORT" >> "$NEW/postgresql.conf"

# Hold-start: first start applies the WAL window, reconstructs, and holds
# in quarantine (pg_ctl returns non-zero by design as it exits at the hold).
"$BIN/pg_ctl" -D "$NEW" -l "$WORK/hold.log" -w start >/dev/null 2>&1 || true

log "pg_upgrade --commit"
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" --commit > "$WORK/commit.log" 2>&1 \
    || { echo "---- commit.log tail ----"; tail -20 "$WORK/commit.log"; exit 1; }

log "start new cluster (triggers WAL-replay recovery)"
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
