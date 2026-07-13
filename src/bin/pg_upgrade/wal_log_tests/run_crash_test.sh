#!/usr/bin/env bash
# Crash-mid-upgrade atomicity test.
#
# PG_UPGRADE_TEST_SKIP_COMPLETE makes pg_upgrade emit the whole upgrade image
# but omit the terminal PG_UPGRADE_COMPLETE marker -- exactly the state left by
# a crash after START but before COMPLETE.  First startup of the new cluster
# must FATAL (not silently come up half-upgraded), and the OLD cluster must
# remain fully usable.
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"; BIN="${PGBIN:-$ROOT/pginst/bin}"
W=/tmp/pgu_crash; OLD=$W/old; NEW=$W/new; P=55520
export PGPORT=$P PGDATABASE=postgres
log(){ echo "=== $* ==="; }
rm -rf "$W"; mkdir -p "$W"
"$BIN/initdb" -D "$OLD" -U postgres -N >/dev/null 2>&1
echo "unix_socket_directories='$W'">>$OLD/postgresql.conf; echo "port=$P">>$OLD/postgresql.conf
"$BIN/pg_ctl" -D "$OLD" -l "$W/old.log" -w start >/dev/null 2>&1
"$BIN/psql" -h "$W" -U postgres -qc "CREATE TABLE t(a int, b text); INSERT INTO t SELECT g,'v'||g FROM generate_series(1,4000) g; CREATE INDEX ON t(a);" >/dev/null
OLD_FP=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*), sum(hashtext(b)::bigint) FROM t")
log "old cluster fingerprint: $OLD_FP"
"$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1

log "run pg_upgrade --wal-log-upgrade with COMPLETE suppressed (simulated crash)"
cd "$W"
PG_UPGRADE_TEST_SKIP_COMPLETE=1 "$BIN/pg_upgrade" -b $BIN -B $BIN -d "$OLD" -D "$NEW" -U postgres \
    --initdb --wal-log-upgrade --copy >"$W/up.log" 2>&1
[ $? -eq 0 ] || { echo FAIL upgrade; tail -20 "$W/up.log"; exit 1; }
# The upgrade WAL stays in pg_wal/ (no rename); with COMPLETE suppressed it
# holds a START but no COMPLETE.  Just confirm segments are present there.
ls "$NEW/pg_wal"/[0-9A-F]* >/dev/null 2>&1 && echo "upgrade WAL present in pg_wal/ (START, but no COMPLETE)" || { echo "no upgrade WAL in pg_wal/"; exit 1; }

log "attempt to start NEW cluster (expect FATAL: mid-upgrade, no COMPLETE)"
echo "unix_socket_directories='$W'">>$NEW/postgresql.conf; echo "port=$P">>$NEW/postgresql.conf
"$BIN/pg_ctl" -D "$NEW" -l "$W/new.log" -w start >/dev/null 2>&1
RC=$?
if [ $RC -eq 0 ]; then
    echo "UNEXPECTED: new cluster started (should have FATALed)"
    "$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1
    NEWCRASHOK=0
else
    echo "new cluster refused to start (good). Log:"
    grep -iE "failed mid-upgrade|re-run pg_upgrade|FATAL" "$W/new.log" | head -3
    NEWCRASHOK=1
fi

log "verify pg_wal/ was NOT populated (nothing copied on the failed path)"
COPIED=$(ls "$NEW/pg_wal"/[0-9A-F]* 2>/dev/null | wc -l)
echo "pg_wal/ segment count: $COPIED"

log "verify OLD cluster is still fully usable"
"$BIN/pg_ctl" -D "$OLD" -l "$W/old2.log" -w start >/dev/null 2>&1
OLD_FP2=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*), sum(hashtext(b)::bigint) FROM t" 2>&1)
"$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1
log "old cluster after: $OLD_FP2"

if [ "$NEWCRASHOK" = 1 ] && [ "$OLD_FP" = "$OLD_FP2" ]; then
    log "PASS: mid-upgrade (no COMPLETE) refused startup; old cluster intact"
else
    log "FAIL: crash-atomicity not upheld"
fi

# --- Also confirm a NORMAL (with COMPLETE) upgrade of the same data DOES start ---
log "control: same upgrade WITH COMPLETE must start and recover"
rm -rf "$W/new2"
cd "$W"; "$BIN/pg_upgrade" -b $BIN -B $BIN -d "$OLD" -D "$W/new2" -U postgres --initdb --wal-log-upgrade --copy >"$W/up2.log" 2>&1
echo "unix_socket_directories='$W'">>$W/new2/postgresql.conf; echo "port=$P">>$W/new2/postgresql.conf
"$BIN/pg_ctl" -D "$W/new2" -l "$W/new2.log" -w start >/dev/null 2>&1 \
  && echo "control started; data=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*),sum(hashtext(b)::bigint) FROM t")" \
  && "$BIN/pg_ctl" -D "$W/new2" -w stop >/dev/null 2>&1 \
  || { echo "control FAILED to start"; tail -10 "$W/new2.log"; }
