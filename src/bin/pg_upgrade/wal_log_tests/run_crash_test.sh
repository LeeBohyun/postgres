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

FAIL=0

log "attempt to start NEW cluster (expect FATAL: mid-upgrade, no COMPLETE)"
echo "unix_socket_directories='$W'">>$NEW/postgresql.conf; echo "port=$P">>$NEW/postgresql.conf
"$BIN/pg_ctl" -D "$NEW" -l "$W/new.log" -w start >/dev/null 2>&1
RC=$?
if [ $RC -eq 0 ]; then
    echo "UNEXPECTED: new cluster started (should have FATALed)"
    "$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1
    FAIL=1
elif grep -qiE "failed mid-upgrade|new cluster is unusable" "$W/new.log"; then
    # Refusal must be for the RIGHT reason (mid-upgrade), not any random error.
    echo "new cluster refused to start with the mid-upgrade FATAL (good):"
    grep -iE "failed mid-upgrade|re-run pg_upgrade" "$W/new.log" | head -2
else
    echo "FAIL: new cluster did not start, but NOT with the mid-upgrade FATAL:"
    tail -6 "$W/new.log"; FAIL=1
fi

log "verify OLD cluster is still fully usable"
"$BIN/pg_ctl" -D "$OLD" -l "$W/old2.log" -w start >/dev/null 2>&1
OLD_FP2=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*), sum(hashtext(b)::bigint) FROM t" 2>&1)
"$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1
log "old cluster after: $OLD_FP2"
[ "$OLD_FP" = "$OLD_FP2" ] || { echo "FAIL: old cluster damaged (was '$OLD_FP', now '$OLD_FP2')"; FAIL=1; }

# --- Control: a NORMAL (with COMPLETE) upgrade of the same data MUST start AND
# recover the exact data -- proving the FATAL above is specific to the missing
# COMPLETE, not a general inability to start these clusters.
log "control: same upgrade WITH COMPLETE must start and recover the data"
rm -rf "$W/new2"
cd "$W"; "$BIN/pg_upgrade" -b $BIN -B $BIN -d "$OLD" -D "$W/new2" -U postgres --initdb --wal-log-upgrade --copy >"$W/up2.log" 2>&1
# --wal-log-upgrade holds the new cluster in quarantine; commit to adopt it.
"$BIN/pg_upgrade" -b $BIN -B $BIN -d "$OLD" -D "$W/new2" --commit >"$W/commit2.log" 2>&1 \
    || { echo "FAIL: control commit"; tail -20 "$W/commit2.log"; FAIL=1; }
echo "unix_socket_directories='$W'">>$W/new2/postgresql.conf; echo "port=$P">>$W/new2/postgresql.conf
if "$BIN/pg_ctl" -D "$W/new2" -l "$W/new2.log" -w start >/dev/null 2>&1; then
    CTRL_FP=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*),sum(hashtext(b)::bigint) FROM t")
    "$BIN/pg_ctl" -D "$W/new2" -w stop >/dev/null 2>&1
    log "control recovered: $CTRL_FP"
    [ "$CTRL_FP" = "$OLD_FP" ] || { echo "FAIL: control data mismatch (old '$OLD_FP' vs control '$CTRL_FP')"; FAIL=1; }
else
    echo "FAIL: control (with COMPLETE) did not start"; tail -10 "$W/new2.log"; FAIL=1
fi

[ "$FAIL" = 0 ] && log "PASS: mid-upgrade refused (correct FATAL); old cluster intact; control recovers" \
                || log "FAIL: crash-atomicity not upheld"
exit $FAIL
