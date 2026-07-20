#!/usr/bin/env bash
# Crash-mid-upgrade atomicity test (AUTO-SERVE model).
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

log "start NEW cluster (mid-upgrade, no COMPLETE): must FATAL, never serve"
# Auto-serve atomicity model: a local window with START but no COMPLETE is a
# crash-truncated (half-built) upgrade.  Since the new cluster now auto-serves on
# a good start, PerformWalUpgradeIfNeeded() refuses to arm/replay a partial window
# and FATALs instead -- it must NOT serve a half-upgraded catalog.  (The old model
# held it in quarantine; there is no quarantine anymore.)
echo "unix_socket_directories='$W'">>$NEW/postgresql.conf; echo "port=$P">>$NEW/postgresql.conf
"$BIN/pg_ctl" -D "$NEW" -l "$W/new.log" -w start >/dev/null 2>&1
if "$BIN/psql" -h "$W" -p $P -U postgres -tAc "SELECT 1" >/dev/null 2>&1; then
    "$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1
    echo "FAIL: half-upgraded (no COMPLETE) cluster ACCEPTED a connection"; FAIL=1
elif grep -qi "pg_upgrade WAL is incomplete\|found START without COMPLETE" "$W/new.log"; then
    echo "new cluster FATALed on the partial window (good):"
    grep -i "incomplete\|START without COMPLETE" "$W/new.log" | head -1
else
    echo "FAIL: new cluster neither served nor FATALed-as-incomplete:"; tail -6 "$W/new.log"; FAIL=1
fi

log "partial cluster: COMPLETE marker absent (nothing to adopt)"
# The COMPLETE marker must be ABSENT for a crash-truncated window.
[ -e "$NEW/pg_upgrade_complete.done" ] && { echo "FAIL: COMPLETE marker present on a partial (no-COMPLETE) cluster"; FAIL=1; } || echo "COMPLETE marker absent (good)"

log "rollback the half-upgraded cluster (discard it); old must be untouched"
# old_dir is intact (--copy, never started), so rollback is allowed.
"$BIN/pg_upgrade" -b $BIN -B $BIN -d "$OLD" -D "$NEW" --wal-log-rollback >"$W/rollback.log" 2>&1 || { cat "$W/rollback.log"; echo "FAIL: rollback of half-upgraded cluster"; FAIL=1; }
[ -d "$NEW" ] && { echo "FAIL: rollback did not remove the half-upgraded new cluster"; FAIL=1; }

log "verify OLD cluster is still fully usable"
"$BIN/pg_ctl" -D "$OLD" -l "$W/old2.log" -w start >/dev/null 2>&1
OLD_FP2=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*), sum(hashtext(b)::bigint) FROM t" 2>&1)
"$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1
log "old cluster after: $OLD_FP2"
[ "$OLD_FP" = "$OLD_FP2" ] || { echo "FAIL: old cluster damaged (was '$OLD_FP', now '$OLD_FP2')"; FAIL=1; }

# --- Control: a NORMAL (with COMPLETE) upgrade of the same data MUST reach
# COMPLETE and AUTO-SERVE, recovering the exact data -- proving the FATAL above
# is specific to the missing COMPLETE, not a general inability to upgrade these
# clusters.
log "control: same upgrade WITH COMPLETE must auto-serve and recover the data"
rm -rf "$W/new2"
cd "$W"; "$BIN/pg_upgrade" -b $BIN -B $BIN -d "$OLD" -D "$W/new2" -U postgres --initdb --wal-log-upgrade --copy >"$W/up2.log" 2>&1
echo "unix_socket_directories='$W'">>$W/new2/postgresql.conf; echo "port=$P">>$W/new2/postgresql.conf
if "$BIN/pg_ctl" -D "$W/new2" -l "$W/new2.log" -w start >/dev/null 2>&1; then
    CTRL_FP=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*),sum(hashtext(b)::bigint) FROM t")
    "$BIN/pg_ctl" -D "$W/new2" -w stop >/dev/null 2>&1
    log "control recovered: $CTRL_FP"
    [ "$CTRL_FP" = "$OLD_FP" ] || { echo "FAIL: control data mismatch (old '$OLD_FP' vs control '$CTRL_FP')"; FAIL=1; }
else
    echo "FAIL: control (with COMPLETE) did not auto-serve"; tail -10 "$W/new2.log"; FAIL=1
fi

[ "$FAIL" = 0 ] && log "PASS: mid-upgrade FATALed (never served) + rolled back; old cluster intact; control auto-serves" \
                || log "FAIL: crash-atomicity not upheld"
exit $FAIL
