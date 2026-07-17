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

log "start NEW cluster (mid-upgrade, no COMPLETE): must HOLD in quarantine, never serve"
# New atomicity model: there is no COMPLETE pre-scan FATAL.  A crash-truncated
# window (START but no COMPLETE) is armed and the partial window is replayed, but
# the cluster is HELD in quarantine at the end-of-recovery hold and NEVER goes
# live.  Because it never reaches COMPLETE, "--wal-log-commit" would refuse; the operator
# discards it with "--wal-log-rollback".  So a half-upgraded cluster never serves --
# atomicity via quarantine + rollback, not via a pre-scan refusal.
echo "unix_socket_directories='$W'">>$NEW/postgresql.conf; echo "port=$P">>$NEW/postgresql.conf
"$BIN/pg_ctl" -D "$NEW" -l "$W/new.log" -w start >/dev/null 2>&1
# It must NOT be serving.
if "$BIN/psql" -h "$W" -p $P -U postgres -tAc "SELECT 1" >/dev/null 2>&1; then
    "$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1
    echo "FAIL: half-upgraded (no COMPLETE) cluster ACCEPTED a connection"; FAIL=1
elif grep -qi "holding in quarantine" "$W/new.log"; then
    echo "new cluster held in quarantine without COMPLETE (good, did not go live):"
    grep -i "holding in quarantine" "$W/new.log" | head -1
else
    echo "FAIL: new cluster neither served nor held in quarantine:"; tail -6 "$W/new.log"; FAIL=1
fi

log "partial cluster: held in quarantine, COMPLETE marker absent (cannot be committed)"
# State is visible via pg_controldata (no --status flag).
st=$("$BIN/pg_controldata" -D "$NEW" | grep -i "cluster state" | sed 's/.*: *//')
case "$st" in *quarantine*) echo "control state: $st (good)";; *) echo "FAIL: partial cluster not in quarantine (state='$st')"; FAIL=1;; esac
# The COMPLETE marker must be ABSENT for a crash-truncated window.
[ -e "$NEW/pg_upgrade_complete.done" ] && { echo "FAIL: COMPLETE marker present on a partial (no-COMPLETE) cluster"; FAIL=1; }

log "--wal-log-commit MUST REFUSE the partial cluster (quarantined but not fully replayed)"
if "$BIN/pg_upgrade" -b $BIN -B $BIN -d "$OLD" -D "$NEW" --wal-log-commit >"$W/commit_partial.log" 2>&1; then
    echo "FAIL: --wal-log-commit finalized a partial (crash-truncated) cluster -- must have refused:"; cat "$W/commit_partial.log"; FAIL=1
else
    grep -qi "did not fully replay\|PARTIAL\|crash-truncated" "$W/commit_partial.log" \
        && echo "commit refused the partial cluster (good):" && grep -i "did not fully replay" "$W/commit_partial.log" | head -1 \
        || { echo "FAIL: --wal-log-commit refused but for the wrong reason:"; cat "$W/commit_partial.log"; FAIL=1; }
    # Refusing must NOT have touched the old cluster (no superseded stamp).
    [ -e "$OLD/global/pg_control.old" ] && { echo "FAIL: refused commit still stamped the old cluster superseded"; FAIL=1; }
fi

log "rollback the half-upgraded cluster (discard it); old must be untouched"
"$BIN/pg_upgrade" -D "$NEW" --wal-log-rollback >"$W/rollback.log" 2>&1 || { cat "$W/rollback.log"; echo "FAIL: rollback of half-upgraded cluster"; FAIL=1; }
[ -d "$NEW" ] && { echo "FAIL: rollback did not remove the half-upgraded new cluster"; FAIL=1; }

log "verify OLD cluster is still fully usable"
"$BIN/pg_ctl" -D "$OLD" -l "$W/old2.log" -w start >/dev/null 2>&1
OLD_FP2=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*), sum(hashtext(b)::bigint) FROM t" 2>&1)
"$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1
log "old cluster after: $OLD_FP2"
[ "$OLD_FP" = "$OLD_FP2" ] || { echo "FAIL: old cluster damaged (was '$OLD_FP', now '$OLD_FP2')"; FAIL=1; }

# --- Control: a NORMAL (with COMPLETE) upgrade of the same data MUST reach
# COMPLETE, commit, and recover the exact data -- proving the quarantine hold
# above is specific to the missing COMPLETE, not a general inability to upgrade
# these clusters.
log "control: same upgrade WITH COMPLETE must start and recover the data"
rm -rf "$W/new2"
cd "$W"; "$BIN/pg_upgrade" -b $BIN -B $BIN -d "$OLD" -D "$W/new2" -U postgres --initdb --wal-log-upgrade --copy >"$W/up2.log" 2>&1
echo "unix_socket_directories='$W'">>$W/new2/postgresql.conf; echo "port=$P">>$W/new2/postgresql.conf
# --wal-log-upgrade holds the new cluster in quarantine.  Hold-start it (applies
# the window, reconstructs, holds; pg_ctl exits non-zero by design), then commit.
"$BIN/pg_ctl" -D "$W/new2" -l "$W/new2_hold.log" -w start >/dev/null 2>&1 || true
"$BIN/pg_upgrade" -b $BIN -B $BIN -d "$OLD" -D "$W/new2" --wal-log-commit >"$W/commit2.log" 2>&1 \
    || { echo "FAIL: control commit"; tail -20 "$W/commit2.log"; FAIL=1; }
if "$BIN/pg_ctl" -D "$W/new2" -l "$W/new2.log" -w start >/dev/null 2>&1; then
    CTRL_FP=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*),sum(hashtext(b)::bigint) FROM t")
    "$BIN/pg_ctl" -D "$W/new2" -w stop >/dev/null 2>&1
    log "control recovered: $CTRL_FP"
    [ "$CTRL_FP" = "$OLD_FP" ] || { echo "FAIL: control data mismatch (old '$OLD_FP' vs control '$CTRL_FP')"; FAIL=1; }
else
    echo "FAIL: control (with COMPLETE) did not start"; tail -10 "$W/new2.log"; FAIL=1
fi

[ "$FAIL" = 0 ] && log "PASS: mid-upgrade held in quarantine (never served) + rolled back; old cluster intact; control recovers" \
                || log "FAIL: crash-atomicity not upheld"
exit $FAIL
