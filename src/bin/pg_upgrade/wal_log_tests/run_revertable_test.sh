#!/usr/bin/env bash
#
# Lifecycle test for the revertable --wal-log-upgrade feature (AUTO-SERVE model).
#
# Exercises the primary lifecycle after the commit/quarantine gate was removed:
#
#   upgrade   -> new cluster is a normal "shut down" cluster (NOT quarantined)
#   start     -> AUTO-SERVES read-write on first start, no --wal-log-commit
#   ROLLBACK  -> new discarded, old cluster still starts and serves (untouched),
#                allowed even after the new cluster was started (with a warning)
#   delete-old -> removes the old cluster once the operator has adopted new
#
# Proves: (a) the new cluster auto-serves like upstream (no hold, no commit),
# (b) rollback is gated on old_dir integrity and restores the old cluster,
# (c) data matches after auto-serve, (d) --wal-log-delete-old removes old_dir.
#
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
BIN="${PGBIN:-$ROOT/tmp_install/bin}"
export DYLD_LIBRARY_PATH="${DYLD_LIBRARY_PATH:-}:$ROOT/tmp_install/lib"
WORK=${WORK:-/tmp/pgu_rev}
PORT=${PORT:-55444}
export PGPORT=$PORT
export PGDATABASE=postgres

log() { echo "=== $* ==="; }
fail() { echo "FAIL: $*"; exit 1; }

# ---- helpers ---------------------------------------------------------------
db_state() { "$BIN/pg_controldata" -D "$1" 2>/dev/null | grep -i "Database cluster state" | sed 's/.*: *//'; }

make_old() {
    local OLD=$1
    "$BIN/initdb" -D "$OLD" -U postgres -N >/dev/null 2>&1 || fail "initdb old"
    echo "unix_socket_directories = '$WORK'" >> "$OLD/postgresql.conf"
    echo "port = $PORT" >> "$OLD/postgresql.conf"
    "$BIN/pg_ctl" -D "$OLD" -l "$WORK/old.log" -w start >/dev/null 2>&1 || fail "start old"
    "$BIN/psql" -h "$WORK" -U postgres -v ON_ERROR_STOP=1 >/dev/null 2>&1 <<'SQL' || fail "load"
CREATE TABLE t1 (id int primary key, val text);
INSERT INTO t1 SELECT g, 'row-'||g FROM generate_series(1,20000) g;
CREATE INDEX t1_val_idx ON t1(val);
SQL
    "$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1
}

run_upgrade() {
    local OLD=$1 NEW=$2
    ( cd "$WORK" && "$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" \
        -U postgres --initdb --wal-log-upgrade --copy > "$WORK/upgrade.log" 2>&1 )
    return $?
}

rm -rf "$WORK"; mkdir -p "$WORK"

# ====================================================== Scenario 1: AUTO-SERVE
log "Scenario 1: upgrade leaves a normal cluster that auto-serves on first start"
OLD=$WORK/old1; NEW=$WORK/new1
make_old "$OLD"
OLD_T1=$("$BIN/pg_ctl" -D "$OLD" -l "$WORK/o.log" -w start >/dev/null 2>&1; \
         "$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT count(*),sum(hashtext(val)::bigint) FROM t1"; \
         "$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1)
run_upgrade "$OLD" "$NEW" || { tail -30 "$WORK/upgrade.log"; fail "upgrade exited nonzero"; }

# The upgraded new cluster is a NORMAL, cleanly shut-down cluster on disk -- NOT
# quarantined.  It comes up read-write on first start, exactly like upstream.
echo "unix_socket_directories = '$WORK'" >> "$NEW/postgresql.conf"
echo "port = $PORT" >> "$NEW/postgresql.conf"
STATE=$(db_state "$NEW")
log "new cluster state after upgrade: '$STATE'"
echo "$STATE" | grep -qi "quarantine" && fail "new cluster is quarantined; auto-serve expected (got '$STATE')"
echo "$STATE" | grep -qi "shut down" || fail "new cluster not cleanly shut down (got '$STATE')"

# First start must SUCCEED and serve (no --wal-log-commit).
"$BIN/pg_ctl" -D "$NEW" -l "$WORK/new_live.log" -w start >/dev/null 2>&1 \
    || { tail -30 "$WORK/new_live.log"; fail "new cluster did not auto-serve on first start"; }
"$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT 1" >/dev/null 2>&1 \
    || fail "new cluster started but did not accept a connection"
NEW_T1=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT count(*),sum(hashtext(val)::bigint) FROM t1" 2>&1)
"$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1
[ "$OLD_T1" = "$NEW_T1" ] || fail "auto-served cluster data mismatch (old=$OLD_T1 new=$NEW_T1)"
log "PASS: new cluster auto-served on first start with data intact (no commit)"

# ====================================================== Scenario 2: ROLLBACK
# Rollback is now gated on old_dir being intact, NOT on "before first write".
# It is allowed even though we already started (and could have written to) the
# new cluster above -- discarding those changes with a warning.
log "Scenario 2: rollback discards new (even after it served), old still serves"
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" --wal-log-rollback > "$WORK/rollback.log" 2>&1 \
    || { cat "$WORK/rollback.log"; fail "rollback exited nonzero"; }
grep -qi "WARNING" "$WORK/rollback.log" || fail "rollback did not warn about discarding new-cluster changes"
[ -d "$NEW" ] && fail "rollback did not remove new cluster dir"
# old cluster must still start and have the data (it was frozen throughout).
"$BIN/pg_ctl" -D "$OLD" -l "$WORK/old2.log" -w start >/dev/null 2>&1 || fail "old cluster did not start after rollback"
AFTER_T1=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT count(*),sum(hashtext(val)::bigint) FROM t1")
"$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1
[ "$OLD_T1" = "$AFTER_T1" ] || fail "old cluster data changed after rollback (old=$OLD_T1 after=$AFTER_T1)"
log "PASS: rollback (old_dir intact) restored the old cluster untouched"

# ====================================================== Scenario 3: ADOPT + DELETE-OLD
# Adopting the new cluster is just: upgrade, then start it (auto-serve).  Once
# adopted, --wal-log-delete-old removes the now-unneeded old cluster.
log "Scenario 3: upgrade again, auto-serve, then delete-old removes old_dir"
NEW=$WORK/new3
run_upgrade "$OLD" "$NEW" || { tail -30 "$WORK/upgrade.log"; fail "second upgrade exited nonzero"; }
echo "unix_socket_directories = '$WORK'" >> "$NEW/postgresql.conf"
echo "port = $PORT" >> "$NEW/postgresql.conf"
"$BIN/pg_ctl" -D "$NEW" -l "$WORK/new3_live.log" -w start >/dev/null 2>&1 \
    || { tail -30 "$WORK/new3_live.log"; fail "second new cluster did not auto-serve"; }
NEW3_T1=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT count(*),sum(hashtext(val)::bigint) FROM t1" 2>&1)
"$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1
[ "$OLD_T1" = "$NEW3_T1" ] || fail "second auto-served cluster data mismatch (old=$OLD_T1 new=$NEW3_T1)"
log "PASS: second upgrade auto-served with data intact"

log "Scenario 4: delete-old removes the old cluster"
# delete-old now requires -D too, to confirm a completed new cluster exists
# before removing the old one (there is no commit stamp anymore).
"$BIN/pg_upgrade" -d "$OLD" -D "$NEW" --wal-log-delete-old > "$WORK/del.log" 2>&1 || { cat "$WORK/del.log"; fail "--wal-log-delete-old exited nonzero"; }
[ -d "$OLD" ] && fail "--wal-log-delete-old did not remove old dir"
log "PASS: delete-old removed the old cluster"

log "ALL SCENARIOS PASSED"
exit 0
