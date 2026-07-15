#!/usr/bin/env bash
#
# Lifecycle test for the revertable --wal-log-upgrade feature.
#
# Exercises the full state machine on a primary:
#
#   upgrade  -> new cluster is QUARANTINED (held, not serving)
#   status   -> reports QUARANTINED
#   ROLLBACK -> new discarded, old cluster still starts and serves (untouched)
#   upgrade  -> QUARANTINED again
#   COMMIT   -> new goes live, data matches; old stamped superseded
#   delete-old (before commit) -> refused
#   delete-old (after commit)  -> removes old cluster
#
# Proves: (a) the hold actually happens, (b) rollback restores the old cluster,
# (c) commit adopts the new cluster with data intact, (d) --delete-old is gated
# on the superseded stamp.
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
    local rc=$?
    # The run must emit the lifecycle scripts (paths baked in), like
    # delete_old_cluster.sh, so the operator need not re-type -b/-B/-d/-D.
    [ $rc -eq 0 ] && { [ -x "$WORK/pg_upgrade_commit.sh" ] || { echo "FAIL: pg_upgrade_commit.sh not generated"; return 1; }
                       [ -x "$WORK/pg_upgrade_rollback.sh" ] || { echo "FAIL: pg_upgrade_rollback.sh not generated"; return 1; }; }
    return $rc
}

rm -rf "$WORK"; mkdir -p "$WORK"

# ============================================================ Scenario 1: HOLD
log "Scenario 1: upgrade must leave new cluster QUARANTINED (held)"
OLD=$WORK/old1; NEW=$WORK/new1
make_old "$OLD"
OLD_T1=$("$BIN/pg_ctl" -D "$OLD" -l "$WORK/o.log" -w start >/dev/null 2>&1; \
         "$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT count(*),sum(hashtext(val)::bigint) FROM t1"; \
         "$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1)
run_upgrade "$OLD" "$NEW" || { tail -30 "$WORK/upgrade.log"; fail "upgrade exited nonzero"; }

# In the new model the upgrade leaves the window PENDING in pg_wal/ (not yet
# reconstructed).  The FIRST start reconstructs the cluster, writes its
# end-of-recovery checkpoint, then HOLDS in quarantine (not serving).
echo "unix_socket_directories = '$WORK'" >> "$NEW/postgresql.conf"
echo "port = $PORT" >> "$NEW/postgresql.conf"
"$BIN/pg_ctl" -D "$NEW" -l "$WORK/new_hold.log" -w start >/dev/null 2>&1
# It must NOT serve while held: no connection should be possible.
if "$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT 1" >/dev/null 2>&1; then
    "$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1
    fail "held cluster accepted a connection (should be held/dark)"
fi
grep -qi "holding in quarantine" "$WORK/new_hold.log" \
    || fail "expected quarantine-hold message in startup log"
STATE=$(db_state "$NEW")
log "new cluster state after first start: '$STATE'"
echo "$STATE" | grep -qi "quarantine" || fail "new cluster is not quarantined (got '$STATE')"
# A restart must re-hold (stay quarantined), not serve or re-replay.
"$BIN/pg_ctl" -D "$NEW" -l "$WORK/new_rehold.log" -w start >/dev/null 2>&1
if "$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT 1" >/dev/null 2>&1; then
    "$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1
    fail "held cluster served on restart (should re-hold)"
fi
echo "$(db_state "$NEW")" | grep -qi "quarantine" \
    || fail "state not quarantined after restart (got '$(db_state "$NEW")')"
log "PASS: first start reconstructed + held; restart re-held (never served)"

# =========================================================== Scenario 2: ROLLBACK
log "Scenario 2: rollback discards new, old still serves"
"$BIN/pg_upgrade" -D "$NEW" --rollback > "$WORK/rollback.log" 2>&1 || { cat "$WORK/rollback.log"; fail "rollback exited nonzero"; }
[ -d "$NEW" ] && fail "rollback did not remove new cluster dir"
# old cluster must still start and have the data
"$BIN/pg_ctl" -D "$OLD" -l "$WORK/old2.log" -w start >/dev/null 2>&1 || fail "old cluster did not start after rollback"
AFTER_T1=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT count(*),sum(hashtext(val)::bigint) FROM t1")
"$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1
[ "$OLD_T1" = "$AFTER_T1" ] || fail "old cluster data changed after rollback (old=$OLD_T1 after=$AFTER_T1)"
log "PASS: rollback restored the old cluster intact"

# =========================================================== Scenario 3: COMMIT
log "Scenario 3: upgrade again, then commit (directly from a pending cluster)"
NEW=$WORK/new3
run_upgrade "$OLD" "$NEW" || { tail -30 "$WORK/upgrade.log"; fail "second upgrade exited nonzero"; }
# New model: the window is pending until first start.  We commit directly here
# (without a prior hold-start), exercising commit-from-pending: commit's start
# reconstructs the cluster and, seeing the commit sentinel, goes straight live.
echo "unix_socket_directories = '$WORK'" >> "$NEW/postgresql.conf"
echo "port = $PORT" >> "$NEW/postgresql.conf"

# --delete-old BEFORE commit must be refused (old not yet superseded)
if "$BIN/pg_upgrade" -d "$OLD" --delete-old > "$WORK/del_early.log" 2>&1; then
    fail "--delete-old succeeded before commit (should be refused)"
fi
[ -d "$OLD" ] || fail "--delete-old removed old dir despite being refused"
log "PASS: --delete-old refused before commit"

# commit
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" --commit > "$WORK/commit.log" 2>&1 || { cat "$WORK/commit.log"; fail "commit exited nonzero"; }
STATE=$(db_state "$NEW")
log "new cluster state after commit: '$STATE'"
echo "$STATE" | grep -qi "production\|shut down" || fail "committed cluster not in a live/normal state (got '$STATE')"

# new cluster must now serve, with the data intact
"$BIN/pg_ctl" -D "$NEW" -l "$WORK/new_live.log" -w start >/dev/null 2>&1 || { tail -30 "$WORK/new_live.log"; fail "committed cluster did not start"; }
NEW_T1=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT count(*),sum(hashtext(val)::bigint) FROM t1" 2>&1)
"$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1
[ "$OLD_T1" = "$NEW_T1" ] || fail "committed cluster data mismatch (old=$OLD_T1 new=$NEW_T1)"
log "PASS: commit adopted the new cluster with data intact"

# old must now be stamped superseded
[ -f "$OLD/global/pg_control.old" ] || fail "commit did not stamp old cluster superseded"
[ -f "$OLD/global/pg_control" ] && fail "old cluster still has a live pg_control after commit"
log "PASS: old cluster stamped superseded"

# ======================================================= Scenario 4: DELETE-OLD
log "Scenario 4: delete-old after commit"
"$BIN/pg_upgrade" -d "$OLD" --delete-old > "$WORK/del.log" 2>&1 || { cat "$WORK/del.log"; fail "--delete-old exited nonzero"; }
[ -d "$OLD" ] && fail "--delete-old did not remove old dir"
log "PASS: delete-old removed the superseded old cluster"

log "ALL SCENARIOS PASSED"
exit 0
