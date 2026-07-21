#!/usr/bin/env bash
#
# Edge cases for "pg_upgrade --wal-signal-handoff" (emit the streaming-handoff
# trigger on the live old primary).
#
#   D1. No standbys connected: --wal-signal-handoff still succeeds and writes exactly
#       one HANDOFF record to the primary's WAL (the trigger propagates via the
#       WAL path; nobody consuming it right now is fine).
#   D2. Called twice: idempotent in effect -- each call writes another harmless
#       trigger; the primary keeps running and serving.
#   D3. Against a STOPPED primary: clean failure (cannot connect), not a crash.
#
set -u
BIN="${PGBIN:?set PGBIN}"
W=${WORK:-/tmp/pgu_handoff_edge}; PORT=${PORT:-56860}
export PGPORT=$PORT PGDATABASE=postgres
log(){ echo "=== $* ==="; }
fail(){ echo "FAIL: $*"; exit 1; }
rm -rf "$W"; mkdir -p "$W"
count_handoff(){ # $1=datadir  -> number of PG_UPGRADE_HANDOFF records in its WAL
    local lo
    lo=$(ls "$1/pg_wal/" 2>/dev/null | grep -E '^[0-9A-F]{24}$' | sort | head -1)
    [ -z "$lo" ] && { echo 0; return; }
    "$BIN/pg_waldump" -p "$1/pg_wal" -s 0/1000000 2>/dev/null | grep -c "PG_UPGRADE_HANDOFF"
}

log "build a live primary (no standby attached)"
"$BIN/initdb" -D "$W/old" -U postgres -N >/dev/null 2>&1 || fail "initdb"
cat >> "$W/old/postgresql.conf" <<CONF
unix_socket_directories='$W'
port=$PORT
wal_level=replica
max_wal_senders=5
CONF
"$BIN/pg_ctl" -D "$W/old" -l "$W/o.log" -w start >/dev/null 2>&1 || fail "start old"
"$BIN/psql" -h "$W" -U postgres -qc "CREATE TABLE t(id int); INSERT INTO t SELECT generate_series(1,100);" >/dev/null 2>&1 || fail "load"

# =========================================================== D1: no standbys
log "D1: --wal-signal-handoff with NO standbys connected"
BEFORE=$(count_handoff "$W/old")
"$BIN/pg_upgrade" --wal-signal-handoff -d "$W/old" -U postgres >"$W/d1.log" 2>&1 || { cat "$W/d1.log"; fail "D1: signal-handoff failed"; }
grep -qi "handoff trigger written" "$W/d1.log" || { cat "$W/d1.log"; fail "D1: missing success message"; }
# force the record to disk so waldump sees it, then count
"$BIN/psql" -h "$W" -U postgres -qc "CHECKPOINT" >/dev/null 2>&1
AFTER1=$(count_handoff "$W/old")
[ "${AFTER1:-0}" -ge $(( ${BEFORE:-0} + 1 )) ] || fail "D1: no new HANDOFF record (before=$BEFORE after=$AFTER1)"
# primary must still be serving
"$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*) FROM t" >/dev/null 2>&1 || fail "D1: primary stopped serving after signal-handoff"
log "PASS D1 (handoff emitted, primary still serving; records $BEFORE -> $AFTER1)"

# =========================================================== D2: called twice
log "D2: --wal-signal-handoff called again is idempotent (harmless second trigger)"
"$BIN/pg_upgrade" --wal-signal-handoff -d "$W/old" -U postgres >"$W/d2.log" 2>&1 || { cat "$W/d2.log"; fail "D2: second signal-handoff failed"; }
"$BIN/psql" -h "$W" -U postgres -qc "CHECKPOINT" >/dev/null 2>&1
AFTER2=$(count_handoff "$W/old")
[ "${AFTER2:-0}" -ge $(( ${AFTER1:-0} + 1 )) ] || fail "D2: second call wrote no additional record (was=$AFTER1 now=$AFTER2)"
"$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*) FROM t" >/dev/null 2>&1 || fail "D2: primary stopped serving"
log "PASS D2 (records $AFTER1 -> $AFTER2, primary still serving)"

"$BIN/pg_ctl" -D "$W/old" -w stop >/dev/null 2>&1

# =========================================================== D3: stopped primary
log "D3: --wal-signal-handoff against a STOPPED primary fails cleanly"
if "$BIN/pg_upgrade" --wal-signal-handoff -d "$W/old" -U postgres >"$W/d3.log" 2>&1; then
    fail "D3: signal-handoff succeeded against a stopped primary (should fail to connect)"
fi
grep -qiE "could not connect|connection.*failed|no such file|is the server running" "$W/d3.log" || { cat "$W/d3.log"; fail "D3: wrong/absent connection-failure message"; }
# the stopped cluster must be undamaged (still has a normal pg_control, startable)
[ -f "$W/old/global/pg_control" ] || fail "D3: stopped cluster's pg_control disturbed"
"$BIN/pg_ctl" -D "$W/old" -l "$W/o3.log" -w start >/dev/null 2>&1 || fail "D3: cluster no longer starts after a failed signal-handoff"
"$BIN/pg_ctl" -D "$W/old" -w stop >/dev/null 2>&1
log "PASS D3 (clean connection failure, cluster undamaged)"

log "ALL SIGNAL-HANDOFF EDGE CASES PASSED"
exit 0
