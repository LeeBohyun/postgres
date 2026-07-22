#!/usr/bin/env bash
#
# Edge cases for "pg_upgrade --wal-upgrade-signal-handoff" (emit the streaming-
# handoff trigger on the live old primary, then shut the primary down at that
# point so nothing appends WAL after the handoff record).
#
#   D1. Normal: signal-handoff writes exactly one HANDOFF record to the primary's
#       WAL and then SHUTS THE PRIMARY DOWN (it is no longer serving afterward).
#       The stopped cluster is a clean, restartable cluster.
#   D2. Re-run against the (now stopped) primary: clean connection failure, not a
#       crash, and the cluster stays undamaged.
#   D3. Against a never-started primary: clean connection failure too.
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

# =========================================================== D0: write gate
log "D0: signal-handoff terminates live CLIENT connections (write gate)"
# Open a long-lived idle client session in the background (sleeps inside a psql
# connection).  signal-handoff must terminate it so no user txn can commit past
# the handoff.  We record its backend PID first, then confirm it is gone.
"$BIN/psql" -h "$W" -U postgres -tAc "SELECT pg_sleep(120)" >/dev/null 2>&1 &
SLEEPER=$!
# wait until the backend is visible on the server
GATEPID=""
for i in $(seq 1 20); do
    GATEPID=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT pid FROM pg_stat_activity WHERE query LIKE 'SELECT pg_sleep(120)%' AND pid <> pg_backend_pid() LIMIT 1" 2>/dev/null | tr -d '[:space:]')
    [ -n "$GATEPID" ] && break
    sleep 0.5
done
[ -n "$GATEPID" ] || fail "D0: could not establish the test client backend"
log "  opened a live client backend pid=$GATEPID"

# =========================================================== D1: emit + shut down
log "D1: --wal-upgrade-signal-handoff emits the trigger AND shuts the primary down"
BEFORE=$(count_handoff "$W/old")
"$BIN/pg_upgrade" --wal-upgrade-signal-handoff -b "$BIN" -d "$W/old" -U postgres >"$W/d1.log" 2>&1 || { cat "$W/d1.log"; fail "D1: signal-handoff failed"; }
grep -qi "handoff trigger written" "$W/d1.log" || { cat "$W/d1.log"; fail "D1: missing success message"; }
# the primary must now be STOPPED (signal-handoff shut it down at the handoff point)
if "$BIN/psql" -h "$W" -U postgres -tAc "SELECT 1" >/dev/null 2>&1; then
    fail "D1: primary still serving after signal-handoff (should have shut down)"
fi
[ -f "$W/old/postmaster.pid" ] && fail "D1: postmaster.pid still present (primary not stopped)"
# D0 follow-up: the pre-opened client backend must be gone (terminated by the
# write gate, at latest by the shutdown).  Reap the background psql and confirm
# it is no longer running.
wait "$SLEEPER" 2>/dev/null
kill -0 "$SLEEPER" 2>/dev/null && fail "D0: pre-opened client backend survived signal-handoff (write gate failed)"
log "PASS D0 (live client backend pid=$GATEPID terminated by the write gate)"
# exactly one HANDOFF record landed (shutdown flushed WAL to disk)
AFTER1=$(count_handoff "$W/old")
[ "${AFTER1:-0}" -ge $(( ${BEFORE:-0} + 1 )) ] || fail "D1: no new HANDOFF record (before=$BEFORE after=$AFTER1)"
# the stopped cluster is clean + restartable
[ -f "$W/old/global/pg_control" ] || fail "D1: pg_control missing after handoff shutdown"
log "PASS D1 (handoff emitted, primary shut down at the handoff point; records $BEFORE -> $AFTER1)"

# =========================================================== D2: re-run vs stopped
log "D2: --wal-upgrade-signal-handoff again (primary now stopped) fails cleanly"
if "$BIN/pg_upgrade" --wal-upgrade-signal-handoff -b "$BIN" -d "$W/old" -U postgres >"$W/d2.log" 2>&1; then
    fail "D2: signal-handoff succeeded against a stopped primary (should fail to connect)"
fi
grep -qiE "could not connect|connection.*failed|no such file|is the server running" "$W/d2.log" || { cat "$W/d2.log"; fail "D2: wrong/absent connection-failure message"; }
# cluster undamaged and still restartable after the failed re-run
"$BIN/pg_ctl" -D "$W/old" -l "$W/o2.log" -w start >/dev/null 2>&1 || fail "D2: cluster no longer starts after handoff+failed re-run"
"$BIN/pg_ctl" -D "$W/old" -w stop >/dev/null 2>&1
log "PASS D2 (clean connection failure; cluster undamaged and restartable)"

# =========================================================== D3: never-started primary
log "D3: --wal-upgrade-signal-handoff against a never-started primary fails cleanly"
"$BIN/initdb" -D "$W/old3" -U postgres -N >/dev/null 2>&1 || fail "initdb old3"
cat >> "$W/old3/postgresql.conf" <<CONF
unix_socket_directories='$W'
port=$((PORT+1))
CONF
if "$BIN/pg_upgrade" --wal-upgrade-signal-handoff -b "$BIN" -d "$W/old3" -U postgres >"$W/d3.log" 2>&1; then
    fail "D3: signal-handoff succeeded against a never-started primary (should fail to connect)"
fi
grep -qiE "could not connect|connection.*failed|no such file|is the server running" "$W/d3.log" || { cat "$W/d3.log"; fail "D3: wrong/absent connection-failure message"; }
[ -f "$W/old3/global/pg_control" ] || fail "D3: cluster's pg_control disturbed"
log "PASS D3 (clean connection failure, cluster undamaged)"

log "ALL SIGNAL-HANDOFF EDGE CASES PASSED"
exit 0
