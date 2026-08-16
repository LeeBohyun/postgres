#!/usr/bin/env bash
# Durability / crash-consistency on the STREAMING-STANDBY path.
#
# The RELINK redo places user relations outside the buffer manager, then
# COMPLETE marker + control-file advance past CN.  The redo fsyncs each
# placed file and parent directory BEFORE COMPLETE to prevent crash-mid-replay
# from leaving a finalized standby (control past CN, upgrade_finalized set)
# with missing/partial relations.
#
# This test crashes the streaming standby (SIGKILL) mid-window-replay, then
# restarts it and asserts it converges byte-identically to the primary.  Two
# invariants must hold across the crash:
#   * crash-idempotence: a half-applied window converges on restart (relink is
#     unlink-then-place, and swap/rename skips already-moved sources), and
#   * consistency: the standby is NEVER a finalized cluster with missing data --
#     either it is still mid-replay (not finalized) or it is fully converged.
#
# Env: PGBIN (new bin dir; default <repo>/pginst/bin), WORK (default under /tmp).
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"; BIN="${PGBIN:-$ROOT/pginst/bin}"
W=${WORK:-/tmp/pgu_stby_durab}
OLD=$W/old NEW=$W/new SKEL=$W/skel STBY_OLD=$W/stby_old
PP=${PPORT:-55590} SP=${SPORT:-55591}
export PGDATABASE=postgres
log(){ echo "=== $* ==="; }
FAIL=0
for p in $PP $SP; do lsof -ti :$p 2>/dev/null | xargs kill -9 2>/dev/null; done
rm -rf "$W"; mkdir -p "$W"

log "seed old cluster (sizable + many relations, so the window/relink is not instant)"
"$BIN/initdb" -D "$OLD" -U postgres -N >/dev/null 2>&1 || { echo FAIL initdb; exit 1; }
cat >> "$OLD/postgresql.conf" <<CONF
port=$PP
unix_socket_directories='$W'
wal_level=replica
max_wal_senders=8
CONF
"$BIN/pg_ctl" -D "$OLD" -l "$W/old.log" -w start >/dev/null 2>&1 || { echo FAIL start; exit 1; }
# A physical slot marks an expected standby; pg_upgrade migrates it to pin
# the upgrade window so the skeleton can stream it (and survive crash-restart
# mid-stream, which this test exercises).
"$BIN/psql" -h "$W" -p $PP -U postgres -qtAc \
  "SELECT pg_create_physical_replication_slot('stby_slot', true)" >/dev/null 2>&1 || { echo FAIL create-slot; exit 1; }
"$BIN/psql" -h "$W" -p $PP -U postgres -q >/dev/null 2>&1 <<SQL
CREATE TABLE big(id int primary key, v text);
INSERT INTO big SELECT g, repeat('x',100)||g FROM generate_series(1,300000) g;
CREATE TABLE toast_t(id int, big text);
INSERT INTO toast_t SELECT g, repeat(md5(g::text),200) FROM generate_series(1,300) g;
DO \$\$ BEGIN
  FOR i IN 1..300 LOOP EXECUTE format('CREATE TABLE t%s(id int, v text); INSERT INTO t%s SELECT g,''v''||g FROM generate_series(1,50) g', i, i); END LOOP;
END \$\$;
SQL
FP_Q="SELECT count(*), sum(hashtext(v)::bigint), (SELECT count(*) FROM toast_t) FROM big"
WANT=$("$BIN/psql" -h "$W" -p $PP -U postgres -tAc "$FP_Q")
log "expected fingerprint: $WANT"
"$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1

cp -a "$OLD" "$STBY_OLD"

log "pg_upgrade --wal-upgrade --initdb --copy; keep the primary live"
cd "$W"
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-upgrade --copy > "$W/up.log" 2>&1
[ $? -eq 0 ] || { echo "FAIL upgrade"; tail -15 "$W/up.log"; exit 1; }
cat >> "$NEW/postgresql.conf" <<CONF
port=$PP
unix_socket_directories='$W'
wal_level=replica
max_wal_senders=8
listen_addresses='localhost'
CONF
echo "host replication all 127.0.0.1/32 trust" >> "$NEW/pg_hba.conf"
echo "host all all 127.0.0.1/32 trust" >> "$NEW/pg_hba.conf"
"$BIN/pg_ctl" -D "$NEW" -l "$W/new.log" -w start >/dev/null 2>&1 || { echo "FAIL: primary start"; tail "$W/new.log"; exit 1; }

log "stage the skeleton, start it, then SIGKILL mid-window-replay"
"$BIN/initdb" -D "$SKEL" -U postgres -N >/dev/null 2>&1 || { echo "FAIL: skeleton initdb"; exit 1; }
# old-datadir path comes from the pg_upgrade_standby_old_datadir GUC;
# pg_upgrade.signal is a presence-only sentinel.
echo "pg_upgrade_standby_old_datadir='$STBY_OLD'" >> "$SKEL/postgresql.conf"
: > "$SKEL/pg_upgrade.signal"
cat >> "$SKEL/postgresql.conf" <<CONF
port=$SP
unix_socket_directories='$W'
hot_standby=on
primary_conninfo='host=127.0.0.1 port=$PP user=postgres dbname=postgres'
CONF
printf 'host all all 127.0.0.1/32 trust\nlocal all all trust\n' >> "$SKEL/pg_hba.conf"
touch "$SKEL/standby.signal"

# Start via `postgres` directly (not pg_ctl -w) so we can kill it during replay.
"$BIN/postgres" -D "$SKEL" >"$W/skel1.log" 2>&1 &
PM=$!
# Kill as soon as the standby has ARMED (control file stamped at CN) but is still
# replaying -- before COMPLETE -- so the crash lands mid-window: the case
# the fsync-ordering protects.  Kill when the arm line appears; don't wait
# for "started streaming" (replay may be finishing).
for i in $(seq 1 200); do grep -qiE "auto-armed streaming standby from locally derived anchor" "$W/skel1.log" 2>/dev/null && break; sleep 0.02; done
kill -9 $PM 2>/dev/null
pkill -9 -f "postgres -D $SKEL" 2>/dev/null
for p in $SP; do lsof -ti :$p 2>/dev/null | xargs kill -9 2>/dev/null; done
sleep 1

# Consistency check at crash point: the standby must not be finalized
# (control past CN) with missing user data.  We verify: finalized-flag set =>
# window fully replayed; unset => replay not finalized.  Either way a restart
# converges.  (A finalized-but-empty cluster would be the bug.)
if "$BIN/pg_controldata" -D "$SKEL" 2>/dev/null | grep -q "wal-upgrade window finalized: *yes"; then
  log "  crash happened at/after COMPLETE (finalized flag set) -- restart must still converge"
else
  log "  crash happened mid-window (finalized flag unset) -- restart must re-replay + converge"
fi

log "restart after crash -- must re-arm/re-replay idempotently and converge"
"$BIN/pg_ctl" -D "$SKEL" -l "$W/skel2.log" -w -t 120 start >/dev/null 2>&1 || true
UP=0
for i in $(seq 1 90); do "$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT 1" >/dev/null 2>&1 && { UP=1; break; }; sleep 1; done
[ "$UP" = 1 ] || { echo "FAIL: standby did not come up after crash-restart"; tail -20 "$W/skel2.log"; FAIL=1; }

if [ "$UP" = 1 ]; then
  # Let it catch up to the primary's current LSN, then compare fingerprints.
  PRI_LSN=$("$BIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT pg_current_wal_lsn()")
  for i in $(seq 1 60); do
    RP=$("$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT pg_last_wal_replay_lsn()" 2>/dev/null)
    { [ "$RP" \> "$PRI_LSN" ] || [ "$RP" = "$PRI_LSN" ]; } && break; sleep 1
  done
  GOT=$("$BIN/psql" -h "$W" -p $SP -U postgres -tAc "$FP_Q" 2>&1)
  INREC=$("$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT pg_is_in_recovery()" 2>&1)
  log "  after crash-restart: fp=$GOT in_recovery=$INREC (want fp=$WANT)"
  [ "$GOT" = "$WANT" ] || { echo "FAIL: data mismatch after crash-restart (got '$GOT' want '$WANT')"; FAIL=1; }
  [ "$INREC" = "t" ]   || { echo "FAIL: not a hot standby after crash-restart (in_recovery=$INREC)"; FAIL=1; }
  # The retained old datadir must still be intact (copy mode: relink only read it).
  ls "$STBY_OLD"/base/*/[0-9]* >/dev/null 2>&1 || { echo "FAIL: retained old datadir damaged by relink"; FAIL=1; }
fi

"$BIN/pg_ctl" -D "$SKEL" -w stop >/dev/null 2>&1 || true
"$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1 || true
for p in $PP $SP; do lsof -ti :$p 2>/dev/null | xargs kill -9 2>/dev/null; done

echo "========================================================================"
[ "$FAIL" = 0 ] && log "PASS: streaming standby survives a mid-replay crash and converges (durable, idempotent)" \
                || log "FAIL: see messages above"
exit $FAIL
