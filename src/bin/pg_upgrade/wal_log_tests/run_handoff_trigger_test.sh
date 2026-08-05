#!/usr/bin/env bash
# Prove the OLD-format streaming-handoff TRIGGER pauses a live streaming standby.
#
# This tests the TRIGGER mechanism, NOT cross-version replay:
# a caught-up physical standby is streaming the primary; the operator runs
# "pg_upgrade --wal-upgrade-signal-handoff", which arms a sentinel and fast-stops
# the primary, whose ShutdownXLOG() emits the XLOG_UPGRADE_HANDOFF record into
# the primary's OWN (old-format) WAL just before the shutdown checkpoint; the
# standby streams that record and MUST PAUSE recovery at the boundary -- it stays
# up serving read-only queries while a fresh new-version skeleton is provisioned,
# rather than following the new-format START burst (which a streaming standby can
# never read).  The pause is reversible (pg_wal_replay_resume()).
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
BIN="${PGBIN:-$ROOT/pginst/bin}"
W=${WORK:-/tmp/pgu_handoff}; rm -rf "$W"; mkdir -p "$W"
PP=55940 SP=55941
for p in $PP $SP; do lsof -ti :$p 2>/dev/null | xargs kill -9 2>/dev/null; done
log(){ echo "=== $* ==="; }
FAIL=0

log "0. verify the binary supports --wal-upgrade-signal-handoff"
"$BIN/pg_upgrade" --help 2>&1 | grep -qi "wal-upgrade-signal-handoff" \
    && log "  --wal-upgrade-signal-handoff supported" \
    || { echo "FAIL: binary does not support --wal-upgrade-signal-handoff"; FAIL=1; }
"$BIN/initdb" -D "$W/p" -U postgres -N >/dev/null 2>&1
cat >> "$W/p/postgresql.conf" <<CONF
port=$PP
unix_socket_directories='$W'
wal_level=replica
max_wal_senders=5
wal_keep_size=256MB
CONF
echo "local replication all trust" >> "$W/p/pg_hba.conf"
"$BIN/pg_ctl" -D "$W/p" -l "$W/p.log" -w start >/dev/null 2>&1 || { echo FAIL start; exit 1; }

log "1. create data + a caught-up streaming standby"
"$BIN/psql" -h "$W" -p $PP -U postgres -qc "CREATE TABLE t(id int primary key, v text); INSERT INTO t SELECT g,'v'||g FROM generate_series(1,500) g;" >/dev/null
"$BIN/pg_basebackup" -h "$W" -p $PP -U postgres -D "$W/s" -R >/dev/null 2>&1 || { echo FAIL basebackup; exit 1; }
cat >> "$W/s/postgresql.conf" <<CONF
port=$SP
unix_socket_directories='$W'
primary_conninfo='host=$W port=$PP user=postgres'
CONF
touch "$W/s/standby.signal"
"$BIN/pg_ctl" -D "$W/s" -l "$W/s.log" -w start >/dev/null 2>&1 || { echo FAIL standby start; tail -10 "$W/s.log"; exit 1; }
sleep 2
# confirm streaming + hot standby serving reads
SB_ROWS=$("$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT count(*) FROM t" 2>&1)
log "standby streaming, sees $SB_ROWS rows (want 500)"
[ "$SB_ROWS" = 500 ] || FAIL=1
# capture the standby postmaster pid so we can prove IT stays up (paused)
SPID=$(head -1 "$W/s/postmaster.pid" 2>/dev/null)
log "standby postmaster pid=$SPID"

log "2. operator runs --wal-upgrade-signal-handoff: emits the trigger AND shuts the primary down"
# Drive the real operator path (the CLI): it arms the handoff sentinel and
# fast-stops the primary, whose ShutdownXLOG() emits the trigger into its own
# WAL just before the shutdown checkpoint, so nothing appends WAL after it.
HLSN=$("$BIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT pg_current_wal_lsn()")
"$BIN/pg_upgrade" --wal-upgrade-signal-handoff -b "$BIN" -d "$W/p" -U postgres >"$W/handoff.log" 2>&1 \
    || { echo "FAIL: --wal-upgrade-signal-handoff"; cat "$W/handoff.log"; FAIL=1; }
grep -qi "handoff trigger written" "$W/handoff.log" || { echo "FAIL: no handoff success message"; cat "$W/handoff.log"; FAIL=1; }
# the primary must be STOPPED now (the CLI shut it down at the handoff point)
if "$BIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT 1" >/dev/null 2>&1; then
    echo "FAIL: primary still serving after signal-handoff (should have shut down at the handoff point)"; FAIL=1
else
    log "  primary shut down at the handoff point (no WAL can follow the trigger)"
fi

log "3. standby must PAUSE recovery upon replaying the handoff trigger"
# give the standby time to stream + replay the trigger and pause
for i in $(seq 1 20); do
  grep -qiE "reached pg_upgrade handoff on standby" "$W/s.log" && break
  sleep 1
done
if grep -qiE "reached pg_upgrade handoff on standby; pausing recovery" "$W/s.log"; then
  log "  PAUSED at the handoff trigger (as designed):"
  grep -iE "reached pg_upgrade handoff|initiated a --wal-upgrade|re-provision|resume" "$W/s.log" | tail -3
else
  echo "  FAIL: standby did not pause at the handoff trigger; log tail:"
  tail -12 "$W/s.log"; FAIL=1
fi

log "4. the standby must STAY UP (paused), serving read-only queries -- not shut down"
# It must NOT exit: a paused hot standby keeps serving reads while a fresh
# new-version skeleton is provisioned.  We never call pg_ctl stop on it here.
sleep 3
if [ -n "$SPID" ] && kill -0 "$SPID" 2>/dev/null; then
  log "  standby postmaster (pid $SPID) still running (good -- paused, not stopped)"
else
  echo "  FAIL: standby postmaster $SPID exited; it should pause and stay up"; FAIL=1
fi
# corroborate: pg_ctl reports running, still in recovery, still answering reads
if "$BIN/pg_ctl" -D "$W/s" status >/dev/null 2>&1; then
  log "  pg_ctl status: running (good)"
else echo "  FAIL: pg_ctl reports the standby not running"; FAIL=1; fi
INREC=$("$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT pg_is_in_recovery()" 2>/dev/null | tr -d '[:space:]')
[ "$INREC" = "t" ] && log "  still in recovery (good)" \
                   || { echo "  FAIL: expected pg_is_in_recovery()=t, got '$INREC'"; FAIL=1; }
ROWS=$("$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT count(*) FROM t" 2>/dev/null | tr -d '[:space:]')
[ "$ROWS" = 500 ] && log "  paused standby still serves the pre-upgrade data ($ROWS rows)" \
                  || { echo "  FAIL: expected 500 rows from the paused standby, got '$ROWS'"; FAIL=1; }
NF=$(grep -c "reached pg_upgrade handoff on standby" "$W/s.log")
log "  handoff pause message count in log: $NF (want 1 -- proves no restart loop)"
[ "$NF" = 1 ] || { echo "  FAIL: message appears $NF times -- standby is loop-restarting"; FAIL=1; }

log "5. pg_waldump shows the trigger with old-format identify string"
# The primary is stopped (signal-handoff shut it down), so scan its WAL directly
# rather than querying it.  The handoff was written at/after $HLSN.
HREC=""
for seg in "$W/p/pg_wal"/[0-9A-F]*; do
  HREC=$("$BIN/pg_waldump" "$seg" 2>/dev/null | grep -i "PG_UPGRADE_HANDOFF" | head -1)
  [ -n "$HREC" ] && break
done
[ -n "$HREC" ] && log "  $HREC" \
              || { echo "  FAIL: no PG_UPGRADE_HANDOFF record found in the primary's WAL"; FAIL=1; }

"$BIN/pg_ctl" -D "$W/s" -w stop >/dev/null 2>&1 || true
"$BIN/pg_ctl" -D "$W/p" -w stop >/dev/null 2>&1 || true
lsof -ti :$PP :$SP 2>/dev/null | xargs kill -9 2>/dev/null

echo "========================================================================"
[ "$FAIL" = 0 ] && log "PASS: streaming standby received the old-format handoff trigger and paused, staying up to serve read-only queries" \
                || log "FAIL: see messages above"
exit $FAIL
