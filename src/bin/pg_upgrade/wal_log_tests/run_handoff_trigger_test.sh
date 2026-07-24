#!/usr/bin/env bash
# Prove the OLD-format streaming-handoff TRIGGER halts a live streaming standby.
#
# This tests the TRIGGER mechanism (item 1 in TODO.md), NOT cross-version replay:
# a caught-up physical standby is streaming the primary; the operator runs
# "pg_upgrade --wal-upgrade-signal-handoff", which arms a sentinel and fast-stops
# the primary, whose ShutdownXLOG() emits the XLOG_UPGRADE_HANDOFF record into
# the primary's OWN (old-format) WAL just before the shutdown checkpoint; the
# standby streams that record and MUST halt cleanly with the handoff FATAL --
# reaching it (unlike the new-format START burst, which a streaming standby can
# never read).
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
# capture the standby postmaster pid so we can prove IT shuts itself down
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

log "3. standby must HALT (FATAL) upon replaying the handoff trigger"
# give the standby time to stream + replay the trigger and die
for i in $(seq 1 20); do
  grep -qiE "reached pg_upgrade handoff on standby" "$W/s.log" && break
  sleep 1
done
if grep -qiE "reached pg_upgrade handoff on standby" "$W/s.log"; then
  log "  HALTED at the handoff trigger (as designed):"
  grep -iE "reached pg_upgrade handoff|initiated a --wal-upgrade|re-provision" "$W/s.log" | tail -3
else
  echo "  FAIL: standby did not halt at the handoff trigger; log tail:"
  tail -12 "$W/s.log"; FAIL=1
fi

log "4. the standby must SHUT ITSELF DOWN (we never call pg_ctl stop on it)"
DOWN=0
for i in $(seq 1 30); do
  if [ -n "$SPID" ] && ! kill -0 "$SPID" 2>/dev/null; then DOWN=1; break; fi
  sleep 1
done
[ "$DOWN" = 1 ] && log "  standby postmaster (pid $SPID) EXITED on its own" \
               || { echo "  FAIL: standby postmaster $SPID still alive 30s after the trigger"; FAIL=1; }
# corroborate: pg_ctl status not running, port refused, clean pid removal, no restart loop
if "$BIN/pg_ctl" -D "$W/s" status >/dev/null 2>&1; then
  echo "  FAIL: pg_ctl still reports the standby running"; FAIL=1
else log "  pg_ctl status: NOT running (good)"; fi
if "$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT 1" >/dev/null 2>&1; then
  echo "  FAIL: standby still accepting connections on port $SP"; FAIL=1
else log "  port $SP: refused (good)"; fi
[ -f "$W/s/postmaster.pid" ] && log "  NOTE: postmaster.pid still present" \
                            || log "  postmaster.pid removed (clean exit)"
NF=$(grep -c "reached pg_upgrade handoff on standby" "$W/s.log")
log "  handoff FATAL count in log: $NF (want 1 -- proves no restart loop)"
[ "$NF" = 1 ] || { echo "  FAIL: FATAL appears $NF times -- standby is loop-restarting, not halted"; FAIL=1; }

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
[ "$FAIL" = 0 ] && log "PASS: streaming standby received the old-format handoff trigger and shut itself down cleanly" \
                || log "FAIL: see messages above"
exit $FAIL
