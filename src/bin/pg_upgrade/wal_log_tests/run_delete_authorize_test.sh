#!/usr/bin/env bash
# Set-wide delete-authorize signal (task #4).
#
# After a --wal-upgrade, both the primary and a streaming standby are
# live (the primary auto-serves on first start).  Running
# "pg_upgrade --wal-upgrade-delete-old" on the PRIMARY must:
#   1. delete the primary's own old cluster, and
#   2. emit an XLOG_UPGRADE_DELETE_AUTHORIZE signal into the live primary's WAL,
#      which streams to the standby; the standby replays it and drops the durable
#      marker pg_upgrade_delete_authorized in its data dir (it does NOT rm in redo).
# Then "pg_upgrade --wal-upgrade-delete-old" on the STANDBY removes the standby's old cluster,
# with the marker present as the fleet-wide authorization.
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
BIN="${PGBIN:-$ROOT/pginst/bin}"
W=${WORK:-/tmp/pgu_delauth}; POLD=$W/pold PNEW=$W/pnew SOLD=$W/sold SNEW=$W/snew
PP=${PPORT:-55952} SP=${SPORT:-55953}
export PGDATABASE=postgres
log(){ echo "=== $* ==="; }
FAIL=0
for p in $PP $SP; do lsof -ti :$p 2>/dev/null | xargs kill -9 2>/dev/null; done
rm -rf "$W"; mkdir -p "$W"

log "1. old primary with data + a committed, live upgraded PRIMARY (retention slot on)"
"$BIN/initdb" -D "$POLD" -U postgres -N >/dev/null 2>&1 || { echo FAIL initdb; exit 1; }
cat >> "$POLD/postgresql.conf" <<CONF
port=$PP
unix_socket_directories='$W'
CONF
"$BIN/pg_ctl" -D "$POLD" -l "$W/pold.log" -w start >/dev/null 2>&1 || { echo FAIL start; exit 1; }
"$BIN/psql" -h "$W" -p $PP -U postgres -qc "CREATE TABLE t(id int primary key, v text); INSERT INTO t SELECT g,'v'||g FROM generate_series(1,1000) g;" >/dev/null
"$BIN/pg_ctl" -D "$POLD" -w stop >/dev/null 2>&1
cd "$W"
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$POLD" -D "$PNEW" -U postgres --initdb --wal-upgrade --copy >"$W/up.log" 2>&1
[ $? -eq 0 ] || { echo FAIL upgrade; tail -20 "$W/up.log"; exit 1; }
cat >> "$PNEW/postgresql.conf" <<CONF
port=$PP
unix_socket_directories='$W'
wal_level=replica
max_wal_senders=8
listen_addresses='localhost'
CONF
echo "host replication all 127.0.0.1/32 trust" >> "$PNEW/pg_hba.conf"
echo "host all all 127.0.0.1/32 trust" >> "$PNEW/pg_hba.conf"
# --wal-upgrade auto-serves: the first start brings the primary up read-write
# directly -- no quarantine hold, no commit step.
"$BIN/pg_ctl" -D "$PNEW" -l "$W/pnew.log" -w start >/dev/null 2>&1 || { echo "FAIL primary start"; tail "$W/pnew.log"; exit 1; }
log "live primary up; old primary dir retained at $POLD"

log "2. bring up a STREAMING standby (auto-anchor; no prepare step, no commit)"
"$BIN/initdb" -D "$SNEW" -U postgres -N >/dev/null 2>&1 || { echo FAIL initdb-skel; exit 1; }
rm -f "$SNEW"/base/*/[0-9]* 2>/dev/null
rm -f "$SNEW"/global/[0-9]* "$SNEW"/global/pg_filenode.map 2>/dev/null
cat >> "$SNEW/postgresql.conf" <<CONF
port=$SP
unix_socket_directories='$W'
hot_standby=on
primary_conninfo='host=127.0.0.1 port=$PP user=postgres dbname=postgres'
CONF
touch "$SNEW/standby.signal"
# A streamed standby does NOT hold/commit: with primary_conninfo set it
# auto-fetches the window anchor from the primary, streams it, and becomes a hot
# standby of the primary directly.  Start it and confirm it is following.
"$BIN/pg_ctl" -D "$SNEW" -l "$W/snew.log" -w -t 90 start >/dev/null 2>&1 || true
INREC=""
for i in $(seq 1 60); do
  INREC=$("$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT pg_is_in_recovery()" 2>/dev/null)
  [ "$INREC" = "t" ] && break; sleep 1
done
log "streamed standby up; pg_is_in_recovery=$INREC (want t -- a hot standby)"
[ "$INREC" = "t" ] || { echo "FAIL: streamed standby did not come up as a hot standby"; tail -12 "$W/snew.log"; FAIL=1; }
# It has no old dir of its own (it was a fresh streamed skeleton); the decisive
# thing is that the primary's later delete-authorize signal REACHES it and drops
# the marker.  Provide a stand-in superseded old dir so we can also exercise the
# local --wal-upgrade-delete-old gate honoring the marker.
"$BIN/initdb" -D "$SOLD" -U postgres -N >/dev/null 2>&1
mv "$SOLD/global/pg_control" "$SOLD/global/pg_control.old"   # superseded stamp

log "3. --wal-upgrade-delete-old on the PRIMARY: deletes primary old dir + emits delete-authorize"
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$POLD" -D "$PNEW" -U postgres --wal-upgrade-delete-old >"$W/pdel.log" 2>&1
echo "primary delete-old rc=$?"
grep -iE "Signaling standbys|delete-authorize|Old cluster deleted" "$W/pdel.log" | head
[ -d "$POLD" ] && { echo "FAIL: primary old dir not deleted"; FAIL=1; } || log "  primary old dir deleted"
grep -qi "Signaling standbys" "$W/pdel.log" || { echo "FAIL: primary did not emit delete-authorize"; FAIL=1; }

log "4. the STILL-STREAMING standby replays the signal -> drops the marker"
MARK=0
for i in $(seq 1 30); do
  [ -f "$SNEW/pg_upgrade_delete_authorized" ] && { MARK=1; break; }
  sleep 1
done
[ "$MARK" = 1 ] && log "  standby replayed the signal; marker present" \
                || { echo "FAIL: standby never received the delete-authorize marker"; tail -10 "$W/snew.log"; FAIL=1; }

log "5. --wal-upgrade-delete-old on the STANDBY: removes its (stand-in) old dir; marker = fleet authorization"
"$BIN/pg_upgrade" -B "$BIN" -D "$SNEW" -d "$SOLD" --wal-upgrade-delete-old >"$W/sdel.log" 2>&1
echo "standby delete-old rc=$?"
grep -iE "delete-authorize signal present|Old cluster deleted" "$W/sdel.log" | head
[ -d "$SOLD" ] && { echo "FAIL: standby old dir not deleted"; FAIL=1; } || log "  standby old dir deleted"
grep -qi "delete-authorize signal present" "$W/sdel.log" || { echo "FAIL: standby did not report signal authorization"; cat "$W/sdel.log"; FAIL=1; }

"$BIN/pg_ctl" -D "$SNEW" -w stop >/dev/null 2>&1 || true
"$BIN/pg_ctl" -D "$PNEW" -w stop >/dev/null 2>&1 || true
lsof -ti :$PP :$SP 2>/dev/null | xargs kill -9 2>/dev/null

echo "========================================================================"
[ "$FAIL" = 0 ] && log "PASS: --wal-upgrade-delete-old on the primary signaled the standby set-wide; standby honored it and deleted its old cluster" \
                || log "FAIL: see messages above"
exit $FAIL
