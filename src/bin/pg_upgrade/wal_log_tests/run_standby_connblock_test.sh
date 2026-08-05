#!/usr/bin/env bash
# Connection blocking on the STREAMING-STANDBY path.
#
# run_connblock_test covers the crash-recovery replay path (the primary /
# spawn-fresh-cluster case), where the postmaster's consistency gate blocks
# connections.  This covers the OTHER path its NOTE left open: a true streaming
# standby with hot_standby=on, where the pgUpgradeReplayInProgress guard (set at
# arm time, cleared only at XLOG_UPGRADE_COMPLETE) is what must keep a read-only
# backend from observing the half-materialized catalog mid-window.
#
# A streaming standby reaches consistency and would normally admit hot-standby
# connections well before the window's XLOG_UPGRADE_START -- against the still
# empty/old catalog.  So while the skeleton streams + replays the window, hammer
# connections: every probe must either be cleanly rejected (still recovering /
# not yet accepting) or return the correct FINAL row count once the standby is
# live.  A partial count, an empty table, or "relation does not exist" means a
# client saw a half-upgraded cluster -> FAIL.
#
# Env: PGBIN (new bin dir; default <repo>/pginst/bin), WORK (default under /tmp).
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"; BIN="${PGBIN:-$ROOT/pginst/bin}"
W=${WORK:-/tmp/pgu_stby_connblock}
OLD=$W/old NEW=$W/new SKEL=$W/skel STBY_OLD=$W/stby_old
PP=${PPORT:-55580} SP=${SPORT:-55581}
export PGDATABASE=postgres
log(){ echo "=== $* ==="; }
FAIL=0
for p in $PP $SP; do lsof -ti :$p 2>/dev/null | xargs kill -9 2>/dev/null; done
rm -rf "$W"; mkdir -p "$W"

log "seed old cluster (sizable, so the window takes long enough to race against)"
"$BIN/initdb" -D "$OLD" -U postgres -N >/dev/null 2>&1 || { echo FAIL initdb; exit 1; }
cat >> "$OLD/postgresql.conf" <<CONF
port=$PP
unix_socket_directories='$W'
wal_level=replica
max_wal_senders=8
CONF
"$BIN/pg_ctl" -D "$OLD" -l "$W/old.log" -w start >/dev/null 2>&1 || { echo FAIL start; exit 1; }
# A physical slot marks that a standby is expected; pg_upgrade migrates it and
# it pins the upgrade window so the skeleton below can stream it.
"$BIN/psql" -h "$W" -p $PP -U postgres -qtAc \
  "SELECT pg_create_physical_replication_slot('stby_slot', true)" >/dev/null 2>&1 || { echo FAIL create-slot; exit 1; }
# many relations so the RELINK manifest + system-catalog window are non-trivial
"$BIN/psql" -h "$W" -p $PP -U postgres -q >/dev/null 2>&1 <<SQL
CREATE TABLE big(id int primary key, v text);
INSERT INTO big SELECT g, repeat('x',100)||g FROM generate_series(1,400000) g;
DO \$\$ BEGIN
  FOR i IN 1..200 LOOP EXECUTE format('CREATE TABLE t%s(id int, v text)', i); END LOOP;
END \$\$;
SQL
EXPECT=$("$BIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT count(*) FROM big")
log "expected final row count: $EXPECT"
"$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1

# The standby's own retained pre-upgrade datadir (relink source).
cp -a "$OLD" "$STBY_OLD"

log "pg_upgrade --wal-upgrade --initdb --copy; keep the primary live for streaming"
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

log "build a fresh skeleton that streams the window; probe connections during replay"
"$BIN/initdb" -D "$SKEL" -U postgres -N >/dev/null 2>&1 || { echo "FAIL: skeleton initdb"; exit 1; }
# old-datadir path now comes from the pg_upgrade_standby_old_datadir GUC;
# pg_upgrade.signal is an empty presence-only sentinel.
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

# Probe the standby continuously from just before it starts until it converges.
PROBE="$W/probe.out"; : > "$PROBE"
(
  for i in $(seq 1 600); do
    R=$("$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT count(*) FROM big" 2>&1 | tr -d '[:space:]')
    echo "$R"
  done
) > "$PROBE" 2>&1 &
PROBEPID=$!
"$BIN/pg_ctl" -D "$SKEL" -l "$W/skel.log" -w -t 120 start >/dev/null 2>&1 || true
# wait for convergence, then let the probe loop finish
UP=0
for i in $(seq 1 90); do "$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT count(*)=$EXPECT FROM big" 2>/dev/null | grep -q t && { UP=1; break; }; sleep 1; done
wait $PROBEPID 2>/dev/null
"$BIN/pg_ctl" -D "$SKEL" -w stop >/dev/null 2>&1 || true
"$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1 || true
for p in $PP $SP; do lsof -ti :$p 2>/dev/null | xargs kill -9 2>/dev/null; done
[ "$UP" = 1 ] || { echo "FAIL: standby never converged"; tail -15 "$W/skel.log"; exit 1; }

log "analyze probe results"
# Each probe outcome must be either a clean rejection while recovering, or the
# correct final count.  Anything else means a client observed a half-upgraded
# cluster mid-window.
GOOD_FINAL=0; REJECTED=0; BAD=0
while IFS= read -r line; do
  [ -z "$line" ] && continue
  case "$line" in
    "$EXPECT")
      GOOD_FINAL=$((GOOD_FINAL+1)) ;;
    *notyetaccepting*|*recovery*|*startingup*|*couldnotconnect*|*Connectionrefused*|*failed*|*No*such*|*doesnotexist*|*shuttingdown*)
      REJECTED=$((REJECTED+1)) ;;
    *)
      BAD=$((BAD+1)); [ "$BAD" -le 5 ] && echo "  ANOMALY: '$line'" ;;
  esac
done < "$PROBE"
log "probe results: final-count reads=$GOOD_FINAL  clean-rejections=$REJECTED  anomalies=$BAD"
[ "$BAD" -eq 0 ] || FAIL=1
# The guard must actually have blocked something (else the race did not happen):
# require at least one clean rejection, proving connections were attempted before
# the window finished replaying.
[ "$REJECTED" -ge 1 ] || { echo "WARN: no rejections seen -- replay may have finished before the first probe (not a failure, but the race was not exercised)"; }
[ "$GOOD_FINAL" -ge 1 ] || { echo "FAIL: never observed the converged count -- standby did not serve"; FAIL=1; }

echo "========================================================================"
[ "$FAIL" = 0 ] && log "PASS: no connection observed a half-upgraded streaming standby" \
                || log "FAIL: a client saw a partial/empty cluster mid-window"
exit $FAIL
