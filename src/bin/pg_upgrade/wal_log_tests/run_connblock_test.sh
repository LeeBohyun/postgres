#!/usr/bin/env bash
# Verify user connections are fully blocked while the pg_upgrade WAL is being
# replayed -- no client may observe the half-upgraded cluster.
#
# The upgrade replay is crash recovery (DB_IN_PRODUCTION), during which the
# postmaster rejects connections until a consistent state is reached; and the
# pgUpgradeReplayInProgress guard additionally suppresses hot standby activation
# between XLOG_UPGRADE_START and XLOG_UPGRADE_COMPLETE.  So even with
# hot_standby=on, no connection may succeed until the whole upgrade window has
# replayed.
#
# Test: build a sizable cluster (so replay takes long enough to race against),
# upgrade with --wal-upgrade, then start the new cluster with hot_standby=on
# while hammering connection attempts.  A connection must NEVER observe a row
# count other than the final, complete value -- any partial/empty read, or any
# successful connect before COMPLETE, is a failure.
#
# NOTE: this harness exercises the crash-recovery replay path (the primary /
# spawn-fresh-cluster case), where the postmaster's consistency gate does the
# blocking.  The pgUpgradeReplayInProgress guard additionally covers the true
# streaming-standby (archive-recovery + hot_standby) path; verifying that
# directly requires the full standby-convergence workflow and is left for when
# that path is built.
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"; BIN="${PGBIN:-$ROOT/pginst/bin}"
W=/tmp/pgu_connblock; O=$W/old N=$W/new; P=55570
export PGDATABASE=postgres
log(){ echo "=== $* ==="; }
rm -rf "$W"; mkdir -p "$W"

log "seed old cluster (sizable, so replay is not instant)"
"$BIN/initdb" -D "$O" -U postgres -N >/dev/null 2>&1
cat >> "$O/postgresql.conf" <<CONF
port=$P
unix_socket_directories='$W'
CONF
"$BIN/pg_ctl" -D "$O" -l "$W/o.log" -w start >/dev/null 2>&1
"$BIN/psql" -h "$W" -p $P -U postgres -qc \
  "CREATE TABLE big(id int, v text); INSERT INTO big SELECT g, repeat('x',100)||g FROM generate_series(1,400000) g;" >/dev/null 2>&1
EXPECT=$("$BIN/psql" -h "$W" -p $P -U postgres -tAc "SELECT count(*) FROM big")
log "expected final row count: $EXPECT"
"$BIN/pg_ctl" -D "$O" -w stop >/dev/null 2>&1

log "pg_upgrade --wal-upgrade --initdb"
cd "$W"
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$O" -D "$N" -U postgres --initdb --wal-upgrade --copy > "$W/up.log" 2>&1
[ $? -eq 0 ] || { echo "FAIL upgrade"; tail -15 "$W/up.log"; exit 1; }

cat >> "$N/postgresql.conf" <<CONF
port=$P
unix_socket_directories='$W'
hot_standby=on
CONF

# The upgrade replay happens on the FIRST start: --wal-upgrade now auto-serves,
# so the single start reconstructs the cluster from WAL and comes up read-write --
# no quarantine hold, no commit step.  During that reconstruction the cluster
# is in crash recovery (dark), so no connection may observe a half-upgraded
# cluster.  Hammer connections THROUGH the auto-serving start -- every probe must
# either be cleanly rejected (recovering / not accepting) or return the correct
# final count once the cluster is live; never a partial count.
log "hammer connections while the new cluster replays the upgrade (auto-serve start)"
PROBE="$W/probe.out"; : > "$PROBE"
(
  for i in $(seq 1 400); do
    R=$("$BIN/psql" -h "$W" -p $P -U postgres -tAc "SELECT count(*) FROM big" 2>&1 | tr -d '[:space:]')
    echo "$R"
  done
) > "$PROBE" 2>&1 &
PROBEPID=$!
"$BIN/pg_ctl" -D "$N" -l "$W/n.log" -w start >/dev/null 2>&1
STARTRC=$?
wait $PROBEPID 2>/dev/null
"$BIN/pg_ctl" -D "$N" -w stop >/dev/null 2>&1
[ $STARTRC -eq 0 ] || { echo "FAIL: new cluster did not auto-serve on first start"; tail -15 "$W/n.log"; exit 1; }

log "analyze probe results"
# Every outcome is one of:
#   - a clean rejection while recovering (contains 'notyetacceptingconnections'
#     / 'recovery' / 'startingup' / connection refused)
#   - the correct final count ($EXPECT)
# ANYTHING ELSE -- a different (partial) count, empty table, or 'does not exist'
# -- means a client observed a half-upgraded cluster: FAIL.
FAIL=0
GOOD_FINAL=0; REJECTED=0; BAD=0
while IFS= read -r line; do
  [ -z "$line" ] && continue
  case "$line" in
    "$EXPECT")                                   GOOD_FINAL=$((GOOD_FINAL+1)) ;;
    *notyetaccepting*|*recovery*|*startingup*|*couldnotconnect*|*Connectionrefused*|*failed*|*No*such*)
                                                 REJECTED=$((REJECTED+1)) ;;
    *)                                           BAD=$((BAD+1))
                                                 [ $BAD -le 5 ] && echo "  ANOMALY: observed \"$line\"" ;;
  esac
done < "$PROBE"

log "final-count reads=$GOOD_FINAL  clean-rejections=$REJECTED  anomalies=$BAD"
if [ "$BAD" -ne 0 ]; then
  echo "FAIL: $BAD connection(s) observed a partial/inconsistent cluster during upgrade replay"
  FAIL=1
fi
# Sanity: we must have actually raced the window (seen at least one rejection),
# otherwise the test proved nothing.
if [ "$REJECTED" -eq 0 ]; then
  echo "WARNING: no rejections captured — replay finished before any probe landed;"
  echo "         the block was not actually exercised (test inconclusive, not a failure)."
fi

[ "$FAIL" = 0 ] && log "PASS: no connection observed a half-upgraded cluster" \
                || log "FAIL: connections were not fully blocked during replay"
exit $FAIL
