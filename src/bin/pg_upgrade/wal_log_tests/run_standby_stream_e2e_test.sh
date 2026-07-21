#!/usr/bin/env bash
# END-TO-END standby upgrade by STREAMING the window (NO cp).
#
# The standby is delivered the upgrade window by STREAMING, never by cp: the fresh
# new-version skeleton STREAMS the window from the LIVE committed primary via a
# replication connection, using:
#
#   - the retention slot (UPGRADE_WINDOW_SLOT) that pins the window on the primary
#     so it survives commit and is streamable, and
#   - "pg_upgrade --wal-prepare-standby", which stamps the skeleton's control file
#     with the primary's sysid + CN anchor + TLI so its walreceiver accepts the
#     primary and recovery starts at CN.
#
# A streamed standby does NOT hold/commit: it streams the window from the
# already-committed primary and continues as an ordinary hot standby following it.
#
# DECISIVE ASSERTIONS:
#   * the operator NEVER cp's a WAL segment into the skeleton (this script copies
#     nothing; it only runs --wal-prepare-standby + pg_ctl start), and
#   * the skeleton's log shows it STREAMED (walreceiver "started streaming" /
#     "arming streaming standby from anchor"), came up as a hot standby
#     (pg_is_in_recovery=t), and serves data byte-identical to the primary.
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
BIN="${PGBIN:-$ROOT/pginst/bin}"
W=${WORK:-/tmp/pgu_stream_e2e}; OLD=$W/old NEW=$W/new SKEL=$W/skel
PP=${PPORT:-55948} SP=${SPORT:-55949}
export PGDATABASE=postgres
log(){ echo "=== $* ==="; }
FAIL=0
for p in $PP $SP; do lsof -ti :$p 2>/dev/null | xargs kill -9 2>/dev/null; done
rm -rf "$W"; mkdir -p "$W"

log "1. old primary with data"
"$BIN/initdb" -D "$OLD" -U postgres -N >/dev/null 2>&1 || { echo FAIL initdb; exit 1; }
cat >> "$OLD/postgresql.conf" <<CONF
port=$PP
unix_socket_directories='$W'
CONF
"$BIN/pg_ctl" -D "$OLD" -l "$W/old.log" -w start >/dev/null 2>&1 || { echo FAIL start; exit 1; }
"$BIN/psql" -h "$W" -p $PP -U postgres -q >/dev/null 2>&1 <<SQL
CREATE TABLE t(id int primary key, v text);
INSERT INTO t SELECT g,'v'||g FROM generate_series(1,2000) g;
CREATE TABLE toast_t(id int, big text);
INSERT INTO toast_t SELECT g, repeat(md5(g::text),300) FROM generate_series(1,300) g;
SQL
OLD_FP=$("$BIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint),(SELECT count(*) FROM toast_t) FROM t")
"$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1

log "2. upgrade the primary (--wal-upgrade), auto-serve -> live; slot retains the window"
cd "$W"
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-upgrade --copy >"$W/up.log" 2>&1
[ $? -eq 0 ] || { echo FAIL upgrade; tail -20 "$W/up.log"; exit 1; }
cat >> "$NEW/postgresql.conf" <<CONF
port=$PP
unix_socket_directories='$W'
wal_level=replica
max_wal_senders=8
listen_addresses='localhost'
CONF
echo "host replication all 127.0.0.1/32 trust" >> "$NEW/pg_hba.conf"
echo "host all all 127.0.0.1/32 trust" >> "$NEW/pg_hba.conf"
# Auto-serve: the primary comes up read-write on first start (no commit step).
# The retention slot keeps the upgrade window streamable for the standby.
"$BIN/pg_ctl" -D "$NEW" -l "$W/new.log" -w start >/dev/null 2>&1 || { echo "FAIL new start"; tail -15 "$W/new.log"; exit 1; }
NEW_FP=$("$BIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint),(SELECT count(*) FROM toast_t) FROM t")
NEW_ID=$("$BIN/pg_controldata" -D "$NEW" | grep -i 'system identifier' | grep -oE '[0-9]+')
log "committed primary: fp=$NEW_FP sysid=$NEW_ID"
# FIRST prove the PRIMARY itself upgraded correctly (data preserved from the old
# cluster) -- otherwise "standby == primary" would only prove the standby faithfully
# replicated a broken primary.  OLD_FP and NEW_FP use the identical query.
[ "$NEW_FP" = "$OLD_FP" ] || { echo "FAIL: upgraded primary data ($NEW_FP) != old source data ($OLD_FP) -- primary upgrade is wrong"; FAIL=1; }
log "primary upgrade verified: data preserved from old cluster ($OLD_FP)"
# confirm the retention slot is present + the anchor is reportable
SLOT=$("$BIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT slot_name FROM pg_replication_slots WHERE slot_name='pg_upgrade_window'")
ANCHOR=$("$BIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT pg_upgrade_wal_window_anchor()")
log "retention slot='$SLOT'  window anchor='$ANCHOR'"
[ "$SLOT" = "pg_upgrade_window" ] || { echo "FAIL: retention slot missing on committed primary"; FAIL=1; }
[ -n "$ANCHOR" ] || { echo "FAIL: primary reports no window anchor"; FAIL=1; }

log "3. fresh new-version SKELETON, prepared to STREAM (no cp of any WAL)"
"$BIN/initdb" -D "$SKEL" -U postgres -N >/dev/null 2>&1 || { echo FAIL initdb-skel; exit 1; }
# Wipe the skeleton's data like a re-provision (only WAL is delivered -- here by
# STREAM, not cp).  Do NOT copy any [0-9A-F] WAL segment into it.
rm -f "$SKEL"/base/*/[0-9]* 2>/dev/null
rm -f "$SKEL"/global/[0-9]* "$SKEL"/global/pg_filenode.map 2>/dev/null
# primary_conninfo is set in the skeleton config (standard for any standby);
# --wal-prepare-standby reads it (no dedicated flag).
cat >> "$SKEL/postgresql.conf" <<CONF
port=$SP
unix_socket_directories='$W'
hot_standby=on
primary_conninfo='host=127.0.0.1 port=$PP user=postgres dbname=postgres'
CONF
SKEL_ID_BEFORE=$("$BIN/pg_controldata" -D "$SKEL" | grep -i 'system identifier' | grep -oE '[0-9]+')
log "skeleton sysid BEFORE prepare: $SKEL_ID_BEFORE (differs from primary $NEW_ID)"

"$BIN/pg_upgrade" -B "$BIN" -D "$SKEL" --wal-prepare-standby >"$W/prep.log" 2>&1 \
    || { echo "FAIL prepare-standby"; cat "$W/prep.log"; FAIL=1; }
# The prepare step must have created the anchor + standby.signal, but NO WAL cp.
[ -f "$SKEL/pg_upgrade_stream.anchor" ] || { echo "FAIL: no streaming anchor written"; FAIL=1; }
[ -f "$SKEL/standby.signal" ]           || { echo "FAIL: no standby.signal written"; FAIL=1; }
grep -q "primary_slot_name" "$SKEL/postgresql.auto.conf" || { echo "FAIL: primary_slot_name not configured"; FAIL=1; }
log "anchor file: $(cat "$SKEL/pg_upgrade_stream.anchor")"

log "4. START the skeleton: it STREAMS the window from the live primary and becomes a hot standby"
# A streamed standby does NOT hold or commit: it streams the window from the
# already-committed primary, replays COMPLETE, and continues as an ordinary hot
# standby following the primary.  So start it normally and wait for it to serve
# read-only queries.
"$BIN/pg_ctl" -D "$SKEL" -l "$W/skel.log" -w -t 90 start >/dev/null 2>&1 || true
UP=0
for i in $(seq 1 60); do
  "$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT 1" >/dev/null 2>&1 && { UP=1; break; }
  sleep 1
done
# Decisive assertions:
grep -q "arming streaming standby from anchor" "$W/skel.log" \
  && log "  skeleton armed from the streaming anchor (sysid+CN+TLI stamped)" \
  || { echo "  FAIL: skeleton did not arm from the streaming anchor"; tail -20 "$W/skel.log"; FAIL=1; }
if grep -qiE "started streaming|streaming WAL" "$W/skel.log"; then
  log "  skeleton STREAMED WAL from the primary (no cp):"
  grep -iE "started streaming|streaming WAL" "$W/skel.log" | head -2
else
  echo "  FAIL: no evidence the skeleton streamed WAL"; tail -25 "$W/skel.log"; FAIL=1
fi
[ "$UP" = 1 ] \
  && log "  skeleton is up and serving as a hot standby" \
  || { echo "  FAIL: skeleton did not come up as a hot standby"; tail -20 "$W/skel.log"; FAIL=1; }
INREC=$("$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT pg_is_in_recovery()" 2>&1)
[ "$INREC" = "t" ] \
  && log "  skeleton is following the primary (pg_is_in_recovery=t)" \
  || { echo "  FAIL: skeleton is not in recovery (state=$INREC)"; FAIL=1; }

log "5. verify the streamed standby serves the upgraded data (converged to the primary)"
STBY_FP=$("$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint),(SELECT count(*) FROM toast_t) FROM t" 2>&1)
STBY_ID=$("$BIN/pg_controldata" -D "$SKEL" | grep -i 'system identifier' | grep -oE '[0-9]+')
log "streamed standby: data=$STBY_FP sysid=$STBY_ID (want data=$NEW_FP sysid=$NEW_ID)"
[ "$STBY_FP" = "$NEW_FP" ] || { echo "FAIL: streamed standby data ($STBY_FP) != primary ($NEW_FP)"; FAIL=1; }
[ "$STBY_ID" = "$NEW_ID" ] || { echo "FAIL: sysid mismatch standby=$STBY_ID primary=$NEW_ID"; FAIL=1; }

"$BIN/pg_ctl" -D "$SKEL" -w stop >/dev/null 2>&1 || true
"$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1 || true
lsof -ti :$PP :$SP 2>/dev/null | xargs kill -9 2>/dev/null

echo "========================================================================"
[ "$FAIL" = 0 ] && log "PASS: standby STREAMED the upgrade window from the live primary (no cp), converged to the upgraded data" \
                || log "FAIL: see messages above"
exit $FAIL
