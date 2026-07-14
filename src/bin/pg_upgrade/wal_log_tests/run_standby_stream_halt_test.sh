#!/usr/bin/env bash
# INVESTIGATION TEST (currently does NOT pass its halt assertion -- see below and
# TODO.md).  Attempts to exercise the LIVE-STREAMING standby halt-at-START path
# via archive recovery.
#
# FINDING (2026-07-14): with the window delivered via the ARCHIVE (recovery.signal
# + restore_command, NOT pre-staged in pg_wal), the standby does NOT halt at
# START -- ordinary archive recovery replays the whole upgrade window and the
# cluster converges (logical fingerprint matches the primary) and is writable,
# with the START guard never firing and PerformWalUpgradeIfNeeded never arming.
# This is either (a) evidence the halt is unnecessary for archive recovery, or
# (b) a silently-unsafe application of old-LSN FPIs that a logical check misses
# and only a physical (LSN/checksum-aware) page comparison would catch.  Until
# that is resolved this test documents the behavior rather than asserting a pass;
# the halt-assertion is reported, not fatal.  See TODO.md item 1.
#
# The halt only fires when the upgrade window arrives AFTER startup, in
# StandbyMode, so PerformWalUpgradeIfNeeded()'s startup scan of pg_wal/ does NOT
# pre-arm the bootstrap.  We reproduce that with ARCHIVE recovery: the window is
# placed in the ARCHIVE only (never pre-staged in the standby's pg_wal/), the
# standby runs with recovery.signal + restore_command, and replay walks from the
# old cluster's end into XLOG_PG_UPGRADE_START -- where it must HALT with the
# standby-boundary FATAL rather than apply the window live.
#
# Then, on relaunch, the window is now in pg_wal/ (restore_command fetched it),
# so the startup scan arms the bootstrap, anchors at CN, and replays -- and the
# converged cluster must match the primary and be writable.
#
# Contiguity requirement: the old cluster must end on a clean segment boundary
# (pg_switch_wal + checkpoint) so the new cluster's WAL (positioned at the old
# cluster's next segment by the --control-only path) is contiguous and archive
# recovery can walk into it.
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"; BIN="${PGBIN:-$ROOT/pginst/bin}"
W=${WORK:-/tmp/pgu_stbyhalt}; OLD=$W/old STBY=$W/stby NEW=$W/new ARCH=$W/arch
PP=${PPORT:-55915} SP=${SPORT:-55916}
export PGDATABASE=postgres
log(){ echo "=== $* ==="; }
FAIL=0
rm -rf "$W"; mkdir -p "$W" "$ARCH"

log "1. old primary (archiving) + caught-up standby; end old cluster on a clean segment boundary"
"$BIN/initdb" -D "$OLD" -U postgres -N >/dev/null 2>&1 || { echo FAIL initdb; exit 1; }
cat >> "$OLD/postgresql.conf" <<CONF
port=$PP
unix_socket_directories='$W'
wal_level=replica
max_wal_senders=5
wal_keep_size=256MB
archive_mode=on
archive_command='cp %p $ARCH/%f'
CONF
echo "local replication all trust" >> "$OLD/pg_hba.conf"
"$BIN/pg_ctl" -D "$OLD" -l "$W/old.log" -w start >/dev/null 2>&1 || { echo FAIL start; exit 1; }
"$BIN/psql" -h "$W" -p $PP -U postgres -q >/dev/null 2>&1 <<SQL
CREATE TABLE t(id int primary key, v text);
INSERT INTO t SELECT g,'v'||g FROM generate_series(1,2000) g;
SQL
OLD_FP=$("$BIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint) FROM t")
"$BIN/pg_basebackup" -h "$W" -p $PP -U postgres -D "$STBY" -R >/dev/null 2>&1 || { echo FAIL basebackup; exit 1; }
# end the old cluster cleanly on a segment boundary + archive that segment
"$BIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT pg_switch_wal()" >/dev/null
"$BIN/psql" -h "$W" -p $PP -U postgres -qc "CHECKPOINT" >/dev/null
sleep 1   # let the archiver flush the last segment
"$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1

log "2. upgrade the primary (--wal-log-upgrade); archive the upgrade WAL"
cd "$W"
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-log-upgrade --copy >"$W/up.log" 2>&1
[ $? -eq 0 ] || { echo FAIL upgrade; tail -20 "$W/up.log"; exit 1; }
# Put the upgrade WAL segments into the ARCHIVE (not the standby's pg_wal).
cp "$NEW/pg_wal"/[0-9A-F]* "$ARCH/" 2>/dev/null || true
# capture the upgraded primary fingerprint
cat >> "$NEW/postgresql.conf" <<CONF
port=$PP
unix_socket_directories='$W'
CONF
"$BIN/pg_ctl" -D "$NEW" -l "$W/new.log" -w start >/dev/null 2>&1 || { echo "FAIL new start"; tail -15 "$W/new.log"; exit 1; }
NEW_FP=$("$BIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint) FROM t")
"$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1
log "upgraded primary fingerprint: $NEW_FP"

log "3. standby in ARCHIVE recovery walks into START -> must HALT (window NOT in pg_wal at startup)"
# Ensure the standby's pg_wal has NO upgrade segments (so the startup scan finds
# nothing and does not pre-arm the bootstrap); the window is only in $ARCH.
rm -f "$STBY/pg_wal"/[0-9A-F]*
rm -f "$STBY/standby.signal"
touch "$STBY/recovery.signal"
cat >> "$STBY/postgresql.conf" <<CONF
port=$SP
unix_socket_directories='$W'
restore_command='cp $ARCH/%f %p'
recovery_target_timeline='latest'
CONF
"$BIN/pg_ctl" -D "$STBY" -l "$W/halt.log" -w -t 20 start >/dev/null 2>&1
sleep 3
if grep -qiE "reached pg_upgrade boundary" "$W/halt.log"; then
    log "  HALTED at the pg_upgrade boundary (as designed):"
    grep -iE "reached pg_upgrade boundary|Install the new-version" "$W/halt.log" | tail -2
else
    echo "  FAIL: standby did not halt at START with the boundary message; log tail:"
    tail -8 "$W/halt.log"; FAIL=1
fi
"$BIN/pg_ctl" -D "$STBY" -w stop >/dev/null 2>&1 || true
lsof -ti :$SP 2>/dev/null | xargs kill -9 2>/dev/null

log "4. relaunch to apply: bootstrap now arms (window fetched into pg_wal), replay from CN"
rm -f "$STBY/standby.signal" "$STBY/recovery.signal"
"$BIN/pg_ctl" -D "$STBY" -l "$W/apply.log" -w -t 60 start >/dev/null 2>&1
if [ $? -ne 0 ]; then echo "FAIL: relaunch did not come up"; tail -20 "$W/apply.log"; FAIL=1; fi
grep -q "arming recovery from end-of-upgrade checkpoint" "$W/apply.log" \
  && log "  re-anchored + replayed from CN" \
  || { echo "  FAIL: did not replay from CN on relaunch"; FAIL=1; }

log "5. verify converged data + writability"
STBY_FP=$("$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint) FROM t" 2>&1)
"$BIN/psql" -h "$W" -p $SP -U postgres -qc "INSERT INTO t VALUES (999999,'post')" >/dev/null 2>&1
WOK=$("$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT count(*) FROM t WHERE id=999999" 2>&1)
"$BIN/pg_ctl" -D "$STBY" -w stop >/dev/null 2>&1
log "standby after replay: data=$STBY_FP writable=$WOK"
[ "$STBY_FP" = "$NEW_FP" ] || { echo "FAIL: data mismatch (standby=$STBY_FP primary=$NEW_FP)"; FAIL=1; }
[ "$WOK" = "1" ]           || { echo "FAIL: upgraded standby not writable"; FAIL=1; }

echo "========================================================================"
[ "$FAIL" = 0 ] && log "PASS: standby streamed to START, HALTED, relaunched, replayed from CN, converged + writable" \
                || log "FAIL: see messages above"
exit $FAIL
