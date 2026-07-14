#!/usr/bin/env bash
# Q1: a standby that applies the upgrade in ARCHIVE recovery must finalize via
# the built-in end-of-recovery TIMELINE SWITCH (not a same-timeline checkpoint
# that would fork the primary's history), AND must survive a subsequent restart
# without re-arming/re-replaying the upgrade.
#
# This exercises two things the earlier standby test did not:
#   1. Delivering the upgrade via recovery.signal (archive recovery) so
#      ArchiveRecoveryRequested=true -> StartupXLOG switches to a new timeline at
#      end-of-recovery (no same-timeline fork).
#   2. Restarting the upgraded standby: PerformWalUpgradeIfNeeded must see, via
#      the control file (checkpoint LSN >= COMPLETE's LSN), that the upgrade is
#      already applied and NOT re-scan/re-arm -- even though the TLI-1
#      START..COMPLETE window is still physically present in pg_wal/ alongside
#      the new TLI-2 segments.  (Before the fix this FATAL'd trying to open a
#      nonexistent TLI-1 segment.)
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"; BIN="${PGBIN:-$ROOT/pginst/bin}"
W=${WORK:-/tmp/pgu_stbytli}; OLD=$W/old STBY=$W/stby NEW=$W/new ARCH=$W/arch
PP=${PPORT:-55820} SP=${SPORT:-55821}
export PGDATABASE=postgres
log(){ echo "=== $* ==="; }
FAIL=0
rm -rf "$W"; mkdir -p "$W" "$ARCH"

log "1. old primary (archiving on) + streaming standby"
"$BIN/initdb" -D "$OLD" -U postgres -N >/dev/null 2>&1 || { echo FAIL initdb; exit 1; }
cat >>"$OLD/postgresql.conf" <<C
port=$PP
unix_socket_directories='$W'
wal_level=replica
max_wal_senders=5
wal_keep_size=256MB
archive_mode=on
archive_command='cp %p $ARCH/%f'
C
echo "local replication all trust" >>"$OLD/pg_hba.conf"
"$BIN/pg_ctl" -D "$OLD" -l "$W/old.log" -w start >/dev/null 2>&1 || { echo FAIL start; exit 1; }
"$BIN/psql" -h "$W" -p $PP -U postgres -qc \
  "CREATE TABLE t(id int primary key, v text); INSERT INTO t SELECT g,'v'||g FROM generate_series(1,2000) g;" >/dev/null
OLD_FP=$("$BIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint) FROM t")
log "old data: $OLD_FP"
"$BIN/pg_basebackup" -h "$W" -p $PP -U postgres -D "$STBY" -R >/dev/null 2>&1 || { echo FAIL basebackup; exit 1; }
"$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1

log "2. upgrade the primary (--wal-log-upgrade)"
cd "$W"
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-log-upgrade --copy >"$W/up.log" 2>&1
[ $? -eq 0 ] || { echo FAIL upgrade; tail -15 "$W/up.log"; exit 1; }
mkdir -p "$W/upwal"; cp "$NEW/pg_wal"/[0-9A-F]* "$W/upwal/" 2>/dev/null || true

log "3. deliver upgrade WAL to standby in ARCHIVE recovery (recovery.signal), fix port"
rm -f "$STBY/pg_wal"/[0-9A-F]*
cp "$W/upwal"/[0-9A-F]* "$STBY/pg_wal/" 2>/dev/null || true
rm -f "$STBY/standby.signal" "$STBY/recovery.signal"
touch "$STBY/recovery.signal"
# basebackup copied port=$PP; force the standby's own port so we can query it.
sed -i.bak "s/^port=.*/port=$SP/" "$STBY/postgresql.conf" 2>/dev/null || \
  { echo "port=$SP" >> "$STBY/postgresql.conf"; }
cat >>"$STBY/postgresql.conf" <<C
restore_command='cp $ARCH/%f %p 2>/dev/null || cp $W/upwal/%f %p 2>/dev/null'
recovery_target_timeline='latest'
C

log "4. first startup: apply upgrade + expect a TIMELINE SWITCH (no same-tli fork)"
"$BIN/pg_ctl" -D "$STBY" -l "$W/s1.log" -w -t 60 start >/dev/null 2>&1
if grep -q "arming recovery from end-of-upgrade checkpoint" "$W/s1.log"; then
    log "  applied upgrade in-band"
else
    echo "  FAIL: standby did not arm/apply the upgrade"; tail -8 "$W/s1.log"; FAIL=1
fi
if grep -q "selected new timeline ID" "$W/s1.log"; then
    log "  $(grep 'selected new timeline ID' "$W/s1.log" | tail -1 | sed 's/.*LOG:  //')"
else
    echo "  FAIL: no end-of-recovery timeline switch (would fork the primary)"; FAIL=1
fi
S1_FP=$("$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint) FROM t" 2>&1)
S1_TLI=$("$BIN/pg_controldata" -D "$STBY" | grep "Latest checkpoint's TimeLineID" | grep -oE "[0-9]+")
log "  standby data=$S1_FP tli=$S1_TLI"
[ "$S1_FP" = "$OLD_FP" ] || { echo "  FAIL: standby data mismatch (old '$OLD_FP' new '$S1_FP')"; FAIL=1; }
[ "${S1_TLI:-1}" -ge 2 ] || { echo "  FAIL: expected timeline >= 2 after switch, got $S1_TLI"; FAIL=1; }
"$BIN/pg_ctl" -D "$STBY" -w stop >/dev/null 2>&1

log "5. RESTART the upgraded standby: must NOT re-arm (control file is past COMPLETE)"
# The TLI-1 START..COMPLETE window is still in pg_wal/ next to the new TLI-2
# segments; the applied-check (control checkpoint LSN >= COMPLETE LSN) must skip.
"$BIN/pg_ctl" -D "$STBY" -l "$W/s2.log" -w -t 30 start >/dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "  FAIL: restart of upgraded standby did not come up"; grep -iE "FATAL|could not open" "$W/s2.log" | head -3; FAIL=1
else
    if grep -qE "arming recovery from end-of-upgrade|pg_upgrade WAL found" "$W/s2.log"; then
        echo "  FAIL: restart RE-ARMED the upgrade (should be applied-and-done)"; FAIL=1
    else
        log "  restart did not re-arm (applied-check held)"
    fi
    S2_FP=$("$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint) FROM t" 2>&1)
    log "  data after restart: $S2_FP"
    [ "$S2_FP" = "$OLD_FP" ] || { echo "  FAIL: data mismatch after restart"; FAIL=1; }
    "$BIN/pg_ctl" -D "$STBY" -w stop >/dev/null 2>&1
fi

echo "========================================================================"
[ "$FAIL" = 0 ] && log "PASS: archive-recovery upgrade -> timeline switch (no fork); restart does not re-arm" \
                || log "FAIL: see messages above"
exit $FAIL
