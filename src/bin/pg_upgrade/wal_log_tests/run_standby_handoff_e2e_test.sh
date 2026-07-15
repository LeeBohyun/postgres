#!/usr/bin/env bash
# END-TO-END standby upgrade via the OLD-FORMAT HANDOFF TRIGGER.
#
# This ties the two proven halves into one operator story:
#
#   (A) TRIGGER / HALT  -- a caught-up physical standby streams the old primary;
#       the primary emits pg_write_pg_upgrade_handoff() into its OWN old-format
#       WAL; the standby replays it and SHUTS ITSELF DOWN cleanly with
#       "shutting down for pg_upgrade" (the halt that the new-format START burst
#       can never reach -- see run_handoff_trigger_test.sh and TODO.md item 1).
#
#   (B) TRANSPORT / RE-PROVISION -- pg_upgrade --wal-log-upgrade runs on the
#       primary; the self-contained upgrade window is delivered to a fresh
#       new-version skeleton (the re-provisioned standby) and replayed from CN
#       in band (the out-of-band model proven by run_standby_xversion_test.sh /
#       run_e2e_equivalence_test.sh).
#
# SCOPE: this exercises the FULL WIRING (halt -> upgrade -> re-provision ->
# converge -> writable) on a SINGLE fork build (the trigger record + its SQL
# function only exist in this fork, so a stock-PG18 primary cannot emit it --
# the trigger is forward-looking, see TODO.md).  It does NOT by itself prove a
# cross-major catalog change; run_standby_xversion_test.sh proves that half
# (18->20 catalog version changes).  Here the decisive new assertion is that the
# standby HALTED ITSELF via the trigger and was then re-provisioned to converge.
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
BIN="${PGBIN:-$ROOT/pginst/bin}"
W=${WORK:-/tmp/pgu_handoff_e2e}; OLD=$W/old STBY=$W/stby NEW=$W/new TGT=$W/target
PP=${PPORT:-55944} SP=${SPORT:-55945}
export PGDATABASE=postgres
log(){ echo "=== $* ==="; }
FAIL=0
for p in $PP $SP; do lsof -ti :$p 2>/dev/null | xargs kill -9 2>/dev/null; done
rm -rf "$W"; mkdir -p "$W"

log "1. OLD (fork) primary + caught-up streaming standby"
"$BIN/initdb" -D "$OLD" -U postgres -N >/dev/null 2>&1 || { echo FAIL initdb; exit 1; }
cat >> "$OLD/postgresql.conf" <<CONF
port=$PP
unix_socket_directories='$W'
wal_level=replica
max_wal_senders=5
wal_keep_size=256MB
CONF
echo "local replication all trust" >> "$OLD/pg_hba.conf"
"$BIN/pg_ctl" -D "$OLD" -l "$W/old.log" -w start >/dev/null 2>&1 || { echo FAIL start; exit 1; }
"$BIN/psql" -h "$W" -p $PP -U postgres -q >/dev/null 2>&1 <<SQL
CREATE TABLE t(id int primary key, v text);
INSERT INTO t SELECT g,'v'||g FROM generate_series(1,2000) g;
CREATE TABLE toast_t(id int, big text);
INSERT INTO toast_t SELECT g, repeat(md5(g::text),300) FROM generate_series(1,300) g;
SQL
OLD_FP=$("$BIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint),(SELECT count(*) FROM toast_t) FROM t")
"$BIN/pg_basebackup" -h "$W" -p $PP -U postgres -D "$STBY" -R >/dev/null 2>&1 || { echo FAIL basebackup; exit 1; }
cat >> "$STBY/postgresql.conf" <<CONF
port=$SP
unix_socket_directories='$W'
primary_conninfo='host=$W port=$PP user=postgres'
CONF
touch "$STBY/standby.signal"
"$BIN/pg_ctl" -D "$STBY" -l "$W/stby.log" -w start >/dev/null 2>&1 || { echo FAIL standby start; tail "$W/stby.log"; exit 1; }
sleep 2
SB_ROWS=$("$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT count(*) FROM t" 2>&1)
log "standby streaming, sees $SB_ROWS rows (want 2000)"
[ "$SB_ROWS" = 2000 ] || FAIL=1
SPID=$(head -1 "$STBY/postmaster.pid" 2>/dev/null)

log "2. (A) primary emits the HANDOFF TRIGGER; the standby must SHUT ITSELF DOWN"
"$BIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT pg_write_pg_upgrade_handoff(20)" >/dev/null
"$BIN/psql" -h "$W" -p $PP -U postgres -qc "SELECT pg_switch_wal()" >/dev/null
DOWN=0
for i in $(seq 1 30); do
  if [ -n "$SPID" ] && ! kill -0 "$SPID" 2>/dev/null; then DOWN=1; break; fi
  sleep 1
done
if [ "$DOWN" = 1 ] && grep -q "shutting down for pg_upgrade" "$W/stby.log"; then
  log "  standby HALTED via the trigger and shut itself down:"
  grep -iE "shutting down for pg_upgrade|database system is shut down" "$W/stby.log" | tail -2
else
  echo "  FAIL: standby did not self-shutdown via the trigger"; tail -10 "$W/stby.log"; FAIL=1
fi
NF=$(grep -c "reached pg_upgrade handoff on standby" "$W/stby.log")
[ "$NF" = 1 ] || { echo "  FAIL: handoff FATAL x$NF -- restart loop, not a clean halt"; FAIL=1; }

log "3. operator now upgrades the primary (--wal-log-upgrade)"
# old primary is still up; pg_upgrade needs it stopped
"$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1
cd "$W"
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-log-upgrade --copy >"$W/up.log" 2>&1
[ $? -eq 0 ] || { echo FAIL upgrade; tail -20 "$W/up.log"; exit 1; }
mkdir -p "$W/upwal"; cp "$NEW/pg_wal"/[0-9A-F]* "$W/upwal/" 2>/dev/null || true
# --wal-log-upgrade holds the primary's new cluster in quarantine; commit it so
# it goes live.  (The upgrade WAL was already copied to $W/upwal above, before
# the commit recycles it, so the standby re-provision below still has it.)
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" --commit >"$W/commit.log" 2>&1 \
    || { echo "FAIL new commit"; tail -20 "$W/commit.log"; exit 1; }
cat >> "$NEW/postgresql.conf" <<CONF
port=$PP
unix_socket_directories='$W'
CONF
"$BIN/pg_ctl" -D "$NEW" -l "$W/new.log" -w start >/dev/null 2>&1 || { echo "FAIL new start"; tail -15 "$W/new.log"; exit 1; }
NEW_FP=$("$BIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint),(SELECT count(*) FROM toast_t) FROM t")
NEW_ID=$("$BIN/pg_controldata" -D "$NEW" | grep -i 'system identifier' | grep -oE '[0-9]+')
"$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1
log "upgraded primary fingerprint: $NEW_FP"

log "4. (B) RE-PROVISION the halted standby: fresh skeleton + delivered window, replay from CN"
"$BIN/initdb" -D "$TGT" -U postgres -N >/dev/null 2>&1 || { echo FAIL initdb-target; exit 1; }
rm -f "$TGT"/base/*/[0-9]* 2>/dev/null
rm -f "$TGT"/global/[0-9]* "$TGT"/global/pg_filenode.map 2>/dev/null
rm -f "$TGT"/pg_xact/* "$TGT"/pg_multixact/offsets/* "$TGT"/pg_multixact/members/* 2>/dev/null
# Do NOT stamp the skeleton's sysid: first startup adopts the delivered burst's
# sysid in-process (no pg_resetwal --system-identifier needed).
rm -f "$TGT/pg_wal"/[0-9A-F]*
cp "$W/upwal"/[0-9A-F]* "$TGT/pg_wal/" 2>/dev/null || true
cat >> "$TGT/postgresql.conf" <<CONF
port=$SP
unix_socket_directories='$W'
CONF
# The re-provisioned target is a fresh skeleton fed the delivered upgrade window;
# --wal-log-upgrade replay holds it in quarantine.  Commit adopts it (this is the
# standby-side commit).  No old cluster here, so only -D is given.  The CN-anchored
# replay happens during commit, logged to the target's pg_upgrade_commit.log.
"$BIN/pg_upgrade" -B "$BIN" -D "$TGT" --commit >"$W/tgt_commit.log" 2>&1 \
  || { echo "FAIL: re-provisioned standby commit"; tail -20 "$W/tgt_commit.log"; FAIL=1; }
grep -q "arming recovery from end-of-upgrade checkpoint" "$TGT/pg_upgrade_commit.log" 2>/dev/null \
  && log "  re-provisioned standby armed + replayed the upgrade from CN in-band" \
  || { echo "  FAIL: re-provisioned standby did not replay from CN"; FAIL=1; }
"$BIN/pg_ctl" -D "$TGT" -l "$W/tgt.log" -w -t 60 start >/dev/null 2>&1
if [ $? -ne 0 ]; then echo "FAIL: re-provisioned standby did not start after commit"; tail -20 "$W/tgt.log"; FAIL=1; fi

log "5. verify the re-provisioned standby converged to the primary and is writable"
STBY_FP=$("$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint),(SELECT count(*) FROM toast_t) FROM t" 2>&1)
"$BIN/psql" -h "$W" -p $SP -U postgres -qc "INSERT INTO t VALUES (999999,'post')" >/dev/null 2>&1
WOK=$("$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT count(*) FROM t WHERE id=999999" 2>&1)
STBY_ID=$("$BIN/pg_controldata" -D "$TGT" | grep -i 'system identifier' | grep -oE '[0-9]+')
"$BIN/pg_ctl" -D "$TGT" -w stop >/dev/null 2>&1
log "re-provisioned standby: data=$STBY_FP writable=$WOK sysid=$STBY_ID"
[ "$STBY_FP" = "$NEW_FP" ] || { echo "FAIL: standby data ($STBY_FP) != upgraded primary ($NEW_FP)"; FAIL=1; }
[ "$WOK" = "1" ]           || { echo "FAIL: re-provisioned standby not writable"; FAIL=1; }
[ "$STBY_ID" = "$NEW_ID" ] || { echo "FAIL: sysid mismatch standby=$STBY_ID primary=$NEW_ID"; FAIL=1; }

"$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1 || true
lsof -ti :$PP :$SP 2>/dev/null | xargs kill -9 2>/dev/null

echo "========================================================================"
[ "$FAIL" = 0 ] && log "PASS: streaming standby halted via the handoff trigger, was re-provisioned from the delivered window, and converged (writable)" \
                || log "FAIL: see messages above"
exit $FAIL
