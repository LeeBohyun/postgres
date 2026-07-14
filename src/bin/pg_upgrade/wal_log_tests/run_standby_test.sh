#!/usr/bin/env bash
# Verify a physical standby can be upgraded by REPLAYING the --wal-log-upgrade
# WAL, instead of the rsync file-copy method.
#
# Scenario:
#   1. Build an old primary + a streaming standby of it.
#   2. Run pg_upgrade --wal-log-upgrade --initdb on the (stopped) primary.
#      The new cluster keeps the OLD cluster's system identifier, so its upgrade
#      WAL is stamped with an id the standby accepts.
#   3. Deliver ONLY the upgrade WAL to the standby and restart on the new binary.
#      The standby keeps its OWN pg_control and PG_VERSION; first startup derives
#      the CN recovery anchor in-band from the WAL (no pg_control/PG_VERSION copy).
#   4. The standby replays the upgrade from CN and converges to the upgraded
#      cluster; its data must match the upgraded primary, and its log must show
#      it armed recovery from CN itself.
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"; BIN="${PGBIN:-$ROOT/pginst/bin}"
W=/tmp/pgu_standby; OLD=$W/old STBY=$W/stby NEW=$W/new
PPORT=55490 SPORT=55491
export PGDATABASE=postgres
log(){ echo "=== $* ==="; }
rm -rf "$W"; mkdir -p "$W"

log "1. init old primary + load data"
"$BIN/initdb" -D "$OLD" -U postgres -N >/dev/null 2>&1
cat >> "$OLD/postgresql.conf" <<CONF
port=$PPORT
unix_socket_directories='$W'
wal_level=replica
max_wal_senders=5
wal_keep_size=256MB
CONF
echo "local replication all trust" >> "$OLD/pg_hba.conf"
"$BIN/pg_ctl" -D "$OLD" -l "$W/old.log" -w start >/dev/null 2>&1 || { echo "primary start FAIL"; exit 1; }
"$BIN/psql" -h "$W" -p $PPORT -U postgres -qc \
  "CREATE TABLE t(id int primary key, v text); INSERT INTO t SELECT g,'v'||g FROM generate_series(1,1000) g;" >/dev/null
OLD_SUM=$("$BIN/psql" -h "$W" -p $PPORT -U postgres -tAc "SELECT count(*), sum(hashtext(v)::bigint) FROM t")
log "primary data: $OLD_SUM"

log "2. base-backup a streaming standby"
"$BIN/pg_basebackup" -h "$W" -p $PPORT -U postgres -D "$STBY" -R >/dev/null 2>&1 || { echo "basebackup FAIL"; exit 1; }
cat >> "$STBY/postgresql.conf" <<CONF
port=$SPORT
unix_socket_directories='$W'
CONF
"$BIN/pg_ctl" -D "$STBY" -l "$W/stby.log" -w start >/dev/null 2>&1 || { echo "standby start FAIL"; exit 1; }
sleep 2
[ "$("$BIN/psql" -h "$W" -p $SPORT -U postgres -tAc 'SELECT pg_is_in_recovery()')" = "t" ] || { echo "standby not in recovery"; exit 1; }
log "standby streaming; sees $("$BIN/psql" -h "$W" -p $SPORT -U postgres -tAc 'SELECT count(*) FROM t') rows"

log "3. stop both, upgrade the primary with --wal-log-upgrade --initdb"
"$BIN/pg_ctl" -D "$STBY" -w stop >/dev/null 2>&1
"$BIN/pg_ctl" -D "$OLD"  -w stop >/dev/null 2>&1
cd "$W"
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-log-upgrade --copy > "$W/up.log" 2>&1
[ $? -eq 0 ] || { echo "FAIL upgrade"; tail -20 "$W/up.log"; exit 1; }

OLD_ID=$("$BIN/pg_controldata" -D "$OLD" | grep -i "system identifier" | grep -oE "[0-9]+")
NEW_ID=$("$BIN/pg_controldata" -D "$NEW" | grep -i "system identifier" | grep -oE "[0-9]+")
log "sysid old=$OLD_ID new=$NEW_ID"
[ "$OLD_ID" = "$NEW_ID" ] || { echo "FAIL: sysid not preserved — standby could not accept the WAL"; exit 1; }

# Snapshot the upgrade WAL AS pg_upgrade LEFT IT, before starting NEW.  Starting
# NEW runs recovery which recycles/overwrites the START..COMPLETE window, so we
# must grab the segments now or they are gone by the time we deliver them.
mkdir -p "$W/upwal"
cp "$NEW/pg_wal"/[0-9A-F]* "$W/upwal/" 2>/dev/null || true

# Bring up the upgraded primary and capture its post-upgrade fingerprint.
echo "port=$PPORT
unix_socket_directories='$W'" >> "$NEW/postgresql.conf"
"$BIN/pg_ctl" -D "$NEW" -l "$W/new.log" -w start >/dev/null 2>&1 || { echo "upgraded primary start FAIL"; tail -20 "$W/new.log"; exit 1; }
NEW_SUM=$("$BIN/psql" -h "$W" -p $PPORT -U postgres -tAc "SELECT count(*), sum(hashtext(v)::bigint) FROM t")
"$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1
log "upgraded primary data: $NEW_SUM"

log "4. deliver ONLY the upgrade WAL to the standby — NO pg_control, NO PG_VERSION copy"
# The standby is a physical copy of the OLD cluster and KEEPS ITS OWN pg_control.
# We deliberately do NOT copy the primary's pg_control: first startup on the new
# binary runs PerformWalUpgradeIfNeeded(), which scans pg_wal/, DERIVES the CN
# recovery anchor from the CN checkpoint record in the upgrade WAL itself, and
# arms the standby's own pg_control in-process.  If the standby still converges,
# the anchor was recovered IN-BAND from the WAL — which is the whole point (and
# the mechanism a real streaming standby must use).  Copying pg_control would
# mask that, so it is intentionally omitted.
#
# PG_VERSION is likewise NOT copied: the XLOG_PG_UPGRADE_START redo handler
# writes $PGDATA/PG_VERSION from the version string embedded in the START record,
# so it is reconstructed from the WAL like everything else.  (In a real
# cross-major upgrade the on-disk PG_VERSION still reflects the OLD version and
# the NEW binary's pre-replay ValidatePgVersion() gate would reject it before
# replay -- a separate bootstrap problem for the replica path, tracked in
# REPLICA_UPGRADE_DESIGN.md; it does not arise in this same-build test.)
rm -f "$STBY/pg_wal"/[0-9A-F]*
cp "$W/upwal"/[0-9A-F]* "$STBY/pg_wal/" 2>/dev/null || true
# Remove standby.signal so it does normal crash recovery of the upgrade WAL.
rm -f "$STBY/standby.signal" "$STBY/recovery.signal"

log "5. restart standby on the new binary — expect it to replay the upgrade"
"$BIN/pg_ctl" -D "$STBY" -l "$W/stby2.log" -w start >/dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "standby did not start; log:"; tail -20 "$W/stby2.log"
    # A FATAL 're-run pg_upgrade' here would indicate the WAL/anchor delivery
    # was incomplete — report and fail.
    exit 1
fi
STBY_SUM=$("$BIN/psql" -h "$W" -p $SPORT -U postgres -tAc "SELECT count(*), sum(hashtext(v)::bigint) FROM t" 2>&1)
STBY_ID=$("$BIN/pg_controldata" -D "$STBY" | grep -i "system identifier" | grep -oE "[0-9]+")
# WRITABILITY: writing exercises catalog access, which fails if any (empty)
# catalog relfile was not reconstructed by replay.  Must succeed.
"$BIN/psql" -h "$W" -p $SPORT -U postgres -qc "INSERT INTO t VALUES (999999,'post-upgrade')" >/dev/null 2>&1
STBY_WOK=$("$BIN/psql" -h "$W" -p $SPORT -U postgres -tAc "SELECT count(*) FROM t WHERE id=999999" 2>&1)
"$BIN/pg_ctl" -D "$STBY" -w stop >/dev/null 2>&1
log "standby after upgrade: data=$STBY_SUM sysid=$STBY_ID writable=$STBY_WOK"

FAIL=0
# Prove the anchor was derived IN-BAND from the WAL (not copied): the startup
# log must show PerformWalUpgradeIfNeeded arming recovery from CN.
if grep -q "arming recovery from end-of-upgrade checkpoint" "$W/stby2.log"; then
    log "  standby derived CN in-band ($(grep 'arming recovery' "$W/stby2.log" | tail -1 | sed 's/.*checkpoint //'))"
else
    echo "MISSING: standby log has no in-band CN arming message (did it use a copied anchor?)"; FAIL=1
fi
[ "$STBY_WOK" = "1" ] || { echo "MISMATCH: upgraded standby not writable (a catalog relfile missing?)"; FAIL=1; }
[ "$STBY_SUM" = "$NEW_SUM" ] || { echo "MISMATCH: standby data ($STBY_SUM) != upgraded primary ($NEW_SUM)"; FAIL=1; }
[ "$STBY_ID"  = "$NEW_ID"  ] || { echo "MISMATCH: standby sysid ($STBY_ID) != upgraded primary ($NEW_ID)"; FAIL=1; }
[ "$FAIL" = 0 ] && log "PASS: standby upgraded via in-band WAL replay; data + sysid match upgraded primary" \
                || log "FAIL: standby did not converge"
exit $FAIL
