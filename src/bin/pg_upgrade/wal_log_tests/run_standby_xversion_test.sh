#!/usr/bin/env bash
# CROSS-VERSION standby upgrade-via-WAL-replay test.
#
# This is the test that actually PROVES the standby replayed the upgrade: the old
# cluster is a genuinely older major version (PG18) and the new binary is 20devel.
# A same-version test cannot prove anything -- the standby is a physical copy of
# the old cluster, so if old and new are the same build its data matches the
# "upgraded" primary whether or not any upgrade happened.  Across 18 -> 20 the
# old on-disk cluster is v18 (catalog version 202506291); only a real replay of
# the upgrade WAL can turn it into a working v20 cluster (catalog 202607022).
#
# Flow: PG18 old primary + PG18 basebackup standby -> pg_upgrade 18->20
# --wal-log-upgrade on the primary -> deliver the upgrade WAL to the standby ->
# relaunch the standby ON THE 20devel BINARY -> it replays from CN and must
# become a v20 cluster that matches the upgraded primary and is writable.
#
# Requires: OLDBIN (an 18.x bin dir) and NEWBIN (the 20devel wal_log_upgrade
# build).  Set them via env; defaults match the Arca layout used in development.
set -u
NEWBIN="${PGBIN:?set PGBIN to the 20devel bin dir}"
OLDBIN="${OLDBIN:?set OLDBIN to an older-major (e.g. 18.x) bin dir}"
W=${WORK:-/tmp/pgu_xver}; OLD=$W/old STBY=$W/stby NEW=$W/new
PP=${PPORT:-55925} SP=${SPORT:-55926}
export PGDATABASE=postgres
log(){ echo "=== $* ==="; }
FAIL=0
rm -rf "$W"; mkdir -p "$W"

log "0. binary versions"
OLDVER=$("$OLDBIN/pg_controldata" --version | grep -oE '[0-9]+devel|[0-9]+\.[0-9]+')
NEWVER=$("$NEWBIN/pg_controldata" --version | grep -oE '[0-9]+devel|[0-9]+\.[0-9]+')
log "old=$OLDVER  new=$NEWVER"
[ "$OLDVER" != "$NEWVER" ] || { echo "FAIL: OLDBIN and NEWBIN are the same version -- this test needs a real cross-version gap"; exit 1; }

log "1. OLD ($OLDVER) primary + caught-up streaming standby"
"$OLDBIN/initdb" -D "$OLD" -U postgres -N >/dev/null 2>&1 || { echo FAIL initdb; exit 1; }
cat >> "$OLD/postgresql.conf" <<CONF
port=$PP
unix_socket_directories='$W'
wal_level=replica
max_wal_senders=5
wal_keep_size=256MB
CONF
echo "local replication all trust" >> "$OLD/pg_hba.conf"
"$OLDBIN/pg_ctl" -D "$OLD" -l "$W/old.log" -w start >/dev/null 2>&1 || { echo FAIL start; exit 1; }
"$OLDBIN/psql" -h "$W" -p $PP -U postgres -q >/dev/null 2>&1 <<SQL
CREATE TABLE t(id int primary key, v text);
INSERT INTO t SELECT g,'v'||g FROM generate_series(1,2000) g;
CREATE INDEX ON t(v);
CREATE TABLE toast_t(id int, big text);
INSERT INTO toast_t SELECT g, repeat(md5(g::text),300) FROM generate_series(1,300) g;
SQL
OLD_FP=$("$OLDBIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint),(SELECT count(*) FROM toast_t) FROM t")
OLD_CATVER=$("$OLDBIN/pg_controldata" -D "$OLD" | grep 'Catalog version' | grep -oE '[0-9]+')
"$OLDBIN/pg_basebackup" -h "$W" -p $PP -U postgres -D "$STBY" -R >/dev/null 2>&1 || { echo FAIL basebackup; exit 1; }
"$OLDBIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1
log "old data: $OLD_FP  old catalog version: $OLD_CATVER"

# sanity: the standby basebackup is genuinely the OLD version on disk
STBY_CATVER_BEFORE=$("$NEWBIN/pg_controldata" -D "$STBY" 2>/dev/null | grep 'Catalog version' | grep -oE '[0-9]+')
log "standby catalog version BEFORE upgrade: $STBY_CATVER_BEFORE (should equal old=$OLD_CATVER)"

log "2. cross-version pg_upgrade ($OLDVER -> $NEWVER) --wal-log-upgrade on the primary"
cd "$W"
"$NEWBIN/pg_upgrade" -b "$OLDBIN" -B "$NEWBIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-log-upgrade --copy >"$W/up.log" 2>&1
[ $? -eq 0 ] || { echo FAIL upgrade; tail -20 "$W/up.log"; exit 1; }
mkdir -p "$W/upwal"; cp "$NEW/pg_wal"/[0-9A-F]* "$W/upwal/" 2>/dev/null || true
cat >> "$NEW/postgresql.conf" <<CONF
port=$PP
unix_socket_directories='$W'
CONF
"$NEWBIN/pg_ctl" -D "$NEW" -l "$W/new.log" -w start >/dev/null 2>&1 || { echo "FAIL new start"; tail -15 "$W/new.log"; exit 1; }
NEW_FP=$("$NEWBIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint),(SELECT count(*) FROM toast_t) FROM t")
NEW_CATVER=$("$NEWBIN/pg_controldata" -D "$NEW" | grep 'Catalog version' | grep -oE '[0-9]+')
"$NEWBIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1
log "upgraded primary ($NEWVER) data: $NEW_FP  new catalog version: $NEW_CATVER"
[ "$OLD_CATVER" != "$NEW_CATVER" ] || { echo "FAIL: catalog versions equal -- not a real cross-version gap"; FAIL=1; }

log "3. build a fresh NEW-version SKELETON as the standby target, deliver upgrade WAL, replay from CN"
# A cross-version standby CANNOT reuse its old v18 data dir: the new binary's
# PG_VERSION and pg_control version gates reject it BEFORE replay.  So the target
# is a fresh new-version initdb skeleton (like a re-provisioned standby), fed only
# the upgrade WAL.  This is the same model as run_e2e_equivalence_test.sh.
#
# NOTE: we deliberately do NOT stamp the skeleton's system identifier.  The
# skeleton keeps its OWN fresh initdb sysid (which differs from the burst's), and
# first startup adopts the burst's sysid in-process (PerformWalUpgradeIfNeeded ->
# ArmControlFileForUpgradeRecovery reads xlp_sysid from the delivered WAL).  This
# is what proves the pg_resetwal --system-identifier flag is no longer needed:
# if in-process adoption were broken, replay would FATAL with "WAL file is from
# different database system".
TGT=$W/target
"$NEWBIN/initdb" -D "$TGT" -U postgres -N >/dev/null 2>&1 || { echo FAIL initdb-target; exit 1; }
SKEL_SYSID=$("$NEWBIN/pg_controldata" -D "$TGT" | grep -i "system identifier" | grep -oE "[0-9]+")
BURST_SYSID=$("$NEWBIN/pg_controldata" -D "$NEW" | grep -i "system identifier" | grep -oE "[0-9]+")
log "  skeleton sysid=$SKEL_SYSID  burst sysid=$BURST_SYSID (intentionally DIFFERENT; adoption happens in-process)"
# wipe the skeleton's data so nothing masks a missing WAL image (keep runtime dirs)
rm -f "$TGT"/base/*/[0-9]* 2>/dev/null
rm -f "$TGT"/global/[0-9]* "$TGT"/global/pg_filenode.map 2>/dev/null
rm -f "$TGT"/pg_xact/* "$TGT"/pg_multixact/offsets/* "$TGT"/pg_multixact/members/* 2>/dev/null
rm -f "$TGT/pg_wal"/[0-9A-F]* 2>/dev/null
cp "$W/upwal"/[0-9A-F]* "$TGT/pg_wal/" 2>/dev/null || true
cat >> "$TGT/postgresql.conf" <<CONF
port=$SP
unix_socket_directories='$W'
CONF
"$NEWBIN/pg_ctl" -D "$TGT" -l "$W/tgt.log" -w -t 60 start >/dev/null 2>&1
if [ $? -ne 0 ]; then echo "FAIL: target did not start/replay"; tail -20 "$W/tgt.log"; FAIL=1; fi
grep -q "arming recovery from end-of-upgrade checkpoint" "$W/tgt.log" \
  && log "  target armed + replayed the $OLDVER->$NEWVER upgrade from CN in-band" \
  || { echo "  FAIL: target did not replay the upgrade from CN"; FAIL=1; }
STBY="$TGT"   # everything below inspects the replayed target

log "4. verify the standby is now a $NEWVER cluster matching the primary, and writable"
STBY_CATVER=$("$NEWBIN/pg_controldata" -D "$STBY" | grep 'Catalog version' | grep -oE '[0-9]+')
STBY_FP=$("$NEWBIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint),(SELECT count(*) FROM toast_t) FROM t" 2>&1)
"$NEWBIN/psql" -h "$W" -p $SP -U postgres -qc "INSERT INTO t VALUES (999999,'post')" >/dev/null 2>&1
WOK=$("$NEWBIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT count(*) FROM t WHERE id=999999" 2>&1)
STBY_ID=$("$NEWBIN/pg_controldata" -D "$STBY" | grep -i 'system identifier' | grep -oE '[0-9]+')
NEW_ID=$("$NEWBIN/pg_controldata" -D "$NEW" | grep -i 'system identifier' | grep -oE '[0-9]+')
"$NEWBIN/pg_ctl" -D "$STBY" -w stop >/dev/null 2>&1
log "standby AFTER upgrade: catalog=$STBY_CATVER data=$STBY_FP writable=$WOK sysid=$STBY_ID"

# The decisive assertions -- only true if the WAL replay actually upgraded it:
[ "$STBY_CATVER" = "$NEW_CATVER" ] || { echo "FAIL: standby catalog version $STBY_CATVER != new $NEW_CATVER (upgrade did NOT happen)"; FAIL=1; }
[ "$STBY_CATVER" != "$OLD_CATVER" ] || { echo "FAIL: standby still at OLD catalog version -- upgrade did NOT replay"; FAIL=1; }
[ "$STBY_FP" = "$NEW_FP" ] || { echo "FAIL: standby data ($STBY_FP) != upgraded primary ($NEW_FP)"; FAIL=1; }
[ "$WOK" = "1" ]           || { echo "FAIL: upgraded standby not writable"; FAIL=1; }
[ "$STBY_ID" = "$NEW_ID" ] || { echo "FAIL: sysid mismatch standby=$STBY_ID primary=$NEW_ID"; FAIL=1; }

echo "========================================================================"
[ "$FAIL" = 0 ] && log "PASS: standby upgraded $OLDVER->$NEWVER purely by WAL replay (catalog version changed; data matches; writable)" \
                || log "FAIL: see messages above"
exit $FAIL
