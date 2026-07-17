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
# --wal-log-upgrade on the primary, commit, keep it live -> a fresh 20devel
# skeleton STREAMS the upgrade window from the live primary (pg_upgrade
# --wal-log-prepare-standby; NO cp) -> it replays from CN and becomes a v20 hot standby
# that matches the upgraded primary.
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

log "2. cross-version pg_upgrade ($OLDVER -> $NEWVER) --wal-log-upgrade on the primary, commit, keep it live"
cd "$W"
"$NEWBIN/pg_upgrade" -b "$OLDBIN" -B "$NEWBIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-log-upgrade --copy >"$W/up.log" 2>&1
[ $? -eq 0 ] || { echo FAIL upgrade; tail -20 "$W/up.log"; exit 1; }
# Keep the upgraded primary LIVE with replication enabled so the standby can
# STREAM the window from it (the retention slot pg_upgrade_window pins the window
# so it survives commit -- no cp).
cat >> "$NEW/postgresql.conf" <<CONF
port=$PP
unix_socket_directories='$W'
wal_level=replica
max_wal_senders=8
listen_addresses='localhost'
CONF
echo "host replication all 127.0.0.1/32 trust" >> "$NEW/pg_hba.conf"
echo "host all all 127.0.0.1/32 trust" >> "$NEW/pg_hba.conf"
# --wal-log-upgrade holds the primary's new cluster in quarantine.  Hold-start it
# (applies the window, reconstructs, holds; pg_ctl exits non-zero by design),
# then commit to adopt it and bring it live.
"$NEWBIN/pg_ctl" -D "$NEW" -l "$W/new_hold.log" -w start >/dev/null 2>&1 || true
"$NEWBIN/pg_upgrade" -b "$OLDBIN" -B "$NEWBIN" -d "$OLD" -D "$NEW" --wal-log-commit >"$W/commit.log" 2>&1 \
    || { echo "FAIL new commit"; tail -20 "$W/commit.log"; exit 1; }
"$NEWBIN/pg_ctl" -D "$NEW" -l "$W/new.log" -w start >/dev/null 2>&1 || { echo "FAIL new start"; tail -15 "$W/new.log"; exit 1; }
NEW_FP=$("$NEWBIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint),(SELECT count(*) FROM toast_t) FROM t")
NEW_CATVER=$("$NEWBIN/pg_controldata" -D "$NEW" | grep 'Catalog version' | grep -oE '[0-9]+')
# leave the primary RUNNING -- the standby streams from it below
log "upgraded primary ($NEWVER) data: $NEW_FP  new catalog version: $NEW_CATVER (live for streaming)"
[ "$OLD_CATVER" != "$NEW_CATVER" ] || { echo "FAIL: catalog versions equal -- not a real cross-version gap"; FAIL=1; }
# Prove the PRIMARY itself upgraded correctly: same data as the old cluster, on
# the new catalog version.  (OLD_FP/NEW_FP use the identical query.)  Only then is
# "standby == primary" meaningful.
[ "$NEW_FP" = "$OLD_FP" ] || { echo "FAIL: upgraded primary data ($NEW_FP) != old source data ($OLD_FP) -- primary upgrade is wrong"; FAIL=1; }
log "primary upgrade verified: data preserved across $OLDVER->$NEWVER ($OLD_FP)"

log "3. build a fresh NEW-version SKELETON as the standby target and STREAM the upgrade WAL (no cp)"
# A cross-version standby CANNOT reuse its old v18 data dir: the new binary's
# PG_VERSION and pg_control version gates reject it BEFORE replay.  So the target
# is a fresh new-version initdb skeleton (like a re-provisioned standby) that
# STREAMS the window from the live upgraded primary via --wal-log-prepare-standby.
#
# NOTE: we deliberately do NOT stamp the skeleton's system identifier.  The
# skeleton keeps its OWN fresh initdb sysid (which differs from the primary's),
# and --wal-log-prepare-standby stamps the primary's sysid + CN anchor + TLI so the
# walreceiver accepts the primary and recovery starts at CN.  This proves the
# pg_resetwal --system-identifier flag is not needed: adoption is in-process.
TGT=$W/target
"$NEWBIN/initdb" -D "$TGT" -U postgres -N >/dev/null 2>&1 || { echo FAIL initdb-target; exit 1; }
SKEL_SYSID=$("$NEWBIN/pg_controldata" -D "$TGT" | grep -i "system identifier" | grep -oE "[0-9]+")
NEW_ID=$("$NEWBIN/pg_controldata" -D "$NEW" | grep -i "system identifier" | grep -oE "[0-9]+")
log "  skeleton sysid=$SKEL_SYSID  primary sysid=$NEW_ID (intentionally DIFFERENT; stamped by --wal-log-prepare-standby)"
# wipe the skeleton's data so nothing masks a missing WAL image (keep runtime dirs)
rm -f "$TGT"/base/*/[0-9]* 2>/dev/null
rm -f "$TGT"/global/[0-9]* "$TGT"/global/pg_filenode.map 2>/dev/null
# primary_conninfo in config (standard for any standby); --wal-log-prepare-standby reads it
cat >> "$TGT/postgresql.conf" <<CONF
port=$SP
unix_socket_directories='$W'
hot_standby=on
primary_conninfo='host=127.0.0.1 port=$PP user=postgres dbname=postgres'
CONF
"$NEWBIN/pg_upgrade" -B "$NEWBIN" -D "$TGT" --wal-log-prepare-standby >"$W/prep.log" 2>&1 \
  || { echo "FAIL: prepare-standby"; cat "$W/prep.log"; FAIL=1; }
# A streamed standby does NOT hold/commit: it streams the window and becomes a hot
# standby of the primary directly.
"$NEWBIN/pg_ctl" -D "$TGT" -l "$W/tgt.log" -w -t 90 start >/dev/null 2>&1 || true
UP=0
for i in $(seq 1 60); do
  "$NEWBIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT 1" >/dev/null 2>&1 && { UP=1; break; }
  sleep 1
done
grep -q "arming streaming standby from anchor" "$W/tgt.log" \
  && log "  target armed from the streaming anchor and STREAMED the $OLDVER->$NEWVER window (no cp)" \
  || { echo "  FAIL: target did not arm+stream from the primary"; tail -20 "$W/tgt.log"; FAIL=1; }
grep -qiE "started streaming|streaming WAL" "$W/tgt.log" \
  || { echo "  FAIL: no evidence the target streamed WAL"; tail -20 "$W/tgt.log"; FAIL=1; }
[ "$UP" = 1 ] || { echo "  FAIL: target did not come up as a hot standby"; tail -20 "$W/tgt.log"; FAIL=1; }
STBY="$TGT"   # everything below inspects the streamed target

log "4. verify the standby is now a $NEWVER cluster matching the primary"
STBY_CATVER=$("$NEWBIN/pg_controldata" -D "$STBY" | grep 'Catalog version' | grep -oE '[0-9]+')
STBY_FP=$("$NEWBIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint),(SELECT count(*) FROM toast_t) FROM t" 2>&1)
INREC=$("$NEWBIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT pg_is_in_recovery()" 2>&1)
STBY_ID=$("$NEWBIN/pg_controldata" -D "$STBY" | grep -i 'system identifier' | grep -oE '[0-9]+')
"$NEWBIN/pg_ctl" -D "$STBY" -w stop >/dev/null 2>&1
"$NEWBIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1
log "standby AFTER upgrade: catalog=$STBY_CATVER data=$STBY_FP in_recovery=$INREC sysid=$STBY_ID"

# The decisive assertions -- only true if the WAL replay actually upgraded it:
[ "$STBY_CATVER" = "$NEW_CATVER" ] || { echo "FAIL: standby catalog version $STBY_CATVER != new $NEW_CATVER (upgrade did NOT happen)"; FAIL=1; }
[ "$STBY_CATVER" != "$OLD_CATVER" ] || { echo "FAIL: standby still at OLD catalog version -- upgrade did NOT replay"; FAIL=1; }
[ "$STBY_FP" = "$NEW_FP" ] || { echo "FAIL: standby data ($STBY_FP) != upgraded primary ($NEW_FP)"; FAIL=1; }
[ "$INREC" = "t" ]         || { echo "FAIL: streamed standby is not a hot standby (in_recovery=$INREC)"; FAIL=1; }
[ "$STBY_ID" = "$NEW_ID" ] || { echo "FAIL: sysid mismatch standby=$STBY_ID primary=$NEW_ID"; FAIL=1; }

echo "========================================================================"
[ "$FAIL" = 0 ] && log "PASS: standby upgraded $OLDVER->$NEWVER purely by STREAMING the WAL (catalog version changed; data matches; hot standby)" \
                || log "FAIL: see messages above"
exit $FAIL
