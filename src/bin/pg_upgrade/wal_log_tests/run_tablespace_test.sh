#!/usr/bin/env bash
# Regression test: a relation in a USER-DEFINED TABLESPACE must survive
# --wal-log-upgrade WAL replay.
#
# KNOWN BUG (this test currently FAILS and documents it): the --wal-log-upgrade
# machinery ignores pg_tblspc/ in THREE places, so user-tablespace relations are
# not WAL-logged and would be lost on a real fresh-target/standby replay:
#   1. capture  (pg_write_upgrade_relfile_data) walks only global/ + base/,
#      never pg_tblspc/<spcoid>/PG_*/<dboid>/  -> no FPI emitted.
#   2. dirskel   (collect_upgrade_dirs) skips symlinks -> the pg_tblspc/<spcoid>
#      symlink and its subtree are never recreated on replay.
#   3. wipe      (revert_wal_logged_disk_writes) skips pg_tblspc/ -> the data is
#      left on disk, which is why a SAME-NODE upgrade appears to work (the table
#      is read from the un-wiped files, NOT reconstructed from WAL).
# This test's disk-wipe assertion is what exposes (3), proving the data is not
# actually WAL-recoverable.  See REPLICA_UPGRADE_DESIGN.md.
#
# When the bug is fixed (capture + dirskel + wipe all handle pg_tblspc/), this
# test should PASS end to end.
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"; BIN="${PGBIN:-$ROOT/pginst/bin}"
W=${WORK:-/tmp/pgu_tblspc}; OLD=$W/old; NEW=$W/new; P=${PORT:-55560}
export PGPORT=$P PGDATABASE=postgres
log(){ echo "=== $* ==="; }
rm -rf "$W"; mkdir -p "$W"

log "init old cluster + create a USER TABLESPACE with a table in it"
"$BIN/initdb" -D "$OLD" -U postgres -N >/dev/null 2>&1 || { echo FAIL initdb; exit 1; }
# allow_in_place_tablespaces lets us use an in-place tablespace (relative path
# under pg_tblspc/<oid>/), whose path differs between the old and new clusters
# -- this sidesteps pg_upgrade's "same catalog version + tablespaces" refusal
# that only triggers for SAME-BUILD tests with absolute-path tablespaces.  The
# relfile layout we are testing (pg_tblspc/<spcoid>/PG_*/<dboid>/<relfile>) is
# identical either way.
echo "unix_socket_directories='$W'">>$OLD/postgresql.conf; echo "port=$P">>$OLD/postgresql.conf
echo "allow_in_place_tablespaces=on">>$OLD/postgresql.conf
"$BIN/pg_ctl" -D "$OLD" -l "$W/old.log" -w start >/dev/null 2>&1 || { echo FAIL start; exit 1; }
"$BIN/psql" -h "$W" -U postgres -v ON_ERROR_STOP=1 -q >/dev/null <<SQL
CREATE TABLESPACE userts LOCATION '';
-- table AND its index in the user tablespace
CREATE TABLE ts_t(id int primary key, v text) TABLESPACE userts;
INSERT INTO ts_t SELECT g, repeat('t',40)||g FROM generate_series(1,5000) g;
CREATE INDEX ts_t_v ON ts_t(v) TABLESPACE userts;
-- a normal-tablespace table too, to prove we don't regress base/
CREATE TABLE base_t(id int, v text);
INSERT INTO base_t SELECT g,'b'||g FROM generate_series(1,1000) g;
SQL
TS_FP=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*), sum(hashtext(v)::bigint) FROM ts_t")
BASE_FP=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*), sum(hashtext(v)::bigint) FROM base_t")
# index-only scan fingerprint (forces reading the index in the user tablespace)
TS_IDX=$("$BIN/psql" -h "$W" -U postgres -tAc "SET enable_seqscan=off; SELECT count(*) FROM ts_t WHERE v > 't'")
log "old: ts_t=$TS_FP base_t=$BASE_FP ts_idx=$TS_IDX"
"$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1

log "pg_upgrade --wal-log-upgrade --initdb --copy"
cd "$W"
# -O passes the in-place-tablespaces GUC to the new cluster's server so the
# restore can recreate the in-place tablespace.
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-log-upgrade --copy \
    -O "-c allow_in_place_tablespaces=on" >"$W/up.log" 2>&1
[ $? -eq 0 ] || { echo FAIL upgrade; tail -25 "$W/up.log"; exit 1; }

# The user tablespace's data files must be WIPED off disk (like base/), so the
# match below proves WAL replay, not leftover files.  Find the tablespace's
# per-db dir and confirm its main-fork data files are gone.
TSDATA=$(find "$NEW"/pg_tblspc -type d -name '[0-9]*' 2>/dev/null | head -1)
if [ -n "$TSDATA" ]; then
    TSBYTES=$(find "$TSDATA" -type f -regextype posix-extended -regex '.*/[0-9]+(\.[0-9]+)?' -printf '%s\n' 2>/dev/null | awk '{s+=$1}END{print s+0}')
    log "user-tablespace data bytes on disk after pg_upgrade (should be 0 = wiped): $TSBYTES"
    [ "${TSBYTES:-0}" = "0" ] || { echo "FAIL: user-tablespace data not wiped ($TSBYTES) -- replay claim unproven"; exit 1; }
fi

echo "unix_socket_directories='$W'">>$NEW/postgresql.conf; echo "port=$P">>$NEW/postgresql.conf
log "start new cluster (WAL replay) and verify the user-tablespace table"
"$BIN/pg_ctl" -D "$NEW" -l "$W/new.log" -w start >/dev/null 2>&1 || { echo FAIL start new; tail -30 "$W/new.log"; exit 1; }
NTS_FP=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*), sum(hashtext(v)::bigint) FROM ts_t" 2>&1)
NBASE_FP=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*), sum(hashtext(v)::bigint) FROM base_t" 2>&1)
NTS_IDX=$("$BIN/psql" -h "$W" -U postgres -tAc "SET enable_seqscan=off; SELECT count(*) FROM ts_t WHERE v > 't'" 2>&1)
log "new: ts_t=$NTS_FP base_t=$NBASE_FP ts_idx=$NTS_IDX"
"$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1

FAIL=0
[ "$TS_FP"  = "$NTS_FP"  ] || { echo "FAIL: user-tablespace TABLE lost/corrupt (old '$TS_FP' new '$NTS_FP')"; FAIL=1; }
[ "$TS_IDX" = "$NTS_IDX" ] || { echo "FAIL: user-tablespace INDEX lost/corrupt (old '$TS_IDX' new '$NTS_IDX')"; FAIL=1; }
[ "$BASE_FP" = "$NBASE_FP" ] || { echo "FAIL: base/ table regressed (old '$BASE_FP' new '$NBASE_FP')"; FAIL=1; }
[ "$FAIL" = 0 ] && log "PASS: user-tablespace relation survived WAL replay" \
                || log "FAIL: user-tablespace data did not survive --wal-log-upgrade"
exit $FAIL
