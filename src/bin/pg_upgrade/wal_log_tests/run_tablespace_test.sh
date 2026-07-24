#!/usr/bin/env bash
# Regression test: a relation in a USER-DEFINED TABLESPACE must survive
# --wal-upgrade WAL replay.
#
# KNOWN BUG (this test currently FAILS and documents it): the --wal-upgrade
# machinery ignores pg_tblspc/ in THREE places, so user-tablespace relations are
# not WAL-logged and would be lost on a real fresh-target/standby replay:
#   1. capture  (pg_upgrade_wal_log_relfile) walks only global/ + base/,
#      never pg_tblspc/<spcoid>/PG_*/<dboid>/  -> no FPI emitted.
#   2. dirtree   (collect_upgrade_dirs) skips symlinks -> the pg_tblspc/<spcoid>
#      symlink and its subtree are never recreated on replay.
#   3. wipe      (revert_wal_logged_disk_writes) skips pg_tblspc/ -> the data is
#      left on disk, which is why a SAME-NODE upgrade appears to work (the table
#      is read from the un-wiped files, NOT reconstructed from WAL).
# This test's disk-wipe assertion is what exposes (3), proving the data is not
# actually WAL-recoverable.  See REPLICA_UPGRADE_DESIGN.md.
#
# When the bug is fixed (capture + dirtree + wipe all handle pg_tblspc/), this
# test should PASS end to end.
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"; BIN="${PGBIN:-$ROOT/pginst/bin}"
W=${WORK:-/tmp/pgu_tblspc}; OLD=$W/old; NEW=$W/new; P=${PORT:-55560}
MODE=${MODE:---copy}   # transfer mode: --copy --copy-file-range --link --clone --swap
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
# EXTERNAL-location tablespaces are only reachable in a real CROSS-VERSION
# upgrade: pg_upgrade refuses "same system catalog version + tablespaces" when
# the tablespace path is identical between clusters (tablespace.c), which is
# always true for an absolute external path in a same-build test.  In-place
# tablespaces (relative path, differs per cluster) are allowed, so we drive the
# capture/wipe path with an in-place tablespace here.  The Q7b symlink
# capture/replay itself is covered directly by run_tblspc_symlink_test.sh.
"$BIN/psql" -h "$W" -U postgres -v ON_ERROR_STOP=1 -q >/dev/null <<SQL
-- IN-PLACE tablespace (relative, under pg_tblspc/<oid>/)
CREATE TABLESPACE userts LOCATION '';
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

cd "$W"
log "pg_upgrade --wal-upgrade $MODE"
# -O passes the in-place-tablespaces GUC to the new cluster's server so the
# restore can recreate the in-place tablespace.
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-upgrade $MODE \
    -O "-c allow_in_place_tablespaces=on" >"$W/up.log" 2>&1
[ $? -eq 0 ] || { echo FAIL upgrade; tail -25 "$W/up.log"; exit 1; }

# BOTH tablespaces' data files must be WIPED off disk (like base/), so the match
# below proves WAL replay, not leftover files.  In-place data lives under
# $NEW/pg_tblspc/<oid>/PG_*/<dboid>/; external data lives under the external
# location's PG_*/<dboid>/ (reached via the symlink).
tsbytes() { find "$1" -type f -regextype posix-extended -regex '.*/[0-9]+(\.[0-9]+)?' -printf '%s\n' 2>/dev/null | awk '{s+=$1}END{print s+0}'; }
IP_BYTES=$(tsbytes "$NEW/pg_tblspc")
log "tablespace data on disk after pg_upgrade (should be 0=wiped): $IP_BYTES"
[ "${IP_BYTES:-0}" = "0" ]  || { echo "FAIL: tablespace data not wiped ($IP_BYTES) -- replay unproven"; exit 1; }

# --wal-upgrade auto-serves: the new cluster comes up read-write on the
# first start (no quarantine hold, no commit).  The wiped-on-disk assertion
# above ran before first start, so it still reflects the wipe.
echo "unix_socket_directories='$W'">>$NEW/postgresql.conf; echo "port=$P">>$NEW/postgresql.conf
log "start new cluster (WAL replay) and verify tablespace table"
"$BIN/pg_ctl" -D "$NEW" -l "$W/new.log" -w start >/dev/null 2>&1 || { echo FAIL start new; tail -30 "$W/new.log"; exit 1; }
NTS_FP=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*), sum(hashtext(v)::bigint) FROM ts_t" 2>&1)
NBASE_FP=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*), sum(hashtext(v)::bigint) FROM base_t" 2>&1)
NTS_IDX=$("$BIN/psql" -h "$W" -U postgres -tAc "SET enable_seqscan=off; SELECT count(*) FROM ts_t WHERE v > 't'" 2>&1)
log "new: ts_t=$NTS_FP base_t=$NBASE_FP ts_idx=$NTS_IDX"
"$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1

FAIL=0
[ "$TS_FP"   = "$NTS_FP"  ]  || { echo "FAIL: in-place tablespace TABLE lost/corrupt (old '$TS_FP' new '$NTS_FP')"; FAIL=1; }
[ "$TS_IDX"  = "$NTS_IDX" ]  || { echo "FAIL: in-place tablespace INDEX lost/corrupt (old '$TS_IDX' new '$NTS_IDX')"; FAIL=1; }
[ "$BASE_FP" = "$NBASE_FP" ] || { echo "FAIL: base/ table regressed (old '$BASE_FP' new '$NBASE_FP')"; FAIL=1; }
[ "$FAIL" = 0 ] && log "PASS: user-tablespace relation survived WAL replay" \
                || log "FAIL: user-tablespace data did not survive --wal-upgrade"
exit $FAIL
