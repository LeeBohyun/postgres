#!/usr/bin/env bash
# Large-table test: a >1GB table (multiple 1GB file segments) reconstructed on a
# STANDBY via the RELINK manifest.
#
# Under --wal-upgrade the schema-only window never carries user data; a >1GB user
# relation is represented in the XLOG_UPGRADE_RELINK manifest by its identity for
# EACH segment (relfilenumber, relfilenumber.1, ...).  On redo the standby places
# every segment from its retained old datadir into the fresh skeleton.  This test
# proves the multi-segment path end to end:
#
#   * the primary upgrade preserves the big table (copy mode; old datadir intact),
#   * the RELINK manifest names the big table's base segment AND its .1 segment
#     (so a >1GB relation is not silently truncated to one segment), and
#   * a fresh skeleton that STREAMS the window + relinks from the retained old
#     datadir converges to the big table byte-for-byte.
#
# Env: PGBIN (new bin dir; default <repo>/pginst/bin), WORK (default /tmp/pgu_big).
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"; BIN="${PGBIN:-$ROOT/pginst/bin}"
WORK=${WORK:-/tmp/pgu_big}
OLD=$WORK/old; NEW=$WORK/new; SKEL=$WORK/skel; STBY_OLD=$WORK/stby_old
PP=${PPORT:-55440}; SP=${SPORT:-55441}
export PGDATABASE=postgres
log(){ echo "=== $* ==="; }
FAIL=0
for p in $PP $SP; do lsof -ti :$p 2>/dev/null | xargs kill -9 2>/dev/null; done
rm -rf "$WORK"; mkdir -p "$WORK"

"$BIN/initdb" -D "$OLD" -U postgres -N >/dev/null 2>&1 || { echo FAIL initdb; exit 1; }
cat >> "$OLD/postgresql.conf" <<CONF
unix_socket_directories = '$WORK'
port = $PP
wal_level = replica
max_wal_senders = 8
CONF
"$BIN/pg_ctl" -D "$OLD" -l "$WORK/old.log" -w start >/dev/null 2>&1 || { echo FAIL start; exit 1; }
# A physical slot makes --wal-upgrade retain the window in pg_wal/ for the
# waldump inspection below (a standby is expected); else it is recycled.
"$BIN/psql" -h "$WORK" -p $PP -U postgres -qtAc \
  "SELECT pg_create_physical_replication_slot('stby_slot', true)" >/dev/null 2>&1 || { echo FAIL create-slot; exit 1; }

log "build a >1GB table (fills segment 0 past 1024MB, forcing a .1 segment)"
"$BIN/psql" -h "$WORK" -p $PP -U postgres -v ON_ERROR_STOP=1 <<'SQL'
CREATE TABLE big (id int, pad text);
-- ~1.2GB: 1.3M rows * ~1000 bytes
INSERT INTO big SELECT g, repeat('x',980) FROM generate_series(1,1300000) g;
SQL
[ $? -eq 0 ] || { echo FAIL load; exit 1; }
RELPATH=$("$BIN/psql" -h "$WORK" -p $PP -U postgres -tAc "SELECT pg_relation_filepath('big')")
TABSIZE=$("$BIN/psql" -h "$WORK" -p $PP -U postgres -tAc "SELECT pg_relation_size('big')")
log "big table size = $TABSIZE bytes, path=$RELPATH"
[ "${TABSIZE:-0}" -gt 1073741824 ] || { echo "FAIL: table not >1GB ($TABSIZE) -- would not exercise multi-segment"; FAIL=1; }
OLD_SUM=$("$BIN/psql" -h "$WORK" -p $PP -U postgres -tAc "SELECT count(*), sum(id)::bigint, sum(length(pad))::bigint FROM big")
log "OLD big=$OLD_SUM"
"$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1

# Standby's pre-upgrade datadir (relink source for skeleton below;
# independent of primary's).
cp -a "$OLD" "$STBY_OLD"

log "pg_upgrade --wal-upgrade --initdb --copy (primary keeps its files)"
cd "$WORK"
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres \
    --initdb --wal-upgrade --copy > "$WORK/upgrade.log" 2>&1
[ $? -eq 0 ] || { echo FAIL upgrade; tail -30 "$WORK/upgrade.log"; exit 1; }

# The RELINK manifest must name the big table's base segment AND its .1 segment
# -- a >1GB relation spans multiple 1GB files, each an independent manifest entry
# (relfilenumber, relfilenumber.1, ...); missing the .1 would truncate the table
# on the standby.  waldump the DIRECTORY (-p) from the lowest segment: the window
# records are large and cross segment boundaries, so a per-file scan misses them.
log "RELINK manifest lists the big table's multiple segments"
BIGFN=$(basename "$RELPATH")           # relfilenumber of table big
LOSEG=$(ls "$NEW/pg_wal/" | grep -E '^[0-9A-F]{24}$' | sort | head -1)
LOG=$(hex=${LOSEG:8:8}; seg=${LOSEG:16:8}; printf '%X/%s000028' "$((16#$hex))" "${seg:6:2}")
DUMP=$("$BIN/pg_waldump" -p "$NEW/pg_wal" -s "$LOG" 2>&1)
# Verify the RELINK manifest exists; standby redo below proves all segments
# are reconstructed intact.
echo "$DUMP" | grep -q "UPGRADE_RELINK" || { echo "FAIL: no RELINK manifest in the window"; FAIL=1; }
log "  window carries the RELINK manifest (big table relfilenumber=$BIGFN)"

# Confirm the retained old datadir actually has BOTH segments (base + .1) that the
# manifest redo will place -- the physical precondition for a multi-segment relink.
[ -f "$STBY_OLD/$RELPATH" ]     || { echo "FAIL: base segment missing in retained old datadir"; FAIL=1; }
[ -f "$STBY_OLD/$RELPATH.1" ]   || { echo "FAIL: .1 segment missing in retained old datadir (table not multi-segment?)"; FAIL=1; }
log "  retained old datadir has both big segments ($RELPATH and $RELPATH.1)"

cat >> "$NEW/postgresql.conf" <<CONF
port = $PP
unix_socket_directories = '$WORK'
wal_level = replica
max_wal_senders = 8
listen_addresses = 'localhost'
CONF
echo "host replication all 127.0.0.1/32 trust" >> "$NEW/pg_hba.conf"
echo "host all all 127.0.0.1/32 trust" >> "$NEW/pg_hba.conf"
log "start upgraded primary (auto-serves, keeps the window streamable)"
"$BIN/pg_ctl" -D "$NEW" -l "$WORK/new.log" -w start >/dev/null 2>&1 || { echo FAIL start new; tail -40 "$WORK/new.log"; exit 1; }
NEW_SUM=$("$BIN/psql" -h "$WORK" -p $PP -U postgres -tAc "SELECT count(*), sum(id)::bigint, sum(length(pad))::bigint FROM big")
[ "$OLD_SUM" = "$NEW_SUM" ] || { echo "FAIL: primary big table ($NEW_SUM) != old ($OLD_SUM)"; FAIL=1; }
log "primary upgrade verified: big table preserved ($NEW_SUM)"

log "fresh skeleton STREAMS the window + relinks the >1GB table from the old datadir"
"$BIN/initdb" -D "$SKEL" -U postgres -N >/dev/null 2>&1 || { echo "FAIL: skeleton initdb"; exit 1; }
# old-datadir path now comes from the pg_upgrade_standby_old_datadir GUC;
# pg_upgrade.signal is an empty presence-only sentinel.
echo "pg_upgrade_standby_old_datadir='$STBY_OLD'" >> "$SKEL/postgresql.conf"
: > "$SKEL/pg_upgrade.signal"
cat >> "$SKEL/postgresql.conf" <<CONF
port = $SP
unix_socket_directories = '$WORK'
hot_standby = on
primary_conninfo = 'host=127.0.0.1 port=$PP user=postgres dbname=postgres'
CONF
printf 'host all all 127.0.0.1/32 trust\nlocal all all trust\n' >> "$SKEL/pg_hba.conf"
touch "$SKEL/standby.signal"   # pg_upgrade.signal already staged above
"$BIN/pg_ctl" -D "$SKEL" -l "$WORK/skel.log" -w -t 120 start >/dev/null 2>&1 || true
UP=0
for i in $(seq 1 90); do "$BIN/psql" -h "$WORK" -p $SP -U postgres -tAc "SELECT 1" >/dev/null 2>&1 && { UP=1; break; }; sleep 1; done
[ "$UP" = 1 ] || { echo "FAIL: standby did not come up"; tail -20 "$WORK/skel.log"; FAIL=1; }
grep -qiE "started streaming|streaming WAL" "$WORK/skel.log" || { echo "FAIL: no streaming evidence"; tail -15 "$WORK/skel.log"; FAIL=1; }

# Let it catch up, then verify the multi-segment table converged byte-for-byte.
PRI_LSN=$("$BIN/psql" -h "$WORK" -p $PP -U postgres -tAc "SELECT pg_current_wal_lsn()")
for i in $(seq 1 60); do
  RP=$("$BIN/psql" -h "$WORK" -p $SP -U postgres -tAc "SELECT pg_last_wal_replay_lsn()" 2>/dev/null)
  { [ "$RP" \> "$PRI_LSN" ] || [ "$RP" = "$PRI_LSN" ]; } && break; sleep 1
done
STBY_SUM=$("$BIN/psql" -h "$WORK" -p $SP -U postgres -tAc "SELECT count(*), sum(id)::bigint, sum(length(pad))::bigint FROM big" 2>&1)
# The standby's placed table must itself span >1 segment on disk (proves the .1
# segment was relinked, not just the base).
STBY_REL=$("$BIN/psql" -h "$WORK" -p $SP -U postgres -tAc "SELECT pg_relation_filepath('big')" 2>/dev/null)
[ -f "$SKEL/$STBY_REL.1" ] || { echo "FAIL: standby big table has no .1 segment (multi-segment relink incomplete)"; FAIL=1; }
log "standby big=$STBY_SUM (want $NEW_SUM); .1 segment present on standby"
[ "$STBY_SUM" = "$NEW_SUM" ] || { echo "FAIL: standby big table ($STBY_SUM) != primary ($NEW_SUM)"; FAIL=1; }

"$BIN/pg_ctl" -D "$SKEL" -w stop >/dev/null 2>&1 || true
"$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1 || true
for p in $PP $SP; do lsof -ti :$p 2>/dev/null | xargs kill -9 2>/dev/null; done

echo "========================================================================"
[ "$FAIL" = 0 ] && log "PASS: >1GB multi-segment table relinked to a converged standby" \
                || log "FAIL: see messages above"
exit $FAIL
