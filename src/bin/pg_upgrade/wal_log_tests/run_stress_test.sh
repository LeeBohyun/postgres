#!/usr/bin/env bash
# Stress test for --wal-log-upgrade:
#   * a ~10GB user table (many multi-GB relfile segments -> lots of chunked
#     RELFILE records spanning many WAL segments), AND
#   * a bloated system catalog (pg_attribute) driven past 1GB by creating a huge
#     number of columns/tables, so a CATALOG relfile also exercises multi-segment
#     chunking (segno 0 + segno 1 + ...).
#
# Verifies: pg_upgrade succeeds, the on-disk data is wiped (so recovery is real
# WAL replay, not leftover files), the cluster reconstructs from WAL, and the
# data matches.
#
# Tunables (env): GB (target user-table size, default 10), run on a box with
# enough disk -- 10GB old + ~10GB WAL + 10GB rebuilt ~= 30GB+ free needed.
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"; BIN="${PGBIN:-$ROOT/pginst/bin}"
WORK=${WORK:-/tmp/pgu_stress}; OLD=$WORK/old; NEW=$WORK/new; PORT=${PORT:-55480}
GB=${GB:-10}
export PGPORT=$PORT PGDATABASE=postgres
log(){ echo "=== $* ==="; }
rm -rf "$WORK"; mkdir -p "$WORK"

log "init old cluster (checksums on; large maintenance mem for fast build)"
"$BIN/initdb" -D "$OLD" -U postgres -N >/dev/null 2>&1 || { echo FAIL initdb; exit 1; }
cat >> "$OLD/postgresql.conf" <<CONF
unix_socket_directories='$WORK'
port=$PORT
maintenance_work_mem=1GB
max_wal_size=8GB
fsync=off
CONF
"$BIN/pg_ctl" -D "$OLD" -l "$WORK/old.log" -w start >/dev/null 2>&1 || { echo FAIL start; exit 1; }

# ---- ~${GB}GB user table -------------------------------------------------
# ~1KB/row -> GB*1e6 rows.  Build in chunks to bound memory.
ROWS=$(( GB * 1000000 ))
log "build a ~${GB}GB user table ($ROWS rows) -- this is the slow part"
"$BIN/psql" -h "$WORK" -U postgres -q >/dev/null 2>&1 <<SQL
CREATE TABLE big(id bigint, pad text);
INSERT INTO big SELECT g, repeat('x', 1000) FROM generate_series(1, $ROWS) g;
CREATE INDEX ON big(id);
SQL
BIGSZ=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT pg_total_relation_size('big')")
log "big table total size = $BIGSZ bytes"

# ---- bloat pg_attribute past 1GB ----------------------------------------
# Each pg_attribute row is ~140 bytes, so >1GB needs ~7.7M rows.  Postgres caps
# a table at 1600 columns, so we use the max width and MANY tables:
# 5200 tables x 1600 cols ~= 8.3M user attrs (+ system) ~= 1.2GB > 1GB.
# (Measured earlier: 1600x1000 gave only 227MB -- far too few; hence these
# numbers.)  Tune WIDE_TABLES/WIDE_COLS up if pg_attribute still lands <1GB.
WIDE_TABLES=${WIDE_TABLES:-5200}
WIDE_COLS=${WIDE_COLS:-1590}   # <1600 to leave room for system + PK columns
log "bloat pg_attribute past 1GB ($WIDE_TABLES tables x $WIDE_COLS cols)"
"$BIN/psql" -h "$WORK" -U postgres -q >/dev/null 2>&1 <<SQL
DO \$\$
DECLARE
  i int; cols text;
BEGIN
  SELECT string_agg('c'||g||' int', ', ') INTO cols FROM generate_series(1,$WIDE_COLS) g;
  FOR i IN 1..$WIDE_TABLES LOOP
    EXECUTE format('CREATE TABLE wide_%s (%s)', i, cols);
  END LOOP;
END \$\$;
SQL
PGATTR_SZ=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT pg_relation_size('pg_attribute')")
PGATTR_PATH=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT pg_relation_filepath('pg_attribute')")
log "pg_attribute size = $PGATTR_SZ bytes, path = $PGATTR_PATH"
# Require >1GB so the catalog's relfile spans multiple 1GB segments (base/N,
# base/N.1, ...) -- the whole point of this test.  A 1GB+ catalog is what forces
# CATALOG (not just user-table) relfile chunking through the FPI capture.
if [ "${PGATTR_SZ:-0}" -lt 1073741824 ]; then
    echo "FAIL: pg_attribute is under 1GB ($PGATTR_SZ) -- catalog chunking not exercised; raise WIDE_TABLES/WIDE_COLS"; exit 1
fi
# Confirm the catalog physically has a second 1GB segment on disk (path.1).
[ -f "$OLD/${PGATTR_PATH}.1" ] && log "pg_attribute has multi-segment relfile (${PGATTR_PATH}.1 exists) OK" \
                              || log "note: pg_attribute >1GB but no .1 segment yet (size=$PGATTR_SZ)"

OLD_FP=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT count(*), sum(id)::bigint, sum(length(pad))::bigint FROM big")
OLD_NREL=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT count(*) FROM pg_class WHERE relname LIKE 'wide_%'")
log "old big fingerprint: $OLD_FP ; wide tables: $OLD_NREL"
"$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1

# ---- upgrade -------------------------------------------------------------
log "pg_upgrade --wal-log-upgrade --initdb --copy"
cd "$WORK"
t0=$SECONDS
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-log-upgrade --copy >"$WORK/up.log" 2>&1
[ $? -eq 0 ] || { echo FAIL upgrade; tail -30 "$WORK/up.log"; exit 1; }
log "pg_upgrade wall time: $((SECONDS - t0))s"

# ---- assert chunking of BOTH a user relfile and the catalog --------------
LOSEG=$(ls "$NEW/pg_wal/" | grep -E '^[0-9A-F]{24}$' | sort | head -1)
LOLSN=$("$BIN/pg_waldump" -p "$NEW/pg_wal" "$LOSEG" -n 1 2>&1 | grep -oE 'lsn: [0-9A-F]+/[0-9A-F]+' | head -1 | awk '{print $2}')
NREL=$("$BIN/pg_waldump" -p "$NEW/pg_wal" -s "${LOLSN:-0/0}" 2>/dev/null | grep -c "UPGRADE_RELFILE_DATA")
log "UPGRADE_RELFILE_DATA record count: $NREL"
# 10GB user table alone is >>1 record; a >1GB catalog adds more.
[ "${NREL:-0}" -ge 3 ] || { echo "FAIL: expected many chunked RELFILE records, got $NREL"; exit 1; }

# ---- assert the data was wiped off disk (real WAL replay) ----------------
TOTAL_BASE=$(find "$NEW/base" -type f -regextype posix-extended -regex '.*/[0-9]+(\.[0-9]+)?' -printf '%s\n' 2>/dev/null | awk '{s+=$1} END{print s+0}')
log "base/ data-file bytes on disk after pg_upgrade (should be 0 = wiped): $TOTAL_BASE"
[ "${TOTAL_BASE:-0}" = "0" ] || { echo "FAIL: data not wiped ($TOTAL_BASE bytes) -- WAL-replay claim unproven"; exit 1; }

# ---- start (WAL replay) + verify -----------------------------------------
cat >> "$NEW/postgresql.conf" <<CONF
unix_socket_directories='$WORK'
port=$PORT
CONF
log "start new cluster (triggers WAL-replay recovery of ~${GB}GB + >1GB catalog)"
t0=$SECONDS
"$BIN/pg_ctl" -D "$NEW" -l "$WORK/new.log" -w -t 900 start >/dev/null 2>&1 || { echo FAIL start new; tail -40 "$WORK/new.log"; exit 1; }
log "recovery wall time: $((SECONDS - t0))s"
NEW_FP=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT count(*), sum(id)::bigint, sum(length(pad))::bigint FROM big")
NEW_NREL=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT count(*) FROM pg_class WHERE relname LIKE 'wide_%'")
NEW_PGATTR=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT pg_relation_size('pg_attribute')")
log "new big fingerprint: $NEW_FP ; wide tables: $NEW_NREL ; pg_attribute: $NEW_PGATTR bytes"
"$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1

FAIL=0
[ "$OLD_FP" = "$NEW_FP" ]     || { echo "FAIL: big table mismatch old='$OLD_FP' new='$NEW_FP'"; FAIL=1; }
[ "$OLD_NREL" = "$NEW_NREL" ] || { echo "FAIL: wide-table count mismatch old=$OLD_NREL new=$NEW_NREL"; FAIL=1; }
[ "$FAIL" = 0 ] && log "PASS: 10GB + >1GB-catalog reconstructed from WAL; data matches" \
                || log "FAIL: stress reconstruction mismatch"
exit $FAIL
