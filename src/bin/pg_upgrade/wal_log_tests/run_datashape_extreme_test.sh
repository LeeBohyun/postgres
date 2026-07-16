#!/usr/bin/env bash
#
# Data-shape extremes for --wal-log-upgrade capture/replay, targeting edges the
# existing tests skip:
#
#   B1. MULTI-SEGMENT single relation: one table > 2GB so its relfilenode is
#       split into 3+ 1GB segment files (relfilenode, relfilenode.1, .2 ...).
#       The capture must emit every segment and replay must stitch them back.
#   B2. LARGE-OBJECTS-only database: data lives in pg_largeobject /
#       pg_largeobject_metadata (a known pg_upgrade edge), no user heap tables.
#   B3. EMPTY database (created, nothing in it) must round-trip.
#   B4. Many tiny relations that straddle the RELFILE max-payload BATCH boundary
#       (hundreds of 0-8KB relfiles) -- exercises the batch-flush cut points.
#
# All reconstructed from WAL (disk wiped), then committed and verified.
#
set -u
BIN="${PGBIN:?set PGBIN}"
W=${WORK:-/tmp/pgu_datashape}; PORT=${PORT:-56880}
export PGPORT=$PORT PGDATABASE=postgres
log(){ echo "=== $* ==="; }
fail(){ echo "FAIL: $*"; exit 1; }
rm -rf "$W"; mkdir -p "$W"

log "init old cluster"
"$BIN/initdb" -D "$W/old" -U postgres -N >/dev/null 2>&1 || fail "initdb"
cat >> "$W/old/postgresql.conf" <<CONF
unix_socket_directories='$W'
port=$PORT
max_connections=100
CONF
"$BIN/pg_ctl" -D "$W/old" -l "$W/o.log" -w start >/dev/null 2>&1 || fail "start old"

log "B1: build a >2GB single relation (multi-segment relfilenode)"
# ~2.3GB of heap so the relfilenode spans 3 segment files (0, .1, .2).
"$BIN/psql" -h "$W" -U postgres -q -v ON_ERROR_STOP=1 >/dev/null 2>&1 <<'SQL' || fail "B1 load"
CREATE TABLE big (id bigint, pad text);
-- ~1KB/row * ~2.4M rows ≈ 2.3GB
INSERT INTO big SELECT g, repeat('x', 1000) FROM generate_series(1, 2400000) g;
SQL
BIG_FP=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*), sum(hashtext(pad)::bigint) FROM big")
# confirm it really is multi-segment on disk
BIG_RFN=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT relfilenode FROM pg_class WHERE relname='big'")
BIG_DBOID=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT oid FROM pg_database WHERE datname='postgres'")
NSEG=$(ls "$W/old/base/$BIG_DBOID/$BIG_RFN" "$W/old/base/$BIG_DBOID/$BIG_RFN".[0-9]* 2>/dev/null | wc -l | tr -d ' ')
log "B1: big relfilenode=$BIG_RFN has $NSEG segment file(s) on disk (want >=3)"
[ "${NSEG:-0}" -ge 3 ] || fail "B1: table not multi-segment (only $NSEG files) -- test would not exercise stitching"

log "B2: large-objects-only database"
"$BIN/psql" -h "$W" -U postgres -qc "CREATE DATABASE lodb" >/dev/null 2>&1 || fail "B2 createdb"
"$BIN/psql" -h "$W" -U postgres -d lodb -q -v ON_ERROR_STOP=1 >/dev/null 2>&1 <<'SQL' || fail "B2 load"
SELECT lo_from_bytea(0, decode(repeat('deadbeef', 2000), 'hex'));
SELECT lo_from_bytea(0, decode(repeat('cafebabe', 3000), 'hex'));
SELECT lo_from_bytea(0, decode(repeat('0badf00d', 1500), 'hex'));
SQL
LO_FP=$("$BIN/psql" -h "$W" -U postgres -d lodb -tAc "SELECT count(*), sum(length(lo_get(oid))) FROM pg_largeobject_metadata")

log "B3: empty database"
"$BIN/psql" -h "$W" -U postgres -qc "CREATE DATABASE emptydb" >/dev/null 2>&1 || fail "B3 createdb"
# "empty" is relative: a fresh db still carries information_schema etc.  Capture
# the OLD-side relation count and require the upgrade to round-trip it exactly,
# rather than assuming a hard zero.
EMP_FP=$("$BIN/psql" -h "$W" -U postgres -d emptydb -tAc "SELECT count(*) FROM pg_class")

log "B4: many tiny relations straddling the RELFILE batch boundary"
# 800 tiny tables (each ~1 page) -> hundreds of small relfiles packed into the
# batched UPGRADE_RELFILE_DATA records; forces many batch cut points.
"$BIN/psql" -h "$W" -U postgres -q -v ON_ERROR_STOP=1 >/dev/null 2>&1 <<'SQL' || fail "B4 load"
DO $$ BEGIN
  FOR i IN 1..800 LOOP
    EXECUTE format('CREATE TABLE tiny%s(a int)', i);
    EXECUTE format('INSERT INTO tiny%s VALUES (%s)', i, i);
  END LOOP;
END $$;
SQL
TINY_FP=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*), sum(a::bigint) FROM (SELECT (regexp_replace(relname,'tiny',''))::int AS a FROM pg_class WHERE relname LIKE 'tiny%' AND relkind='r') s")

"$BIN/pg_ctl" -D "$W/old" -w stop >/dev/null 2>&1

log "pg_upgrade --wal-log-upgrade (--copy)"
( cd "$W" && "$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$W/old" -D "$W/new" -U postgres \
    --initdb --wal-log-upgrade --copy >"$W/up.log" 2>&1 ) || { tail -25 "$W/up.log"; fail "upgrade"; }

# disk must be wiped (proves reconstruction from WAL, not leftover files)
TOTAL_BASE=$(find "$W/new/base" -type f -regextype posix-extended -regex '.*/[0-9]+(\.[0-9]+)?' -printf '%s\n' 2>/dev/null | awk '{s+=$1}END{print s+0}')
log "base/ bytes on disk after upgrade (should be 0): $TOTAL_BASE"
[ "${TOTAL_BASE:-0}" = "0" ] || fail "data not wiped ($TOTAL_BASE) -- replay unproven"

echo "unix_socket_directories='$W'">>"$W/new/postgresql.conf"; echo "port=$PORT">>"$W/new/postgresql.conf"
log "hold-start (reconstruct + hold), then commit"
"$BIN/pg_ctl" -D "$W/new" -l "$W/hold.log" -w -t 600 start >/dev/null 2>&1 || true
"$BIN/pg_controldata" -D "$W/new" | grep -qi quarantine || fail "new not quarantined after hold-start"
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$W/old" -D "$W/new" --commit >"$W/commit.log" 2>&1 || { cat "$W/commit.log"; fail "commit"; }
"$BIN/pg_ctl" -D "$W/new" -l "$W/new.log" -w -t 600 start >/dev/null 2>&1 || { tail -30 "$W/new.log"; fail "start after commit"; }

log "verify each data shape survived"
FAIL=0
NBIG=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*), sum(hashtext(pad)::bigint) FROM big" 2>&1)
[ "$BIG_FP" = "$NBIG" ] || { echo "  FAIL B1 multi-segment: old='$BIG_FP' new='$NBIG'"; FAIL=1; }
# confirm multi-segment on the NEW side too (reconstruction rebuilt all segments)
NRFN=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT relfilenode FROM pg_class WHERE relname='big'")
NDBOID=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT oid FROM pg_database WHERE datname='postgres'")
NNSEG=$(ls "$W/new/base/$NDBOID/$NRFN" "$W/new/base/$NDBOID/$NRFN".[0-9]* 2>/dev/null | wc -l | tr -d ' ')
[ "${NNSEG:-0}" -ge 3 ] || { echo "  FAIL B1: new 'big' not multi-segment after replay ($NNSEG files)"; FAIL=1; }
NLO=$("$BIN/psql" -h "$W" -U postgres -d lodb -tAc "SELECT count(*), sum(length(lo_get(oid))) FROM pg_largeobject_metadata" 2>&1)
[ "$LO_FP" = "$NLO" ] || { echo "  FAIL B2 large-objects: old='$LO_FP' new='$NLO'"; FAIL=1; }
EMP=$("$BIN/psql" -h "$W" -U postgres -d emptydb -tAc "SELECT count(*) FROM pg_class" 2>&1)
[ "$EMP" = "$EMP_FP" ] || { echo "  FAIL B3 empty db: pg_class count changed old='$EMP_FP' new='$EMP'"; FAIL=1; }
NTINY=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*), sum(a::bigint) FROM (SELECT (regexp_replace(relname,'tiny',''))::int AS a FROM pg_class WHERE relname LIKE 'tiny%' AND relkind='r') s" 2>&1)
[ "$TINY_FP" = "$NTINY" ] || { echo "  FAIL B4 tiny-rels: old='$TINY_FP' new='$NTINY'"; FAIL=1; }
"$BIN/pg_ctl" -D "$W/new" -w stop >/dev/null 2>&1

[ $FAIL -eq 0 ] || { log "FAIL: data-shape mismatch"; exit 1; }
log "PASS: B1 multi-segment ($NNSEG segs), B2 large-objects, B3 empty db, B4 800 tiny rels -- all reconstructed from WAL"
exit 0
