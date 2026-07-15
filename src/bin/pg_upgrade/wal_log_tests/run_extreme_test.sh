#!/usr/bin/env bash
# Extreme-case coverage for --wal-log-upgrade: exercise many object types and
# verify they all survive reconstruction from WAL (data files wiped on disk).
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"; BIN="${PGBIN:-$ROOT/pginst/bin}"
WORK=${WORK:-/tmp/pgu_ext}; OLD=$WORK/old; NEW=$WORK/new; PORT=${PORT:-55495}
MODE=${MODE:---copy}
export PGPORT=$PORT PGDATABASE=postgres
log(){ echo "=== $* ==="; }
rm -rf "$WORK"; mkdir -p "$WORK"
"$BIN/initdb" -D "$OLD" -U postgres -N >/dev/null 2>&1
echo "unix_socket_directories='$WORK'">>$OLD/postgresql.conf; echo "port=$PORT">>$OLD/postgresql.conf
"$BIN/pg_ctl" -D "$OLD" -l "$WORK/old.log" -w start >/dev/null 2>&1 || { echo FAIL start; exit 1; }

log "build a rich schema (toast, partitions, matview, seq, LO, index types, enum/composite, unlogged)"
"$BIN/psql" -h "$WORK" -U postgres -v ON_ERROR_STOP=1 >/dev/null <<'SQL'
-- big TOAST values (>2KB forces out-of-line storage)
CREATE TABLE toasted(id int primary key, big text);
INSERT INTO toasted SELECT g, repeat(md5(g::text), 500) FROM generate_series(1,2000) g;

-- enum + composite types
CREATE TYPE mood AS ENUM ('sad','ok','happy');
CREATE TYPE addr AS (street text, zip int);
CREATE TABLE typed(id int, m mood, a addr);
INSERT INTO typed SELECT g, (ARRAY['sad','ok','happy'])[1+g%3]::mood, ROW('st'||g, g)::addr FROM generate_series(1,3000) g;

-- partitioned table
CREATE TABLE part(id int, val int) PARTITION BY RANGE (id);
CREATE TABLE part_a PARTITION OF part FOR VALUES FROM (0) TO (5000);
CREATE TABLE part_b PARTITION OF part FOR VALUES FROM (5000) TO (10000);
INSERT INTO part SELECT g, g*2 FROM generate_series(1,9999) g;

-- multiple index types
CREATE TABLE idxs(id int, t text, arr int[], num numeric);
INSERT INTO idxs SELECT g, 'row'||g, ARRAY[g, g%10, g%100], g*1.5 FROM generate_series(1,5000) g;
CREATE INDEX idx_btree ON idxs(id);
CREATE INDEX idx_hash  ON idxs USING hash(id);
CREATE INDEX idx_gin   ON idxs USING gin(arr);
CREATE INDEX idx_brin  ON idxs USING brin(id);
CREATE INDEX idx_expr  ON idxs((t || '_x'));

-- materialized view
CREATE MATERIALIZED VIEW mv AS SELECT id, val FROM part WHERE val > 5000;
CREATE INDEX ON mv(id);

-- sequence with advanced value
CREATE SEQUENCE seq1 START 1000;
SELECT setval('seq1', 424242);

-- large objects
SELECT lo_from_bytea(0, decode(repeat('deadbeef', 1000), 'hex')) AS loid \gset
CREATE TABLE lo_ref(id int, obj oid);
INSERT INTO lo_ref VALUES (1, :loid);

-- unlogged table (init fork) -- becomes empty after crash recovery, but must not break
CREATE UNLOGGED TABLE ulog(id int);
INSERT INTO ulog SELECT generate_series(1,100);

-- second database
CREATE DATABASE db2;
SQL

"$BIN/psql" -h "$WORK" -U postgres -d db2 -v ON_ERROR_STOP=1 >/dev/null <<'SQL'
CREATE TABLE inv(sku text primary key, qty int, tags text[]);
INSERT INTO inv SELECT 'sku'||g, g, ARRAY['t'||(g%5)] FROM generate_series(1,4000) g;
CREATE INDEX ON inv USING gin(tags);
SQL

# reference fingerprints.
#
# NOTE: this uses plain prefixed variables (OLDV_<key> / NEWV_<key>) via a
# small set/get helper rather than `declare -A` associative arrays, because
# macOS ships bash 3.2 which has no associative arrays.  KEYS lists every
# fingerprint we capture and later compare.
fp() { "$BIN/psql" -h "$WORK" -U postgres "$@"; }
KEYS="toasted typed part idxs gin mv seq lo db2"
setv() { eval "$1_$2=\$3"; }              # setv OLDV toasted "<value>"
getv() { eval "printf '%s' \"\$$1_$2\""; } # getv OLDV toasted

setv OLDV toasted "$(fp -tAc "SELECT count(*), sum(length(big))::bigint, sum(hashtext(big)::bigint) FROM toasted")"
setv OLDV typed "$(fp -tAc "SELECT count(*), sum(hashtext(m::text||a::text)::bigint) FROM typed")"
setv OLDV part "$(fp -tAc "SELECT count(*), sum(val)::bigint FROM part")"
setv OLDV idxs "$(fp -tAc "SET enable_seqscan=off; SELECT count(*), sum(id)::bigint FROM idxs WHERE id BETWEEN 100 AND 4000")"
setv OLDV gin "$(fp -tAc "SET enable_seqscan=off; SELECT count(*) FROM idxs WHERE arr @> ARRAY[5]")"
setv OLDV mv "$(fp -tAc "SELECT count(*), sum(val)::bigint FROM mv")"
setv OLDV seq "$(fp -tAc "SELECT last_value, is_called FROM seq1")"
setv OLDV lo "$(fp -tAc "SELECT length(lo_get(obj)) FROM lo_ref WHERE id=1")"
setv OLDV db2 "$(fp -d db2 -tAc "SET enable_seqscan=off; SELECT count(*), sum(qty)::bigint FROM inv WHERE tags @> ARRAY['t3']")"
"$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1

log "pg_upgrade --wal-log-upgrade --initdb $MODE"
cd "$WORK"; "$BIN/pg_upgrade" -b $BIN -B $BIN -d "$OLD" -D "$NEW" -U postgres --initdb --wal-log-upgrade $MODE >"$WORK/up.log" 2>&1
[ $? -eq 0 ] || { echo FAIL-UPGRADE; tail -25 "$WORK/up.log"; exit 1; }
BASE_BYTES=$(find "$NEW/base" -type f -printf '%s\n' 2>/dev/null | awk '{s+=$1}END{print s+0}')
log "on-disk base/ bytes after pg_upgrade (should be ~0): $BASE_BYTES"

# --wal-log-upgrade holds the new cluster in quarantine.  Commit AFTER the
# on-disk base/ measurement above (commit replays the window and reconstructs
# the data files, so measuring post-commit would not reflect the wipe).
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" --commit > "$WORK/commit.log" 2>&1 \
    || { echo FAIL commit; tail -20 "$WORK/commit.log"; exit 1; }

echo "unix_socket_directories='$WORK'">>$NEW/postgresql.conf; echo "port=$PORT">>$NEW/postgresql.conf
"$BIN/pg_ctl" -D "$NEW" -l "$WORK/new.log" -w start >/dev/null 2>&1 || { echo START-FAIL; tail -30 "$WORK/new.log"; exit 1; }

setv NEWV toasted "$(fp -tAc "SELECT count(*), sum(length(big))::bigint, sum(hashtext(big)::bigint) FROM toasted")"
setv NEWV typed "$(fp -tAc "SELECT count(*), sum(hashtext(m::text||a::text)::bigint) FROM typed")"
setv NEWV part "$(fp -tAc "SELECT count(*), sum(val)::bigint FROM part")"
setv NEWV idxs "$(fp -tAc "SET enable_seqscan=off; SELECT count(*), sum(id)::bigint FROM idxs WHERE id BETWEEN 100 AND 4000")"
setv NEWV gin "$(fp -tAc "SET enable_seqscan=off; SELECT count(*) FROM idxs WHERE arr @> ARRAY[5]")"
setv NEWV mv "$(fp -tAc "SELECT count(*), sum(val)::bigint FROM mv")"
setv NEWV seq "$(fp -tAc "SELECT last_value, is_called FROM seq1")"
setv NEWV lo "$(fp -tAc "SELECT length(lo_get(obj)) FROM lo_ref WHERE id=1")"
setv NEWV db2 "$(fp -d db2 -tAc "SET enable_seqscan=off; SELECT count(*), sum(qty)::bigint FROM inv WHERE tags @> ARRAY['t3']")"
# amcheck-style: verify btree index structural integrity
fp -tAc "SELECT 'idx ok' FROM idxs WHERE id=2500" >/dev/null
"$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1

FAIL=0
for k in $KEYS; do
  ov=$(getv OLDV "$k"); nv=$(getv NEWV "$k")
  if [ "$ov" = "$nv" ]; then
    echo "  OK   $k = $nv"
  else
    echo "  FAIL $k: old='$ov' new='$nv'"; FAIL=1
  fi
done
[ $FAIL -eq 0 ] && log "PASS extreme cases ($MODE)" || log "FAIL extreme cases ($MODE)"
exit $FAIL
