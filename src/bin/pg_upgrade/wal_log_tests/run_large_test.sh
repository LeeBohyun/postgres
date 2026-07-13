#!/usr/bin/env bash
# Large-table test: a table whose first 1GB segment is completely full, forcing
# the >1020MB relfile-chunking path (segno 0 split into >=2 chunked records).
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"; BIN="${PGBIN:-$ROOT/pginst/bin}"
WORK=/tmp/pgu_big
OLD=$WORK/old; NEW=$WORK/new; PORT=55440
export PGPORT=$PORT PGDATABASE=postgres
log(){ echo "=== $* ==="; }
rm -rf "$WORK"; mkdir -p "$WORK"

"$BIN/initdb" -D "$OLD" -U postgres -N >/dev/null 2>&1 || { echo FAIL initdb; exit 1; }
echo "unix_socket_directories = '$WORK'" >> "$OLD/postgresql.conf"
echo "port = $PORT" >> "$OLD/postgresql.conf"
"$BIN/pg_ctl" -D "$OLD" -l "$WORK/old.log" -w start >/dev/null 2>&1 || { echo FAIL start; exit 1; }

log "build a >1GB table (fills segment 0 past 1024MB)"
"$BIN/psql" -h "$WORK" -U postgres -v ON_ERROR_STOP=1 <<'SQL'
CREATE TABLE big (id int, pad text);
-- ~1.2GB: 1.3M rows * ~1000 bytes
INSERT INTO big SELECT g, repeat('x',980) FROM generate_series(1,1300000) g;
SQL
[ $? -eq 0 ] || { echo FAIL load; exit 1; }
SEGS=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT pg_relation_filepath('big')")
TABSIZE=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT pg_relation_size('big')")
log "big table size = $TABSIZE bytes, path=$SEGS"
OLD_SUM=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT count(*), sum(id)::bigint, sum(length(pad))::bigint FROM big")
log "OLD big=$OLD_SUM"
"$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1

log "pg_upgrade --wal-log-upgrade --initdb --copy"
cd "$WORK"
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres \
    --initdb --wal-log-upgrade --copy > "$WORK/upgrade.log" 2>&1
[ $? -eq 0 ] || { echo FAIL upgrade; tail -30 "$WORK/upgrade.log"; exit 1; }

log "chunked RELFILE records for the big table (expect seg 0 split into >=2)"
for seg in "$NEW/pg_wal_upgrade"/[0-9A-F]*; do
    "$BIN/pg_waldump" "$seg" 2>/dev/null
done | grep "UPGRADE_RELFILE_DATA" | awk '$0 ~ /seg 0 blkoff/' | grep -E "blkoff (0|1305) " | head

echo "unix_socket_directories = '$WORK'" >> "$NEW/postgresql.conf"
echo "port = $PORT" >> "$NEW/postgresql.conf"
log "start new cluster (WAL replay)"
"$BIN/pg_ctl" -D "$NEW" -l "$WORK/new.log" -w start >/dev/null 2>&1 || { echo FAIL start new; tail -40 "$WORK/new.log"; exit 1; }
NEW_SUM=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT count(*), sum(id)::bigint, sum(length(pad))::bigint FROM big")
log "NEW big=$NEW_SUM"
"$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1

if [ "$OLD_SUM" = "$NEW_SUM" ]; then log "PASS large-table chunking"; exit 0; else log "FAIL: $OLD_SUM != $NEW_SUM"; exit 1; fi
