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

# The upgrade WAL lives in pg_wal/ (there is no pg_wal_upgrade/ rename).  Assert
# the big table's relfile was split across MULTIPLE RELFILE records (chunking):
# a >1GB relation cannot fit in one XLOG_UPGRADE_RELFILE_DATA record, so we must
# see more than one.
#
# NOTE: waldump the DIRECTORY (-p) from the lowest segment, NOT file-by-file.
# The RELFILE records are large and span segment boundaries, so waldumping each
# segment file in isolation fails to find a record start ("could not find a
# valid record") and silently reports zero.
log "chunked RELFILE records for the big table (expect >=2 RELFILE records)"
LOSEG=$(ls "$NEW/pg_wal/" | grep -E '^[0-9A-F]{24}$' | sort | head -1)
LOLSN=$("$BIN/pg_waldump" -p "$NEW/pg_wal" "$LOSEG" -n 1 2>&1 | grep -oE 'lsn: [0-9A-F]+/[0-9A-F]+' | head -1 | awk '{print $2}')
NCHUNK=$("$BIN/pg_waldump" -p "$NEW/pg_wal" -s "${LOLSN:-0/0}" 2>/dev/null | grep -c "UPGRADE_RELFILE_DATA")
log "UPGRADE_RELFILE_DATA record count: $NCHUNK"
[ "${NCHUNK:-0}" -ge 2 ] || { echo "FAIL: expected >=2 RELFILE records (chunking), got $NCHUNK"; exit 1; }

# Verify the data was actually WIPED from disk, so the match below proves WAL
# replay rather than leftover files.
TOTAL_BASE=$(find "$NEW/base" -type f -name '[0-9]*' -printf '%s\n' 2>/dev/null | awk '{s+=$1} END{print s+0}')
log "base/ data-file bytes on disk after pg_upgrade (should be 0 = wiped): $TOTAL_BASE"
[ "${TOTAL_BASE:-0}" = "0" ] || { echo "FAIL: data not wiped ($TOTAL_BASE bytes) — WAL-replay claim unproven"; exit 1; }

echo "unix_socket_directories = '$WORK'" >> "$NEW/postgresql.conf"
echo "port = $PORT" >> "$NEW/postgresql.conf"
# --wal-log-upgrade holds the new cluster in quarantine; commit to adopt it.
    # Hold-start: first start applies the WAL window, reconstructs, and holds
    # in quarantine (pg_ctl returns non-zero by design as it exits at the hold).
    "$BIN/pg_ctl" -D "$NEW" -l "$WORK/hold.log" -w start >/dev/null 2>&1 || true
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" --commit > "$WORK/commit.log" 2>&1 \
    || { echo FAIL commit; tail -20 "$WORK/commit.log"; exit 1; }
log "start new cluster (WAL replay)"
"$BIN/pg_ctl" -D "$NEW" -l "$WORK/new.log" -w start >/dev/null 2>&1 || { echo FAIL start new; tail -40 "$WORK/new.log"; exit 1; }
NEW_SUM=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT count(*), sum(id)::bigint, sum(length(pad))::bigint FROM big")
log "NEW big=$NEW_SUM"
"$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1

if [ "$OLD_SUM" = "$NEW_SUM" ]; then log "PASS large-table chunking"; exit 0; else log "FAIL: $OLD_SUM != $NEW_SUM"; exit 1; fi
