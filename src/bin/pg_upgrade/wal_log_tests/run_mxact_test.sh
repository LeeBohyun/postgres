#!/usr/bin/env bash
# Verify pg_multixact SLRU is skipped on disk and reconstructed from WAL.
# Creates real multixacts (two overlapping FOR SHARE lockers on the same rows),
# which populate pg_multixact/offsets and members, then upgrades and checks that
# the locked rows and multixact state survive pure WAL replay.
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"; BIN="${PGBIN:-$ROOT/pginst/bin}"
WORK=/tmp/pgu_mx; OLD=$WORK/old; NEW=$WORK/new; PORT=55460
export PGPORT=$PORT PGDATABASE=postgres
log(){ echo "=== $* ==="; }
rm -rf "$WORK"; mkdir -p "$WORK"
"$BIN/initdb" -D "$OLD" -U postgres -N >/dev/null 2>&1
echo "unix_socket_directories = '$WORK'" >> "$OLD/postgresql.conf"; echo "port=$PORT" >> "$OLD/postgresql.conf"
"$BIN/pg_ctl" -D "$OLD" -l "$WORK/old.log" -w start >/dev/null 2>&1

"$BIN/psql" -h "$WORK" -U postgres -qc "CREATE TABLE m(id int primary key, v text); INSERT INTO m SELECT g,'v'||g FROM generate_series(1,1000) g;" >/dev/null

log "create real multixacts (two overlapping FOR SHARE lockers)"
# Session A holds FOR SHARE on rows 1..500 for 3s
( "$BIN/psql" -h "$WORK" -U postgres -qc "BEGIN; SELECT id FROM m WHERE id<=500 FOR SHARE; SELECT pg_sleep(3); COMMIT;" >/dev/null 2>&1 ) &
sleep 1
# Session B also locks the same rows -> forces multixact creation on those rows
"$BIN/psql" -h "$WORK" -U postgres -qc "BEGIN; SELECT id FROM m WHERE id<=500 FOR SHARE; COMMIT;" >/dev/null 2>&1
wait

MX_OFF=$(find "$OLD/pg_multixact/offsets" -type f | wc -l)
# CHECKPOINT so pg_control_checkpoint() reflects the live nextMulti (post-lockers)
"$BIN/psql" -h "$WORK" -U postgres -qc "CHECKPOINT" >/dev/null
MX_NEXT=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT next_multixact_id FROM pg_control_checkpoint()")
log "old cluster: multixact offset segs=$MX_OFF next_multixact_id=$MX_NEXT"
OLD_SUM=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT count(*), sum(hashtext(v)::bigint) FROM m")
"$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1

log "pg_upgrade --wal-log-upgrade --initdb --copy"
cd "$WORK"
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-log-upgrade --copy > "$WORK/up.log" 2>&1
[ $? -eq 0 ] || { echo FAIL upgrade; tail -25 "$WORK/up.log"; exit 1; }

MXOFF_BYTES=$(find "$NEW/pg_multixact/offsets" -type f -printf '%s\n' 2>/dev/null | awk '{s+=$1}END{print s+0}')
MXMEM_BYTES=$(find "$NEW/pg_multixact/members" -type f -printf '%s\n' 2>/dev/null | awk '{s+=$1}END{print s+0}')
log "after pg_upgrade: pg_multixact offsets=$MXOFF_BYTES members=$MXMEM_BYTES bytes on disk (should be 0 = skipped)"
# ASSERT the SLRU was actually skipped on disk — otherwise "reconstructed from
# WAL" below is unproven (the data could just be the leftover on-disk segments).
[ "${MXOFF_BYTES:-0}" = "0" ] && [ "${MXMEM_BYTES:-0}" = "0" ] || {
    echo "FAIL: pg_multixact not skipped on disk (offsets=$MXOFF_BYTES members=$MXMEM_BYTES) — reconstruction claim unproven"; exit 1; }

echo "unix_socket_directories = '$WORK'" >> "$NEW/postgresql.conf"; echo "port=$PORT" >> "$NEW/postgresql.conf"
"$BIN/pg_ctl" -D "$NEW" -l "$WORK/new.log" -w start >/dev/null 2>&1 || { echo FAIL start; tail -30 "$WORK/new.log"; exit 1; }
NEW_SUM=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT count(*), sum(hashtext(v)::bigint) FROM m")
NEW_NEXT=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT next_multixact_id FROM pg_control_checkpoint()")
MXOFF_AFTER=$(find "$NEW/pg_multixact/offsets" -type f -printf '%s\n' 2>/dev/null | awk '{s+=$1}END{print s+0}')
log "after startup: data=$NEW_SUM next_multixact_id=$NEW_NEXT offsets_bytes=$MXOFF_AFTER (reconstructed)"
"$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1

FAIL=0
# The SLRU must have been RECONSTRUCTED by replay: 0 bytes on disk after
# pg_upgrade (asserted above), nonzero after WAL-replay startup.
[ "${MXOFF_AFTER:-0}" != "0" ] || { echo "MISMATCH: pg_multixact/offsets not reconstructed by replay (still 0 bytes)"; FAIL=1; }
[ "$OLD_SUM" = "$NEW_SUM" ] || { echo "MISMATCH data"; FAIL=1; }
[ "$MX_NEXT" = "$NEW_NEXT" ] || { echo "MISMATCH next_multixact_id: old=$MX_NEXT new=$NEW_NEXT"; FAIL=1; }
[ "$FAIL" = 0 ] && log "PASS multixact skip+reconstruct" || log "FAIL multixact"
exit $FAIL
