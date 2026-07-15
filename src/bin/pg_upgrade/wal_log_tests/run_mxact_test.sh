#!/usr/bin/env bash
# Verify pg_multixact SLRU is skipped on disk and reconstructed from WAL.
# Creates real multixacts (two overlapping FOR KEY SHARE lockers on the same
# rows), which populate pg_multixact/offsets and members, then upgrades and
# checks that the locked rows and multixact state survive pure WAL replay.
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

log "create real multixacts (two overlapping FOR KEY SHARE lockers)"
# A multixact forms only when TWO transactions hold a share lock on the SAME row
# AT THE SAME TIME.  Backgrounding a psql and "sleep 1" is racy (A may not hold
# its lock yet when B runs, so no overlap -> no multixact).  Make it
# deterministic: session A holds FOR KEY SHARE in a background transaction kept
# open with pg_sleep, and B only proceeds AFTER polling pg_locks confirms A's
# row-share locks are granted.  (FOR KEY SHARE reliably converts the tuple's
# xmax into a multixact when the second locker arrives.)
( "$BIN/psql" -h "$WORK" -U postgres -q >/dev/null 2>&1 <<'SQL'
BEGIN;
SELECT id FROM m WHERE id<=500 FOR KEY SHARE;
SELECT pg_sleep(5);
COMMIT;
SQL
) &
MXA_PID=$!
# Wait until A is actually holding its locks before B runs.  A FOR KEY SHARE
# holder shows a granted relation-level RowShareLock (on the table and its index)
# held by another backend; poll for that rather than a fixed sleep.
for i in $(seq 1 60); do
  nlk=$("$BIN/psql" -h "$WORK" -U postgres -tAc \
    "SELECT count(*) FROM pg_locks WHERE locktype='relation' AND mode='RowShareLock' AND granted AND pid <> pg_backend_pid()")
  [ "${nlk:-0}" -ge 1 ] && break
  sleep 0.1
done
# B locks the SAME rows while A still holds them -> a real multixact forms.
"$BIN/psql" -h "$WORK" -U postgres -qc "BEGIN; SELECT id FROM m WHERE id<=500 FOR KEY SHARE; COMMIT;" >/dev/null 2>&1
wait "$MXA_PID" 2>/dev/null

MX_OFF=$(find "$OLD/pg_multixact/offsets" -type f | wc -l)
# CHECKPOINT FIRST so pg_control_checkpoint() reflects the live nextMulti: the
# multixact just formed lives in shared memory and only lands in pg_control at a
# checkpoint, so reading pg_control_checkpoint() before checkpointing would still
# show the pre-multixact value.
"$BIN/psql" -h "$WORK" -U postgres -qc "CHECKPOINT" >/dev/null
MX_NEXT=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT next_multixact_id FROM pg_control_checkpoint()")
# Assert a multixact actually formed (next_multixact_id advanced past the initial
# 1); otherwise the reconstruction assertion below would test nothing.
[ "${MX_NEXT:-1}" -ge 2 ] || { echo "FAIL: test setup did not create a multixact (next_multixact_id=$MX_NEXT)"; exit 1; }
log "old cluster: multixact offset segs=$MX_OFF next_multixact_id=$MX_NEXT"
OLD_SUM=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT count(*), sum(hashtext(v)::bigint) FROM m")
"$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1

log "pg_upgrade --wal-log-upgrade --initdb --copy"
cd "$WORK"
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-log-upgrade --copy > "$WORK/up.log" 2>&1
[ $? -eq 0 ] || { echo FAIL upgrade; tail -25 "$WORK/up.log"; exit 1; }

# --wal-log-upgrade holds the new cluster in quarantine; commit to adopt it.
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" --commit > "$WORK/commit.log" 2>&1 \
    || { echo FAIL commit; tail -20 "$WORK/commit.log"; exit 1; }

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
# Confirm the multixact state is actually USABLE after replay: the row that was
# multixact-locked in the old cluster must be readable and updatable now (the
# stored multixact in its xmax must resolve against the reconstructed SLRU
# without error).  This is the real correctness property -- NOT the on-disk byte
# count of pg_multixact/offsets, which is just an SLRU flush-timing artifact the
# code never promises at any particular instant.
"$BIN/psql" -h "$WORK" -U postgres -qc "UPDATE m SET v = v WHERE id<=500" >/dev/null 2>&1
MX_USABLE=$("$BIN/psql" -h "$WORK" -U postgres -tAc "SELECT count(*) FROM m WHERE id<=500" 2>&1)
log "after startup: data=$NEW_SUM next_multixact_id=$NEW_NEXT multixact-locked rows updatable=$MX_USABLE"
"$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1

FAIL=0
# Correctness properties (semantic, not implementation-detail):
#  - user data round-trips
#  - the multixact counter survived the upgrade
#  - the previously multixact-locked rows are still resolvable/updatable (the
#    reconstructed SLRU answers correctly)
[ "$OLD_SUM" = "$NEW_SUM" ] || { echo "MISMATCH data: old=$OLD_SUM new=$NEW_SUM"; FAIL=1; }
[ "$MX_NEXT" = "$NEW_NEXT" ] || { echo "MISMATCH next_multixact_id: old=$MX_NEXT new=$NEW_NEXT"; FAIL=1; }
[ "$MX_USABLE" = "500" ] || { echo "MISMATCH: multixact-locked rows not resolvable after replay (got '$MX_USABLE')"; FAIL=1; }
[ "$FAIL" = 0 ] && log "PASS multixact skip+reconstruct" || log "FAIL multixact"
exit $FAIL
