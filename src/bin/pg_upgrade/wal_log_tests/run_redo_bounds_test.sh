#!/usr/bin/env bash
# Redo-hardening test: a valid-CRC but internally inconsistent upgrade WAL
# record must be REJECTED by redo (PANIC with ERRCODE_DATA_CORRUPTED), not
# over-read or silently applied.
#
# PG_UPGRADE_TEST_CORRUPT_SLRU_LEN (a test-only fault-injection hook) makes the
# SLRU emit path stamp total_bytes larger than the payload it actually
# registers, exactly the shape a corrupt/hostile record would have.
#
# The auto-served primary reconstructs from the transferred files and never
# replays its own window, so the redo path is exercised by a STREAMING STANDBY:
# a fresh skeleton streams the window from the primary and replays it, and MUST
# halt at the corrupt SLRU record with the bounds-check message rather than
# over-reading the WAL buffer or converging on truncated SLRU state.
#
# The hook is gated on USE_ASSERT_CHECKING (a production build never emits such a
# record), so this test only runs on a cassert build; otherwise it SKIPS.
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"; BIN="${PGBIN:-$ROOT/pginst/bin}"
if ! grep -q "^#define USE_ASSERT_CHECKING 1" "$("$BIN/pg_config" --includedir)/pg_config.h" 2>/dev/null; then
    echo "=== SKIP: redo-bounds fault injection requires a cassert build ==="
    exit 0
fi

W=${WORK:-/tmp/pgu_redo_bounds}; OLD=$W/old; NEW=$W/new; SKEL=$W/standby
PP=55630 SP=55631
export PGDATABASE=postgres
log(){ echo "=== $* ==="; }
FAIL=0
for p in $PP $SP; do lsof -ti :$p 2>/dev/null | xargs kill -9 2>/dev/null; done
rm -rf "$W"; mkdir -p "$W"

log "1. old primary with data"
"$BIN/initdb" -D "$OLD" -U postgres -N >/dev/null 2>&1 || { echo FAIL initdb; exit 1; }
cat >> "$OLD/postgresql.conf" <<CONF
port=$PP
unix_socket_directories='$W'
CONF
"$BIN/pg_ctl" -D "$OLD" -l "$W/old.log" -w start >/dev/null 2>&1 || { echo FAIL start; exit 1; }
"$BIN/psql" -h "$W" -p $PP -U postgres -qc "CREATE TABLE t(id int); INSERT INTO t SELECT generate_series(1,2000);" >/dev/null
"$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1

log "2. upgrade the primary (--wal-upgrade) with an inflated SLRU total_bytes"
cd "$W"
PG_UPGRADE_TEST_CORRUPT_SLRU_LEN=1 "$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres \
    --initdb --wal-upgrade --copy >"$W/up.log" 2>&1
[ $? -eq 0 ] || { echo FAIL upgrade; tail -20 "$W/up.log"; exit 1; }
cat >> "$NEW/postgresql.conf" <<CONF
port=$PP
unix_socket_directories='$W'
wal_level=replica
max_wal_senders=8
listen_addresses='localhost'
CONF
echo "host replication all 127.0.0.1/32 trust" >> "$NEW/pg_hba.conf"
echo "host all all 127.0.0.1/32 trust" >> "$NEW/pg_hba.conf"
# The primary auto-serves from the transferred files (it never replays its own
# window), so it comes up fine despite the corrupt record.
"$BIN/pg_ctl" -D "$NEW" -l "$W/new.log" -w start >/dev/null 2>&1 || { echo "FAIL new start"; tail -15 "$W/new.log"; exit 1; }
[ "$("$BIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT count(*) FROM t")" = 2000 ] \
    || { echo "FAIL: primary data wrong"; FAIL=1; }
log "primary auto-served (does not replay its own window)"

log "3. bare standby skeleton streams the window and REPLAYS it -> must halt"
mkdir -p "$SKEL"; chmod 700 "$SKEL"
cat > "$SKEL/postgresql.conf" <<CONF
port=$SP
unix_socket_directories='$W'
hot_standby=on
primary_conninfo='host=127.0.0.1 port=$PP user=postgres dbname=postgres'
CONF
printf 'host all all 127.0.0.1/32 trust\nlocal all all trust\n' > "$SKEL/pg_hba.conf"
touch "$SKEL/standby.signal"
touch "$SKEL/pg_upgrade_stream.signal"
# Start in the background; the standby streams + replays and should hit the
# corrupt SLRU record in redo and PANIC.  It never converges, so do not wait for
# a query -- watch the log.
"$BIN/pg_ctl" -D "$SKEL" -l "$W/standby.log" -w start >/dev/null 2>&1 || true

log "4. the standby must halt with the SLRU bounds-check message"
for i in $(seq 1 30); do
    grep -qiE "SLRU record claims .* shorter" "$W/standby.log" && break
    sleep 1
done
if grep -qiE "SLRU record claims .* shorter" "$W/standby.log"; then
    log "  redo REJECTED the malformed SLRU record (good):"
    grep -iE "SLRU record claims" "$W/standby.log" | tail -1
else
    echo "  FAIL: standby did not halt with the bounds-check message; log tail:"
    tail -15 "$W/standby.log"; FAIL=1
fi
# The standby must NOT have converged to a queryable state on corrupt SLRU.
if "$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT 1" >/dev/null 2>&1; then
    echo "  FAIL: standby came up despite the malformed record"; FAIL=1
fi

"$BIN/pg_ctl" -D "$SKEL" -w stop >/dev/null 2>&1 || true
"$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1 || true
for p in $PP $SP; do lsof -ti :$p 2>/dev/null | xargs kill -9 2>/dev/null; done

echo "========================================================================"
[ "$FAIL" = 0 ] && log "PASS: redo rejected the malformed SLRU record during standby replay" \
                || log "FAIL: see messages above"
exit $FAIL
