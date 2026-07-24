#!/usr/bin/env bash
# Transfer-mode coverage for --wal-upgrade.
#
# --wal-upgrade composes with any transfer mode; there is no revert/adopt
# interface, so the only thing that differs per mode is what happens to the old
# cluster:
#   --copy, --clone : duplicate the old cluster's files, leaving it intact (as in
#         upstream).  The old cluster is removed later by the stock
#         delete_old_cluster script when the operator is ready.
#   --copy-file-range : same family as copy, but needs the copy_file_range syscall
#         (Linux); skipped where unavailable.
#   --link, --swap : disable the old cluster during the upgrade, exactly as in
#         upstream (--swap moves its files into the new cluster; --link shares
#         inodes, so running the old cluster after the new one starts is unsafe).
#
# In every mode the upgrade must generate the WAL window and the new cluster must
# AUTO-SERVE the upgraded data on first start.  This test asserts exactly that,
# per mode.
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
BIN="${PGBIN:-$ROOT/pginst/bin}"
W=${WORK:-/tmp/pgu_tmodes}; P=${PPORT:-55640}
export PGDATABASE=postgres
log(){ echo "=== $* ==="; }
FAIL=0
rm -rf "$W"; mkdir -p "$W"

# seed a fresh old cluster with data
seed() {
  local OLD=$1
  "$BIN/initdb" -D "$OLD" -U postgres -N >/dev/null 2>&1 || return 1
  { echo "unix_socket_directories='$W'"; echo "port=$P"; } >> "$OLD/postgresql.conf"
  "$BIN/pg_ctl" -D "$OLD" -l "$W/seed.log" -w start >/dev/null 2>&1 || return 1
  "$BIN/psql" -h "$W" -p $P -U postgres -qc "CREATE TABLE t(a int); INSERT INTO t SELECT g FROM generate_series(1,500) g; CREATE INDEX ON t(a);" >/dev/null
  "$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1
}

# upgrade with the given transfer mode, then assert the upgrade generated a WAL
# window and the new cluster auto-serves the upgraded data on first start.
run_mode() {
  local MODE=$1
  local OLD=$W/${MODE}_old NEW=$W/${MODE}_new
  seed "$OLD" || { echo "FAIL: seed --$MODE"; FAIL=1; return; }

  cd "$W"
  if ! "$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-upgrade --$MODE >"$W/${MODE}_up.log" 2>&1; then
    if grep -qi "copy_file_range not supported" "$W/${MODE}_up.log"; then
      log "  --$MODE: SKIP (not supported on this platform)"; cd /; return
    fi
    echo "FAIL: --$MODE upgrade"; tail -8 "$W/${MODE}_up.log"; FAIL=1; cd /; return
  fi

  # the upgrade must have generated a WAL window (upgrade segments in new pg_wal/)
  ls "$NEW/pg_wal"/[0-9A-F]* >/dev/null 2>&1 \
    || { echo "FAIL: --$MODE generated no upgrade WAL window"; FAIL=1; }

  # the new cluster must AUTO-SERVE the upgraded data on first start
  { echo "unix_socket_directories='$W'"; echo "port=$P"; } >> "$NEW/postgresql.conf"
  if "$BIN/pg_ctl" -D "$NEW" -l "$W/${MODE}_new.log" -w -t 40 start >/dev/null 2>&1; then
    ROWS=$("$BIN/psql" -h "$W" -p $P -U postgres -tAc "SELECT count(*) FROM t" 2>&1 | head -1)
    "$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1
    [ "$ROWS" = 500 ] && log "  --$MODE: new cluster auto-served the upgraded data (500 rows)" \
                      || { echo "FAIL: --$MODE auto-served cluster wrong data (rows=$ROWS)"; FAIL=1; }
  else
    echo "FAIL: --$MODE new cluster did not auto-serve"; tail -8 "$W/${MODE}_new.log"; FAIL=1
  fi
  cd /
}

for MODE in copy clone copy-file-range link swap; do
  log "MODE --$MODE"
  run_mode "$MODE"
done

echo "========================================================================"
[ "$FAIL" = 0 ] && log "PASS: --wal-upgrade works across all transfer modes (WAL window generated; new cluster auto-serves)" \
                || log "FAIL: see messages above"
exit $FAIL
