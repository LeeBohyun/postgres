#!/usr/bin/env bash
# Transfer-mode coverage for --wal-log-upgrade.
#
# pg_upgrade has 5 transfer modes; their interaction with revertability differs:
#   --copy, --clone, --link : the old cluster's data SURVIVES the transfer, so the
#         upgrade is REVERTABLE.  --copy/--clone duplicate the files; --link only
#         READS them (hard links).  For all three, --wal-log-upgrade must keep the
#         old cluster intact until --commit (the old-cluster disable is DEFERRED to
#         commit), so --rollback can restore it.
#   --copy-file-range : same family as copy, but needs the copy_file_range syscall
#         (Linux); skipped where unavailable.
#   --swap  : MOVES the old cluster's data into the new cluster, so there is nothing
#         to roll back to -- it must be REFUSED with --wal-log-upgrade.
#
# This test asserts, per mode:
#   copy/clone/link -> old cluster stays intact through the hold; --rollback
#                      restores it (data intact); --commit disables it and the new
#                      cluster serves the upgraded data.
#   swap            -> pg_upgrade refuses the combination up front.
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
BIN="${PGBIN:-$ROOT/pginst/bin}"
W=${WORK:-/tmp/pgu_tmodes}; P=${PPORT:-55640}
export PGDATABASE=postgres
log(){ echo "=== $* ==="; }
FAIL=0
rm -rf "$W"; mkdir -p "$W"

# seed a template old cluster once, then copy it per mode
seed() {
  local OLD=$1
  "$BIN/initdb" -D "$OLD" -U postgres -N >/dev/null 2>&1 || return 1
  { echo "unix_socket_directories='$W'"; echo "port=$P"; } >> "$OLD/postgresql.conf"
  "$BIN/pg_ctl" -D "$OLD" -l "$W/seed.log" -w start >/dev/null 2>&1 || return 1
  "$BIN/psql" -h "$W" -p $P -U postgres -qc "CREATE TABLE t(a int); INSERT INTO t SELECT g FROM generate_series(1,500) g; CREATE INDEX ON t(a);" >/dev/null
  "$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1
}

# ---- revertable modes: copy, clone, link ----
for MODE in copy clone link; do
  log "MODE --$MODE : upgrade -> hold -> rollback restores old; then upgrade -> commit"
  OLD=$W/${MODE}_old NEW=$W/${MODE}_new
  seed "$OLD" || { echo "FAIL: seed --$MODE"; FAIL=1; continue; }

  cd "$W"
  "$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-log-upgrade --$MODE >"$W/${MODE}_up.log" 2>&1
  if [ $? -ne 0 ]; then echo "FAIL: --$MODE upgrade"; tail -8 "$W/${MODE}_up.log"; FAIL=1; cd /; continue; fi

  # old cluster must stay INTACT through the upgrade (disable deferred to commit)
  [ -f "$OLD/global/pg_control" ] || { echo "FAIL: --$MODE disabled old cluster during upgrade (not revertable)"; FAIL=1; }

  { echo "unix_socket_directories='$W'"; echo "port=$P"; } >> "$NEW/postgresql.conf"
  "$BIN/pg_ctl" -D "$NEW" -l "$W/${MODE}_hold.log" -w -t 40 start >/dev/null 2>&1 || true
  ST=$("$BIN/pg_controldata" -D "$NEW" | grep -i "cluster state" | sed 's/.*: *//')
  case "$ST" in *quarantine*) : ;; *) echo "FAIL: --$MODE did not hold in quarantine (state='$ST')"; FAIL=1;; esac

  # ROLLBACK: new discarded, old restored + startable with data
  "$BIN/pg_upgrade" -D "$NEW" --rollback >"$W/${MODE}_rb.log" 2>&1 || { echo "FAIL: --$MODE rollback"; cat "$W/${MODE}_rb.log"; FAIL=1; }
  [ -d "$NEW" ] && { echo "FAIL: --$MODE rollback left new_dir"; FAIL=1; }
  "$BIN/pg_ctl" -D "$OLD" -l "$W/${MODE}_old2.log" -w -t 20 start >/dev/null 2>&1
  ROWS=$("$BIN/psql" -h "$W" -p $P -U postgres -tAc "SELECT count(*) FROM t" 2>&1 | head -1)
  "$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1
  [ "$ROWS" = 500 ] && log "  --$MODE rollback: old cluster intact (500 rows)" \
                    || { echo "FAIL: --$MODE old cluster not intact after rollback (rows=$ROWS)"; FAIL=1; }

  # Now COMMIT a fresh upgrade of the same old cluster.
  rm -rf "$NEW"
  "$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-log-upgrade --$MODE >"$W/${MODE}_up2.log" 2>&1 \
    || { echo "FAIL: --$MODE re-upgrade"; tail -8 "$W/${MODE}_up2.log"; FAIL=1; cd /; continue; }
  { echo "unix_socket_directories='$W'"; echo "port=$P"; } >> "$NEW/postgresql.conf"
  "$BIN/pg_ctl" -D "$NEW" -l "$W/${MODE}_hold2.log" -w -t 40 start >/dev/null 2>&1 || true
  "$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres --commit >"$W/${MODE}_commit.log" 2>&1 \
    || { echo "FAIL: --$MODE commit"; tail -8 "$W/${MODE}_commit.log"; FAIL=1; }
  # commit must have disabled the old cluster (deferred disable happens here)
  [ -f "$OLD/global/pg_control.old" ] || { echo "FAIL: --$MODE commit did not disable old cluster"; FAIL=1; }
  "$BIN/pg_ctl" -D "$NEW" -l "$W/${MODE}_new.log" -w -t 20 start >/dev/null 2>&1
  CROWS=$("$BIN/psql" -h "$W" -p $P -U postgres -tAc "SELECT count(*) FROM t" 2>&1 | head -1)
  "$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1
  [ "$CROWS" = 500 ] && log "  --$MODE commit: new cluster serves upgraded data (500 rows); old disabled" \
                     || { echo "FAIL: --$MODE committed cluster wrong data (rows=$CROWS)"; FAIL=1; }
  cd /
done

# ---- copy-file-range: only where the syscall exists ----
log "MODE --copy-file-range : upgrade (skipped if the platform lacks copy_file_range)"
OLD=$W/cfr_old NEW=$W/cfr_new
seed "$OLD" || { echo "FAIL: seed --copy-file-range"; FAIL=1; }
cd "$W"
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-log-upgrade --copy-file-range >"$W/cfr_up.log" 2>&1
if [ $? -eq 0 ]; then
  [ -f "$OLD/global/pg_control" ] && log "  --copy-file-range: upgrade OK, old cluster intact" \
                                  || { echo "FAIL: --copy-file-range disabled old cluster"; FAIL=1; }
elif grep -qi "copy_file_range not supported" "$W/cfr_up.log"; then
  log "  --copy-file-range: SKIP (copy_file_range not supported on this platform)"
else
  echo "FAIL: --copy-file-range failed for a non-platform reason"; tail -8 "$W/cfr_up.log"; FAIL=1
fi
cd /

# ---- swap: must be REFUSED ----
log "MODE --swap : must be REFUSED with --wal-log-upgrade (non-revertable)"
OLD=$W/swap_old NEW=$W/swap_new
seed "$OLD" || { echo "FAIL: seed --swap"; FAIL=1; }
cd "$W"
if "$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-log-upgrade --swap >"$W/swap_up.log" 2>&1; then
  echo "FAIL: --swap + --wal-log-upgrade was ACCEPTED (must be refused)"; FAIL=1
else
  grep -qi "swap cannot be used with --wal-log-upgrade" "$W/swap_up.log" \
    && log "  --swap correctly refused" \
    || { echo "FAIL: --swap refused for the wrong reason:"; tail -5 "$W/swap_up.log"; FAIL=1; }
  [ -f "$OLD/global/pg_control" ] || { echo "FAIL: --swap refusal still touched the old cluster"; FAIL=1; }
fi
cd /

echo "========================================================================"
[ "$FAIL" = 0 ] && log "PASS: transfer modes -- copy/clone/link revertable (rollback restores old, commit disables it); swap refused" \
                || log "FAIL: see messages above"
exit $FAIL
