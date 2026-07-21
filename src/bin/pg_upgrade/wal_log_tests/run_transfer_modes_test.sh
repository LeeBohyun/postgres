#!/usr/bin/env bash
# Transfer-mode coverage for --wal-upgrade.
#
# pg_upgrade has 5 transfer modes; their interaction with revertability differs:
#   --copy, --clone, --link : the old cluster's data SURVIVES the transfer, so the
#         upgrade is REVERTABLE.  --copy/--clone duplicate the files; --link only
#         READS them (hard links).  For all three, --wal-upgrade keeps the old
#         cluster intact (its removal is a separate, explicit --wal-upgrade-delete-old
#         step), so --wal-upgrade-rollback can restore it.
#   --copy-file-range : same family as copy, but needs the copy_file_range syscall
#         (Linux); skipped where unavailable.
#   --swap  : MOVES the old cluster's data into the new cluster, so there is nothing
#         to roll back to -- it must be REFUSED with --wal-upgrade.
#
# This test asserts, per mode:
#   copy/clone/link -> old cluster stays intact after the upgrade; --wal-upgrade-rollback
#                      restores it (data intact); the new cluster auto-serves the
#                      upgraded data.
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
  log "MODE --$MODE : upgrade -> rollback restores old; then upgrade -> auto-serve"
  OLD=$W/${MODE}_old NEW=$W/${MODE}_new
  seed "$OLD" || { echo "FAIL: seed --$MODE"; FAIL=1; continue; }

  cd "$W"
  "$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-upgrade --$MODE >"$W/${MODE}_up.log" 2>&1
  if [ $? -ne 0 ]; then echo "FAIL: --$MODE upgrade"; tail -8 "$W/${MODE}_up.log"; FAIL=1; cd /; continue; fi

  # old cluster must stay INTACT through the upgrade (nothing disables it now --
  # only --wal-upgrade-delete-old does, explicitly, later)
  [ -f "$OLD/global/pg_control" ] || { echo "FAIL: --$MODE disabled old cluster during upgrade (not revertable)"; FAIL=1; }

  # After upgrade the new cluster is a normal "shut down" cluster (auto-serve),
  # NOT quarantined.
  { echo "unix_socket_directories='$W'"; echo "port=$P"; } >> "$NEW/postgresql.conf"
  ST=$("$BIN/pg_controldata" -D "$NEW" | grep -i "cluster state" | sed 's/.*: *//')
  case "$ST" in *quarantine*) echo "FAIL: --$MODE quarantined; auto-serve expected (state='$ST')"; FAIL=1;; esac

  # ROLLBACK before starting new.  On this branch --wal-upgrade keeps the old
  # cluster's files INTACT for every allowed transfer mode (the primary is not
  # demolished; old-cluster deletion is a separate, deferred step).  copy and link
  # therefore both leave old_dir intact, so rollback SUCCEEDS and restores it.
  # (--swap is rejected at parse time, tested separately below.)
  "$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" --wal-upgrade-rollback >"$W/${MODE}_rb.log" 2>&1 \
    || { echo "FAIL: --$MODE rollback should succeed (old_dir intact on this branch)"; cat "$W/${MODE}_rb.log"; FAIL=1; }
  [ -d "$NEW" ] && { echo "FAIL: --$MODE rollback left new_dir"; FAIL=1; }
  "$BIN/pg_ctl" -D "$OLD" -l "$W/${MODE}_old2.log" -w -t 20 start >/dev/null 2>&1
  ROWS=$("$BIN/psql" -h "$W" -p $P -U postgres -tAc "SELECT count(*) FROM t" 2>&1 | head -1)
  "$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1
  [ "$ROWS" = 500 ] && log "  --$MODE rollback: old cluster intact (500 rows)" \
                    || { echo "FAIL: --$MODE old cluster not intact after rollback (rows=$ROWS)"; FAIL=1; }

  # Now ADOPT a fresh upgrade of the same old cluster by simply starting it.
  rm -rf "$NEW"
  "$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-upgrade --$MODE >"$W/${MODE}_up2.log" 2>&1 \
    || { echo "FAIL: --$MODE re-upgrade"; tail -8 "$W/${MODE}_up2.log"; FAIL=1; cd /; continue; }
  { echo "unix_socket_directories='$W'"; echo "port=$P"; } >> "$NEW/postgresql.conf"
  "$BIN/pg_ctl" -D "$NEW" -l "$W/${MODE}_new.log" -w -t 40 start >/dev/null 2>&1 \
    || { echo "FAIL: --$MODE new cluster did not auto-serve"; tail -8 "$W/${MODE}_new.log"; FAIL=1; cd /; continue; }
  CROWS=$("$BIN/psql" -h "$W" -p $P -U postgres -tAc "SELECT count(*) FROM t" 2>&1 | head -1)
  "$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1
  [ "$CROWS" = 500 ] && log "  --$MODE auto-serve: new cluster serves upgraded data (500 rows)" \
                     || { echo "FAIL: --$MODE auto-served cluster wrong data (rows=$CROWS)"; FAIL=1; }
  cd /
done

# ---- copy-file-range: only where the syscall exists ----
log "MODE --copy-file-range : upgrade (skipped if the platform lacks copy_file_range)"
OLD=$W/cfr_old NEW=$W/cfr_new
seed "$OLD" || { echo "FAIL: seed --copy-file-range"; FAIL=1; }
cd "$W"
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-upgrade --copy-file-range >"$W/cfr_up.log" 2>&1
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
log "MODE --swap : must be REFUSED with --wal-upgrade (non-revertable)"
OLD=$W/swap_old NEW=$W/swap_new
seed "$OLD" || { echo "FAIL: seed --swap"; FAIL=1; }
cd "$W"
if "$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-upgrade --swap >"$W/swap_up.log" 2>&1; then
  echo "FAIL: --swap + --wal-upgrade was ACCEPTED (must be refused)"; FAIL=1
else
  grep -qi "swap cannot be used with --wal-upgrade" "$W/swap_up.log" \
    && log "  --swap correctly refused" \
    || { echo "FAIL: --swap refused for the wrong reason:"; tail -5 "$W/swap_up.log"; FAIL=1; }
  [ -f "$OLD/global/pg_control" ] || { echo "FAIL: --swap refusal still touched the old cluster"; FAIL=1; }
fi
cd /

echo "========================================================================"
[ "$FAIL" = 0 ] && log "PASS: transfer modes -- copy/clone/link revertable (rollback restores old, commit disables it); swap refused" \
                || log "FAIL: see messages above"
exit $FAIL
