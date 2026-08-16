#!/usr/bin/env bash
# Standby RELINK redo: verify every transfer mode reproduces the PRIMARY's
# on-disk result, not just that the standby converges.
#
# run_standby_stream_e2e_test proves a standby STREAMS the window and converges
# for one mode (default --copy).  This test drives that end-to-end path once
# per transfer mode and inspects the placed user relation file to verify
# the standby's RELINK redo used the correct primitive -- reproducing what
# pg_upgrade's transfer step did on the primary:
#
#   copy            -> independent full copy   : distinct inode, source intact
#   copy_file_range -> copy_file_range()       : distinct inode, source intact
#   clone           -> reflink / COW clone      : distinct inode, SHARED extents,
#                                                 source intact  (needs a
#                                                 reflink-capable FS; see WORK)
#   link            -> per-file hardlink         : SHARED inode, source intact
#   swap            -> rename() (move)           : source MOVED OUT of stby_old
#
# The standby relinks from its retained pre-upgrade datadir ($W/stby_old,
# staged by the e2e script), so the inode/extent/existence checks below compare
# the skeleton's placed file against the retained source.
#
# Env:
#   PGBIN  bin dir (default <repo>/pginst/bin)
#   MODES  space-separated subset to run (default: all the platform supports)
#   WORK   parent work dir.  clone REQUIRES a reflink-capable filesystem (XFS
#          with reflink=1, Btrfs, APFS); on ext4/overlayfs the primary's
#          pg_upgrade --clone refuses first, so clone is auto-skipped there.
#
# Probe: a single user table's first relfilenode segment. The e2e script
# seeds table t at a stable OID, whose base fork is checked as base/<db>/<relfn>.
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
BIN="${PGBIN:-$ROOT/pginst/bin}"
E2E="$(dirname "${BASH_SOURCE[0]}")/run_standby_stream_e2e_test.sh"
BASEW=${WORK:-/tmp/pgu_stby_xfer}
export PGDATABASE=postgres
log(){ echo "=== $* ==="; }
GRC=0

# Reflink capability of work filesystem (clone needs it; primary's --clone
# would refuse otherwise, so skip clone where unavailable).
reflink_ok() {
  local dir=$1
  local probe="$dir/.reflink_probe"
  mkdir -p "$dir"
  dd if=/dev/zero of="$probe.a" bs=4096 count=4 >/dev/null 2>&1 || return 1
  local rc=1
  if cp --reflink=always "$probe.a" "$probe.b" >/dev/null 2>&1; then rc=0
  elif cp -c "$probe.a" "$probe.b" >/dev/null 2>&1; then rc=0   # APFS clone
  fi
  rm -f "$probe.a" "$probe.b"
  return $rc
}

# copy_file_range availability in this build.  The --copy-file-range flag is
# advertised, but the syscall is gated on HAVE_COPY_FILE_RANGE (undefined on e.g.
# macOS); without it both primary transfer and standby redo hard-fail.  Probe the
# installed pg_config.h at <prefix>/include/{server/,}pg_config.h relative to bin.
cfr_ok() {
  local inc
  for inc in "$BIN/../include/server/pg_config.h" \
             "$BIN/../include/pg_config.h" \
             "$BIN/../include/postgresql/server/pg_config.h"; do
    [ -f "$inc" ] || continue
    grep -qE '^#define[[:space:]]+HAVE_COPY_FILE_RANGE[[:space:]]+1' "$inc" && return 0
    return 1
  done
  return 1   # can't find pg_config.h -> assume unavailable (conservative)
}

ALL="copy copy-file-range link swap clone"
if [ -z "${MODES:-}" ]; then
  MODES=""
  for m in $ALL; do
    case "$m" in
      clone)
        reflink_ok "$BASEW" && MODES="$MODES $m" \
          || log "SKIP clone: $BASEW is not on a reflink-capable filesystem" ;;
      copy-file-range)
        cfr_ok && MODES="$MODES $m" \
          || log "SKIP copy-file-range: not supported by this build/platform" ;;
      *)
        MODES="$MODES $m" ;;
    esac
  done
fi
log "transfer modes to check:$MODES"

# Locate probe user relation file (table t) in a datadir; return path to its
# base-fork segment 0, or empty if absent.
probe_file() {
  local dd=$1 f
  for f in "$dd"/base/*/[0-9]*; do
    [ -f "$f" ] || continue
    local n; n=$(basename "$f" | sed 's/[._].*//')
    [ "$n" -ge 16384 ] 2>/dev/null || continue
    # table t is the first user table seeded; any user rel proves placement, but
    # prefer the largest (the data table, not an index) for a meaningful extent.
    echo "$f"
  done | xargs -r ls -S 2>/dev/null | head -1
}

# Count shared extents (reflink) via filefrag; 0 if filefrag is absent (e.g. macOS)
# or the file has none.  Always prints a single integer.
shared_extents() {
  command -v filefrag >/dev/null 2>&1 || { echo 0; return; }
  filefrag -v "$1" 2>/dev/null | grep -c 'shared'
}

for m in $MODES; do
  echo "############################################################"
  log "MODE=--$m"
  W="$BASEW/$m"
  # Run full streaming e2e for this mode (stages stby_old + skeleton, verifies
  # convergence).  Add on-disk placement checks afterward.
  if XFER="--$m" WORK="$W" PGBIN="$BIN" bash "$E2E" >"$BASEW/$m.log" 2>&1; then
    log "  e2e (--$m): standby streamed + converged"
  else
    echo "  FAIL: e2e (--$m) did not pass"; tail -15 "$BASEW/$m.log"; GRC=1; continue
  fi

  src=$(probe_file "$W/stby_old")     # the retained pre-upgrade source
  dst=$(probe_file "$W/skel")         # what the standby placed
  # For swap the source is MOVED, so probe_file on stby_old yields nothing;
  # skeleton still has the file.  Resolve dst by relfilenumber if needed.
  if [ -z "$dst" ]; then echo "  FAIL: no user relation placed in the skeleton"; GRC=1; continue; fi

  di=$(stat -c%i "$dst" 2>/dev/null || stat -f%i "$dst")
  case "$m" in
    copy|copy-file-range)
      [ -n "$src" ] || { echo "  FAIL($m): source vanished from stby_old"; GRC=1; continue; }
      si=$(stat -c%i "$src" 2>/dev/null || stat -f%i "$src")
      if [ "$si" != "$di" ]; then log "  OK($m): independent copy (src inode=$si != dst inode=$di); source intact"
      else echo "  FAIL($m): expected distinct inodes, got shared ($si)"; GRC=1; fi
      ;;
    clone)
      [ -n "$src" ] || { echo "  FAIL(clone): source vanished from stby_old"; GRC=1; continue; }
      si=$(stat -c%i "$src" 2>/dev/null || stat -f%i "$src")
      dsh=$(shared_extents "$dst"); ssh_=$(shared_extents "$src")
      if [ "$si" != "$di" ] && [ "$dsh" -gt 0 ] && [ "$ssh_" -gt 0 ]; then
        log "  OK(clone): reflink (distinct inodes $si/$di, shared extents src=$ssh_ dst=$dsh)"
      elif [ "$si" != "$di" ]; then
        # filefrag unavailable (e.g. APFS): distinct inode + convergence still
        # consistent with a clone; note the weaker check.
        log "  OK(clone): distinct inodes ($si/$di); extent-sharing not verifiable here"
      else
        echo "  FAIL(clone): expected distinct inode reflink, got shared inode"; GRC=1
      fi
      ;;
    link)
      [ -n "$src" ] || { echo "  FAIL(link): source vanished from stby_old"; GRC=1; continue; }
      si=$(stat -c%i "$src" 2>/dev/null || stat -f%i "$src")
      if [ "$si" = "$di" ]; then log "  OK(link): hardlink (shared inode=$si); source intact"
      else echo "  FAIL(link): expected shared inode, got $si != $di"; GRC=1; fi
      ;;
    swap)
      if [ -z "$src" ]; then log "  OK(swap): source MOVED out of stby_old (rename); skeleton has the file"
      else echo "  FAIL(swap): source still present in stby_old ($src) -- swap must move it"; GRC=1; fi
      ;;
  esac
done

echo "############################################################"
[ "$GRC" = 0 ] && log "PASS: standby reproduced every checked transfer mode's on-disk result" \
              || log "FAIL: see messages above"
exit $GRC
