#!/usr/bin/env bash
#
# ALL-VERSION permutation matrix for --wal-log-upgrade (TODO.md §3).
#
# The single 18->20 pair proven by run_standby_xversion_test.sh cannot surface
# version-specific regressions (catalog layout, SLRU format, control-version
# gates) that only appear for a particular OLD major.  This test upgrades EVERY
# available old major to the current NEW (20devel) via --wal-log-upgrade and
# asserts, for each pair, that:
#   * pg_upgrade succeeds,
#   * the new cluster HOLDS in quarantine on first start (then commits),
#   * the catalog version actually changed old->new (a real cross-version jump),
#   * the data matches after the WAL-replay reconstruction, and
#   * the committed cluster is writable.
#
# Drive it from a set of OLDBIN dirs (one per major) + the single NEWBIN.  Pairs
# whose OLDBIN is unavailable are SKIPPED and logged -- never silently dropped.
#
# Env:
#   PGBIN            NEW (20devel) bin dir (required)
#   OLDBIN_DIRS      space-separated list of old-major bin dirs; if unset, a
#                    default Arca layout (hadron/pg_install/vNN/bin) is probed.
set -u
NEWBIN="${PGBIN:?set PGBIN to the 20devel bin dir}"
W=${WORK:-/tmp/pgu_matrix}
BASEPORT=${BASEPORT:-56700}
export PGDATABASE=postgres
log(){ echo "=== $* ==="; }
rm -rf "$W"; mkdir -p "$W"

NEWVER=$("$NEWBIN/pg_controldata" --version | grep -oE '[0-9]+devel|[0-9]+\.[0-9]+')

# Default candidate old-major bin dirs (Arca layout); override with OLDBIN_DIRS.
if [ -z "${OLDBIN_DIRS:-}" ]; then
    OLDBIN_DIRS=""
    for v in 14 15 16 17 18; do
        d="$HOME/hadron/pg_install/v$v/bin"
        [ -x "$d/pg_ctl" ] && OLDBIN_DIRS="$OLDBIN_DIRS $d"
    done
fi

RAN=0; PASSED=0; SKIPPED=0; FAILED=0
SKIP_LIST=""; FAIL_LIST=""

run_pair() { # $1=OLDBIN  $2=port
    local OLDBIN=$1 P=$2
    local OLD=$W/old_$P NEW=$W/new_$P
    local OLDVER OLD_CATVER NEW_CATVER OLD_FP NEW_FP

    OLDVER=$("$OLDBIN/pg_controldata" --version 2>/dev/null | grep -oE '[0-9]+devel|[0-9]+\.[0-9]+')
    log ">>> pair $OLDVER -> $NEWVER  (OLDBIN=$OLDBIN)"

    if [ "$OLDVER" = "$NEWVER" ]; then
        echo "  SKIP: same version as NEW (no cross-version gap)"; SKIP_LIST="$SKIP_LIST $OLDVER"; SKIPPED=$((SKIPPED+1)); return
    fi

    # ---- build an OLD-version cluster with representative data ----
    "$OLDBIN/initdb" -D "$OLD" -U postgres -N >/dev/null 2>&1 || { echo "  FAIL: initdb $OLDVER"; FAIL_LIST="$FAIL_LIST $OLDVER(initdb)"; FAILED=$((FAILED+1)); return; }
    echo "unix_socket_directories='$W'">>"$OLD/postgresql.conf"; echo "port=$P">>"$OLD/postgresql.conf"
    "$OLDBIN/pg_ctl" -D "$OLD" -l "$W/old_$P.log" -w start >/dev/null 2>&1 || { echo "  FAIL: start $OLDVER"; tail -5 "$W/old_$P.log"; FAIL_LIST="$FAIL_LIST $OLDVER(start)"; FAILED=$((FAILED+1)); return; }
    "$OLDBIN/psql" -h "$W" -p $P -U postgres -q >/dev/null 2>&1 <<SQL
CREATE TABLE t(id int primary key, v text);
INSERT INTO t SELECT g, 'v'||g FROM generate_series(1,5000) g;
CREATE INDEX t_v ON t(v);
CREATE TABLE toast_t(id int, big text);
INSERT INTO toast_t SELECT g, repeat(md5(g::text),300) FROM generate_series(1,400) g;
CREATE DATABASE appdb;
SQL
    "$OLDBIN/psql" -h "$W" -p $P -U postgres -d appdb -q >/dev/null 2>&1 -c \
        "CREATE TABLE o(id int primary key, n text); INSERT INTO o SELECT g,'n'||g FROM generate_series(1,3000) g;"
    OLD_FP=$("$OLDBIN/psql" -h "$W" -p $P -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint),(SELECT count(*) FROM toast_t) FROM t")
    OLD_CATVER=$("$OLDBIN/pg_controldata" -D "$OLD" | grep 'Catalog version' | grep -oE '[0-9]+')
    "$OLDBIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1

    # ---- pg_upgrade OLD -> NEW via --wal-log-upgrade ----
    ( cd "$W" && "$NEWBIN/pg_upgrade" -b "$OLDBIN" -B "$NEWBIN" -d "$OLD" -D "$NEW" \
        -U postgres --initdb --wal-log-upgrade --copy >"$W/up_$P.log" 2>&1 )
    if [ $? -ne 0 ]; then echo "  FAIL: pg_upgrade $OLDVER->$NEWVER"; tail -15 "$W/up_$P.log"; FAIL_LIST="$FAIL_LIST $OLDVER(upgrade)"; FAILED=$((FAILED+1)); return; fi

    echo "unix_socket_directories='$W'">>"$NEW/postgresql.conf"; echo "port=$P">>"$NEW/postgresql.conf"

    # ---- hold-start: reconstruct + hold (pg_ctl exits non-zero at the hold) ----
    "$NEWBIN/pg_ctl" -D "$NEW" -l "$W/hold_$P.log" -w start >/dev/null 2>&1 || true
    local st; st=$("$NEWBIN/pg_controldata" -D "$NEW" | grep -i 'cluster state' | sed 's/.*: *//')
    echo "$st" | grep -qi quarantine || { echo "  FAIL: $OLDVER new cluster not quarantined after hold-start (got '$st')"; FAIL_LIST="$FAIL_LIST $OLDVER(nohold)"; FAILED=$((FAILED+1)); return; }

    # ---- commit -> live ----
    "$NEWBIN/pg_upgrade" -b "$OLDBIN" -B "$NEWBIN" -d "$OLD" -D "$NEW" --commit >"$W/commit_$P.log" 2>&1 \
        || { echo "  FAIL: $OLDVER commit"; tail -15 "$W/commit_$P.log"; FAIL_LIST="$FAIL_LIST $OLDVER(commit)"; FAILED=$((FAILED+1)); return; }

    "$NEWBIN/pg_ctl" -D "$NEW" -l "$W/new_$P.log" -w start >/dev/null 2>&1 || { echo "  FAIL: $OLDVER start after commit"; tail -15 "$W/new_$P.log"; FAIL_LIST="$FAIL_LIST $OLDVER(newstart)"; FAILED=$((FAILED+1)); return; }
    NEW_FP=$("$NEWBIN/psql" -h "$W" -p $P -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint),(SELECT count(*) FROM toast_t) FROM t" 2>&1)
    NEW_CATVER=$("$NEWBIN/pg_controldata" -D "$NEW" | grep 'Catalog version' | grep -oE '[0-9]+')
    # writable?
    "$NEWBIN/psql" -h "$W" -p $P -U postgres -qc "INSERT INTO t VALUES (999999,'post')" >/dev/null 2>&1
    local WOK; WOK=$("$NEWBIN/psql" -h "$W" -p $P -U postgres -tAc "SELECT count(*) FROM t WHERE id=999999" 2>&1)
    # appdb data survived?
    local APP; APP=$("$NEWBIN/psql" -h "$W" -p $P -U postgres -d appdb -tAc "SELECT count(*) FROM o" 2>&1)
    "$NEWBIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1

    RAN=$((RAN+1))
    local ok=1
    [ "$OLD_CATVER" != "$NEW_CATVER" ] || { echo "  FAIL: $OLDVER catalog version unchanged ($OLD_CATVER) -- no real jump"; ok=0; }
    [ "$OLD_FP" = "$NEW_FP" ]          || { echo "  FAIL: $OLDVER data mismatch old='$OLD_FP' new='$NEW_FP'"; ok=0; }
    [ "$WOK" = "1" ]                   || { echo "  FAIL: $OLDVER committed cluster not writable"; ok=0; }
    [ "$APP" = "3000" ]                || { echo "  FAIL: $OLDVER appdb data lost (got '$APP')"; ok=0; }
    if [ $ok -eq 1 ]; then
        echo "  PASS: $OLDVER->$NEWVER  catver $OLD_CATVER->$NEW_CATVER  data=$NEW_FP  writable  appdb=3000"
        PASSED=$((PASSED+1))
    else
        FAIL_LIST="$FAIL_LIST $OLDVER(assert)"; FAILED=$((FAILED+1))
    fi
}

port=$BASEPORT
for d in $OLDBIN_DIRS; do
    if [ ! -x "$d/pg_ctl" ]; then
        echo "=== SKIP (unavailable): $d ==="; SKIP_LIST="$SKIP_LIST $d"; SKIPPED=$((SKIPPED+1)); continue
    fi
    run_pair "$d" "$port"
    port=$((port+1))
done

echo "========================================================================"
log "matrix summary: ran=$RAN passed=$PASSED failed=$FAILED skipped=$SKIPPED"
[ -n "$SKIP_LIST" ] && log "SKIPPED pairs (no coverage):$SKIP_LIST"
[ -n "$FAIL_LIST" ] && log "FAILED pairs:$FAIL_LIST"

# Require at least one real cross-version pair actually ran, else the matrix
# proved nothing (don't let "all skipped" masquerade as success).
if [ "$RAN" -eq 0 ]; then
    echo "FAIL: no cross-version pairs were available to run (set OLDBIN_DIRS)"; exit 1
fi
[ "$FAILED" -eq 0 ] && { log "PASS: all $PASSED available old->$NEWVER pairs upgraded via WAL replay"; exit 0; } \
                    || { log "FAIL: $FAILED pair(s) failed"; exit 1; }
