#!/usr/bin/env bash
# Priority edge-case variants for --wal-upgrade, each a self-contained case:
#
#   checksums-off   : cluster built with --no-data-checksums.  The RELFILE FPI
#                     path and page validation differ when checksums are off
#                     (no pd_checksum); replay must still reconstruct exactly.
#   segsize-1MB     : cluster with --wal-segsize=1.  The upgrade-WAL scanner
#                     reads segsize from the files and the chunk cap is relative
#                     to XLogRecordMaxSize (not segsize); a non-16MB segment size
#                     must still scan + replay.
#   crash-in-replay : kill the server mid first-startup replay (after START,
#                     before COMPLETE applied), then restart.  The `applied`
#                     idempotency guard was only tested for "already applied";
#                     this checks "half applied then restarted" converges and
#                     does not double-apply or refuse.
#
# Usage: run_variants_test.sh [checksums|segsize|crash|all]   (default: all)
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"; BIN="${PGBIN:-$ROOT/pginst/bin}"
WHICH=${1:-all}
BASEW=${WORK:-/tmp/pgu_variants}
export PGDATABASE=postgres
log(){ echo "=== $* ==="; }
RC=0

# common: build old cluster with extra initdb args ($2), load data, upgrade,
# assert disk wiped, replay, compare.  $1=name $2=initdb_extra $3=port
run_variant() {
    local name=$1 initdb_extra=$2 port=$3
    local W=$BASEW/$name
    local OLD=$W/old NEW=$W/new
    rm -rf "$W"; mkdir -p "$W"
    lsof -ti :$port 2>/dev/null | xargs kill -9 2>/dev/null
    log "[$name] initdb $initdb_extra"
    "$BIN/initdb" -D "$OLD" -U postgres -N $initdb_extra >/dev/null 2>&1 || { echo "[$name] FAIL initdb"; return 1; }
    echo "unix_socket_directories='$W'">>$OLD/postgresql.conf; echo "port=$port">>$OLD/postgresql.conf
    "$BIN/pg_ctl" -D "$OLD" -l "$W/old.log" -w start >/dev/null 2>&1 || { echo "[$name] FAIL start"; tail -5 "$W/old.log"; return 1; }
    "$BIN/psql" -h "$W" -p $port -U postgres -q >/dev/null 2>&1 <<SQL
CREATE TABLE t(id int primary key, v text);
INSERT INTO t SELECT g, repeat('v',60)||g FROM generate_series(1,20000) g;
CREATE INDEX ON t(v);
CREATE TABLE toast_t(id int, big text);
INSERT INTO toast_t SELECT g, repeat(md5(g::text),400) FROM generate_series(1,500) g;
SQL
    local OLD_FP=$("$BIN/psql" -h "$W" -p $port -U postgres -tAc "SELECT count(*), sum(hashtext(v)::bigint), (SELECT count(*) FROM toast_t) FROM t")
    log "[$name] old fingerprint: $OLD_FP"
    "$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1

    cd "$W"
    "$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-upgrade --copy >"$W/up.log" 2>&1
    [ $? -eq 0 ] || { echo "[$name] FAIL upgrade"; tail -20 "$W/up.log"; return 1; }
    # confirm the new cluster inherited the variant setting
    "$BIN/pg_controldata" -D "$NEW" | grep -iE "checksum version|Bytes per WAL segment" | sed "s/^/[$name] /"

    local TOTAL_BASE=$(find "$NEW/base" -type f -regextype posix-extended -regex '.*/[0-9]+(\.[0-9]+)?' -printf '%s\n' 2>/dev/null | awk '{s+=$1}END{print s+0}')
    [ "${TOTAL_BASE:-0}" = "0" ] || { echo "[$name] FAIL: base/ not wiped ($TOTAL_BASE)"; return 1; }

    # --wal-upgrade auto-serves the new cluster (after the disk-wiped
    # assertion above): the first start applies the WAL window, reconstructs, and
    # comes up read-write -- no quarantine hold, no commit step.
    echo "unix_socket_directories='$W'">>$NEW/postgresql.conf; echo "port=$port">>$NEW/postgresql.conf
    "$BIN/pg_ctl" -D "$NEW" -l "$W/new.log" -w start >/dev/null 2>&1 || { echo "[$name] FAIL start new"; tail -20 "$W/new.log"; return 1; }
    local NEW_FP=$("$BIN/psql" -h "$W" -p $port -U postgres -tAc "SELECT count(*), sum(hashtext(v)::bigint), (SELECT count(*) FROM toast_t) FROM t")
    "$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1
    if [ "$OLD_FP" = "$NEW_FP" ]; then log "[$name] PASS (fp=$NEW_FP)"; return 0
    else echo "[$name] FAIL: data mismatch old='$OLD_FP' new='$NEW_FP'"; return 1; fi
}

if [ "$WHICH" = checksums ] || [ "$WHICH" = all ]; then
    run_variant checksums_off "--no-data-checksums" 55571 || RC=1
fi
if [ "$WHICH" = segsize ] || [ "$WHICH" = all ]; then
    run_variant segsize_1mb "--wal-segsize=1" 55572 || RC=1
fi

# ---- crash-during-replay idempotency ------------------------------------
if [ "$WHICH" = crash ] || [ "$WHICH" = all ]; then
    name=crash_in_replay; W=$BASEW/$name; OLD=$W/old; NEW=$W/new; port=55573
    rm -rf "$W"; mkdir -p "$W"; lsof -ti :$port 2>/dev/null | xargs kill -9 2>/dev/null
    log "[$name] build + upgrade"
    "$BIN/initdb" -D "$OLD" -U postgres -N >/dev/null 2>&1
    echo "unix_socket_directories='$W'">>$OLD/postgresql.conf; echo "port=$port">>$OLD/postgresql.conf
    "$BIN/pg_ctl" -D "$OLD" -l "$W/old.log" -w start >/dev/null 2>&1
    "$BIN/psql" -h "$W" -p $port -U postgres -q >/dev/null 2>&1 <<SQL
CREATE TABLE t(id int primary key, v text);
INSERT INTO t SELECT g, repeat('v',60)||g FROM generate_series(1,20000) g;
SQL
    OLD_FP=$("$BIN/psql" -h "$W" -p $port -U postgres -tAc "SELECT count(*), sum(hashtext(v)::bigint) FROM t")
    "$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1
    cd "$W"
    "$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-upgrade --copy >"$W/up.log" 2>&1
    [ $? -eq 0 ] || { echo "[$name] FAIL upgrade"; tail -15 "$W/up.log"; RC=1; }
    echo "unix_socket_directories='$W'">>$NEW/postgresql.conf; echo "port=$port">>$NEW/postgresql.conf

    # Start recovery, then IMMEDIATE-kill the postmaster mid-replay to simulate a
    # crash before COMPLETE is durably applied.  pg_ctl start waits for readiness,
    # so start in the background and SIGKILL fast.
    log "[$name] start recovery then SIGKILL mid-replay"
    "$BIN/postgres" -D "$NEW" >"$W/crash.log" 2>&1 &
    PM=$!
    # let it begin replay (arm bootstrap + start applying) then kill hard
    for i in $(seq 1 40); do grep -q "arming recovery from end-of-upgrade" "$W/crash.log" 2>/dev/null && break; sleep 0.1; done
    kill -9 $PM 2>/dev/null
    # kill any child procs too
    pkill -9 -f "postgres -D $NEW" 2>/dev/null
    lsof -ti :$port 2>/dev/null | xargs kill -9 2>/dev/null
    sleep 1
    # After a mid-replay crash, a restart must re-arm and converge idempotently,
    # then auto-serve read-write with the data intact (no re-hold, no
    # commit step).  This proves crash-idempotency: a half-applied window
    # converges on restart and comes up live without double-applying or refusing.
    log "[$name] restart after crash -- must re-arm, converge, and auto-serve (idempotent)"
    "$BIN/pg_ctl" -D "$NEW" -l "$W/new2.log" -w -t 120 start >/dev/null 2>&1
    if [ $? -ne 0 ]; then echo "[$name] FAIL: did not come up after crash-restart"; tail -20 "$W/new2.log"; RC=1
    else
        NEW_FP=$("$BIN/psql" -h "$W" -p $port -U postgres -tAc "SELECT count(*), sum(hashtext(v)::bigint) FROM t" 2>&1)
        "$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1
        if [ "$OLD_FP" = "$NEW_FP" ]; then log "[$name] PASS (converged after crash-restart: $NEW_FP)"
        else echo "[$name] FAIL: mismatch after crash-restart old='$OLD_FP' new='$NEW_FP'"; RC=1; fi
    fi
fi

echo "========================================================================"
[ "$RC" = 0 ] && log "PASS: all requested variants" || log "FAIL: see messages above"
exit $RC
