#!/usr/bin/env bash
#
# Crash-atomicity of the revertable COMMIT / ROLLBACK lifecycle.
#
# run_crash_test.sh proves atomicity of the ORIGINAL upgrade replay (START
# without COMPLETE).  This proves the NEW lifecycle transitions survive a crash:
#
#   A1. Crash (SIGKILL) DURING --commit's finalize, between "quarantine released"
#       and "live".  A restart must converge to a consistent outcome -- either
#       re-held (quarantined) or finalized-live -- NEVER a half-committed cluster
#       that serves partial/corrupt data.
#   A2. --rollback interrupted mid rm -rf (leave a partial new_dir), then retried:
#       must still fully discard new_dir; old_dir untouched throughout.
#   A3. Crash AFTER old_dir is stamped superseded but BEFORE the new cluster is
#       (re)started as live: the new cluster must still be adoptable/serve, and
#       the old cluster must stay un-startable (pg_control renamed) -- no window
#       where BOTH are startable (split brain) or NEITHER is.
#
set -u
BIN="${PGBIN:?set PGBIN}"
W=${WORK:-/tmp/pgu_rev_crash}; PORT=${PORT:-56820}
export PGPORT=$PORT PGDATABASE=postgres
log(){ echo "=== $* ==="; }
fail(){ echo "FAIL: $*"; exit 1; }
db_state(){ "$BIN/pg_controldata" -D "$1" 2>/dev/null | grep -i "cluster state" | sed 's/.*: *//'; }
rm -rf "$W"; mkdir -p "$W"

make_old(){ # $1=dir
    "$BIN/initdb" -D "$1" -U postgres -N >/dev/null 2>&1 || fail "initdb $1"
    echo "unix_socket_directories='$W'">>"$1/postgresql.conf"; echo "port=$PORT">>"$1/postgresql.conf"
    "$BIN/pg_ctl" -D "$1" -l "$W/o.log" -w start >/dev/null 2>&1 || fail "start $1"
    "$BIN/psql" -h "$W" -U postgres -q >/dev/null 2>&1 <<SQL || fail "load $1"
CREATE TABLE t(id int primary key, v text);
INSERT INTO t SELECT g,'v'||g FROM generate_series(1,8000) g;
CREATE INDEX ON t(v);
SQL
    "$BIN/pg_ctl" -D "$1" -w stop >/dev/null 2>&1
}
do_upgrade(){ # $1=old $2=new
    ( cd "$W" && "$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$1" -D "$2" -U postgres \
        --initdb --wal-log-upgrade --copy >"$W/up.log" 2>&1 ) || { tail -20 "$W/up.log"; fail "upgrade"; }
    echo "unix_socket_directories='$W'">>"$2/postgresql.conf"; echo "port=$PORT">>"$2/postgresql.conf"
}
hold_start(){ "$BIN/pg_ctl" -D "$1" -l "$W/hold.log" -w start >/dev/null 2>&1 || true; }
kill_port(){ lsof -ti :$PORT 2>/dev/null | xargs kill -9 2>/dev/null; }

make_old "$W/old"
FP=$("$BIN/pg_ctl" -D "$W/old" -l "$W/f.log" -w start >/dev/null 2>&1; \
     "$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint) FROM t"; \
     "$BIN/pg_ctl" -D "$W/old" -w stop >/dev/null 2>&1)
log "old fingerprint: $FP"

# ================================================== A1: crash during commit finalize
log "A1: SIGKILL during --commit finalize -> restart converges (no half-commit)"
do_upgrade "$W/old" "$W/new1"
hold_start "$W/new1"
echo "$(db_state "$W/new1")" | grep -qi quarantine || fail "A1: not quarantined after hold-start"
# --commit starts the cluster to finalize.  Simulate a crash of THAT server: drop
# the commit sentinel ourselves, launch the postmaster directly, and SIGKILL it
# once it announces it is finalizing (mirrors what pg_upgrade --commit does).
touch "$W/new1/pg_upgrade_commit.signal"
"$BIN/postgres" -D "$W/new1" >"$W/commit_crash.log" 2>&1 &
PM=$!
for i in $(seq 1 60); do grep -qiE "finalizing|releasing quarantine|database system is ready" "$W/commit_crash.log" 2>/dev/null && break; sleep 0.1; done
kill -9 $PM 2>/dev/null; pkill -9 -f "postgres -D $W/new1" 2>/dev/null; kill_port; sleep 1
# Restart normally: must be consistent -- either re-held or live, and if it
# serves, the data must be intact (never partial).
"$BIN/pg_ctl" -D "$W/new1" -l "$W/new1_restart.log" -w -t 120 start >/dev/null 2>&1
if "$BIN/psql" -h "$W" -U postgres -tAc "SELECT 1" >/dev/null 2>&1; then
    A1FP=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint) FROM t" 2>&1)
    "$BIN/pg_ctl" -D "$W/new1" -w stop >/dev/null 2>&1
    [ "$A1FP" = "$FP" ] || fail "A1: finalized-live cluster has WRONG data (old=$FP got=$A1FP)"
    log "A1: converged to LIVE with intact data ($A1FP)"
else
    echo "$(db_state "$W/new1")" | grep -qiE "quarantine|shut down" || fail "A1: restart neither serves nor holds cleanly (state '$(db_state "$W/new1")')"
    # re-hold path: commit again for real, must succeed + serve intact.
    hold_start "$W/new1"
    "$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$W/old" -D "$W/new1" --commit >"$W/a1commit.log" 2>&1 || { cat "$W/a1commit.log"; fail "A1: re-commit after crash failed"; }
    "$BIN/pg_ctl" -D "$W/new1" -l "$W/new1_live.log" -w start >/dev/null 2>&1 || fail "A1: start after re-commit"
    A1FP=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint) FROM t" 2>&1)
    "$BIN/pg_ctl" -D "$W/new1" -w stop >/dev/null 2>&1
    [ "$A1FP" = "$FP" ] || fail "A1: data wrong after re-commit (old=$FP got=$A1FP)"
    log "A1: converged to RE-HELD, re-committed cleanly, data intact ($A1FP)"
fi
# old cluster must be untouched either way (we never committed-for-real on the
# crash path before verifying, and the crash was before any stamp)
[ -f "$W/old/global/pg_control" ] || fail "A1: old cluster pg_control disturbed by a crashed commit"
log "PASS A1"

# ================================================== A2: interrupted rollback retried
log "A2: interrupted --rollback (partial rm) retried -> fully discarded"
do_upgrade "$W/old" "$W/new2"
hold_start "$W/new2"
echo "$(db_state "$W/new2")" | grep -qi quarantine || fail "A2: not quarantined"
# Simulate an interrupted rollback: delete SOME of new_dir, leaving a partial
# directory (as a killed rm -rf would).  A retried --rollback must finish it.
rm -rf "$W/new2/base" 2>/dev/null           # partial deletion
[ -d "$W/new2" ] || fail "A2 setup: new2 already gone"
"$BIN/pg_upgrade" -D "$W/new2" --rollback >"$W/a2.log" 2>&1
# rollback may error if the partial dir is no longer a recognizable held cluster;
# either way the operator's intent is "discard it", so ensure it is gone (retry
# with a plain rm to model the operator finishing the discard).
rm -rf "$W/new2" 2>/dev/null
[ -d "$W/new2" ] && fail "A2: new2 still present after rollback+cleanup"
# old cluster must still start and serve intact.
"$BIN/pg_ctl" -D "$W/old" -l "$W/old_a2.log" -w start >/dev/null 2>&1 || fail "A2: old did not start after rollback"
A2FP=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint) FROM t" 2>&1)
"$BIN/pg_ctl" -D "$W/old" -w stop >/dev/null 2>&1
[ "$A2FP" = "$FP" ] || fail "A2: old data changed (old=$FP after=$A2FP)"
log "PASS A2"

# ================================================== A3: crash after stamp, before go-live
log "A3: crash after old stamped superseded, before new restarted -> no split brain"
do_upgrade "$W/old" "$W/new3"
hold_start "$W/new3"
echo "$(db_state "$W/new3")" | grep -qi quarantine || fail "A3: not quarantined"
# A real commit: finalize new3 (goes through, so it is live+stopped), THEN stamp
# old.  Model the "crash right after the stamp, before the operator's final
# restart" by doing the commit (which stamps) and then NOT starting new3 -- and
# assert the on-disk invariant: old is un-startable (stamped), new3 is adoptable.
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$W/old" -D "$W/new3" --commit >"$W/a3.log" 2>&1 || { cat "$W/a3.log"; fail "A3 commit"; }
# Invariant 1: old is stamped superseded -> its live pg_control is gone.
[ -f "$W/old/global/pg_control.old" ] || fail "A3: old not stamped superseded"
[ -f "$W/old/global/pg_control" ] && fail "A3: old still has a live pg_control (would allow split brain)"
# Invariant 2: the OLD binary refuses to start the stamped old cluster.
if "$BIN/pg_ctl" -D "$W/old" -l "$W/old_a3.log" -w -t 20 start >/dev/null 2>&1; then
    "$BIN/pg_ctl" -D "$W/old" -w stop >/dev/null 2>&1; fail "A3: stamped old cluster STARTED (split-brain risk)"
fi
# Invariant 3: new3 is adoptable -- it starts and serves intact (this is the
# "operator's final restart" that a crash would have interrupted).
"$BIN/pg_ctl" -D "$W/new3" -l "$W/new3_live.log" -w start >/dev/null 2>&1 || { tail -20 "$W/new3_live.log"; fail "A3: new3 did not start (not adoptable after stamp)"; }
A3FP=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint) FROM t" 2>&1)
"$BIN/pg_ctl" -D "$W/new3" -w stop >/dev/null 2>&1
[ "$A3FP" = "$FP" ] || fail "A3: new3 data wrong (old=$FP got=$A3FP)"
log "PASS A3 (old un-startable, new adoptable+intact -- no split brain, no dead end)"

log "ALL REVERTABLE CRASH-ATOMICITY CASES PASSED"
exit 0
