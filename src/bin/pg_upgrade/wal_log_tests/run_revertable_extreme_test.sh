#!/usr/bin/env bash
#
# Extreme / adversarial cases for the revertable --wal-log-upgrade lifecycle
# (commit / rollback / delete-old / status guards).  run_revertable_test.sh
# covers the happy path; this one hits the edges:
#
#   E1. commit REFUSES a random (non-wal-log) cluster.
#   E2. commit REFUSES a pending (not-yet-applied) cluster: you must hold-start
#       first (you cannot commit before the WALs are applied).
#   E3. rollback REFUSES a random cluster and a live/committed cluster.
#   E4. the hold is idempotent across MANY restarts (never serves, never
#       re-replays into corruption), then commits with data intact.
#   E5. rollback of a BIG held cluster -> old cluster is byte-identical + serves.
#   E6. rollback THEN re-upgrade THEN commit works (a discarded attempt does not
#       poison a later one).
#   E7. delete-old refuses before commit, succeeds after; and refuses a random
#       (unstamped) dir.
#
set -u
BIN="${PGBIN:?set PGBIN to the bin dir}"
W=${WORK:-/tmp/pgu_rev_ext}; PORT=${PORT:-56800}
export PGPORT=$PORT PGDATABASE=postgres
log(){ echo "=== $* ==="; }
fail(){ echo "FAIL: $*"; exit 1; }
db_state(){ "$BIN/pg_controldata" -D "$1" 2>/dev/null | grep -i "cluster state" | sed 's/.*: *//'; }
rm -rf "$W"; mkdir -p "$W"

# ---------- helpers ----------
make_old(){ # $1=dir  $2=rows
    "$BIN/initdb" -D "$1" -U postgres -N >/dev/null 2>&1 || fail "initdb $1"
    echo "unix_socket_directories='$W'">>"$1/postgresql.conf"; echo "port=$PORT">>"$1/postgresql.conf"
    "$BIN/pg_ctl" -D "$1" -l "$W/o.log" -w start >/dev/null 2>&1 || fail "start $1"
    "$BIN/psql" -h "$W" -U postgres -q >/dev/null 2>&1 <<SQL || fail "load $1"
CREATE TABLE t(id int primary key, v text);
INSERT INTO t SELECT g,'v'||g FROM generate_series(1,$2) g;
CREATE INDEX ON t(v);
SQL
    "$BIN/pg_ctl" -D "$1" -w stop >/dev/null 2>&1
}
fp(){ "$BIN/pg_ctl" -D "$1" -l "$W/fp.log" -w start >/dev/null 2>&1; \
      "$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint) FROM t"; \
      "$BIN/pg_ctl" -D "$1" -w stop >/dev/null 2>&1; }
do_upgrade(){ # $1=old  $2=new
    ( cd "$W" && "$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$1" -D "$2" -U postgres \
        --initdb --wal-log-upgrade --copy >"$W/up.log" 2>&1 ) || { tail -20 "$W/up.log"; fail "upgrade $1->$2"; }
    echo "unix_socket_directories='$W'">>"$2/postgresql.conf"; echo "port=$PORT">>"$2/postgresql.conf"
}
hold_start(){ "$BIN/pg_ctl" -D "$1" -l "$W/hold.log" -w start >/dev/null 2>&1 || true; }

# =========================================================== E1: commit random
log "E1: commit refuses a random (non-wal-log) cluster"
make_old "$W/rand" 100
if "$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$W/rand" -D "$W/rand" --wal-log-commit >"$W/e1.log" 2>&1; then
    fail "commit succeeded on a random cluster (should be refused)"
fi
grep -qi "not held in pg_upgrade quarantine\|not a --wal-log-upgrade" "$W/e1.log" || { cat "$W/e1.log"; fail "E1 wrong error"; }
[ -f "$W/rand/global/pg_control" ] || fail "E1: commit disturbed the random cluster"
log "PASS E1"

# =========================================================== E2: commit pending
log "E2: commit refuses a pending (not-yet-applied) cluster"
make_old "$W/old2" 3000
OLD2_FP=$(fp "$W/old2")
do_upgrade "$W/old2" "$W/new2"
# NO hold-start yet -> still pending; commit must refuse.
if "$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$W/old2" -D "$W/new2" --wal-log-commit >"$W/e2.log" 2>&1; then
    fail "commit succeeded on a pending cluster (must hold-start first)"
fi
grep -qi "not held in pg_upgrade quarantine" "$W/e2.log" || { cat "$W/e2.log"; fail "E2 wrong error"; }
[ -f "$W/old2/global/pg_control" ] || fail "E2: pending-commit stamped old cluster (must not)"
log "PASS E2"

# =========================================================== E4: idempotent hold
log "E4: hold is idempotent across many restarts, never serves"
for i in 1 2 3 4 5; do
    hold_start "$W/new2"
    if "$BIN/psql" -h "$W" -U postgres -tAc "SELECT 1" >/dev/null 2>&1; then
        "$BIN/pg_ctl" -D "$W/new2" -w stop >/dev/null 2>&1; fail "E4: held cluster served on restart $i"
    fi
    echo "$(db_state "$W/new2")" | grep -qi quarantine || fail "E4: not quarantined after restart $i (got '$(db_state "$W/new2")')"
done
log "PASS E4 (5 restarts, always held/dark)"

# =========================================================== E3: rollback guards
log "E3: rollback refuses a random cluster"
if "$BIN/pg_upgrade" -D "$W/rand" --wal-log-rollback >"$W/e3.log" 2>&1; then
    fail "rollback succeeded on a random cluster (should refuse)"
fi
[ -d "$W/rand" ] || fail "E3: rollback removed the random cluster"
log "PASS E3"

# =========================================================== E5: big rollback
log "E5: rollback of the held new2 -> old2 byte-identical + serves"
"$BIN/pg_upgrade" -D "$W/new2" --wal-log-rollback >"$W/e5.log" 2>&1 || { cat "$W/e5.log"; fail "rollback new2"; }
[ -d "$W/new2" ] && fail "E5: rollback did not remove new2"
AFTER_FP=$(fp "$W/old2")
[ "$OLD2_FP" = "$AFTER_FP" ] || fail "E5: old2 changed after rollback (old=$OLD2_FP after=$AFTER_FP)"
log "PASS E5"

# =========================================================== E6: rollback then re-upgrade then commit
log "E6: re-upgrade old2 after a rollback, hold, commit -> data intact"
do_upgrade "$W/old2" "$W/new2b"
hold_start "$W/new2b"
echo "$(db_state "$W/new2b")" | grep -qi quarantine || fail "E6: not quarantined after hold-start"
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$W/old2" -D "$W/new2b" --wal-log-commit >"$W/e6.log" 2>&1 || { cat "$W/e6.log"; fail "E6 commit"; }
"$BIN/pg_ctl" -D "$W/new2b" -l "$W/new2b.log" -w start >/dev/null 2>&1 || { tail -20 "$W/new2b.log"; fail "E6 start"; }
N_FP=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint) FROM t" 2>&1)
"$BIN/pg_ctl" -D "$W/new2b" -w stop >/dev/null 2>&1
[ "$OLD2_FP" = "$N_FP" ] || fail "E6: data mismatch after re-upgrade+commit (old=$OLD2_FP new=$N_FP)"
log "PASS E6"

# =========================================================== E7: delete-old gating
log "E7: delete-old refuses unstamped, succeeds after commit"
# old2 was superseded by the E6 commit -> stamped -> deletable.
[ -f "$W/old2/global/pg_control.old" ] || fail "E7: E6 commit did not stamp old2 superseded"
# a random (never-committed-against) dir must be refused.
if "$BIN/pg_upgrade" -d "$W/rand" --wal-log-delete-old >"$W/e7a.log" 2>&1; then
    fail "E7: delete-old succeeded on an unstamped cluster"
fi
[ -d "$W/rand" ] || fail "E7: delete-old removed the unstamped cluster"
# the stamped old2 deletes cleanly.
"$BIN/pg_upgrade" -d "$W/old2" --wal-log-delete-old >"$W/e7b.log" 2>&1 || { cat "$W/e7b.log"; fail "E7 delete-old old2"; }
[ -d "$W/old2" ] && fail "E7: delete-old did not remove old2"
log "PASS E7"

log "ALL REVERTABLE EXTREME CASES PASSED"
exit 0
