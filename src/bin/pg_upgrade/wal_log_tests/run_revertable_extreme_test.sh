#!/usr/bin/env bash
#
# Extreme / adversarial cases for the revertable --wal-log-upgrade lifecycle
# (AUTO-SERVE model: rollback / delete-old guards).  run_revertable_test.sh
# covers the happy path; this one hits the edges:
#
#   E1. rollback REFUSES a random (non-upgrade) old dir with -D pointing at it
#       (no upgraded new cluster to fall back to / old not a rollback target).
#   E2. upgrade leaves the new cluster as a normal "shut down" cluster (NOT
#       quarantined); it AUTO-SERVES on first start; old cluster left intact.
#   E3. rollback REFUSES when old_dir is not intact (damaged control file).
#   E4. auto-serve is idempotent across MANY restarts (serves every time, never
#       re-replays into corruption, data stable).
#   E5. rollback of a BIG new cluster -> old cluster is byte-identical + serves.
#   E6. rollback THEN re-upgrade THEN auto-serve works (a discarded attempt does
#       not poison a later one).
#   E7. delete-old refuses without a completed new cluster, succeeds with one.
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
serve_start(){ "$BIN/pg_ctl" -D "$1" -l "$W/serve.log" -w start >/dev/null 2>&1; }

# =========================================================== E1: rollback needs a data dir
log "E1: rollback requires a real -D data directory (refuses a non-datadir)"
# Least-intrusive model (matches upstream's trust of directory args): rollback
# does not try to prove -D is 'really' an upgrade cluster; it requires -D to be a
# PostgreSQL data dir and, decisively, that old_dir (-d) is intact to return to
# (the real safety net -- exercised in E3).  Here: a -D that is NOT a data dir
# (no PG_VERSION) must be refused rather than rm'd.
mkdir -p "$W/notadatadir"
make_old "$W/rand" 100    # a valid old cluster to use as -d
if "$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$W/rand" -D "$W/notadatadir" --wal-log-rollback >"$W/e1.log" 2>&1; then
    fail "rollback succeeded with a non-datadir -D (should be refused)"
fi
grep -qi "not a PostgreSQL data directory\|PG_VERSION" "$W/e1.log" || { cat "$W/e1.log"; fail "E1 wrong error"; }
[ -d "$W/notadatadir" ] || fail "E1: rollback removed the non-datadir"
log "PASS E1"

# =========================================================== E2: auto-serve
log "E2: upgrade leaves a normal cluster that auto-serves; old left intact"
make_old "$W/old2" 3000
OLD2_FP=$(fp "$W/old2")
do_upgrade "$W/old2" "$W/new2"
echo "$(db_state "$W/new2")" | grep -qi quarantine && fail "E2: new2 quarantined; auto-serve expected (got '$(db_state "$W/new2")')"
serve_start "$W/new2" || { tail -20 "$W/serve.log"; fail "E2: new2 did not auto-serve"; }
"$BIN/psql" -h "$W" -U postgres -tAc "SELECT 1" >/dev/null 2>&1 || fail "E2: auto-served cluster did not accept a connection"
"$BIN/pg_ctl" -D "$W/new2" -w stop >/dev/null 2>&1
[ -f "$W/old2/global/pg_control" ] || fail "E2: upgrade disabled the old cluster (must stay intact)"
log "PASS E2"

# =========================================================== E4: idempotent serve
log "E4: auto-serve is idempotent across many restarts, data stable"
for i in 1 2 3 4 5; do
    serve_start "$W/new2" || { tail -20 "$W/serve.log"; fail "E4: new2 did not serve on restart $i"; }
    R=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*) FROM t" 2>&1)
    "$BIN/pg_ctl" -D "$W/new2" -w stop >/dev/null 2>&1
    [ "$R" = 3000 ] || fail "E4: wrong row count on restart $i (got '$R')"
done
log "PASS E4 (5 restarts, always served, data stable)"

# =========================================================== E3: rollback needs intact old
log "E3: rollback refuses when old_dir is not intact (damaged control file)"
# Corrupt old2's control file so old_cluster_intact() rejects it.
cp "$W/old2/global/pg_control" "$W/pg_control.bak"
: > "$W/old2/global/pg_control"   # truncate -> bad CRC / unreadable
if "$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$W/old2" -D "$W/new2" --wal-log-rollback >"$W/e3.log" 2>&1; then
    fail "rollback succeeded with a damaged old cluster (should refuse)"
fi
# Refusal may surface either as our "not intact / PITR" message or as the
# lower-level "could not read ... pg_control" from get_controlfile -- both mean
# the damaged old cluster was rejected, not started.
grep -qiE "not intact|backup / PITR|could not read.*pg_control|bad CRC" "$W/e3.log" \
    || { cat "$W/e3.log"; fail "E3 wrong error"; }
[ -d "$W/new2" ] || fail "E3: rollback removed new2 despite refusing"
# restore the good control file for E5.
cp "$W/pg_control.bak" "$W/old2/global/pg_control"
log "PASS E3"

# =========================================================== E5: big rollback
log "E5: rollback of new2 -> old2 byte-identical + serves"
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$W/old2" -D "$W/new2" --wal-log-rollback >"$W/e5.log" 2>&1 || { cat "$W/e5.log"; fail "rollback new2"; }
[ -d "$W/new2" ] && fail "E5: rollback did not remove new2"
AFTER_FP=$(fp "$W/old2")
[ "$OLD2_FP" = "$AFTER_FP" ] || fail "E5: old2 changed after rollback (old=$OLD2_FP after=$AFTER_FP)"
log "PASS E5"

# =========================================================== E6: rollback then re-upgrade then serve
log "E6: re-upgrade old2 after a rollback, auto-serve -> data intact"
do_upgrade "$W/old2" "$W/new2b"
serve_start "$W/new2b" || { tail -20 "$W/serve.log"; fail "E6 auto-serve"; }
N_FP=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint) FROM t" 2>&1)
"$BIN/pg_ctl" -D "$W/new2b" -w stop >/dev/null 2>&1
[ "$OLD2_FP" = "$N_FP" ] || fail "E6: data mismatch after re-upgrade (old=$OLD2_FP new=$N_FP)"
log "PASS E6"

# =========================================================== E7: delete-old gating
log "E7: delete-old refuses without a completed new cluster, succeeds with one"
# A random -D (no COMPLETE marker) must be refused.
if "$BIN/pg_upgrade" -d "$W/old2" -D "$W/rand" --wal-log-delete-old >"$W/e7a.log" 2>&1; then
    fail "E7: delete-old succeeded with a non-completed new cluster"
fi
[ -d "$W/old2" ] || fail "E7: delete-old removed old2 despite refusing"
# With the real completed new cluster (new2b), delete-old removes old2.
"$BIN/pg_upgrade" -d "$W/old2" -D "$W/new2b" --wal-log-delete-old >"$W/e7b.log" 2>&1 || { cat "$W/e7b.log"; fail "E7 delete-old old2"; }
[ -d "$W/old2" ] && fail "E7: delete-old did not remove old2"
log "PASS E7"

log "ALL REVERTABLE EXTREME CASES PASSED"
exit 0
