#!/usr/bin/env bash
#
# --wal-log-commit must NOT stamp the old cluster superseded if the new cluster fails to
# come up live.  This is the C4 point-of-no-return ordering under a REAL failure:
# commit does (1) start new -> finalize, (2) verify live, and only THEN (3) stamp
# old.  If step 1/2 fails, old_dir must be left fully startable so the operator
# still has a good cluster.
#
# We force the new cluster's start to fail by occupying its port with a
# blocker process before running --wal-log-commit.
#
set -u
BIN="${PGBIN:?set PGBIN}"
W=${WORK:-/tmp/pgu_commit_fail}; PORT=${PORT:-56840}
export PGPORT=$PORT PGDATABASE=postgres
log(){ echo "=== $* ==="; }
fail(){ echo "FAIL: $*"; exit 1; }
rm -rf "$W"; mkdir -p "$W"

log "build old cluster + upgrade + hold-start (quarantined)"
"$BIN/initdb" -D "$W/old" -U postgres -N >/dev/null 2>&1 || fail "initdb"
echo "unix_socket_directories='$W'">>"$W/old/postgresql.conf"; echo "port=$PORT">>"$W/old/postgresql.conf"
"$BIN/pg_ctl" -D "$W/old" -l "$W/o.log" -w start >/dev/null 2>&1 || fail "start old"
"$BIN/psql" -h "$W" -U postgres -q >/dev/null 2>&1 -c \
    "CREATE TABLE t(id int primary key, v text); INSERT INTO t SELECT g,'v'||g FROM generate_series(1,3000) g;" || fail "load"
OLD_FP=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint) FROM t")
"$BIN/pg_ctl" -D "$W/old" -w stop >/dev/null 2>&1

( cd "$W" && "$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$W/old" -D "$W/new" -U postgres \
    --initdb --wal-log-upgrade --copy >"$W/up.log" 2>&1 ) || { tail -20 "$W/up.log"; fail "upgrade"; }
echo "unix_socket_directories='$W'">>"$W/new/postgresql.conf"; echo "port=$PORT">>"$W/new/postgresql.conf"
"$BIN/pg_ctl" -D "$W/new" -l "$W/hold.log" -w start >/dev/null 2>&1 || true
"$BIN/pg_controldata" -D "$W/new" | grep -qi quarantine || fail "new not quarantined after hold-start"

log "poison the new cluster's config so commit's start cannot succeed"
# An invalid GUC value makes the postmaster refuse to start (parse error at
# startup) -- a deterministic, portable way to force commit's finalize-start to
# fail after the cluster is already quarantined.  This drives the C4 ordering:
# commit must NOT stamp old_dir when the new cluster cannot come up.
echo "shared_buffers = 'not_a_valid_size'" >> "$W/new/postgresql.conf"

log "run --wal-log-commit (expected to FAIL: new cannot start)"
if "$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$W/old" -D "$W/new" --wal-log-commit >"$W/commit.log" 2>&1; then
    fail "commit SUCCEEDED despite the new cluster being unable to start"
fi
log "commit failed as expected:"; grep -iE "could not|fail|old cluster is untouched" "$W/commit.log" | head -3
# Un-poison so the old-cluster checks below are unaffected (new stays discardable).
sed -i.bak '/not_a_valid_size/d' "$W/new/postgresql.conf" 2>/dev/null || true

log "verify old cluster was NOT stamped superseded (must stay startable)"
[ -f "$W/old/global/pg_control.old" ] && fail "old cluster was stamped superseded despite commit failure"
[ -f "$W/old/global/pg_control" ] || fail "old cluster pg_control missing after failed commit"

log "verify the old cluster still starts and serves intact"
"$BIN/pg_ctl" -D "$W/old" -l "$W/old2.log" -w start >/dev/null 2>&1 || { tail -15 "$W/old2.log"; fail "old cluster did not start after failed commit"; }
AFTER_FP=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint) FROM t" 2>&1)
"$BIN/pg_ctl" -D "$W/old" -w stop >/dev/null 2>&1
[ "$OLD_FP" = "$AFTER_FP" ] || fail "old cluster data changed (old=$OLD_FP after=$AFTER_FP)"

log "PASS: a failed commit left the old cluster untouched, startable, and intact"
exit 0
