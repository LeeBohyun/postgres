#!/usr/bin/env bash
#
# A FAILED first start of the auto-serving new cluster must leave the OLD cluster
# fully usable.  Under the auto-serve model there is no --wal-log-commit step; the
# new cluster simply comes up on first start.  If that start fails, the operator
# must still have a good old cluster to fall back to (old_dir is frozen through
# the whole upgrade) and --wal-log-rollback must still restore it.
#
# We force the new cluster's start to fail with an invalid GUC value.
#
set -u
BIN="${PGBIN:?set PGBIN}"
W=${WORK:-/tmp/pgu_commit_fail}; PORT=${PORT:-56840}
export PGPORT=$PORT PGDATABASE=postgres
log(){ echo "=== $* ==="; }
fail(){ echo "FAIL: $*"; exit 1; }
rm -rf "$W"; mkdir -p "$W"

log "build old cluster + upgrade (auto-serve, no commit step)"
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

log "poison the new cluster's config so its auto-serve start cannot succeed"
# An invalid GUC value makes the postmaster refuse to start (parse error at
# startup) -- a deterministic, portable way to force the new cluster's first
# (auto-serve) start to fail.  The old cluster must be unaffected regardless.
echo "shared_buffers = 'not_a_valid_size'" >> "$W/new/postgresql.conf"

log "start the new cluster (expected to FAIL: invalid config)"
if "$BIN/pg_ctl" -D "$W/new" -l "$W/new.log" -w start >/dev/null 2>&1; then
    "$BIN/pg_ctl" -D "$W/new" -w stop >/dev/null 2>&1
    fail "new cluster started despite an invalid GUC"
fi
log "new cluster start failed as expected:"; grep -iE "invalid|not_a_valid_size|fatal" "$W/new.log" | head -2

log "verify the OLD cluster is untouched, startable, and intact"
[ -f "$W/old/global/pg_control" ] || fail "old cluster pg_control missing after failed new-cluster start"
"$BIN/pg_ctl" -D "$W/old" -l "$W/old2.log" -w start >/dev/null 2>&1 || { tail -15 "$W/old2.log"; fail "old cluster did not start after failed new-cluster start"; }
AFTER_FP=$("$BIN/psql" -h "$W" -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint) FROM t" 2>&1)
"$BIN/pg_ctl" -D "$W/old" -w stop >/dev/null 2>&1
[ "$OLD_FP" = "$AFTER_FP" ] || fail "old cluster data changed (old=$OLD_FP after=$AFTER_FP)"

log "rollback must still discard the (un-startable) new cluster; old stays intact"
# Un-poison is not needed: rollback just removes new_dir.  old_dir is intact.
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$W/old" -D "$W/new" --wal-log-rollback >"$W/rollback.log" 2>&1 \
    || { cat "$W/rollback.log"; fail "rollback after failed start"; }
[ -d "$W/new" ] && fail "rollback did not remove the new cluster"

log "PASS: a failed new-cluster start left the old cluster untouched, startable, and intact"
exit 0
