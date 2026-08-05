#!/usr/bin/env bash
#
# AUTO-ANCHOR standby: a fresh new-version skeleton streams the upgrade window
# from the live (auto-served) primary WITHOUT the operator staging any anchor
# file.  At first startup it DERIVES the CN anchor LOCALLY from its retained old
# data directory (reproducing pg_resetwal's byte-contiguous placement), takes only
# the system identifier from the primary's standard IDENTIFY_SYSTEM, arms its
# control file at CN, and streams -- becoming a hot standby that serves the
# upgraded data.  User relations are copied from the retained old datadir (named
# by the pg_upgrade_standby_old_datadir GUC) by the relink-manifest redo.
#
# This focuses on the auto-arm at first startup (no operator prep);
# run_standby_stream_e2e_test.sh covers the same skeleton+relink path with fuller
# data/assertions.
#
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
BIN="${PGBIN:-$ROOT/tmp_install/bin}"
export DYLD_LIBRARY_PATH="${DYLD_LIBRARY_PATH:-}:$ROOT/tmp_install/lib"
W=${WORK:-/tmp/pgu_autoanchor}; OLD=$W/old NEW=$W/new SKEL=$W/skel
PP=${PORT:-55960}; SP=$((PP+1))
export PGDATABASE=postgres
log(){ echo "=== $* ==="; }
FAIL=0
rm -rf "$W"; mkdir -p "$W"

log "1. old primary with data"
"$BIN/initdb" -D "$OLD" -U postgres -N >/dev/null 2>&1 || { echo FAIL initdb-old; exit 1; }
cat >> "$OLD/postgresql.conf" <<CONF
port=$PP
unix_socket_directories='$W'
wal_level=replica
max_wal_senders=8
listen_addresses='localhost'
CONF
printf 'host replication all 127.0.0.1/32 trust\nhost all all 127.0.0.1/32 trust\n' >> "$OLD/pg_hba.conf"
"$BIN/pg_ctl" -D "$OLD" -l "$W/old.log" -w start >/dev/null 2>&1 || { echo FAIL start-old; exit 1; }
"$BIN/psql" -h "$W" -p $PP -U postgres -qc \
    "CREATE TABLE t(v text); INSERT INTO t SELECT 'r'||g FROM generate_series(1,2000) g; CREATE INDEX ON t(v);" >/dev/null 2>&1 || { echo FAIL load; exit 1; }
WANT=$("$BIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint) FROM t")
# A physical slot marks that a standby is expected; pg_upgrade migrates it and it
# pins the upgrade window so the skeleton below can stream it (no dedicated slot
# is created without one).
"$BIN/psql" -h "$W" -p $PP -U postgres -qtAc \
    "SELECT pg_create_physical_replication_slot('stby_slot', true)" >/dev/null 2>&1 || { echo FAIL create-slot; exit 1; }
"$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1

log "2. upgrade primary (--wal-upgrade) and auto-serve"
cd "$W"
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-upgrade --copy >"$W/up.log" 2>&1 \
    || { echo FAIL upgrade; tail -20 "$W/up.log"; exit 1; }
cat >> "$NEW/postgresql.conf" <<CONF
port=$PP
unix_socket_directories='$W'
wal_level=replica
max_wal_senders=8
listen_addresses='localhost'
CONF
printf 'host replication all 127.0.0.1/32 trust\nhost all all 127.0.0.1/32 trust\n' >> "$NEW/pg_hba.conf"
"$BIN/pg_ctl" -D "$NEW" -l "$W/new.log" -w start >/dev/null 2>&1 || { echo FAIL primary-autoserve; tail "$W/new.log"; exit 1; }
GOT=$("$BIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint) FROM t")
[ "$GOT" = "$WANT" ] || { echo "FAIL: primary data mismatch (want $WANT got $GOT)"; FAIL=1; }

log "3. FRESH SKELETON: stream the window; the relink manifest copies user files from the old datadir"
# The window carries only pg_upgrade-touched system files plus the relink
# manifest, not user data.  The standby is a fresh new-version initdb skeleton;
# on redo it copies the user relations from its retained old datadir (named by
# the pg_upgrade_standby_old_datadir GUC) into the skeleton.  Here $OLD (left intact by
# --copy) is that retained old datadir.
"$BIN/initdb" -D "$SKEL" -U postgres -N >/dev/null 2>&1 || { echo "FAIL: skeleton initdb"; exit 1; }
# old-datadir path now comes from the pg_upgrade_standby_old_datadir GUC;
# pg_upgrade.signal is an empty presence-only sentinel.
echo "pg_upgrade_standby_old_datadir='$OLD'" >> "$SKEL/postgresql.conf"
: > "$SKEL/pg_upgrade.signal"
cat >> "$SKEL/postgresql.conf" <<CONF
port=$SP
unix_socket_directories='$W'
primary_conninfo='host=127.0.0.1 port=$PP user=postgres dbname=postgres'
CONF
printf 'host all all 127.0.0.1/32 trust\nlocal all all trust\n' >> "$SKEL/pg_hba.conf"
touch "$SKEL/standby.signal"
[ -f "$SKEL/pg_upgrade_stream.anchor" ] && { echo "FAIL: unexpected pre-staged anchor file"; FAIL=1; }

log "4. START skeleton: must AUTO-ARM (derive CN locally) + stream (no operator prep)"
"$BIN/pg_ctl" -D "$SKEL" -l "$W/skel.log" -w -t 60 start >/dev/null 2>&1 \
    || { echo "FAIL: skeleton did not start"; tail -20 "$W/skel.log"; exit 1; }

grep -qi "auto-armed streaming standby from locally derived anchor" "$W/skel.log" \
    && log "  skeleton auto-armed (derived CN locally, sysid from IDENTIFY_SYSTEM)" \
    || { echo "FAIL: no auto-arm evidence (did it use the anchor file or fail?)"; tail -20 "$W/skel.log"; FAIL=1; }
grep -qiE "started streaming|streaming WAL" "$W/skel.log" \
    && log "  skeleton STREAMED the window (no manual WAL copy, no prepare step)" \
    || { echo "FAIL: no streaming evidence"; tail -20 "$W/skel.log"; FAIL=1; }

log "5. skeleton is a hot standby serving the upgraded data (converged to primary)"
# give replay a moment to converge
for i in $(seq 1 20); do
    R=$("$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint) FROM t" 2>/dev/null)
    [ "$R" = "$WANT" ] && break
    sleep 0.5
done
INREC=$("$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT pg_is_in_recovery()" 2>/dev/null)
[ "$INREC" = "t" ] || { echo "FAIL: skeleton not in recovery (state=$INREC)"; FAIL=1; }
[ "$R" = "$WANT" ] || { echo "FAIL: standby data (want $WANT got $R)"; FAIL=1; }

"$BIN/pg_ctl" -D "$SKEL" -w stop >/dev/null 2>&1
"$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1

[ "$FAIL" = 0 ] && log "PASS: standby AUTO-ARMED (locally derived CN) and streamed (fresh skeleton + relink manifest)" \
                || log "FAIL: auto-anchor standby not upheld"
exit $FAIL
