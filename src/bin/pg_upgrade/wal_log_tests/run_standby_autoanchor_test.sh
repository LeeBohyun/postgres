#!/usr/bin/env bash
#
# AUTO-ANCHOR standby: a fresh vN+1 skeleton streams the upgrade window from the
# live (auto-served) primary WITHOUT the operator running
# "pg_upgrade --wal-prepare-standby" and WITHOUT any pg_upgrade_stream.anchor
# file.  The skeleton, at first startup, auto-fetches the CN anchor from the
# primary over the replication connection via the PG_UPGRADE_WINDOW_ANCHOR
# command (chicken-and-egg bootstrap solved on the wire), arms its control file at
# CN, and streams -- becoming a hot standby that serves the upgraded data.
#
# This is the automatic counterpart of run_standby_stream_e2e_test.sh (which uses
# the explicit --wal-prepare-standby staging).
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

log "3. BARE SKELETON -- NO initdb; only config + standby.signal + pg_upgrade_stream.signal"
# No initdb, no pg_control, no data.  The postmaster synthesizes pg_control +
# PG_VERSION from the sentinel on start, then streams the window from the primary.
mkdir -p "$SKEL"; chmod 700 "$SKEL"
cat > "$SKEL/postgresql.conf" <<CONF
port=$SP
unix_socket_directories='$W'
primary_conninfo='host=127.0.0.1 port=$PP user=postgres dbname=postgres'
CONF
printf 'host all all 127.0.0.1/32 trust\nlocal all all trust\n' > "$SKEL/pg_hba.conf"
touch "$SKEL/standby.signal"
touch "$SKEL/pg_upgrade_stream.signal"
[ -e "$SKEL/global/pg_control" ] && { echo "FAIL: skeleton has pg_control before start (not bare)"; FAIL=1; }
[ -f "$SKEL/pg_upgrade_stream.anchor" ] && { echo "FAIL: unexpected pre-staged anchor file"; FAIL=1; }

log "4. START skeleton: must AUTO-FETCH the anchor + stream (no operator prep)"
"$BIN/pg_ctl" -D "$SKEL" -l "$W/skel.log" -w -t 60 start >/dev/null 2>&1 \
    || { echo "FAIL: skeleton did not start"; tail -20 "$W/skel.log"; exit 1; }

grep -qi "auto-armed streaming standby from primary" "$W/skel.log" \
    && log "  skeleton auto-armed from the primary (fetched CN over replication)" \
    || { echo "FAIL: no auto-arm evidence (did it use the anchor file or fail?)"; tail -20 "$W/skel.log"; FAIL=1; }
grep -qiE "started streaming|streaming WAL" "$W/skel.log" \
    && log "  skeleton STREAMED the window (no cp, no prepare-standby)" \
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

[ "$FAIL" = 0 ] && log "PASS: standby AUTO-FETCHED the anchor over replication and streamed (no --wal-prepare-standby)" \
                || log "FAIL: auto-anchor standby not upheld"
exit $FAIL
