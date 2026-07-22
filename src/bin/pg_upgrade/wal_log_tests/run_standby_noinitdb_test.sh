#!/usr/bin/env bash
# Standby provisioning WITHOUT initdb.
#
# Proves the core-startup synthesis path: a --wal-upgrade streaming standby needs
# NO initdb, NO pg_basebackup, NO data copy.  The operator stages only:
#   - a config file (port, primary_conninfo)
#   - standby.signal
#   - the pg_upgrade_stream.signal sentinel
# ...into an otherwise BARE directory (no pg_control, no PG_VERSION, no base/).
# On start, the postmaster's checkControlFile() sees the sentinel + primary_conninfo,
# synthesizes a valid pg_control + PG_VERSION from the binary's constants, and the
# standby then streams the upgrade window from the primary and reconstructs itself.
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"; BIN="${PGBIN:-$ROOT/pginst/bin}"
W=${WORK:-/tmp/pgu_noinitdb}; OLD=$W/old; NEW=$W/new; SKEL=$W/skel
PP=${PPORT:-55710}; SP=${SPORT:-55711}
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

log "3. BARE skeleton -- NO initdb, NO pg_control, NO PG_VERSION"
mkdir -p "$SKEL"
chmod 700 "$SKEL"        # data dir must be 0700 (checkDataDir); provisioner's job
# Only the minimum an operator supplies: config + standby.signal + the sentinel.
cat > "$SKEL/postgresql.conf" <<CONF
port=$SP
unix_socket_directories='$W'
primary_conninfo='host=127.0.0.1 port=$PP user=postgres dbname=postgres'
CONF
printf 'host all all 127.0.0.1/32 trust\nlocal all all trust\n' > "$SKEL/pg_hba.conf"
touch "$SKEL/standby.signal"
touch "$SKEL/pg_upgrade_stream.signal"
# Prove the directory really is bare (no cluster scaffolding present).
[ -e "$SKEL/global/pg_control" ] && { echo "FAIL: skeleton unexpectedly has pg_control before start"; FAIL=1; }
[ -e "$SKEL/PG_VERSION" ]        && { echo "FAIL: skeleton unexpectedly has PG_VERSION before start"; FAIL=1; }
[ -d "$SKEL/base" ]              && { echo "FAIL: skeleton unexpectedly has base/ before start"; FAIL=1; }
log "  skeleton is bare: only postgresql.conf + standby.signal + pg_upgrade_stream.signal"

log "4. START bare skeleton: postmaster must SYNTHESIZE pg_control, then stream + reconstruct"
"$BIN/pg_ctl" -D "$SKEL" -l "$W/skel.log" -w -t 60 start >/dev/null 2>&1 \
    || { echo "FAIL: bare skeleton did not start"; tail -25 "$W/skel.log"; exit 1; }

grep -qi "synthesized a fresh pg_control" "$W/skel.log" \
    && log "  postmaster synthesized pg_control (no initdb)" \
    || { echo "FAIL: no synthesis log line"; tail -15 "$W/skel.log"; FAIL=1; }

# pg_control + PG_VERSION must now exist (created by synthesis).
[ -e "$SKEL/global/pg_control" ] || { echo "FAIL: pg_control not synthesized"; FAIL=1; }
[ -e "$SKEL/PG_VERSION" ]        || { echo "FAIL: PG_VERSION not synthesized"; FAIL=1; }

log "5. standby reconstructs the data by streaming (byte-identical to primary)"
# wait for the standby to reach a consistent, queryable state
ok=0
for _ in $(seq 1 30); do
  if "$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT 1" >/dev/null 2>&1; then ok=1; break; fi
  sleep 1
done
[ "$ok" = 1 ] || { echo "FAIL: standby never accepted read-only queries"; tail -25 "$W/skel.log"; FAIL=1; }
SGOT=$("$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT count(*),sum(hashtext(v)::bigint) FROM t" 2>&1 | head -1)
[ "$SGOT" = "$WANT" ] && log "  standby data matches primary ($SGOT)" \
                      || { echo "FAIL: standby data mismatch (want $WANT got $SGOT)"; FAIL=1; }
# confirm it is a standby (in recovery), not a promoted primary
INREC=$("$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT pg_is_in_recovery()" 2>&1 | head -1)
[ "$INREC" = "t" ] && log "  standby is in recovery (hot standby), good" \
                   || { echo "FAIL: expected pg_is_in_recovery()=t, got '$INREC'"; FAIL=1; }

"$BIN/pg_ctl" -D "$SKEL" -w stop >/dev/null 2>&1
"$BIN/pg_ctl" -D "$NEW"  -w stop >/dev/null 2>&1
cd /

echo "========================================================================"
[ "$FAIL" = 0 ] && log "PASS: standby provisioned with NO initdb -- pg_control synthesized, window streamed, data byte-identical" \
                || log "FAIL: see messages above"
exit $FAIL
