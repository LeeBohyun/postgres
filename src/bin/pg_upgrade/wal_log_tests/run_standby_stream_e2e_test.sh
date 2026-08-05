#!/usr/bin/env bash
# End-to-end standby upgrade by streaming the window (no WAL segment copied into
# the skeleton).  The fresh new-version skeleton streams the window from the live
# committed primary over a replication connection, using:
#
#   - the migrated physical slot (created on the old primary, carried over by
#     pg_upgrade) that pins the window on the primary so it survives the upgrade
#     and is streamable, and
#   - local anchor derivation: at first startup the skeleton derives CN LOCALLY
#     from its retained old datadir (reproducing pg_resetwal's byte-contiguous
#     placement) and TLI (always 1), taking only the system identifier from the
#     primary's standard IDENTIFY_SYSTEM, then arms its control file at CN.  No
#     operator prepare step, no manual WAL copy, and no bespoke replication command.
#
# A streamed standby continues as an ordinary hot standby following the primary.
#
# Assertions:
#   * the script copies no WAL segment into the skeleton (it only sets
#     primary_conninfo + standby.signal and starts the server), and
#   * the skeleton's log shows it streamed (walreceiver "started streaming" /
#     "auto-armed streaming standby from locally derived anchor"), came up as a hot standby
#     (pg_is_in_recovery=t), and serves data byte-identical to the primary.
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
BIN="${PGBIN:-$ROOT/pginst/bin}"
W=${WORK:-/tmp/pgu_stream_e2e}; OLD=$W/old NEW=$W/new SKEL=$W/skel
PP=${PPORT:-55948} SP=${SPORT:-55949}
# Transfer mode to exercise end-to-end (default --copy).  The standby's RELINK
# redo reproduces this mode's primitive: --copy=copy_file, --clone=reflink,
# --copy-file-range=copy_file_range, --link=hardlink.
XFER=${XFER:---copy}
export PGDATABASE=postgres
log(){ echo "=== $* ==="; }
FAIL=0
for p in $PP $SP; do lsof -ti :$p 2>/dev/null | xargs kill -9 2>/dev/null; done
rm -rf "$W"; mkdir -p "$W"

log "1. old primary with data"
"$BIN/initdb" -D "$OLD" -U postgres -N >/dev/null 2>&1 || { echo FAIL initdb; exit 1; }
cat >> "$OLD/postgresql.conf" <<CONF
port=$PP
unix_socket_directories='$W'
wal_level=replica
max_wal_senders=8
CONF
"$BIN/pg_ctl" -D "$OLD" -l "$W/old.log" -w start >/dev/null 2>&1 || { echo FAIL start; exit 1; }
# A physical replication slot on the old primary marks that a standby is
# expected; pg_upgrade migrates it and it (not a dedicated slot) pins the
# upgrade window so the fresh skeleton below can stream it.
SLOT_NAME=stby_slot
"$BIN/psql" -h "$W" -p $PP -U postgres -qtAc \
  "SELECT pg_create_physical_replication_slot('$SLOT_NAME', true)" >/dev/null 2>&1 \
  || { echo FAIL create-slot; exit 1; }
"$BIN/psql" -h "$W" -p $PP -U postgres -q >/dev/null 2>&1 <<SQL
CREATE TABLE t(id int primary key, v text);
INSERT INTO t SELECT g,'v'||g FROM generate_series(1,2000) g;
CREATE TABLE toast_t(id int, big text);
INSERT INTO toast_t SELECT g, repeat(md5(g::text),300) FROM generate_series(1,300) g;
-- Large objects live in pg_largeobject[_metadata], which pg_upgrade transfers
-- verbatim as user data.  Under --wal-upgrade those catalogs are excluded from
-- the window and instead named in the relink manifest, so the standby copies
-- them from the old datadir; seeding LOs exercises that path exactly (see
-- is_transferred_user_data_catalog).
SELECT lo_from_bytea(0, decode(repeat(md5(g::text), 50), 'hex'))
  FROM generate_series(1, 40) g;
SQL
# Fingerprint includes the large-object content (loid + byte length) so a
# missing/mis-delivered pg_largeobject on the standby is caught by convergence.
FP_Q="SELECT count(*),sum(hashtext(v)::bigint),(SELECT count(*) FROM toast_t),(SELECT count(*)||':'||coalesce(sum(length(data))::text,'0') FROM pg_largeobject) FROM t"
OLD_FP=$("$BIN/psql" -h "$W" -p $PP -U postgres -tAc "$FP_Q")
"$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1

# A real standby has its OWN retained pre-upgrade data directory (an independent
# basebackup), separate from the primary's -- the primary can't know or share its
# path.  Snapshot the old cluster HERE, before the primary upgrade, and let the
# standby relink from THIS copy.  This matters for --link/--swap: the standby must
# hardlink into its own retained datadir, never the primary's live files (which
# --swap moves into $NEW and --link would otherwise share).  Copy-family modes only
# read the source, so a shared dir would happen to work, but link/swap would not.
STBY_OLD=$W/stby_old
cp -a "$OLD" "$STBY_OLD"

log "2. upgrade the primary (--wal-upgrade), auto-serve -> live; slot retains the window"
cd "$W"
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-upgrade $XFER >"$W/up.log" 2>&1
[ $? -eq 0 ] || { echo FAIL upgrade; tail -20 "$W/up.log"; exit 1; }
cat >> "$NEW/postgresql.conf" <<CONF
port=$PP
unix_socket_directories='$W'
wal_level=replica
max_wal_senders=8
listen_addresses='localhost'
CONF
echo "host replication all 127.0.0.1/32 trust" >> "$NEW/pg_hba.conf"
echo "host all all 127.0.0.1/32 trust" >> "$NEW/pg_hba.conf"
# Auto-serve: the primary comes up read-write on first start (no commit step).
# The retention slot keeps the upgrade window streamable for the standby.
"$BIN/pg_ctl" -D "$NEW" -l "$W/new.log" -w start >/dev/null 2>&1 || { echo "FAIL new start"; tail -15 "$W/new.log"; exit 1; }
NEW_FP=$("$BIN/psql" -h "$W" -p $PP -U postgres -tAc "$FP_Q")
NEW_ID=$("$BIN/pg_controldata" -D "$NEW" | grep -i 'system identifier' | grep -oE '[0-9]+')
log "committed primary: fp=$NEW_FP sysid=$NEW_ID"
# FIRST prove the PRIMARY itself upgraded correctly (data preserved from the old
# cluster) -- otherwise "standby == primary" would only prove the standby faithfully
# replicated a broken primary.  OLD_FP and NEW_FP use the identical query.
[ "$NEW_FP" = "$OLD_FP" ] || { echo "FAIL: upgraded primary data ($NEW_FP) != old source data ($OLD_FP) -- primary upgrade is wrong"; FAIL=1; }
log "primary upgrade verified: data preserved from old cluster ($OLD_FP)"
# confirm the migrated physical slot pinning the window is present.  (CN itself
# is derived LOCALLY by the standby from its retained old datadir; only the
# system identifier comes from the primary, via IDENTIFY_SYSTEM.)  pg_upgrade no
# longer creates a dedicated pg_upgrade_window slot; the migrated standby slot
# both preserves identity and pins the window.
SLOT=$("$BIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT slot_name FROM pg_replication_slots WHERE slot_name='$SLOT_NAME'")
log "retention (migrated) slot='$SLOT'"
[ "$SLOT" = "$SLOT_NAME" ] || { echo "FAIL: migrated retention slot missing on committed primary"; FAIL=1; }

log "3. FRESH SKELETON + relink manifest: stream the window, copy user files from the old datadir"
# The upgrade window carries only the pg_upgrade-touched system files plus an
# XLOG_UPGRADE_RELINK manifest naming the user relations -- so it is schema-sized,
# not data-sized.  The standby is a FRESH new-version initdb skeleton; on redo of
# the manifest it places the user relations from its OWN retained old datadir
# ($STBY_OLD, the pre-upgrade snapshot) into the skeleton, reproducing the
# primary's transfer mode ($XFER): copy=full copy, clone=reflink,
# copy_file_range=copy_file_range, link/swap=hardlink.  The old datadir's path is
# supplied by the pg_upgrade_standby_old_datadir GUC in the skeleton's postgresql.conf.
"$BIN/initdb" -D "$SKEL" -U postgres -N >/dev/null 2>&1 || { echo "FAIL: skeleton initdb"; exit 1; }
# old-datadir path now comes from the pg_upgrade_standby_old_datadir GUC;
# pg_upgrade.signal is an empty presence-only sentinel.
echo "pg_upgrade_standby_old_datadir='$STBY_OLD'" >> "$SKEL/postgresql.conf"
: > "$SKEL/pg_upgrade.signal"
cat >> "$SKEL/postgresql.conf" <<CONF
port=$SP
unix_socket_directories='$W'
hot_standby=on
primary_conninfo='host=127.0.0.1 port=$PP user=postgres dbname=postgres'
CONF
printf 'host all all 127.0.0.1/32 trust\nlocal all all trust\n' >> "$SKEL/pg_hba.conf"
touch "$SKEL/standby.signal"

# No pre-staged anchor file: CN is derived locally from the retained old datadir.
[ -f "$SKEL/pg_upgrade_stream.anchor" ] && { echo "FAIL: unexpected pre-staged anchor file"; FAIL=1; }
[ -f "$SKEL/standby.signal" ]           || { echo "FAIL: no standby.signal written"; FAIL=1; }
[ -f "$SKEL/pg_upgrade.signal" ]   || { echo "FAIL: no pg_upgrade.signal staged"; FAIL=1; }

log "4. START the skeleton: it STREAMS the window from the live primary and becomes a hot standby"
# A streamed standby does NOT hold or commit: it streams the window from the
# already-committed primary, replays COMPLETE, and continues as an ordinary hot
# standby following the primary.  So start it normally and wait for it to serve
# read-only queries.
"$BIN/pg_ctl" -D "$SKEL" -l "$W/skel.log" -w -t 90 start >/dev/null 2>&1 || true
UP=0
for i in $(seq 1 60); do
  "$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT 1" >/dev/null 2>&1 && { UP=1; break; }
  sleep 1
done
# Decisive assertions:
grep -q "auto-armed streaming standby from locally derived anchor" "$W/skel.log" \
  && log "  skeleton armed from the streaming anchor (sysid+CN+TLI stamped)" \
  || { echo "  FAIL: skeleton did not arm from the streaming anchor"; tail -20 "$W/skel.log"; FAIL=1; }
if grep -qiE "started streaming|streaming WAL" "$W/skel.log"; then
  log "  skeleton STREAMED WAL from the primary (no manual copy):"
  grep -iE "started streaming|streaming WAL" "$W/skel.log" | head -2
else
  echo "  FAIL: no evidence the skeleton streamed WAL"; tail -25 "$W/skel.log"; FAIL=1
fi
[ "$UP" = 1 ] \
  && log "  skeleton is up and serving as a hot standby" \
  || { echo "  FAIL: skeleton did not come up as a hot standby"; tail -20 "$W/skel.log"; FAIL=1; }
INREC=$("$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT pg_is_in_recovery()" 2>&1)
[ "$INREC" = "t" ] \
  && log "  skeleton is following the primary (pg_is_in_recovery=t)" \
  || { echo "  FAIL: skeleton is not in recovery (state=$INREC)"; FAIL=1; }

log "5. verify the streamed standby serves the upgraded data (converged to the primary)"
STBY_FP=$("$BIN/psql" -h "$W" -p $SP -U postgres -tAc "$FP_Q" 2>&1)
STBY_ID=$("$BIN/pg_controldata" -D "$SKEL" | grep -i 'system identifier' | grep -oE '[0-9]+')
log "streamed standby: data=$STBY_FP sysid=$STBY_ID (want data=$NEW_FP sysid=$NEW_ID)"
[ "$STBY_FP" = "$NEW_FP" ] || { echo "FAIL: streamed standby data ($STBY_FP) != primary ($NEW_FP)"; FAIL=1; }
[ "$STBY_ID" = "$NEW_ID" ] || { echo "FAIL: sysid mismatch standby=$STBY_ID primary=$NEW_ID"; FAIL=1; }

"$BIN/pg_ctl" -D "$SKEL" -w stop >/dev/null 2>&1 || true
"$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1 || true
lsof -ti :$PP :$SP 2>/dev/null | xargs kill -9 2>/dev/null

echo "========================================================================"
[ "$FAIL" = 0 ] && log "PASS: standby STREAMED the upgrade window from the live primary (no manual copy), converged to the upgraded data" \
                || log "FAIL: see messages above"
exit $FAIL
