#!/usr/bin/env bash
# CROSS-VERSION streaming-standby STRESS matrix (Arca).
#
# Combines the three axes you care about in one run:
#   - VERSION permutations: each available old major (v14..v18) -> 20devel
#   - DATA shapes: manyrel (many relations/dbs), bigcat (large pg_attribute),
#     bigdata (multi-segment relfiles)
#   - DELIVERY: the standby STREAMS the upgrade window (no cp) via auto-anchor over the replication connection
#
# For each (old-major x shape): upgrade old->20devel --wal-upgrade on the
# primary, commit -> live; a fresh 20devel skeleton streams the window and becomes
# a hot standby.  Assert the primary preserved the data (catalog version changed)
# and the streamed standby converged.
#
# Env: PGBIN (20devel bin, required); OLDBIN_DIRS (default Arca v14..v18 layout);
#      SHAPES (default "manyrel bigcat bigdata"); NDBS/NTAB, ROWS as in the
#      same-version stress test.
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
NEWBIN="${PGBIN:?set PGBIN to the 20devel bin dir}"
BASEW=${WORK:-/tmp/pgu_xstream}
PP=${PPORT:-55970} SP=${SPORT:-55971}
export PGDATABASE=postgres
SHAPES=${SHAPES:-"manyrel bigcat bigdata"}
ROWS=${ROWS:-200000}
NDBS=${NDBS:-3} NTAB=${NTAB:-40}
log(){ echo "=== $* ==="; }
GRC=0

if [ -z "${OLDBIN_DIRS:-}" ]; then
  OLDBIN_DIRS=""
  for v in 14 15 16 17 18; do
    d="$HOME/hadron/pg_install/v$v/bin"
    [ -x "$d/pg_ctl" ] && OLDBIN_DIRS="$OLDBIN_DIRS $d"
  done
fi
[ -n "$OLDBIN_DIRS" ] || { echo "FAIL: no OLDBIN_DIRS available"; exit 1; }

build_shape() {
  local W=$1 shape=$2 BIN=$3
  case "$shape" in
    manyrel)
      for d in $(seq 1 $NDBS); do
        "$BIN/psql" -h "$W" -p $PP -U postgres -qc "CREATE DATABASE db$d" >/dev/null 2>&1
        "$BIN/psql" -h "$W" -p $PP -U postgres -d db$d -q >/dev/null 2>&1 <<SQL
DO \$\$ BEGIN
  FOR i IN 1..$NTAB LOOP
    EXECUTE format('CREATE TABLE t%s(id int primary key, v text, big text)', i);
    EXECUTE format('INSERT INTO t%s SELECT g, ''v''||g, repeat(md5(g::text),20) FROM generate_series(1,300) g', i);
  END LOOP;
END \$\$;
SQL
      done ;;
    bigcat)
      "$BIN/psql" -h "$W" -p $PP -U postgres -q >/dev/null 2>&1 <<SQL
DO \$\$
DECLARE cols text;
BEGIN
  SELECT string_agg(format('c%s int', g), ', ') INTO cols FROM generate_series(1,1000) g;
  FOR i IN 1..40 LOOP EXECUTE format('CREATE TABLE wide%s (%s)', i, cols); END LOOP;
END \$\$;
CREATE TABLE t(id int primary key, v text);
INSERT INTO t SELECT g,'v'||g FROM generate_series(1,2000) g;
SQL
      ;;
    bigdata)
      "$BIN/psql" -h "$W" -p $PP -U postgres -qc \
        "CREATE TABLE t(id int primary key, v text); INSERT INTO t SELECT g, repeat('x',80)||g FROM generate_series(1,$ROWS) g;" >/dev/null 2>&1 ;;
    *) echo "unknown shape $shape"; return 1;;
  esac
}

fingerprint() {
  local W=$1 port=$2 d one fp=""
  for d in $("$NEWBIN/psql" -h "$W" -p $port -U postgres -tAc \
      "SELECT datname FROM pg_database WHERE datname NOT IN ('template0','template1') ORDER BY 1" 2>/dev/null); do
    one=$("$NEWBIN/psql" -h "$W" -p $port -U postgres -d "$d" -tAc \
      "SELECT coalesce(sum(c.reltuples::bigint),0) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind='r'" 2>/dev/null)
    fp="$fp|$d:$one"
  done
  echo "$fp"
}

for OLDBIN in $OLDBIN_DIRS; do
  OLDVER=$("$OLDBIN/pg_controldata" --version 2>/dev/null | grep -oE '[0-9]+devel|[0-9]+\.[0-9]+')
  for shape in $SHAPES; do
    log ">>> $OLDVER -> 20devel  shape=$shape  (STREAM, no cp)"
    W=$BASEW/${OLDVER}_$shape; OLD=$W/old NEW=$W/new SKEL=$W/skel
    for p in $PP $SP; do lsof -ti :$p 2>/dev/null | xargs kill -9 2>/dev/null; done
    rm -rf "$W"; mkdir -p "$W"

    "$OLDBIN/initdb" -D "$OLD" -U postgres -N >/dev/null 2>&1 || { echo "FAIL: initdb $OLDVER/$shape"; GRC=1; continue; }
    { echo "unix_socket_directories='$W'"; echo "port=$PP"; } >> "$OLD/postgresql.conf"
    "$OLDBIN/pg_ctl" -D "$OLD" -l "$W/old.log" -w start >/dev/null 2>&1 || { echo "FAIL: start $OLDVER/$shape"; GRC=1; continue; }
    build_shape "$W" "$shape" "$OLDBIN"
    "$OLDBIN/psql" -h "$W" -p $PP -U postgres -qc "ANALYZE" >/dev/null 2>&1
    for d in $("$OLDBIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT datname FROM pg_database WHERE datname LIKE 'db%'" 2>/dev/null); do
      "$OLDBIN/psql" -h "$W" -p $PP -U postgres -d "$d" -qc "ANALYZE" >/dev/null 2>&1
    done
    OLD_CATVER=$("$OLDBIN/pg_controldata" -D "$OLD" | grep 'Catalog version' | grep -oE '[0-9]+')
    OLD_FP=$(fingerprint "$W" $PP)
    log "  old $OLDVER catver=$OLD_CATVER fp=$OLD_FP"
    "$OLDBIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1

    cd "$W"
    "$NEWBIN/pg_upgrade" -b "$OLDBIN" -B "$NEWBIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-upgrade --copy >"$W/up.log" 2>&1
    if [ $? -ne 0 ]; then echo "FAIL: $OLDVER/$shape upgrade"; tail -12 "$W/up.log"; GRC=1; cd /; continue; fi
    cat >> "$NEW/postgresql.conf" <<CONF
port=$PP
unix_socket_directories='$W'
wal_level=replica
max_wal_senders=8
listen_addresses='localhost'
CONF
    echo "host replication all 127.0.0.1/32 trust" >> "$NEW/pg_hba.conf"
    echo "host all all 127.0.0.1/32 trust" >> "$NEW/pg_hba.conf"
    # Auto-serve: the upgraded primary comes up read-write on first start (no
    # commit step).  It stays live so the standby can stream the window below.
    "$NEWBIN/pg_ctl" -D "$NEW" -l "$W/new.log" -w start >/dev/null 2>&1 || { echo "FAIL: $OLDVER/$shape new start"; tail "$W/new.log"; GRC=1; cd /; continue; }
    NEW_CATVER=$("$NEWBIN/pg_controldata" -D "$NEW" | grep 'Catalog version' | grep -oE '[0-9]+')
    for d in $("$NEWBIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT datname FROM pg_database WHERE datname NOT IN ('template0','template1')" 2>/dev/null); do
      "$NEWBIN/psql" -h "$W" -p $PP -U postgres -d "$d" -qc "ANALYZE" >/dev/null 2>&1
    done
    NEW_FP=$(fingerprint "$W" $PP)
    NEW_ID=$("$NEWBIN/pg_controldata" -D "$NEW" | grep -i 'system identifier' | grep -oE '[0-9]+')
    [ "$OLD_CATVER" != "$NEW_CATVER" ] || { echo "FAIL: $OLDVER/$shape catalog version unchanged"; GRC=1; }
    [ "$NEW_FP" = "$OLD_FP" ] && log "  primary $OLDVER->20devel verified (catver $OLD_CATVER->$NEW_CATVER); data preserved" \
      || { echo "FAIL: $OLDVER/$shape primary data ($NEW_FP) != old ($OLD_FP)"; GRC=1; }

    # BARE skeleton STREAMS the window (NO initdb, no cp): the postmaster
    # synthesizes pg_control + PG_VERSION from the sentinel on start.
    rm -rf "$SKEL"; mkdir -p "$SKEL"; chmod 700 "$SKEL"
    cat > "$SKEL/postgresql.conf" <<CONF
port=$SP
unix_socket_directories='$W'
hot_standby=on
primary_conninfo='host=127.0.0.1 port=$PP user=postgres dbname=postgres'
CONF
    printf 'host all all 127.0.0.1/32 trust\nlocal all all trust\n' > "$SKEL/pg_hba.conf"
    touch "$SKEL/standby.signal"
    touch "$SKEL/pg_upgrade_stream.signal"
    "$NEWBIN/pg_ctl" -D "$SKEL" -l "$W/skel.log" -w -t 120 start >/dev/null 2>&1 || true
    UP=0
    for i in $(seq 1 90); do "$NEWBIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT 1" >/dev/null 2>&1 && { UP=1; break; }; sleep 1; done
    grep -qiE "started streaming|streaming WAL" "$W/skel.log" || { echo "FAIL: $OLDVER/$shape no streaming"; tail -12 "$W/skel.log"; GRC=1; }
    [ "$UP" = 1 ] || { echo "FAIL: $OLDVER/$shape standby did not come up"; tail -12 "$W/skel.log"; GRC=1; cd /; continue; }
    PRI_LSN=$("$NEWBIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT pg_current_wal_lsn()")
    for i in $(seq 1 60); do
      RP=$("$NEWBIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT pg_last_wal_replay_lsn()" 2>/dev/null)
      { [ "$RP" \> "$PRI_LSN" ] || [ "$RP" = "$PRI_LSN" ]; } && break; sleep 1
    done
    for d in $("$NEWBIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT datname FROM pg_database WHERE datname NOT IN ('template0','template1')" 2>/dev/null); do
      "$NEWBIN/psql" -h "$W" -p $SP -U postgres -d "$d" -qc "ANALYZE" >/dev/null 2>&1
    done
    STBY_FP=$(fingerprint "$W" $SP)
    INREC=$("$NEWBIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT pg_is_in_recovery()" 2>&1)
    STBY_ID=$("$NEWBIN/pg_controldata" -D "$SKEL" | grep -i 'system identifier' | grep -oE '[0-9]+')
    STBY_CATVER=$("$NEWBIN/pg_controldata" -D "$SKEL" | grep 'Catalog version' | grep -oE '[0-9]+')
    log "  streamed standby $OLDVER->20devel: catver=$STBY_CATVER fp=$STBY_FP in_recovery=$INREC"
    [ "$STBY_FP" = "$NEW_FP" ]     || { echo "FAIL: $OLDVER/$shape standby fp ($STBY_FP) != primary ($NEW_FP)"; GRC=1; }
    [ "$STBY_CATVER" = "$NEW_CATVER" ] || { echo "FAIL: $OLDVER/$shape standby catver $STBY_CATVER != $NEW_CATVER"; GRC=1; }
    [ "$INREC" = "t" ]             || { echo "FAIL: $OLDVER/$shape standby not hot standby"; GRC=1; }
    [ "$STBY_ID" = "$NEW_ID" ]     || { echo "FAIL: $OLDVER/$shape sysid mismatch"; GRC=1; }

    "$NEWBIN/pg_ctl" -D "$SKEL" -w stop >/dev/null 2>&1 || true
    "$NEWBIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1 || true
    for p in $PP $SP; do lsof -ti :$p 2>/dev/null | xargs kill -9 2>/dev/null; done
    cd /
  done
done

echo "========================================================================"
[ "$GRC" = 0 ] && log "PASS: cross-version streaming stress -- every old-major x shape upgraded and STREAMED (no cp) to a converged standby" \
              || log "FAIL: see messages above"
exit $GRC
