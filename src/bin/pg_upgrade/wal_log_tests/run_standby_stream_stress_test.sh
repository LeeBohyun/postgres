#!/usr/bin/env bash
# Streaming-standby STRESS matrix for --wal-upgrade (no cp).
#
# run_standby_stream_e2e_test proves the streaming path on a small dataset.  This
# runs the SAME path (upgrade primary -> commit -> live; fresh skeleton STREAMS
# the window via --wal-prepare-standby; becomes a hot standby) across several harder
# data SHAPES, to shake out chunking / many-relation / big-catalog issues in the
# streamed window:
#
#   SHAPE manyrel   : many relations across several databases (many RELFILE images)
#   SHAPE bigcat    : a bloated catalog (thousands of columns -> large pg_attribute)
#   SHAPE bigdata   : a larger user table (multi-segment relfiles; size via ROWS)
#   SHAPE toastheavy: heavy TOAST (large out-of-line values)
#
# Each shape asserts: the upgraded PRIMARY preserves the old data (NEW_FP==OLD_FP),
# and the streamed STANDBY converges to it byte-identical (hot standby).  Same
# version on both ends (the mechanism is version-independent; cross-major is
# covered by run_standby_xversion_test on Arca).
#
# Tunables (env): SHAPES (default "manyrel bigcat bigdata toastheavy"),
#   ROWS (bigdata rows, default 300000), NDBS/NTAB (manyrel, default 3/60),
#   NCOLS (bigcat columns per table * tables, default 1600*40).
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
BIN="${PGBIN:-$ROOT/pginst/bin}"
BASEW=${WORK:-/tmp/pgu_stream_stress}
PP=${PPORT:-55960} SP=${SPORT:-55961}
export PGDATABASE=postgres
SHAPES=${SHAPES:-"manyrel bigcat bigdata toastheavy"}
ROWS=${ROWS:-300000}
NDBS=${NDBS:-3} NTAB=${NTAB:-60}
log(){ echo "=== $* ==="; }
GRC=0

# Build shape-specific data into a running old cluster on socket dir $1, port $2.
build_shape() {
  local W=$1 shape=$2
  local q="$BIN/psql -h $W -p $PP -U postgres -q"
  case "$shape" in
    manyrel)
      for d in $(seq 1 $NDBS); do
        $q -qc "CREATE DATABASE db$d" >/dev/null 2>&1
        "$BIN/psql" -h "$W" -p $PP -U postgres -d db$d -q >/dev/null 2>&1 <<SQL
DO \$\$ BEGIN
  FOR i IN 1..$NTAB LOOP
    EXECUTE format('CREATE TABLE t%s(id int primary key, v text, arr int[], big text)', i);
    EXECUTE format('INSERT INTO t%s SELECT g, ''v''||g, ARRAY[g,g%%10], repeat(md5(g::text),20) FROM generate_series(1,300) g', i);
    EXECUTE format('CREATE INDEX ON t%s(v)', i);
  END LOOP;
END \$\$;
SQL
      done
      ;;
    bigcat)
      # Many wide tables -> a large pg_attribute (big system catalog).
      "$BIN/psql" -h "$W" -p $PP -U postgres -q >/dev/null 2>&1 <<SQL
DO \$\$
DECLARE cols text;
BEGIN
  SELECT string_agg(format('c%s int', g), ', ') INTO cols FROM generate_series(1,1000) g;
  FOR i IN 1..40 LOOP
    EXECUTE format('CREATE TABLE wide%s (%s)', i, cols);
  END LOOP;
END \$\$;
CREATE TABLE t(id int primary key, v text);
INSERT INTO t SELECT g,'v'||g FROM generate_series(1,2000) g;
SQL
      ;;
    bigdata)
      # A larger user table -> multi-segment relfiles, chunked RELFILE images.
      "$BIN/psql" -h "$W" -p $PP -U postgres -qc \
        "CREATE TABLE t(id int primary key, v text); INSERT INTO t SELECT g, repeat('x',80)||g FROM generate_series(1,$ROWS) g; CREATE INDEX ON t(v);" >/dev/null 2>&1
      ;;
    toastheavy)
      "$BIN/psql" -h "$W" -p $PP -U postgres -qc \
        "CREATE TABLE t(id int primary key, v text); INSERT INTO t SELECT g, repeat(md5(g::text),2000) FROM generate_series(1,3000) g;" >/dev/null 2>&1
      ;;
    *) echo "unknown shape $shape"; return 1;;
  esac
}

# A stable cross-database fingerprint: per-db row counts + a content hash.
fingerprint() {
  local W=$1 port=$2
  local dbs fp="" d
  dbs=$("$BIN/psql" -h "$W" -p $port -U postgres -tAc \
    "SELECT datname FROM pg_database WHERE datname NOT IN ('template0','template1') ORDER BY 1" 2>/dev/null)
  for d in $dbs; do
    local one
    one=$("$BIN/psql" -h "$W" -p $port -U postgres -d "$d" -tAc \
      "SELECT coalesce(sum(c.reltuples::bigint),0) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind='r'" 2>/dev/null)
    fp="$fp|$d:$one"
  done
  echo "$fp"
}

for shape in $SHAPES; do
  log "SHAPE=$shape : upgrade primary -> commit -> live; standby STREAMS the window (no cp)"
  W=$BASEW/$shape; OLD=$W/old NEW=$W/new SKEL=$W/skel
  for p in $PP $SP; do lsof -ti :$p 2>/dev/null | xargs kill -9 2>/dev/null; done
  rm -rf "$W"; mkdir -p "$W"

  "$BIN/initdb" -D "$OLD" -U postgres -N >/dev/null 2>&1 || { echo "FAIL: initdb $shape"; GRC=1; continue; }
  { echo "unix_socket_directories='$W'"; echo "port=$PP"; } >> "$OLD/postgresql.conf"
  "$BIN/pg_ctl" -D "$OLD" -l "$W/old.log" -w start >/dev/null 2>&1 || { echo "FAIL: start $shape"; GRC=1; continue; }
  build_shape "$W" "$shape"
  # analyze so reltuples is populated for the fingerprint
  "$BIN/psql" -h "$W" -p $PP -U postgres -qc "ANALYZE" >/dev/null 2>&1
  for d in $("$BIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT datname FROM pg_database WHERE datname LIKE 'db%'" 2>/dev/null); do
    "$BIN/psql" -h "$W" -p $PP -U postgres -d "$d" -qc "ANALYZE" >/dev/null 2>&1
  done
  OLD_FP=$(fingerprint "$W" $PP)
  ATTRELS=$("$BIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT count(*) FROM pg_attribute" 2>/dev/null)
  log "  old fingerprint=$OLD_FP  pg_attribute rows=$ATTRELS"
  "$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1

  cd "$W"
  "$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-upgrade --copy >"$W/up.log" 2>&1
  if [ $? -ne 0 ]; then echo "FAIL: $shape upgrade"; tail -12 "$W/up.log"; GRC=1; cd /; continue; fi
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
  "$BIN/pg_ctl" -D "$NEW" -l "$W/new.log" -w start >/dev/null 2>&1 || { echo "FAIL: $shape new start"; tail "$W/new.log"; GRC=1; cd /; continue; }
  for d in $("$BIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT datname FROM pg_database WHERE datname NOT IN ('template0','template1')" 2>/dev/null); do
    "$BIN/psql" -h "$W" -p $PP -U postgres -d "$d" -qc "ANALYZE" >/dev/null 2>&1
  done
  NEW_FP=$(fingerprint "$W" $PP)
  NEW_ID=$("$BIN/pg_controldata" -D "$NEW" | grep -i 'system identifier' | grep -oE '[0-9]+')
  # PRIMARY correctness first
  [ "$NEW_FP" = "$OLD_FP" ] && log "  primary upgrade verified ($shape): data preserved" \
    || { echo "FAIL: $shape primary data ($NEW_FP) != old ($OLD_FP)"; GRC=1; }

  # fresh skeleton STREAMS the window (no cp)
  "$BIN/initdb" -D "$SKEL" -U postgres -N >/dev/null 2>&1
  rm -f "$SKEL"/base/*/[0-9]* 2>/dev/null
  rm -f "$SKEL"/global/[0-9]* "$SKEL"/global/pg_filenode.map 2>/dev/null
  cat >> "$SKEL/postgresql.conf" <<CONF
port=$SP
unix_socket_directories='$W'
hot_standby=on
primary_conninfo='host=127.0.0.1 port=$PP user=postgres dbname=postgres'
CONF
  touch "$SKEL/standby.signal"
  "$BIN/pg_ctl" -D "$SKEL" -l "$W/skel.log" -w -t 120 start >/dev/null 2>&1 || true
  UP=0
  for i in $(seq 1 90); do
    "$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT 1" >/dev/null 2>&1 && { UP=1; break; }
    sleep 1
  done
  grep -qiE "started streaming|streaming WAL" "$W/skel.log" || { echo "FAIL: $shape no streaming evidence"; tail -15 "$W/skel.log"; GRC=1; }
  [ "$UP" = 1 ] || { echo "FAIL: $shape standby did not come up"; tail -15 "$W/skel.log"; GRC=1; cd /; continue; }
  # let it catch up to the primary's current LSN
  PRI_LSN=$("$BIN/psql" -h "$W" -p $PP -U postgres -tAc "SELECT pg_current_wal_lsn()")
  for i in $(seq 1 60); do
    RP=$("$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT pg_last_wal_replay_lsn()" 2>/dev/null)
    [ "$RP" \> "$PRI_LSN" ] || [ "$RP" = "$PRI_LSN" ] && break; sleep 1
  done
  for d in $("$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT datname FROM pg_database WHERE datname NOT IN ('template0','template1')" 2>/dev/null); do
    "$BIN/psql" -h "$W" -p $SP -U postgres -d "$d" -qc "ANALYZE" >/dev/null 2>&1
  done
  STBY_FP=$(fingerprint "$W" $SP)
  INREC=$("$BIN/psql" -h "$W" -p $SP -U postgres -tAc "SELECT pg_is_in_recovery()" 2>&1)
  STBY_ID=$("$BIN/pg_controldata" -D "$SKEL" | grep -i 'system identifier' | grep -oE '[0-9]+')
  log "  streamed standby ($shape): fp=$STBY_FP in_recovery=$INREC sysid=$STBY_ID"
  [ "$STBY_FP" = "$NEW_FP" ] || { echo "FAIL: $shape standby fp ($STBY_FP) != primary ($NEW_FP)"; GRC=1; }
  [ "$INREC" = "t" ]         || { echo "FAIL: $shape standby not a hot standby (in_recovery=$INREC)"; GRC=1; }
  [ "$STBY_ID" = "$NEW_ID" ] || { echo "FAIL: $shape sysid mismatch $STBY_ID != $NEW_ID"; GRC=1; }

  "$BIN/pg_ctl" -D "$SKEL" -w stop >/dev/null 2>&1 || true
  "$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1 || true
  for p in $PP $SP; do lsof -ti :$p 2>/dev/null | xargs kill -9 2>/dev/null; done
  cd /
done

echo "========================================================================"
[ "$GRC" = 0 ] && log "PASS: streaming-standby stress -- all shapes streamed (no cp) and converged to the upgraded primary" \
              || log "FAIL: see messages above"
exit $GRC
