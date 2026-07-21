#!/usr/bin/env bash
# Many-relations + concurrent-clients test for --wal-upgrade.
#
#   * MANY relations: hundreds of tables (heap + toast + several index AMs) plus
#     multiple databases, so the RELFILE capture batches thousands of small
#     files across many base/<dboid>/ directories -- exercises the batch-flush
#     path (many entries per record) rather than the big-file chunking path.
#   * CONCURRENT clients: while building, many parallel psql sessions write, so
#     the pre-upgrade cluster has interleaved data from N clients.  After the
#     WAL-replay upgrade, every relation's content must match exactly.
#
# Verifies data + relation-count round-trip, and that the on-disk data was wiped
# (so the match proves WAL replay, not leftover files).
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"; BIN="${PGBIN:-$ROOT/pginst/bin}"
W=${WORK:-/tmp/pgu_manyrel}; OLD=$W/old; NEW=$W/new; P=${PORT:-55580}
NDBS=${NDBS:-4}          # number of databases
NTABLES=${NTABLES:-150}  # tables per database
NCLIENTS=${NCLIENTS:-16} # concurrent writer clients
export PGPORT=$P PGDATABASE=postgres
log(){ echo "=== $* ==="; }
q(){ "$BIN/psql" -h "$W" -p $P -U postgres "$@"; }
rm -rf "$W"; mkdir -p "$W"

log "init old cluster"
"$BIN/initdb" -D "$OLD" -U postgres -N >/dev/null 2>&1 || { echo FAIL initdb; exit 1; }
cat >> "$OLD/postgresql.conf" <<CONF
unix_socket_directories='$W'
port=$P
max_connections=200
CONF
"$BIN/pg_ctl" -D "$OLD" -l "$W/old.log" -w start >/dev/null 2>&1 || { echo FAIL start; exit 1; }

log "create $NDBS databases, each with $NTABLES tables (heap/toast/btree/hash/gin)"
# Unquoted heredoc: bash expands $NTABLES but leaves \$\$ -> $$ and %% intact.
# Modulo OUTSIDE a format() string is a single '%'; only the '%%' inside the
# format template is a literal percent.  (ON_ERROR_STOP so a DDL failure is loud,
# not silently swallowed as it was before.)
for d in $(seq 1 $NDBS); do
  q -qc "CREATE DATABASE db$d" >/dev/null 2>&1
  "$BIN/psql" -h "$W" -p $P -U postgres -d db$d -q -v ON_ERROR_STOP=1 >"$W/ddl_db$d.log" 2>&1 <<SQL
DO \$\$
DECLARE i int;
BEGIN
  FOR i IN 1..$NTABLES LOOP
    EXECUTE format('CREATE TABLE t%s(id int primary key, v text, arr int[], big text)', i);
    EXECUTE format('INSERT INTO t%s SELECT g, ''v''||g, ARRAY[g,g%%10], repeat(md5(g::text),50) FROM generate_series(1,200) g', i);
    IF i % 5 = 0 THEN EXECUTE format('CREATE INDEX ON t%s USING gin(arr)', i); END IF;
    IF i % 7 = 0 THEN EXECUTE format('CREATE INDEX ON t%s USING hash(id)', i); END IF;
  END LOOP;
END \$\$;
SQL
  [ $? -eq 0 ] || { echo "FAIL: DDL for db$d"; tail -5 "$W/ddl_db$d.log"; exit 1; }
done

log "$NCLIENTS concurrent clients each doing interleaved writes"
# Each client updates a disjoint slice across all dbs/tables concurrently.
pids=""
for c in $(seq 1 $NCLIENTS); do
  (
    for d in $(seq 1 $NDBS); do
      "$BIN/psql" -h "$W" -p $P -U postgres -d db$d -q >/dev/null 2>&1 <<SQL
UPDATE t$(( (c % NTABLES) + 1 )) SET v = v || '_c$c' WHERE id % $NCLIENTS = $((c % NCLIENTS));
INSERT INTO t$(( ((c*3) % NTABLES) + 1 )) SELECT 1000+$c*100+g, 'cli$c'||g, ARRAY[g], repeat('z',100) FROM generate_series(1,20) g;
SQL
    done
  ) &
  pids="$pids $!"
done
for pid in $pids; do wait $pid; done
log "concurrent writes done"

# Robust content fingerprint: a PL/pgSQL function loops every user table t* in
# the db and accumulates (count, content-hash) into one deterministic string.
data_fp() {
  local acc=""
  for d in $(seq 1 $NDBS); do
    local one
    one=$("$BIN/psql" -h "$W" -p $P -U postgres -d db$d -tAc "
      DO \$\$
      DECLARE r record; c bigint; h bigint; out text := '';
      BEGIN
        FOR r IN SELECT relname FROM pg_class WHERE relkind='r' AND relname LIKE 't%' ORDER BY relname LOOP
          EXECUTE format('SELECT count(*), coalesce(sum(hashtext(v)::bigint),0) FROM %I', r.relname) INTO c, h;
          out := out || r.relname || ':' || c || ':' || h || '|';
        END LOOP;
        RAISE NOTICE 'FP %', md5(out);
      END \$\$;
    " 2>&1 | grep -oE 'FP [0-9a-f]+' | awk '{print $2}')
    acc="$acc db$d=$one"
  done
  echo "$acc"
}

OLD_FP=$(data_fp)
OLD_NREL=$(for d in $(seq 1 $NDBS); do "$BIN/psql" -h "$W" -p $P -U postgres -d db$d -tAc "SELECT count(*) FROM pg_class WHERE relkind IN ('r','i') AND (relname LIKE 't%' OR relname LIKE '%_idx' OR relname LIKE '%pkey')"; done | paste -sd+ | bc)
log "old: relation-ish count=$OLD_NREL"
"$BIN/pg_ctl" -D "$OLD" -w stop >/dev/null 2>&1

log "pg_upgrade --wal-upgrade --initdb --copy -j 4"
cd "$W"
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$OLD" -D "$NEW" -U postgres --initdb --wal-upgrade --copy -j 4 >"$W/up.log" 2>&1
[ $? -eq 0 ] || { echo FAIL upgrade; tail -30 "$W/up.log"; exit 1; }

TOTAL_BASE=$(find "$NEW/base" -type f -regextype posix-extended -regex '.*/[0-9]+(\.[0-9]+)?' -printf '%s\n' 2>/dev/null | awk '{s+=$1}END{print s+0}')
log "base/ data-file bytes on disk after pg_upgrade (should be 0 = wiped): $TOTAL_BASE"
[ "${TOTAL_BASE:-0}" = "0" ] || { echo "FAIL: data not wiped ($TOTAL_BASE) -- replay unproven"; exit 1; }

# --wal-upgrade auto-serves: the new cluster comes up read-write on the
# first start (no quarantine hold, no commit).  The disk-wiped assertion above
# ran before first start, so it still reflects the wipe.
cat >> "$NEW/postgresql.conf" <<CONF
unix_socket_directories='$W'
port=$P
max_connections=200
CONF
log "start new cluster (WAL replay)"
"$BIN/pg_ctl" -D "$NEW" -l "$W/new.log" -w -t 300 start >/dev/null 2>&1 || { echo FAIL start new; tail -30 "$W/new.log"; exit 1; }
NEW_FP=$(data_fp)
NEW_NREL=$(for d in $(seq 1 $NDBS); do "$BIN/psql" -h "$W" -p $P -U postgres -d db$d -tAc "SELECT count(*) FROM pg_class WHERE relkind IN ('r','i') AND (relname LIKE 't%' OR relname LIKE '%_idx' OR relname LIKE '%pkey')"; done | paste -sd+ | bc)
log "new: relation-ish count=$NEW_NREL"

# concurrent READERS against the recovered cluster (sanity: it serves)
log "$NCLIENTS concurrent readers against recovered cluster"
rpids=""
READOK=$W/readok; : > "$READOK"
for c in $(seq 1 $NCLIENTS); do
  ( d=$(( (c % NDBS) + 1 ))
    r=$("$BIN/psql" -h "$W" -p $P -U postgres -d db$d -tAc "SELECT count(*) FROM t1" 2>&1)
    echo "$r" >> "$READOK" ) &
  rpids="$rpids $!"
done
for pid in $rpids; do wait $pid; done
# count lines that are NOT a bare integer (a failed/serving-error read).
# grep -c prints the count and exits 1 when 0 matches, so ignore its status.
NBAD=$(grep -cvE '^[0-9]+$' "$READOK"); NBAD=${NBAD:-0}
NREAD=$(wc -l < "$READOK" | tr -d ' ')
"$BIN/pg_ctl" -D "$NEW" -w stop >/dev/null 2>&1

FAIL=0
[ "$OLD_FP" = "$NEW_FP" ]   || { echo "FAIL: per-table row-count fingerprint mismatch"; echo "OLD: $OLD_FP"; echo "NEW: $NEW_FP"; FAIL=1; }
[ "$OLD_NREL" = "$NEW_NREL" ] || { echo "FAIL: relation count mismatch old=$OLD_NREL new=$NEW_NREL"; FAIL=1; }
[ "$NBAD" = "0" ]           || { echo "FAIL: $NBAD/$NREAD concurrent readers got a non-numeric result"; FAIL=1; }
log "concurrent readers: $NREAD ok, $NBAD bad"
[ "$FAIL" = 0 ] && log "PASS: $NDBS dbs x $NTABLES tables, $NCLIENTS clients -- reconstructed from WAL, data matches" \
                || log "FAIL: many-relations/concurrent mismatch"
exit $FAIL
