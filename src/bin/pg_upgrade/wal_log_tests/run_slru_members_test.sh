#!/usr/bin/env bash
# Prove the SLRU-segment-name parse fix (xlog.c:XLogWriteUpgradeSlruData).
#
# pg_multixact/members uses LONG (15-hex-digit) segment names.  The pre-fix
# sscanf("%04" SCNx64) capped the read at 4 hex digits, so members segment
# 000000000000001 parsed to segno 0 -> the upgrade WAL captured it under the
# WRONG number and replay installed it at the wrong SLRU offset (silent
# pg_multixact/members corruption).
#
# Generate MANY distinct 2-member multixacts (one long-lived holder holding
# KEY SHARE on all rows + pgbench firing per-row KEY SHARE txns) to push members
# past segment 0 into 15-digit long-name segments.  Then --wal-log-upgrade and:
#   (a) assert via pg_waldump the upgrade WAL captured a NON-ZERO members segno
#       (pre-fix that segment would have been mis-numbered 0), and
#   (b) start the new cluster, confirm it replays from CN, the non-zero members
#       segment(s) exist, next_multixact_id is preserved, and rows re-lock.
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
BIN="${PGBIN:-$ROOT/pginst/bin}"
W=${WORK:-/tmp/pgu_slru}; rm -rf "$W"; mkdir -p "$W"
PP=55960
lsof -ti :$PP 2>/dev/null | xargs kill -9 2>/dev/null
log(){ echo "=== $* ==="; }
FAIL=0
Q(){ "$BIN/psql" -h "$W" -p $PP -U postgres -tAc "$1"; }

"$BIN/initdb" -D "$W/o" -U postgres -N >/dev/null 2>&1
cat >> "$W/o/postgresql.conf" <<CONF
unix_socket_directories='$W'
port=$PP
wal_level=replica
max_connections=100
CONF
"$BIN/pg_ctl" -D "$W/o" -l "$W/o.log" -w start >/dev/null 2>&1 || { echo FAIL start; exit 1; }

log "1. generate many distinct multixacts -> push members past segment 0"
NROWS=20000
Q "CREATE TABLE t(id int primary key); INSERT INTO t SELECT generate_series(1,$NROWS);" >/dev/null
# long-lived holder: KEY SHARE on ALL rows, held while pgbench runs
( "$BIN/psql" -h "$W" -p $PP -U postgres -q >/dev/null 2>&1 <<SQL
BEGIN; SELECT count(*) FROM (SELECT id FROM t FOR KEY SHARE) z; SELECT pg_sleep(90); COMMIT;
SQL
) &
HOLDER=$!
sleep 2
# each pgbench txn locks one random row FOR KEY SHARE -> a distinct multixact
# {holder, this-txn}; 60k such txns => ~120k members => members segments 1,2,...
cat > "$W/lock.sql" <<EOF
\set r random(1, $NROWS)
BEGIN;
SELECT id FROM t WHERE id = :r FOR KEY SHARE;
END;
EOF
"$BIN/pgbench" -h "$W" -p $PP -U postgres -n -c 10 -t 6000 -f "$W/lock.sql" postgres >/dev/null 2>&1
Q "CHECKPOINT" >/dev/null
NEXT_MXID=$(Q "SELECT next_multixact_id FROM pg_control_checkpoint()")
kill "$HOLDER" 2>/dev/null; wait 2>/dev/null
log "next_multixact_id after workload: $NEXT_MXID"

log "2. members segment files (want NON-ZERO long-name segments)"
MEMBER_SEGS=$(ls -1 "$W/o/pg_multixact/members" | grep -E '^[0-9A-F]+$' | sort)
echo "$MEMBER_SEGS" | tr '\n' ' '; echo
NONZERO=$(echo "$MEMBER_SEGS" | grep -vE '^0+$' | sort | tail -1)   # highest non-zero
log "highest non-zero members segment on disk: '${NONZERO:-none}'"
[ -n "$NONZERO" ] || { echo "  FAIL: could not force members past segment 0"; FAIL=1; }

"$BIN/pg_ctl" -D "$W/o" -w stop >/dev/null 2>&1

log "3. --wal-log-upgrade"
cd "$W"
"$BIN/pg_upgrade" -b "$BIN" -B "$BIN" -d "$W/o" -D "$W/n" -U postgres --initdb --wal-log-upgrade --copy >"$W/up.log" 2>&1
[ $? -eq 0 ] || { echo FAIL upgrade; tail -20 "$W/up.log"; exit 1; }

log "4. DETERMINISTIC parse check: upgrade WAL must capture a NON-ZERO members segno"
# Scan ALL upgrade WAL segments (SLRU records follow the large RELFILE burst,
# so they are NOT in the first segment).  Must run BEFORE starting the new
# cluster, which recycles the upgrade WAL after replay.
: > "$W/members_recs.txt"
for seg in $(ls "$W/n/pg_wal" | grep -E '^[0-9A-F]{24}$' | sort); do
  "$BIN/pg_waldump" -p "$W/n/pg_wal" "$seg" 2>/dev/null \
    | grep -iE "slru pg_multixact/members" >> "$W/members_recs.txt" || true
done
echo "  members SLRU records in upgrade WAL:"; sed 's/^/    /' "$W/members_recs.txt" | head
# desc format: 'slru pg_multixact/members; segs AAAA..BBBB; bytes N'
MAXSEG=$(grep -oiE 'segs [0-9A-F]+\.\.[0-9A-F]+' "$W/members_recs.txt" \
         | sed -E 's/.*\.\.0*([0-9A-F]+)/\1/' | sort -rn | head -1)
log "highest members segment number captured in WAL: '${MAXSEG:-none}'"
if [ -z "$MAXSEG" ] || echo "$MAXSEG" | grep -qE '^0+$'; then
  echo "  FAIL: no non-zero members segment captured (pre-fix truncation parses it as 0)"; FAIL=1
else
  echo "  OK: non-zero members segment $MAXSEG captured with its correct number"
fi

log "5. start new cluster: WAL replay + functional check"
cat >> "$W/n/postgresql.conf" <<CONF
unix_socket_directories='$W'
port=$PP
CONF
"$BIN/pg_ctl" -D "$W/n" -l "$W/n.log" -w start >/dev/null 2>&1 || { echo "FAIL new start"; tail -15 "$W/n.log"; FAIL=1; }
grep -q "arming recovery from end-of-upgrade checkpoint" "$W/n.log" && log "  replayed from CN" || { echo "  FAIL: did not replay from CN"; FAIL=1; }
NEW_MXID=$(Q "SELECT next_multixact_id FROM pg_control_checkpoint()")
RELOCK=$(Q "WITH x AS (SELECT id FROM t FOR KEY SHARE LIMIT 100) SELECT count(*) FROM x")
NEWSEGS=$(ls -1 "$W/n/pg_multixact/members" | grep -E '^[0-9A-F]+$' | sort | tr '\n' ' ')
log "new next_multixact_id: $NEW_MXID (old $NEXT_MXID) ; relock 100 -> $RELOCK ; members segs: $NEWSEGS"
[ "$NEW_MXID" = "$NEXT_MXID" ] || { echo "  FAIL: next_multixact_id not preserved"; FAIL=1; }
[ "$RELOCK" = "100" ]         || { echo "  FAIL: cannot re-lock rows after upgrade"; FAIL=1; }
if [ -n "$NONZERO" ] && [ ! -f "$W/n/pg_multixact/members/$NONZERO" ]; then
  echo "  FAIL: members segment $NONZERO missing on replayed cluster"; FAIL=1
fi

"$BIN/pg_ctl" -D "$W/n" -w stop >/dev/null 2>&1
lsof -ti :$PP 2>/dev/null | xargs kill -9 2>/dev/null

echo "========================================================================"
[ "$FAIL" = 0 ] && log "PASS: non-zero (long-name) members segment captured with correct segno and replayed intact" \
                || log "FAIL: see messages above"
exit $FAIL
