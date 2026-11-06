#!/usr/bin/env bash
# Copyright 2026 OpenS3 Contributors
# SPDX-License-Identifier: Apache-2.0
# Kill secondary volume during replicated write → PUT must fail (fail-closed).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${ROOT}/target/debug/opens3"
DATA="${TMPDIR:-/tmp}/opens3-kill-$$"
mkdir -p "$DATA/m" "$DATA/v1" "$DATA/v2" "$DATA/filer"
cleanup() {
  pkill -f "opens3 clerk --port 39333" 2>/dev/null || true
  pkill -f "opens3 volume --port 38080" 2>/dev/null || true
  pkill -f "opens3 volume --port 38081" 2>/dev/null || true
  pkill -f "opens3 s3 --port 38333" 2>/dev/null || true
  rm -rf "$DATA"
}
trap cleanup EXIT

cargo build -q -p opens3

"$BIN" clerk --port 39333 --default-replication 001 --raft-dir "$DATA/m/raft" &
sleep 0.5
"$BIN" volume --port 38080 --dir "$DATA/v1" --clerk http://127.0.0.1:39333 \
  --node-id vs1 --max-volumes 8 --max-volume-size-mb 16 &
"$BIN" volume --port 38081 --dir "$DATA/v2" --clerk http://127.0.0.1:39333 \
  --node-id vs2 --max-volumes 8 --max-volume-size-mb 16 &
sleep 1
"$BIN" s3 --port 38333 --clerk http://127.0.0.1:39333 \
  --meta-dir "$DATA/filer" --default-replication 001 &
sleep 1
for i in $(seq 1 30); do curl -sf http://127.0.0.1:38333/health >/dev/null && break; sleep 0.3; done
for i in $(seq 1 40); do
  st=$(curl -sf http://127.0.0.1:39333/cluster/status || true)
  if echo "$st" | grep -q vs1 && echo "$st" | grep -q vs2; then
    break
  fi
  sleep 0.5
done

# assign while BOTH peers are alive so locations include both
ASSIGN=$(curl -sf "http://127.0.0.1:39333/dir/assign?replication=001")
FID=$(echo "$ASSIGN" | python3 -c "import sys,json; print(json.load(sys.stdin)['fid'])")
URL=$(echo "$ASSIGN" | python3 -c "import sys,json; print(json.load(sys.stdin)['url'])")
LOCS=$(echo "$ASSIGN" | python3 -c "import sys,json; d=json.load(sys.stdin); print(','.join(l['url'] for l in d.get('locations',[])))")
echo "assigned fid=$FID url=$URL locs=$LOCS"
echo "$LOCS" | grep -q 38081
echo "$LOCS" | grep -q 38080

# kill the non-primary replica so primary write can start then fail replication
if echo "$URL" | grep -q 38080; then
  KILL_PORT=38081
else
  KILL_PORT=38080
fi
kill $(pgrep -f "opens3 volume --port $KILL_PORT") 2>/dev/null || true
sleep 0.3

CODE=$(curl -s -o /tmp/opens3-kill-body -w "%{http_code}" -X PUT \
  "${URL}/${FID}?locations=${LOCS}" --data-binary "payload-should-fail" || true)
echo "write status=$CODE body=$(cat /tmp/opens3-kill-body 2>/dev/null || true)"
test "$CODE" = "503"

# primary should not keep the needle after rollback
GET_CODE=$(curl -s -o /dev/null -w "%{http_code}" "${URL}/${FID}" || true)
echo "primary get after failed replicate=$GET_CODE"
test "$GET_CODE" != "200"

echo "e2e_kill_replica OK (fail-closed)"
