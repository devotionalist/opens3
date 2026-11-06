#!/usr/bin/env bash
# Copyright 2026 OpenS3 Contributors
# SPDX-License-Identifier: Apache-2.0
# 3-master Raft: kill leader, elect new leader, assign still works.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${ROOT}/target/debug/opens3"
DATA="${TMPDIR:-/tmp}/opens3-raft-$$"
mkdir -p "$DATA"/{m1,m2,m3,v1,v2}
cleanup() {
  pkill -f "opens3 clerk --port 4933" 2>/dev/null || true
  pkill -f "opens3 volume --port 27080" 2>/dev/null || true
  pkill -f "opens3 volume --port 27081" 2>/dev/null || true
  rm -rf "$DATA"
}
trap cleanup EXIT

cargo build -q -p opens3

PEERS="http://127.0.0.1:49331,http://127.0.0.1:49332,http://127.0.0.1:49333"
"$BIN" clerk --port 49331 --ip m1 --peers "$PEERS" --raft-dir "$DATA/m1" \
  --advertise-url http://127.0.0.1:49331 &
"$BIN" clerk --port 49332 --ip m2 --peers "$PEERS" --raft-dir "$DATA/m2" \
  --advertise-url http://127.0.0.1:49332 &
"$BIN" clerk --port 49333 --ip m3 --peers "$PEERS" --raft-dir "$DATA/m3" \
  --advertise-url http://127.0.0.1:49333 &
sleep 3

find_leader() {
  for p in 49331 49332 49333; do
    body=$(curl -sf "http://127.0.0.1:$p/raft/status" || true)
    if echo "$body" | grep -q '"role":"Leader"'; then
      echo "$p"
      return 0
    fi
  done
  return 1
}

LEADER=""
for i in $(seq 1 40); do
  if LEADER=$(find_leader); then break; fi
  sleep 0.25
done
test -n "$LEADER"
echo "initial leader=$LEADER"

"$BIN" volume --port 27080 --dir "$DATA/v1" --clerk "http://127.0.0.1:$LEADER" \
  --node-id vs1 --max-volumes 4 --max-volume-size-mb 8 &
sleep 2

curl -sf "http://127.0.0.1:$LEADER/dir/assign" | grep -q fid

# kill leader
pkill -f "opens3 clerk --port $LEADER" || true
sleep 3

NEW=""
for i in $(seq 1 40); do
  if NEW=$(find_leader); then break; fi
  sleep 0.25
done
test -n "$NEW"
test "$NEW" != "$LEADER"
echo "new leader=$NEW"

# new leader has empty in-memory topology until VS register; attach a volume to it
"$BIN" volume --port 27081 --dir "$DATA/v2" --clerk "http://127.0.0.1:$NEW" \
  --node-id vs2 --max-volumes 4 --max-volume-size-mb 8 &
sleep 3
curl -sf "http://127.0.0.1:$NEW/dir/assign" | grep -q fid
echo "e2e_master_failover OK"
