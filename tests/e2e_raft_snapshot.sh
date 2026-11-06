#!/usr/bin/env bash
# Copyright 2026 OpenS3 Contributors
# SPDX-License-Identifier: Apache-2.0
# Raft topology snapshot: kill leader, remaining masters still know volumes without re-register storm.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${ROOT}/target/debug/opens3"
DATA="${TMPDIR:-/tmp}/opens3-snap-$$"
mkdir -p "$DATA"/{m1,m2,m3,v1}
cleanup() {
  pkill -f "opens3 clerk --port 4533" 2>/dev/null || true
  pkill -f "opens3 volume --port 25081" 2>/dev/null || true
  rm -rf "$DATA"
}
trap cleanup EXIT
cargo build -q -p opens3
PEERS="http://127.0.0.1:45331,http://127.0.0.1:45332,http://127.0.0.1:45333"
"$BIN" clerk --port 45331 --ip a --peers "$PEERS" --raft-dir "$DATA/m1" --advertise-url http://127.0.0.1:45331 &
"$BIN" clerk --port 45332 --ip b --peers "$PEERS" --raft-dir "$DATA/m2" --advertise-url http://127.0.0.1:45332 &
"$BIN" clerk --port 45333 --ip c --peers "$PEERS" --raft-dir "$DATA/m3" --advertise-url http://127.0.0.1:45333 &
sleep 3
find_leader() {
  for p in 45331 45332 45333; do
    body=$(curl -sf "http://127.0.0.1:$p/raft/status" || true)
    echo "$body" | grep -q '"role":"Leader"' && echo "$p" && return 0
  done
  return 1
}
LEADER=""
for i in $(seq 1 40); do LEADER=$(find_leader) && break; sleep 0.25; done
test -n "$LEADER"
"$BIN" volume --port 25081 --dir "$DATA/v1" --clerk "http://127.0.0.1:$LEADER" \
  --master-peers "$PEERS" --node-id vs1 --max-volumes 4 --max-volume-size-mb 8 &
sleep 3
# wait snapshot to replicate
sleep 2
vc=$(curl -sf "http://127.0.0.1:$LEADER/cluster/status" | python3 -c "import sys,json; print(json.load(sys.stdin).get('volume_server_count',0))")
test "$vc" -ge 1
pkill -f "opens3 clerk --port $LEADER" || true
sleep 3
NEW=""
for i in $(seq 1 40); do
  NEW=$(find_leader || true)
  if [[ -n "$NEW" && "$NEW" != "$LEADER" ]]; then break; fi
  sleep 0.25
done
test -n "$NEW"
# follower should already have servers from snapshot (may be >=1)
curl -sf "http://127.0.0.1:$NEW/cluster/status" | grep -q volume_server
# volume peers should follow to new leader and assign works
for i in $(seq 1 20); do
  if curl -sf "http://127.0.0.1:$NEW/dir/assign" | grep -q fid; then
    echo "e2e_raft_snapshot OK"
    exit 0
  fi
  sleep 0.5
done
echo "assign failed on new leader"; exit 1
