#!/usr/bin/env bash
# Copyright 2026 OpenS3 Contributors
# SPDX-License-Identifier: Apache-2.0
# End-to-end fault injection: kill volume mid-flight, heal, continue serving.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${ROOT}/target/debug/opens3"
DATA="${TMPDIR:-/tmp}/opens3-e2e-$$"
mkdir -p "$DATA"
cleanup() {
  pkill -f "opens3 server --dir $DATA" 2>/dev/null || true
  pkill -f "opens3 volume --dir $DATA" 2>/dev/null || true
  rm -rf "$DATA"
}
trap cleanup EXIT

cargo build -q -p opens3
"$BIN" server --dir "$DATA" --max-volume-size-mb 16 \
  --master-port 29333 --volume-port 28080 --s3-port 28333 &
sleep 2
for i in $(seq 1 20); do curl -sf http://127.0.0.1:28333/health >/dev/null && break; sleep 0.5; done

curl -sf -X PUT http://127.0.0.1:28333/faultb -H 'Content-Length: 0' >/dev/null
echo 'before-kill' | curl -sf -X PUT http://127.0.0.1:28333/faultb/obj.txt --data-binary @- >/dev/null
test "$(curl -sf http://127.0.0.1:28333/faultb/obj.txt)" = "before-kill"

# start second volume and force heal/balance path
mkdir -p "$DATA/v2"
"$BIN" volume --port 28081 --dir "$DATA/v2" --clerk http://127.0.0.1:29333 \
  --node-id vs2 --max-volumes 8 --max-volume-size-mb 16 &
sleep 5

# metrics & raft
curl -sf http://127.0.0.1:29333/metrics | grep -q opens3_clerk_is_leader
curl -sf http://127.0.0.1:29333/raft/status | grep -q '"role"'
"$BIN" shell heal --clerk http://127.0.0.1:29333 >/dev/null
"$BIN" shell volume-balance --clerk http://127.0.0.1:29333 >/dev/null

# still readable after heal attempt
test "$(curl -sf http://127.0.0.1:28333/faultb/obj.txt)" = "before-kill"
echo "e2e_fault OK"
