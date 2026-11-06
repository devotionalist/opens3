#!/usr/bin/env bash
# Copyright 2026 OpenS3 Contributors
# SPDX-License-Identifier: Apache-2.0
# Two S3 gateways share one Filer via --meta-backend http / --filer-url.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${ROOT}/target/debug/opens3"
DATA="${TMPDIR:-/tmp}/opens3-multi-$$"
mkdir -p "$DATA"/{raft,v1,filer}
cleanup() {
  pkill -f "opens3 clerk --port 16333" 2>/dev/null || true
  pkill -f "opens3 volume --port 16080" 2>/dev/null || true
  pkill -f "opens3 filer --port 16888" 2>/dev/null || true
  pkill -f "opens3 s3 --port 16334" 2>/dev/null || true
  pkill -f "opens3 s3 --port 16335" 2>/dev/null || true
  rm -rf "$DATA"
}
trap cleanup EXIT

cargo build -q -p opens3
"$BIN" clerk --port 16333 --raft-dir "$DATA/raft" &
sleep 0.4
"$BIN" volume --port 16080 --dir "$DATA/v1" --clerk http://127.0.0.1:16333 \
  --node-id vs1 --max-volumes 8 --max-volume-size-mb 16 &
sleep 1
"$BIN" filer --port 16888 --clerk http://127.0.0.1:16333 --meta-dir "$DATA/filer" &
sleep 1
"$BIN" s3 --port 16334 --clerk http://127.0.0.1:16333 --filer-url http://127.0.0.1:16888 &
"$BIN" s3 --port 16335 --clerk http://127.0.0.1:16333 --filer-url http://127.0.0.1:16888 &
sleep 2

curl -sf -X PUT http://127.0.0.1:16334/shared -H 'Content-Length: 0' >/dev/null
echo 'from-s3-a' | curl -sf -X PUT http://127.0.0.1:16334/shared/a.txt --data-binary @- >/dev/null
# read via second gateway
test "$(curl -sf http://127.0.0.1:16335/shared/a.txt)" = "from-s3-a"
echo 'from-s3-b' | curl -sf -X PUT http://127.0.0.1:16335/shared/b.txt --data-binary @- >/dev/null
test "$(curl -sf http://127.0.0.1:16334/shared/b.txt)" = "from-s3-b"
echo "e2e_multi_s3 OK"
