#!/usr/bin/env bash
# Copyright 2026 OpenS3 Contributors
# SPDX-License-Identifier: Apache-2.0
# Write → seal → EC encode on volume; assert EC artifacts exist.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${ROOT}/target/debug/opens3"
DATA="${TMPDIR:-/tmp}/opens3-ec-$$"
mkdir -p "$DATA"
cleanup() {
  pkill -f "opens3 server --dir $DATA" 2>/dev/null || true
  rm -rf "$DATA"
}
trap cleanup EXIT

cargo build -q -p opens3
"$BIN" server --dir "$DATA" --max-volume-size-mb 8 \
  --master-port 25333 --volume-port 25080 --s3-port 25334 &
sleep 2
for i in $(seq 1 20); do curl -sf http://127.0.0.1:25334/health >/dev/null && break; sleep 0.3; done

curl -sf -X PUT http://127.0.0.1:25334/ecb -H 'Content-Length: 0' >/dev/null
dd if=/dev/urandom bs=1024 count=64 2>/dev/null | curl -sf -X PUT http://127.0.0.1:25334/ecb/blob.bin --data-binary @- >/dev/null

ASSIGN=$(curl -sf "http://127.0.0.1:25333/vol/list")
VID=$(echo "$ASSIGN" | python3 -c "import sys,json; print(json.load(sys.stdin)['volumes'][0]['id'])")

curl -sf -X POST "http://127.0.0.1:25080/admin/seal/$VID" >/dev/null
curl -sf -X POST "http://127.0.0.1:25080/admin/ec/$VID?data_shards=2&parity_shards=1" >/dev/null

ls "$DATA/volumes/ec/${VID}.ec."* >/dev/null
test "$(curl -sf http://127.0.0.1:25334/ecb/blob.bin | wc -c)" -eq 65536
echo "e2e_seal_ec OK"
