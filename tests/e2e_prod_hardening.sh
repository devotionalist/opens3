#!/usr/bin/env bash
# Copyright 2026 OpenS3 Contributors
# SPDX-License-Identifier: Apache-2.0
# Production hardening smoke: JWT assign→write, Object Lock, ListMultipartUploads, ACL, TLS listen.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${ROOT}/target/debug/opens3"
TMP="$(mktemp -d)"
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$TMP"' EXIT

cargo build -q -p opens3 2>/dev/null || cargo build -p opens3

JWT_SECRET="prod-jwt-secret-$$"
CLERK_PORT=$((19000 + RANDOM % 1000))
VOL_PORT=$((20000 + RANDOM % 1000))
S3_PORT=$((21000 + RANDOM % 1000))

# --- TLS self-signed ---
openssl req -x509 -newkey rsa:2048 -keyout "$TMP/key.pem" -out "$TMP/cert.pem" \
  -days 1 -nodes -subj "/CN=localhost" 2>/dev/null

# Plaintext JWT cluster (TLS optional on S3 only for this smoke to keep curl simple)
"$BIN" clerk --port "$CLERK_PORT" --ip 127.0.0.1 --raft-dir "$TMP/raft" \
  --jwt-secret "$JWT_SECRET" &
for i in $(seq 1 30); do
  curl -sf "http://127.0.0.1:$CLERK_PORT/health" >/dev/null && break
  sleep 0.2
done
"$BIN" volume --port "$VOL_PORT" --ip 127.0.0.1 --dir "$TMP/vol" \
  --clerk "http://127.0.0.1:$CLERK_PORT" --jwt-secret "$JWT_SECRET" \
  --max-volumes 8 &
ASSIGN=""
for i in $(seq 1 40); do
  if ASSIGN=$(curl -sf "http://127.0.0.1:$CLERK_PORT/dir/assign?collection=&replication=000" 2>/dev/null); then
    echo "$ASSIGN" | grep -q '"fid"' && break
  fi
  ASSIGN=""
  sleep 0.25
done
test -n "$ASSIGN"

# JWT: assign must return jwt; write without bearer must fail; with bearer ok
echo "$ASSIGN" | grep -q '"jwt"'
FID=$(echo "$ASSIGN" | python3 -c "import sys,json; print(json.load(sys.stdin)['fid'])")
JWT=$(echo "$ASSIGN" | python3 -c "import sys,json; print(json.load(sys.stdin)['jwt'])")
URL=$(echo "$ASSIGN" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('publicUrl') or d['url'])")
code=$(curl -s -o /dev/null -w '%{http_code}' -X PUT --data-binary 'noauth' "$URL/$FID" || true)
test "$code" = "401"
curl -sf -X PUT -H "Authorization: Bearer $JWT" --data-binary 'authed' "$URL/$FID" >/dev/null
echo "jwt write OK"

# S3 gateway with same JWT secret (no TLS on S3 for curl simplicity)
"$BIN" s3 --port "$S3_PORT" --clerk "http://127.0.0.1:$CLERK_PORT" \
  --meta-dir "$TMP/filer" --memory --jwt-secret "$JWT_SECRET" &
S3="http://127.0.0.1:$S3_PORT"
for i in $(seq 1 30); do
  curl -sf "$S3/health" >/dev/null && break
  sleep 0.2
done

# Object Lock enforce
curl -sf -X PUT "$S3/lockb" -H "x-amz-bucket-object-lock-enabled: true"
curl -sf -X PUT "$S3/lockb?object-lock" -H 'Content-Type: application/xml' \
  --data '<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>COMPLIANCE</Mode><Days>30</Days></DefaultRetention></Rule></ObjectLockConfiguration>'
curl -sf -X PUT "$S3/lockb/obj" --data-binary 'locked-data'
del_code=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "$S3/lockb/obj")
test "$del_code" = "403"
echo "object lock OK"

# ACL
curl -sf -X PUT "$S3/aclb" -H "x-amz-acl: public-read"
curl -sf "$S3/aclb?acl" | grep -q AllUsers
echo "acl OK"

# ListMultipartUploads
curl -sf -X POST "$S3/mpu/key?uploads" >/tmp/mpu_init.xml
UPLOAD_ID=$(grep -oE '<UploadId>[^<]+' /tmp/mpu_init.xml | head -1 | sed 's/<UploadId>//')
curl -sf "$S3/mpu?uploads" | grep -q "$UPLOAD_ID"
curl -sf -X DELETE "$S3/mpu/key?uploadId=$UPLOAD_ID"
echo "list multipart OK"

# TLS bind smoke (master with cert — just ensure process stays up)
TLS_PORT=$((22000 + RANDOM % 1000))
"$BIN" clerk --port "$TLS_PORT" --ip 127.0.0.1 --raft-dir "$TMP/raft2" \
  --tls-cert "$TMP/cert.pem" --tls-key "$TMP/key.pem" &
ok=0
for i in $(seq 1 30); do
  if curl -skf "https://127.0.0.1:$TLS_PORT/health" | grep -q ok; then
    ok=1
    break
  fi
  sleep 0.2
done
test "$ok" = "1"
echo "tls listen OK"

echo "e2e_prod_hardening OK"
