#!/usr/bin/env bash
# Copyright 2026 OpenS3 Contributors
# SPDX-License-Identifier: Apache-2.0
# S3 compatibility smoke (curl). Optional aws-cli if present.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${ROOT}/target/debug/opens3"
DATA="${TMPDIR:-/tmp}/opens3-s3compat-$$"
S3=http://127.0.0.1:18333
mkdir -p "$DATA"
cleanup() {
  pkill -f "opens3 server --dir $DATA" 2>/dev/null || true
  rm -rf "$DATA"
}
trap cleanup EXIT

cargo build -q -p opens3
"$BIN" server --dir "$DATA" --max-volume-size-mb 32 \
  --master-port 19333 --volume-port 18080 --s3-port 18333 &
sleep 2
for i in $(seq 1 20); do curl -sf "$S3/health" >/dev/null && break; sleep 0.3; done

curl -sf -X PUT "$S3/compat" -H 'Content-Length: 0' >/dev/null
echo hello | curl -sf -X PUT "$S3/compat/hello.txt" --data-binary @- >/dev/null
test "$(curl -sf "$S3/compat/hello.txt")" = "hello"
curl -sf "$S3/compat?list-type=2&max-keys=1" | grep -q IsTruncated
# pagination: create several keys
for i in $(seq 1 5); do echo "k$i" | curl -sf -X PUT "$S3/compat/p/k$i" --data-binary @- >/dev/null; done
PAGE=$(curl -sf "$S3/compat?list-type=2&prefix=p/&max-keys=2")
echo "$PAGE" | grep -q '<IsTruncated>true</IsTruncated>'
TOKEN=$(echo "$PAGE" | sed -n 's/.*<NextContinuationToken>\([^<]*\)<\/NextContinuationToken>.*/\1/p')
test -n "$TOKEN"
curl -sf "$S3/compat?list-type=2&prefix=p/&max-keys=2&continuation-token=$TOKEN" | grep -q Contents

# DeleteObjects
curl -sf -X POST "$S3/compat?delete" -H 'Content-Type: application/xml' \
  -d '<Delete><Object><Key>hello.txt</Key></Object><Object><Key>p/k1</Key></Object></Delete>' \
  | grep -q Deleted

# multipart
INIT=$(curl -sf -X POST "$S3/compat/big.bin?uploads")
UPLOAD_ID=$(echo "$INIT" | sed -n 's/.*<UploadId>\([^<]*\)<\/UploadId>.*/\1/p')
test -n "$UPLOAD_ID"
dd if=/dev/zero bs=1024 count=100 2>/dev/null | curl -sf -X PUT \
  "$S3/compat/big.bin?partNumber=1&uploadId=$UPLOAD_ID" --data-binary @- >/dev/null
curl -sf -X POST "$S3/compat/big.bin?uploadId=$UPLOAD_ID" \
  -d '<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"x"</ETag></Part></CompleteMultipartUpload>' \
  | grep -q ETag

# policy + lifecycle persistence roundtrip
curl -sf -X PUT "$S3/compat?policy" -H 'Content-Type: application/json' \
  -d '{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::compat/*"}]}' >/dev/null
curl -sf "$S3/compat?policy" | grep -q GetObject
curl -sf -X PUT "$S3/compat?lifecycle" \
  -d '<LifecycleConfiguration><Rule><ID>r</ID><Status>Enabled</Status><Filter><Prefix>logs/</Prefix></Filter><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>' >/dev/null
curl -sf "$S3/compat?lifecycle" | grep -q Days

if command -v aws >/dev/null 2>&1; then
  export AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=x AWS_DEFAULT_REGION=us-east-1
  aws --endpoint-url "$S3" --no-sign-request s3 ls >/dev/null || true
  aws --endpoint-url "$S3" --no-sign-request s3 mb s3://awstest 2>/dev/null || \
    curl -sf -X PUT "$S3/awstest" -H 'Content-Length: 0' >/dev/null
  echo aws | aws --endpoint-url "$S3" --no-sign-request s3 cp - s3://awstest/from-aws.txt
  aws --endpoint-url "$S3" --no-sign-request s3 cp s3://awstest/from-aws.txt - | grep -q aws
fi

echo "s3_compat OK"
