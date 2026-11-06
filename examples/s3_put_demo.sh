#!/usr/bin/env bash
# Copyright 2026 OpenS3 Contributors
# SPDX-License-Identifier: Apache-2.0
# OpenS3 S3 write demo (aws cli or curl+Signature skip for open mode).
# Defaults: k3s NodePort on localhost / colima node IP.
set -euo pipefail

ENDPOINT="${OPENS3_ENDPOINT:-http://127.0.0.1:30333}"
AK="${OPENS3_ACCESS_KEY:-admin}"
SK="${OPENS3_SECRET_KEY:-devsecretdevsecret}"
BUCKET="${OPENS3_BUCKET:-demo}"
KEY="${OPENS3_KEY:-client-demo/hello-$(date +%s).txt}"
BODY="${OPENS3_BODY:-hello opens3 from shell demo}"

echo "endpoint=$ENDPOINT bucket=$BUCKET key=$KEY"

if command -v aws >/dev/null 2>&1; then
  export AWS_ACCESS_KEY_ID="$AK"
  export AWS_SECRET_ACCESS_KEY="$SK"
  export AWS_DEFAULT_REGION=us-east-1
  aws --endpoint-url "$ENDPOINT" s3 mb "s3://$BUCKET" 2>/dev/null || true
  printf '%s\n' "$BODY" | aws --endpoint-url "$ENDPOINT" s3 cp - "s3://$BUCKET/$KEY"
  aws --endpoint-url "$ENDPOINT" s3 ls "s3://$BUCKET/client-demo/" || true
  aws --endpoint-url "$ENDPOINT" s3 cp "s3://$BUCKET/$KEY" -
  echo
  echo "done (aws cli)."
  exit 0
fi

echo "aws cli not found; using python demo if present..."
ROOT="$(cd "$(dirname "$0")" && pwd)"
exec python3 "$ROOT/s3_put_demo.py" --endpoint "$ENDPOINT" --access-key "$AK" --secret-key "$SK" --bucket "$BUCKET" --count 1
