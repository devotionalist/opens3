#!/usr/bin/env python3
"""OpenS3 S3 client demo: create bucket, put/get/list objects.

Defaults target the local k3s NodePort (values-dev). Override with env:

  OPENS3_ENDPOINT=http://192.168.5.1:30333
  OPENS3_ACCESS_KEY=admin
  OPENS3_SECRET_KEY=devsecretdevsecret
  OPENS3_BUCKET=demo

Requires: pip install boto3
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone

try:
    import boto3
    from botocore.client import Config
    from botocore.exceptions import ClientError
except ImportError:
    print("pip install boto3", file=sys.stderr)
    sys.exit(1)


def env(name: str, default: str) -> str:
    return os.environ.get(name, default).strip()


def main() -> int:
    p = argparse.ArgumentParser(description="OpenS3 S3 put/get demo")
    p.add_argument(
        "--endpoint",
        default=env("OPENS3_ENDPOINT", "http://127.0.0.1:30333"),
        help="S3 API endpoint (k8s NodePort default :30333)",
    )
    p.add_argument("--access-key", default=env("OPENS3_ACCESS_KEY", "admin"))
    p.add_argument("--secret-key", default=env("OPENS3_SECRET_KEY", "devsecretdevsecret"))
    p.add_argument("--bucket", default=env("OPENS3_BUCKET", "demo"))
    p.add_argument("--prefix", default=env("OPENS3_PREFIX", "client-demo/"))
    p.add_argument("--count", type=int, default=3, help="number of objects to put")
    p.add_argument("--size", type=int, default=256, help="bytes per object body")
    args = p.parse_args()

    if not args.prefix.endswith("/"):
        args.prefix += "/"

    s3 = boto3.client(
        "s3",
        endpoint_url=args.endpoint.rstrip("/"),
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )

    print(f"endpoint = {args.endpoint}")
    print(f"bucket   = {args.bucket}")
    print(f"prefix   = {args.prefix}")

    # Create bucket (ignore if exists)
    try:
        s3.create_bucket(Bucket=args.bucket)
        print(f"[ok] create_bucket {args.bucket}")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists", "400"):
            print(f"[ok] bucket exists {args.bucket}")
        else:
            print(f"[err] create_bucket: {e}", file=sys.stderr)
            return 1

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    keys: list[str] = []
    for i in range(args.count):
        key = f"{args.prefix}{ts}-{i:03d}.txt"
        body = (
            f"opens3 client demo\n"
            f"ts={ts}\n"
            f"index={i}\n"
            f"{'x' * max(0, args.size - 64)}\n"
        ).encode("utf-8")
        t0 = time.perf_counter()
        s3.put_object(Bucket=args.bucket, Key=key, Body=body, ContentType="text/plain")
        dt = (time.perf_counter() - t0) * 1000
        keys.append(key)
        print(f"[ok] put  s3://{args.bucket}/{key}  ({len(body)} B, {dt:.1f} ms)")

    print("--- list ---")
    resp = s3.list_objects_v2(Bucket=args.bucket, Prefix=args.prefix)
    for obj in resp.get("Contents") or []:
        print(f"  {obj['Key']}  {obj['Size']} B")

    print("--- get ---")
    for key in keys:
        out = s3.get_object(Bucket=args.bucket, Key=key)
        data = out["Body"].read()
        head = data.split(b"\n", 1)[0].decode("utf-8", errors="replace")
        print(f"[ok] get  s3://{args.bucket}/{key}  -> {len(data)} B  first_line={head!r}")

    print("done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
