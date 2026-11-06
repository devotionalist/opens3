// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0

// Continuous S3 put+get loop against OpenS3 (1 object / second).
//
//	cd examples/s3_rw_loop && go run .
//
// Env (same as s3_put_demo.py):
//
//	OPENS3_ENDPOINT=http://127.0.0.1:30333
//	OPENS3_ACCESS_KEY=admin
//	OPENS3_SECRET_KEY=devsecretdevsecret
//	OPENS3_BUCKET=demo
//	OPENS3_PREFIX=rw-loop/
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func env(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}

func main() {
	endpoint := flag.String("endpoint", env("OPENS3_ENDPOINT", "http://127.0.0.1:30333"), "S3 API endpoint")
	accessKey := flag.String("access-key", env("OPENS3_ACCESS_KEY", "admin"), "access key")
	secretKey := flag.String("secret-key", env("OPENS3_SECRET_KEY", "devsecretdevsecret"), "secret key")
	bucket := flag.String("bucket", env("OPENS3_BUCKET", "demo"), "bucket")
	prefix := flag.String("prefix", env("OPENS3_PREFIX", "rw-loop/"), "object key prefix")
	interval := flag.Duration("interval", time.Second, "period between put+get cycles")
	size := flag.Int("size", 256, "payload bytes per object")
	keep := flag.Bool("keep", false, "keep objects after get (default: delete)")
	flag.Parse()

	pfx := *prefix
	if pfx != "" && !strings.HasSuffix(pfx, "/") {
		pfx += "/"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(*accessKey, *secretKey, "")),
	)
	if err != nil {
		log.Fatalf("aws config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(strings.TrimRight(*endpoint, "/"))
		o.UsePathStyle = true
		o.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	})

	log.Printf("endpoint=%s bucket=%s prefix=%s interval=%s size=%d", *endpoint, *bucket, pfx, *interval, *size)

	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: bucket}); err != nil {
		var exists *types.BucketAlreadyOwnedByYou
		var exists2 *types.BucketAlreadyExists
		if !errors.As(err, &exists) && !errors.As(err, &exists2) && !strings.Contains(err.Error(), "BucketAlready") {
			// OpenS3 may return generic errors for existing buckets — continue.
			log.Printf("create_bucket: %v (continuing)", err)
		} else {
			log.Printf("bucket ready: %s", *bucket)
		}
	} else {
		log.Printf("created bucket: %s", *bucket)
	}

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	var seq uint64
	// Run first cycle immediately, then on each tick.
	for {
		seq++
		if err := cycle(ctx, client, *bucket, pfx, seq, *size, *keep); err != nil {
			if errors.Is(err, context.Canceled) {
				log.Printf("stopped after %d cycles", seq-1)
				return
			}
			log.Printf("[err] seq=%d %v", seq, err)
		}
		select {
		case <-ctx.Done():
			log.Printf("stopped after %d cycles", seq)
			return
		case <-ticker.C:
		}
	}
}

func cycle(ctx context.Context, client *s3.Client, bucket, prefix string, seq uint64, size int, keep bool) error {
	var rnd [4]byte
	_, _ = rand.Read(rnd[:])
	key := fmt.Sprintf("%s%06d-%s.txt", prefix, seq, hex.EncodeToString(rnd[:]))

	body := make([]byte, size)
	hdr := fmt.Sprintf("opens3-rw-loop seq=%d ts=%s\n", seq, time.Now().UTC().Format(time.RFC3339Nano))
	copy(body, hdr)
	for i := len(hdr); i < size; i++ {
		body[i] = 'x'
	}

	t0 := time.Now()
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String("text/plain"),
	})
	putMs := time.Since(t0).Milliseconds()
	if err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}

	t1 := time.Now()
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	getMs := time.Since(t1).Milliseconds()
	if err != nil {
		return fmt.Errorf("get %s: %w", key, err)
	}
	defer out.Body.Close()
	got, err := io.ReadAll(out.Body)
	if err != nil {
		return fmt.Errorf("read body %s: %w", key, err)
	}
	if !bytes.Equal(got, body) {
		return fmt.Errorf("mismatch %s: put=%d get=%d bytes", key, len(body), len(got))
	}

	if !keep {
		_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
	}

	log.Printf("[ok] seq=%d put=%dms get=%dms key=%s bytes=%d", seq, putMs, getMs, key, len(body))
	return nil
}
