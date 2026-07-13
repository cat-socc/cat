#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON_BIN="${PYTHON_BIN:-python}"
BUCKET_NAME="${BUCKET_NAME:-rawiotest}"
SHADOW_BUCKET_NAME="${SHADOW_BUCKET_NAME:-${BUCKET_NAME}-shadow}"
ITERATIONS="${ITERATIONS:-3}"
RESULT_DIR="${RESULT_DIR:-res/details}"
SUMMARY_DIR="${SUMMARY_DIR:-res/summary}"

# 1GB, 2GB, 4GB, 8GB, 16GB in MB by default.
BASE_OBJECT_SIZE_SETS="${BASE_OBJECT_SIZE_SETS:-1024 2048 4096 }"
RANGE_SIZE_SETS="${RANGE_SIZE_SETS:-1 4 16 64 256}"
RANGE_WRITE_BASE_SIZE_MB="${RANGE_WRITE_BASE_SIZE_MB:-1024}"
FIXED_RANGE_SIZE_MB="${FIXED_RANGE_SIZE_MB:-64}"

if [[ "${CLEAR_S3:-0}" == "1" ]]; then
  aws s3 rm "s3://${SHADOW_BUCKET_NAME}" --recursive
  aws s3 rm "s3://${BUCKET_NAME}" --recursive
else
  echo "Skipping S3 cleanup. Set CLEAR_S3=1 to remove s3://${SHADOW_BUCKET_NAME} and s3://${BUCKET_NAME} before the run."
fi

# Base method:
# Full-object PUT with boto3 for 1GB, 2GB, 4GB, 8GB, and 16GB.
"$PYTHON_BIN" benchmark.py \
  --executor-sets boto3 \
  --test-type initial_write \
  --file-size-sets $BASE_OBJECT_SIZE_SETS \
  --bucket-name "$BUCKET_NAME" \
  --iterations "$ITERATIONS" \
  --result-dir "$RESULT_DIR" \
  --summary-dir "$SUMMARY_DIR" \
  --save-result-flag 1

# Prepare cat base objects.
"$PYTHON_BIN" benchmark.py \
  --executor-sets object_store \
  --test-type initial_write \
  --file-size-sets $BASE_OBJECT_SIZE_SETS \
  --bucket-name "$BUCKET_NAME" \
  --iterations 1 \
  --result-dir "$RESULT_DIR" \
  --summary-dir "$SUMMARY_DIR" \
  --save-result-flag 0

# Experiment 1:
# Base object is 1GB by default. Modified ranges are 1MB, 4MB, 16MB, 64MB, 256MB.
"$PYTHON_BIN" benchmark.py \
  --executor-sets object_store \
  --test-type range_write \
  --file-size-sets "$RANGE_WRITE_BASE_SIZE_MB" \
  --range-size-sets $RANGE_SIZE_SETS \
  --bucket-name "$BUCKET_NAME" \
  --iterations "$ITERATIONS" \
  --result-dir "$RESULT_DIR" \
  --summary-dir "$SUMMARY_DIR" \
  --save-result-flag 1

# Experiment 2:
# Modified range is 64MB. Base object sizes are 1GB, 2GB, 4GB, 8GB, 16GB.
"$PYTHON_BIN" benchmark.py \
  --executor-sets object_store \
  --test-type write_range_in_files \
  --file-size-sets $BASE_OBJECT_SIZE_SETS \
  --range-size-sets "$FIXED_RANGE_SIZE_MB" \
  --bucket-name "$BUCKET_NAME" \
  --iterations "$ITERATIONS" \
  --result-dir "$RESULT_DIR" \
  --summary-dir "$SUMMARY_DIR" \
  --save-result-flag 1
