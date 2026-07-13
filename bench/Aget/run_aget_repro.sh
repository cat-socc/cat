#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_DIR="${SCRIPT_DIR}/../rangePut"

PYTHON_BIN="${PYTHON_BIN:-python}"
BUCKET_NAME="${BUCKET_NAME:-rawiotest}"
SHADOW_BUCKET_NAME="${SHADOW_BUCKET_NAME:-${BUCKET_NAME}-shadow}"
BASE_SIZE_MB="${BASE_SIZE_MB:-1024}"
ITERATIONS="${ITERATIONS:-1}"
RANGE_SIZE_SETS="${RANGE_SIZE_SETS:-32 64 128 256 512 1024}"
RESULT_DIR="${RESULT_DIR:-${SCRIPT_DIR}/res/details}"
SUMMARY_DIR="${SUMMARY_DIR:-${SCRIPT_DIR}/res/summary}"
PLAN_FILE="${PLAN_FILE:-${SCRIPT_DIR}/write_patterns/write_for_read_bs4k-4m_io1024m.1.plan}"
RUN_NO_DELTA="${RUN_NO_DELTA:-1}"
RUN_WITH_DELTA="${RUN_WITH_DELTA:-1}"

if [[ "${CLEAR_S3:-0}" == "1" ]]; then
  aws s3 rm "s3://${SHADOW_BUCKET_NAME}" --recursive
  aws s3 rm "s3://${BUCKET_NAME}" --recursive
else
  echo "Skipping S3 cleanup. Set CLEAR_S3=1 to remove s3://${SHADOW_BUCKET_NAME} and s3://${BUCKET_NAME} before the run."
fi

read -r -a RANGE_SIZES <<< "$RANGE_SIZE_SETS"

if [[ "$RUN_NO_DELTA" == "1" ]]; then
  echo "=== Exp1: read no delta ==="
  "$PYTHON_BIN" "${BENCHMARK_DIR}/benchmark.py" \
    --executor-sets boto3 object_store \
    --test-type initial_write \
    --file-size-sets "$BASE_SIZE_MB" \
    --bucket-name "$BUCKET_NAME" \
    --iterations 1 \
    --result-dir "$RESULT_DIR" \
    --summary-dir "$SUMMARY_DIR" \
    --save-result-flag 0

  "$PYTHON_BIN" "${BENCHMARK_DIR}/benchmark.py" \
    --executor-sets boto3 object_store \
    --test-type read_range \
    --file-size-sets "$BASE_SIZE_MB" \
    --range-size-sets "${RANGE_SIZES[@]}" \
    --bucket-name "$BUCKET_NAME" \
    --iterations "$ITERATIONS" \
    --result-dir "$RESULT_DIR" \
    --summary-dir "$SUMMARY_DIR" \
    --save-result-flag 1
fi

if [[ "$RUN_WITH_DELTA" == "1" ]]; then
  echo "=== Exp2: read with object-store delta ==="
  "$PYTHON_BIN" "${SCRIPT_DIR}/prepare_delta_data.py" \
    --bucket "$BUCKET_NAME" \
    --base-size-mb "$BASE_SIZE_MB" \
    --plan-file "$PLAN_FILE"

  "$PYTHON_BIN" "${BENCHMARK_DIR}/benchmark.py" \
    --executor-sets object_store \
    --test-type read_range \
    --file-size-sets "$BASE_SIZE_MB" \
    --range-size-sets "${RANGE_SIZES[@]}" \
    --bucket-name "$BUCKET_NAME" \
    --iterations "$ITERATIONS" \
    --result-dir "$RESULT_DIR" \
    --summary-dir "$SUMMARY_DIR" \
    --save-result-flag 1
fi
