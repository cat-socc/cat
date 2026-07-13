#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON_BIN="${PYTHON_BIN:-python}"
EXPERIMENT="${EXPERIMENT:-all}"
ALGOS="${ALGOS:-naive,greedy,ptas}"
MIN_PART_MB="${MIN_PART_MB:-5}"
MAX_COPY_CHUNK_MB="${MAX_COPY_CHUNK_MB:-512}"
RESULT_DIR="${RESULT_DIR:-reproduce_results}"
OFFLINE="${OFFLINE:-0}"
PREPARE_S3="${PREPARE_S3:-1}"
EXECUTE_S3="${EXECUTE_S3:-1}"
FORCE_PREPARE_S3="${FORCE_PREPARE_S3:-0}"

MODE_ARGS=()

if [[ "$OFFLINE" == "1" ]]; then
  PREPARE_S3=0
  EXECUTE_S3=0
  DEFAULT_OUTPUT="${RESULT_DIR}/publish_plan.csv"
else
  DEFAULT_OUTPUT="${RESULT_DIR}/publish_runtime.csv"
fi

OUTPUT="${OUTPUT:-${DEFAULT_OUTPUT}}"

if [[ "$PREPARE_S3" == "1" ]]; then
  MODE_ARGS+=("--prepare-s3")
fi

if [[ "$FORCE_PREPARE_S3" == "1" ]]; then
  MODE_ARGS+=("--force-prepare-s3")
fi

if [[ "$EXECUTE_S3" == "1" ]]; then
  MODE_ARGS+=("--execute-s3")
fi

echo "Publish experiment"
echo "  experiment: ${EXPERIMENT}"
echo "  algos: ${ALGOS}"
echo "  min_part_mb: ${MIN_PART_MB}"
echo "  max_copy_chunk_mb: ${MAX_COPY_CHUNK_MB}"
echo "  output: ${OUTPUT}"
echo "  offline: ${OFFLINE}"
echo "  prepare_s3: ${PREPARE_S3}"
echo "  force_prepare_s3: ${FORCE_PREPARE_S3}"
echo "  execute_s3: ${EXECUTE_S3}"

"$PYTHON_BIN" reproduce_publish_experiments.py \
  --experiment "$EXPERIMENT" \
  --algos "$ALGOS" \
  --min-part-mb "$MIN_PART_MB" \
  --max-copy-chunk-mb "$MAX_COPY_CHUNK_MB" \
  --output "$OUTPUT" \
  "${MODE_ARGS[@]}"
