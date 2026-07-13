#!/usr/bin/env python3
"""
Prepare an object-store object with delta ranges for Aget read benchmarks.

This script is intentionally self-contained under bench/Aget. It reads a fio
write-pattern plan from write_patterns/, writes a base object, applies the plan
as object-store range writes, and saves the resulting object map locally.
"""

import argparse
import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import List, Tuple

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from common.models import Range
from object_store.object_meta.object_map import ObjectMap
from object_store.objects_manager import ObjectsManager
from s3_utils.client import boto3_client
from s3_utils.s3_boto3 import S3Boto3

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_PLAN = SCRIPT_DIR / "write_patterns" / "write_for_read_bs4k-4m_io1024m.1.plan"
DEFAULT_BUCKET = "rawiotest"
DEFAULT_BASE_SIZE_MB = 1024


def parse_write_pattern(plan_file: Path) -> List[Tuple[int, int]]:
    writes: List[Tuple[int, int]] = []
    with plan_file.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(",")
            if len(parts) < 2:
                raise ValueError(f"Invalid plan line {line_no}: {line!r}")
            offset = int(parts[0])
            length = int(parts[1])
            if length <= 0:
                raise ValueError(f"Invalid length on line {line_no}: {length}")
            writes.append((offset, length))
    return sorted(writes)


def output_map_path(bucket: str, key: str) -> Path:
    return SCRIPT_DIR / "object_map_res" / f"object_map_{bucket}_{key.replace('/', '_')}.json"


def create_sparse_file(size_bytes: int, prefix: str) -> str:
    tmp = tempfile.NamedTemporaryFile(prefix=prefix, delete=False)
    try:
        tmp.truncate(size_bytes)
        return tmp.name
    finally:
        tmp.close()


async def prepare_delta_object(
    *,
    plan_file: Path,
    bucket: str,
    key: str,
    base_size_bytes: int,
    output_map_file: Path,
) -> dict:
    writes = parse_write_pattern(plan_file)
    if not writes:
        raise ValueError(f"No writes found in {plan_file}")

    max_end = max(offset + length for offset, length in writes)
    if max_end > base_size_bytes:
        raise ValueError(
            f"Plan writes past object end: max_end={max_end}, base_size={base_size_bytes}"
        )

    total_delta_bytes = sum(length for _, length in writes)
    ranges = [Range(offset=offset, length=length) for offset, length in writes]

    print("=== Prepare Aget delta data ===")
    print(f"Plan: {plan_file}")
    print(f"Bucket: {bucket}")
    print(f"Key: {key}")
    print(f"Base size: {base_size_bytes} bytes")
    print(f"Write ops: {len(writes)}")
    print(f"Total delta bytes: {total_delta_bytes}")
    print(f"First write: offset={writes[0][0]}, length={writes[0][1]}")
    print(f"Last write: offset={writes[-1][0]}, length={writes[-1][1]}")

    boto3_client_instance = boto3_client()
    s3_boto3 = S3Boto3(
        s3_client=boto3_client_instance,
        transfer_config=boto3_client_instance.get_transfer_config(),
    )
    objects_manager = ObjectsManager(s3_boto3)

    base_file = create_sparse_file(base_size_bytes, "aget-base-")
    delta_file = create_sparse_file(total_delta_bytes, "aget-delta-")
    try:
        print("Uploading base object...")
        ok = await objects_manager.write_full_object_from_file_path_to_snapshot_bucket(
            bucket=bucket,
            key=key,
            file_path=base_file,
            file_size=base_size_bytes,
        )
        if not ok:
            raise RuntimeError("Failed to upload base object")

        print("Uploading delta ranges...")
        ok = await objects_manager.write_range_object_from_file_path(
            bucket=bucket,
            key=key,
            ranges=ranges,
            file_path=delta_file,
            file_size=base_size_bytes,
            source_offsets_match_range_offsets=False,
        )
        if not ok:
            raise RuntimeError("Failed to upload delta ranges")
    finally:
        for path in (base_file, delta_file):
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass

    object_manager = objects_manager.get_manager(bucket, key)
    meta = await object_manager._get_meta()
    object_map = meta.object_map

    result = {
        "bucket": bucket,
        "key": key,
        "file_size": base_size_bytes,
        "plan_file": str(plan_file),
        "write_count": len(writes),
        "total_delta_bytes": total_delta_bytes,
        "segment_count": len(object_map.segments),
        "object_map": object_map.to_dict(),
        "meta_info": {
            "file_size": meta.file_size,
            "primary_bucket": meta.primary_bucket,
            "primary_key": meta.primary_key,
            "modified_ts": meta.modified_ts.isoformat(),
            "dirty_size": meta.dirty_size,
            "version": meta.version,
            "shadow_bucket": meta.shadow_bucket,
            "shadow_key": meta.shadow_key,
        },
    }

    output_map_file.parent.mkdir(parents=True, exist_ok=True)
    with output_map_file.open("w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    print(f"Object map segments: {len(object_map.segments)}")
    for i, seg in enumerate(object_map.segments[:5], start=1):
        print(
            f"  Segment {i}: offset={seg.offset}, length={seg.length}, "
            f"source_path={seg.source_path}, source_offset={seg.source_offset}"
        )
    if len(object_map.segments) > 5:
        print(f"  ... {len(object_map.segments) - 5} more segments")
    print(f"Saved object map: {output_map_file}")
    print(f"Prepared object: s3://{bucket}/{key}")
    return result


def load_object_map_from_file(map_file: str) -> dict:
    with open(map_file, "r", encoding="utf-8") as f:
        return json.load(f)


def restore_object_map_from_file(map_file: str) -> ObjectMap:
    data = load_object_map_from_file(map_file)
    return ObjectMap.from_dict(data["object_map"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare Aget delta read data")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--key", default=None)
    parser.add_argument("--base-size-mb", type=int, default=DEFAULT_BASE_SIZE_MB)
    parser.add_argument("--plan-file", type=Path, default=DEFAULT_PLAN)
    parser.add_argument("--output-map-file", type=Path, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_size_bytes = args.base_size_mb * 1024 * 1024
    key = args.key or f"object_store_test_file_{args.base_size_mb}MB"
    plan_file = args.plan_file.resolve()
    output_map_file = args.output_map_file or output_map_path(args.bucket, key)

    if not plan_file.exists():
        raise FileNotFoundError(f"Plan file not found: {plan_file}")

    asyncio.run(
        prepare_delta_object(
            plan_file=plan_file,
            bucket=args.bucket,
            key=key,
            base_size_bytes=base_size_bytes,
            output_map_file=output_map_file,
        )
    )


if __name__ == "__main__":
    main()
