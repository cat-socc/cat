"""
Reproduce publish planning and optional S3 execution experiments.

Default mode is offline: build ObjectMap from fio write-pattern plans, run
greedy/PTAS planners, and write CSV summaries. Use --execute-s3 only when the
corresponding object_map_res JSON files already exist in S3/object-map form.
Use --prepare-s3 to create those S3 objects and map files first.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from object_store.object_meta.object_map import ObjectMap
from object_store.object_meta.segment import Segment


MB = 1024 * 1024
DEFAULT_BASE_SIZE = 1024 * MB
DEFAULT_MIN_PART_SIZE = 5 * MB
DEFAULT_MAX_COPY_CHUNK = 512 * MB
DEFAULT_BUCKET = "rawiotest"
DEFAULT_KEY_PREFIX = "test-object-mpu"

BENCH_DIR = ROOT / "bench" / "publish"
WRITE_PATTERN_DIR = BENCH_DIR / "write_pattern"
OUT_DIR = BENCH_DIR / "reproduce_results"
MAP_DIR = BENCH_DIR / "object_map_res"


@dataclass(frozen=True)
class Case:
    experiment: str
    label: str
    plan_file: Path
    base_size: int = DEFAULT_BASE_SIZE

    @property
    def plan_name(self) -> str:
        return self.plan_file.name

    @property
    def key(self) -> str:
        return f"{DEFAULT_KEY_PREFIX}-{self.plan_file.stem}"

    @property
    def map_file(self) -> Path:
        return MAP_DIR / f"object_map_{DEFAULT_BUCKET}_{self.key.replace('/', '_')}.json"


def parse_write_pattern(plan_file: Path) -> list[tuple[int, int]]:
    writes: list[tuple[int, int]] = []
    with plan_file.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(",")
            if len(parts) >= 2:
                writes.append((int(parts[0]), int(parts[1])))
    return sorted(writes)


def merge_writes(writes: Iterable[tuple[int, int]]) -> list[tuple[int, int]]:
    merged: list[tuple[int, int]] = []
    for offset, length in sorted(writes):
        end = offset + length
        if not merged:
            merged.append((offset, length))
            continue
        last_offset, last_length = merged[-1]
        last_end = last_offset + last_length
        if offset < last_end:
            merged[-1] = (last_offset, max(last_end, end) - last_offset)
        else:
            merged.append((offset, length))
    return merged


def build_object_map(case: Case) -> ObjectMap:
    segments: list[Segment] = []
    current_offset = 0
    delta_offset = 0
    base_path = f"{DEFAULT_BUCKET}/{case.key}-base"
    delta_path = f"{DEFAULT_BUCKET}-shadow/{case.key}-data"

    for write_offset, write_length in merge_writes(parse_write_pattern(case.plan_file)):
        if current_offset < write_offset:
            segments.append(
                Segment(current_offset, write_offset - current_offset, base_path, current_offset)
            )
        segments.append(Segment(write_offset, write_length, delta_path, delta_offset))
        current_offset = write_offset + write_length
        delta_offset += write_length

    if current_offset < case.base_size:
        segments.append(
            Segment(current_offset, case.base_size - current_offset, base_path, current_offset)
        )

    return ObjectMap(segments)


def load_object_map(map_file: Path) -> tuple[ObjectMap, dict]:
    with map_file.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return ObjectMap.from_dict(data["object_map"]), data


def summarize_tasks(tasks: list[dict], obj_map: ObjectMap, min_part_size: int) -> dict:
    copy_tasks = [t for t in tasks if t["type"] == "copy"]
    fetch_tasks = [t for t in tasks if t["type"] == "fetch"]
    copy_bytes = sum(int(t["length"]) for t in copy_tasks)
    fetch_bytes = sum(int(t["length"]) for t in fetch_tasks)
    fetch_pieces = sum(len(t.get("pieces", [])) for t in fetch_tasks)

    remote_fetch_bytes = 0
    local_fetch_bytes = 0
    for task in fetch_tasks:
        for piece in task.get("pieces", []):
            logical_offset = int(piece["logical_offset"])
            piece_end = logical_offset + int(piece["length"])
            for seg in obj_map.segments:
                seg_end = seg.offset + seg.length
                if seg.offset <= logical_offset < seg_end:
                    overlap = min(piece_end, seg_end) - logical_offset
                    if seg.length >= min_part_size:
                        remote_fetch_bytes += overlap
                    else:
                        local_fetch_bytes += overlap
                    break

    return {
        "task_count": len(tasks),
        "copy_task_count": len(copy_tasks),
        "fetch_task_count": len(fetch_tasks),
        "fetch_piece_count": fetch_pieces,
        "copy_mb": copy_bytes / MB,
        "fetch_mb": fetch_bytes / MB,
        "fetch_remote_mb": remote_fetch_bytes / MB,
        "fetch_local_mb": local_fetch_bytes / MB,
    }


def plan_case(case: Case, algo: str, min_part_size: int, max_copy_chunk: int) -> dict:
    obj_map = build_object_map(case)
    start = time.perf_counter()
    tasks = obj_map.plan_mpu_tasks(
        min_part_size=min_part_size,
        max_copy_chunk=max_copy_chunk,
        algo=algo,
    )
    elapsed_ms = (time.perf_counter() - start) * 1000
    row = base_row(case, obj_map, algo)
    row.update(summarize_tasks(tasks, obj_map, min_part_size))
    row["plan_elapsed_ms"] = elapsed_ms
    return row


def naive_plan_case(case: Case) -> dict:
    obj_map = build_object_map(case)
    row = base_row(case, obj_map, "naive")
    row.update(
        {
            "task_count": 1,
            "copy_task_count": 0,
            "fetch_task_count": 1,
            "fetch_piece_count": len(obj_map.segments),
            "copy_mb": 0.0,
            "fetch_mb": case.base_size / MB,
            "fetch_remote_mb": case.base_size / MB,
            "fetch_local_mb": 0.0,
            "plan_elapsed_ms": 0.0,
        }
    )
    return row


def base_row(case: Case, obj_map: ObjectMap, algo: str) -> dict:
    writes = parse_write_pattern(case.plan_file)
    merged = merge_writes(writes)
    modified_bytes = sum(length for _, length in merged)
    return {
        "experiment": case.experiment,
        "label": case.label,
        "algo": algo,
        "plan_file": str(case.plan_file),
        "base_size_mb": case.base_size / MB,
        "modified_size_mb": modified_bytes / MB,
        "write_count": len(writes),
        "merged_write_count": len(merged),
        "segment_count": len(obj_map.segments),
    }


def modified_size_cases() -> list[Case]:
    sizes = [64, 128, 256, 512, 1024]
    return [
        Case(
            "modified_size",
            f"{size}m",
            WRITE_PATTERN_DIR
            / "modified_size"
            / f"bs_4k-4m_io{size}m_size1024m_random_1.1.plan",
        )
        for size in sizes
    ]


def block_size_cases() -> list[Case]:
    labels = ["1m", "2m", "3m", "4m", "5m"]
    return [
        Case(
            "block_size",
            label,
            WRITE_PATTERN_DIR
            / "block_size_results"
            / f"bs_{label}_io256m_size1024m_random_1.1.plan",
        )
        for label in labels
    ]


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


async def prepare_s3_maps(cases: list[Case], force: bool = False) -> None:
    for case in cases:
        if case.map_file.exists() and not force:
            print(f"[prepare-s3] skip existing {case.experiment}/{case.label}: {case.map_file}")
            continue
        print(f"[prepare-s3] {case.experiment}/{case.label}: {case.plan_file}")
        await upload_write_pattern_to_s3(
            plan_file=case.plan_file,
            bucket=DEFAULT_BUCKET,
            key=case.key,
            base_file_size=case.base_size,
            output_map_file=case.map_file,
        )


async def upload_write_pattern_to_s3(
    plan_file: Path,
    bucket: str,
    key: str,
    base_file_size: int,
    output_map_file: Path,
) -> dict:
    from common.models import Range
    from object_store.objects_manager import ObjectsManager
    from s3_utils.client import boto3_client
    from s3_utils.s3_boto3 import S3Boto3

    writes = parse_write_pattern(plan_file)
    print(f"  plan_file: {plan_file}")
    print(f"  s3 object: s3://{bucket}/{key}")
    print(f"  base_size_mb: {base_file_size / MB:.2f}")
    print(f"  write_count: {len(writes)}")

    boto3_client_instance = boto3_client()
    s3_boto3 = S3Boto3(
        s3_client=boto3_client_instance,
        transfer_config=boto3_client_instance.get_transfer_config(),
    )
    objects_manager = ObjectsManager(s3_boto3)

    base_data = b"\x00" * base_file_size
    success = await objects_manager.write_full_object_to_snapshot_bucket(
        bucket=bucket,
        key=key,
        data=base_data,
    )
    if not success:
        raise RuntimeError(f"failed to upload base object: s3://{bucket}/{key}")

    ranges = [Range(offset=offset, length=length) for offset, length in writes]
    delta_data = b"".join(b"X" * length for _, length in writes)
    success = await objects_manager.write_range_object(
        bucket=bucket,
        key=key,
        ranges=ranges,
        data=delta_data,
    )
    if not success:
        raise RuntimeError(f"failed to upload range data: s3://{bucket}/{key}")

    object_manager = objects_manager.get_manager(bucket, key)
    meta = await object_manager._get_meta()
    object_map = meta.object_map

    map_dict = {
        "bucket": bucket,
        "key": key,
        "file_size": base_file_size,
        "write_count": len(writes),
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
        json.dump(map_dict, f, indent=2, ensure_ascii=False)

    print(f"  wrote object map: {output_map_file}")
    return map_dict


async def execute_s3_case(case: Case, algo: str, min_part_size: int, max_copy_chunk: int) -> dict:
    from s3_utils.client import boto3_client
    from s3_utils.s3_boto3 import S3Boto3
    from object_store.objects_manager import ObjectsManager

    obj_map, data = load_object_map(case.map_file)
    bucket = data["bucket"]
    key = data["key"]
    file_size = int(data["file_size"])
    boto3_client_instance = boto3_client()
    s3_boto3 = S3Boto3(
        s3_client=boto3_client_instance,
        transfer_config=boto3_client_instance.get_transfer_config(),
    )
    object_manager = ObjectsManager(s3_boto3).get_manager(bucket, key)

    row = base_row(case, obj_map, algo)
    if algo == "naive":
        start = time.perf_counter()
        await object_manager.storage.sync_simple_execute(
            dest_bucket=bucket,
            dest_key=key,
            object_map=obj_map,
            file_size=file_size,
        )
        row["execute_elapsed_s"] = time.perf_counter() - start
        return row

    start = time.perf_counter()
    tasks = obj_map.plan_mpu_tasks(
        min_part_size=min_part_size,
        max_copy_chunk=max_copy_chunk,
        algo=algo,
    )
    row["plan_elapsed_ms"] = (time.perf_counter() - start) * 1000
    row.update(summarize_tasks(tasks, obj_map, min_part_size))

    start = time.perf_counter()
    await object_manager.storage.mpu_execute_plan(
        dest_bucket=bucket,
        dest_key=key,
        tasks=tasks,
    )
    row["execute_elapsed_s"] = time.perf_counter() - start
    return row


async def execute_s3(cases: list[Case], algos: list[str], min_part_size: int, max_copy_chunk: int) -> list[dict]:
    rows = []
    for case in cases:
        if not case.map_file.exists():
            raise FileNotFoundError(f"missing map file for {case.label}: {case.map_file}")
        for algo in algos:
            print(f"[execute-s3] {case.experiment}/{case.label}/{algo}")
            rows.append(await execute_s3_case(case, algo, min_part_size, max_copy_chunk))
    return rows


def run_offline(cases: list[Case], algos: list[str], min_part_size: int, max_copy_chunk: int) -> list[dict]:
    rows = []
    for case in cases:
        if not case.plan_file.exists():
            raise FileNotFoundError(case.plan_file)
        for algo in algos:
            print(f"[offline] {case.experiment}/{case.label}/{algo}")
            if algo == "naive":
                rows.append(naive_plan_case(case))
            else:
                rows.append(plan_case(case, algo, min_part_size, max_copy_chunk))
    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--experiment",
        choices=["modified-size", "block-size", "all"],
        default="all",
    )
    parser.add_argument(
        "--algos",
        default="naive,greedy,ptas",
        help="Comma separated: naive,greedy,ptas",
    )
    parser.add_argument("--prepare-s3", action="store_true")
    parser.add_argument("--force-prepare-s3", action="store_true")
    parser.add_argument("--execute-s3", action="store_true")
    parser.add_argument("--min-part-mb", type=int, default=5)
    parser.add_argument("--max-copy-chunk-mb", type=int, default=512)
    parser.add_argument("--output", default=str(OUT_DIR / "publish_reproduce.csv"))
    return parser.parse_args()


async def async_main() -> None:
    args = parse_args()
    min_part_size = args.min_part_mb * MB
    max_copy_chunk = args.max_copy_chunk_mb * MB
    algos = [algo.strip() for algo in args.algos.split(",") if algo.strip()]

    cases: list[Case] = []
    if args.experiment in ("modified-size", "all"):
        cases.extend(modified_size_cases())
    if args.experiment in ("block-size", "all"):
        cases.extend(block_size_cases())

    if args.prepare_s3:
        await prepare_s3_maps(cases, force=args.force_prepare_s3)

    if args.execute_s3:
        rows = await execute_s3(cases, algos, min_part_size, max_copy_chunk)
    else:
        rows = run_offline(cases, algos, min_part_size, max_copy_chunk)

    out = Path(args.output)
    write_csv(out, rows)
    print(f"Wrote {len(rows)} rows to {out}")


if __name__ == "__main__":
    asyncio.run(async_main())
