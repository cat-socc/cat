import asyncio
import json
import tempfile
import os
from typing import List, Optional, Iterable, Dict, Any, Tuple
from urllib.parse import unquote, urlparse
from s3_utils.s3_boto3 import S3Boto3
import time
import traceback
from common.logger import print_debug, print_error, print_info
from object_store.object_meta.segment import is_zero_source

def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default

def _env_bool(name: str) -> bool:
    value = os.environ.get(name, "")
    return value not in ("", "0", "false", "False", "FALSE", "no", "NO")

def _source_kind(path: str) -> str:
    if path.startswith("file://"):
        return "local"
    if is_zero_source(path):
        return "zero"
    if "-shadow/" in path or "-shadow" in path:
        return "shadow"
    return "remote"

def _fmt_range(start: int, length: int) -> str:
    return f"[{start},{start + length}) len={length}"

def _fmt_source(path: str, limit: int = 96) -> str:
    if len(path) <= limit:
        return path
    return "..." + path[-(limit - 3):]

def _source_read_merge_limits() -> Tuple[int, int]:
    # Keep the policy generic for all sources: regroup by source_path first,
    # then allow moderately large source-side windows so interleaved logical
    # pieces can still be fetched with fewer physical reads.
    return 1 * 1024 * 1024, 32 * 1024 * 1024

def _merge_source_reads(
    source_path: str,
    indexed: List[Tuple[int, Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    max_gap, max_span = _source_read_merge_limits()
    indexed = sorted(indexed, key=lambda item: int(item[1]["source_offset"]))
    reads: List[Dict[str, Any]] = []
    current: Optional[Dict[str, Any]] = None
    for idx, piece in indexed:
        piece_start = int(piece["source_offset"])
        piece_len = int(piece["length"])
        piece_end = piece_start + piece_len
        if current is None:
            current = {
                "source_path": source_path,
                "start": piece_start,
                "end": piece_end,
                "members": [(idx, piece)],
            }
            continue
        gap = piece_start - int(current["end"])
        new_span = piece_end - int(current["start"])
        if gap <= max_gap and new_span <= max_span:
            current["end"] = max(int(current["end"]), piece_end)
            current["members"].append((idx, piece))
        else:
            reads.append(current)
            current = {
                "source_path": source_path,
                "start": piece_start,
                "end": piece_end,
                "members": [(idx, piece)],
            }
    if current is not None:
        reads.append(current)
    return reads

def _pieces_are_logically_contiguous(pieces: List[Dict[str, Any]]) -> bool:
    if not pieces:
        return True
    ordered = sorted(pieces, key=lambda p: int(p.get("logical_offset", 0)))
    return all(
        int(prev.get("logical_offset", 0)) + int(prev.get("length", 0)) ==
        int(cur.get("logical_offset", 0))
        for prev, cur in zip(ordered, ordered[1:])
    )

def _pieces_are_physically_contiguous(pieces: List[Dict[str, Any]]) -> bool:
    if not pieces:
        return True
    ordered = sorted(pieces, key=lambda p: int(p.get("logical_offset", 0)))
    return all(
        prev.get("source_path") == cur.get("source_path") and
        int(prev.get("source_offset", 0)) + int(prev.get("length", 0)) ==
        int(cur.get("source_offset", 0))
        for prev, cur in zip(ordered, ordered[1:])
    )

class S3Storage:
    def __init__(self, s3_client: S3Boto3):
        self.s3_client = s3_client

    @staticmethod
    def is_local_source(path: str) -> bool:
        return path.startswith("file://")

    @staticmethod
    def local_path_from_source(path: str) -> str:
        parsed = urlparse(path)
        if parsed.scheme != "file":
            raise ValueError(f"not a local source path: {path}")
        return unquote(parsed.path)

    async def get_source_range(self, source_path: str, start: int, end: int) -> bytes:
        expected = end - start + 1
        if is_zero_source(source_path):
            data = b"\x00" * expected
        elif self.is_local_source(source_path):
            data = await self.get_local_range(self.local_path_from_source(source_path), start, end)
        else:
            bucket, key = source_path.split("/", 1)
            data = await self.get_range(bucket, key, start, end)
        if len(data) != expected:
            raise IOError(
                f"source range short read source={source_path} "
                f"bytes={start}-{end} expected={expected} got={len(data)}"
            )
        return data

    def _plan_coalesced_reads(
        self,
        pieces: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        indexed_by_source: Dict[str, List[Tuple[int, Dict[str, Any]]]] = {}
        for idx, piece in enumerate(pieces):
            indexed_by_source.setdefault(str(piece["source_path"]), []).append((idx, piece))

        reads: List[Dict[str, Any]] = []
        for source_path, indexed in indexed_by_source.items():
            reads.extend(_merge_source_reads(source_path, indexed))
        return reads

    async def _fetch_pieces_coalesced(
        self,
        pieces: List[Dict[str, Any]],
        piece_concurrency: int,
    ) -> Tuple[bytes, int, Dict[str, float]]:
        total_start = time.time()
        plan_start = time.time()
        reads = self._plan_coalesced_reads(pieces)
        plan_s = time.time() - plan_start
        if not pieces:
            return b"", 0, {
                "plan_s": 0.0,
                "read_s": 0.0,
                "scatter_s": 0.0,
                "total_s": 0.0,
            }

        base_logical = int(min(int(piece["logical_offset"]) for piece in pieces))
        total_length = sum(int(piece["length"]) for piece in pieces)
        assembled = bytearray(total_length)
        scatter_s = 0.0

        read_sem = asyncio.Semaphore(piece_concurrency)

        async def _fetch_read(read: Dict[str, Any]) -> None:
            nonlocal scatter_s
            async with read_sem:
                start = int(read["start"])
                end_exclusive = int(read["end"])
                data = await self.get_source_range(str(read["source_path"]), start, end_exclusive - 1)
                scatter_start = time.time()
                for _, piece in read["members"]:
                    rel_start = int(piece["source_offset"]) - start
                    rel_end = rel_start + int(piece["length"])
                    logical_start = int(piece["logical_offset"]) - base_logical
                    logical_end = logical_start + int(piece["length"])
                    assembled[logical_start:logical_end] = data[rel_start:rel_end]
                scatter_s += time.time() - scatter_start

        read_start = time.time()
        await asyncio.gather(*[_fetch_read(read) for read in reads])
        read_s = time.time() - read_start
        total_s = time.time() - total_start
        return bytes(assembled), len(reads), {
            "plan_s": plan_s,
            "read_s": read_s,
            "scatter_s": scatter_s,
            "total_s": total_s,
        }

    async def get_local_range(self, file_path: str, start: int, end: int) -> bytes:
        length = end - start + 1
        if length <= 0:
            return b""

        def _read() -> bytes:
            with open(file_path, "rb") as f:
                f.seek(start)
                return f.read(length)

        data = await asyncio.to_thread(_read)
        if len(data) != length:
            raise ValueError(
                f"local range read underflow: {file_path} "
                f"offset={start} length={length} got={len(data)}"
            )
        return data

    # ---------- Regular objects ----------
    async def put(self, bucket, key, body, content_length=None):
        await self.s3_client.put_object(bucket, key, body)

    async def put_from_file(self, bucket, key, file_path, content_length=None):
        await self.s3_client.put_object_from_file(bucket, key, file_path)

    async def get_range(self, bucket, key, start, end) -> bytes:
        data = await self.s3_client.get_object(bucket, key, start, end)
        return data

    async def download_object_to_file(self, bucket: str, key: str, file_path: str) -> int:
        """Full-object download to a local path via S3 transfer / CRT."""
        return await self.s3_client.download_object_to_file(bucket, key, file_path)

    async def head(self, bucket, key) -> Optional[dict]:
        try:
            return await asyncio.to_thread(self.s3_client.head_object, bucket, key)
        except Exception:
            return None

    async def delete(self, bucket, key):
        await asyncio.to_thread(self.s3_client.delete_object, bucket, key)

    async def delete_prefix(self, bucket, prefix):
        # S3Boto3.delete_prefix is async; do not use to_thread (that would return an un-awaited coroutine).
        await self.s3_client.delete_prefix(bucket, prefix)

    # ---------- JSON helpers ----------
    async def put_json(self, bucket, key, obj: dict):
        body = json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        await self.put(bucket, key, body)

    async def get_json(self, bucket, key) -> Optional[dict]:
        try:
            obj = await self.s3_client.get_object(bucket, key)
        except Exception:
            return None
        if obj is None:
            return None
        if isinstance(obj, (bytes, bytearray)):
            data = bytes(obj)
        elif isinstance(obj, dict) and "Body" in obj:
            data = obj["Body"].read()
        else:
            return None
        try:
            return json.loads(data.decode("utf-8"))
        except Exception:
            return None

    # ---------- MPU execution ----------
    async def mpu_execute_plan(
        self,
        dest_bucket: str,
        dest_key: str,
        tasks: Iterable[Dict[str, Any]],
        *,
        max_concurrency: int = 24,
        piece_concurrency: int = 8,
    ) -> dict:
        """
        Expected tasks:
          - copy: {'type':'copy','source_path','source_offset','length','logical_offset'}
          - fetch: {'type':'fetch','pieces':[...], 'length': L}

        Concurrency policy:
          - preassign one PartNumber per task
          - limit task concurrency with max_concurrency
          - fetch task pieces can be fetched concurrently with piece_concurrency
        """
        max_concurrency = _env_int("MPU_CONCURRENCY", max_concurrency)
        piece_concurrency = _env_int("MPU_PIECE_CONCURRENCY", piece_concurrency)
        min_part_size = 5 * 1024 * 1024

        def _task_length(t: Dict[str, Any]) -> int:
            if "length" in t:
                return int(t["length"])
            if t.get("type") == "fetch":
                return sum(int(p["length"]) for p in t.get("pieces", []))
            return 0

        def _task_pieces(t: Dict[str, Any]) -> List[Dict[str, Any]]:
            if t["type"] == "copy":
                return [{
                    "source_path": t["source_path"],
                    "source_offset": int(t["source_offset"]),
                    "length": int(t["length"]),
                    "logical_offset": int(t.get("logical_offset", 0)),
                }]
            return [
                {
                    "source_path": p["source_path"],
                    "source_offset": int(p["source_offset"]),
                    "length": int(p["length"]),
                    "logical_offset": int(p.get("logical_offset", 0)),
                }
                for p in t.get("pieces", [])
            ]

        def _task_from_pieces(pieces: List[Dict[str, Any]]) -> Dict[str, Any]:
            pieces = sorted(pieces, key=lambda p: p["logical_offset"])
            total_length = sum(int(p["length"]) for p in pieces)
            if len(pieces) == 1:
                p = pieces[0]
                if not self.is_local_source(p["source_path"]) and not is_zero_source(p["source_path"]):
                    return {
                        "type": "copy",
                        "source_path": p["source_path"],
                        "source_offset": p["source_offset"],
                        "length": p["length"],
                        "logical_offset": p["logical_offset"],
                    }
            return {"type": "fetch", "pieces": pieces, "length": total_length}

        def _normalize_mpu_tasks(input_tasks: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
            normalized: List[Dict[str, Any]] = []
            pending_pieces: List[Dict[str, Any]] = []
            pending_len = 0

            def flush_pending() -> None:
                nonlocal pending_pieces, pending_len
                if pending_pieces:
                    normalized.append(_task_from_pieces(pending_pieces))
                    pending_pieces = []
                    pending_len = 0

            for task in input_tasks:
                task_len = _task_length(task)
                if pending_pieces:
                    pending_pieces.extend(_task_pieces(task))
                    pending_len += task_len
                    if pending_len >= min_part_size:
                        flush_pending()
                    continue
                if task_len >= min_part_size:
                    normalized.append(task)
                    continue
                pending_pieces.extend(_task_pieces(task))
                pending_len += task_len

            flush_pending()

            for idx, task in enumerate(normalized[:-1], start=1):
                task_len = _task_length(task)
                if task_len < min_part_size:
                    raise ValueError(
                        f"normalized MPU task {idx} is smaller than S3 minimum: "
                        f"{task_len} < {min_part_size}"
                    )
            return normalized

        total_start = time.time()
        print_info(
            f"[cat_mpu_breakdown] phase=start dest={dest_bucket}/{dest_key} "
            f"max_concurrency={max_concurrency} piece_concurrency={piece_concurrency}"
        )

        phase_start = time.time()
        tasks = _normalize_mpu_tasks(tasks)
        copy_tasks = sum(1 for t in tasks if t.get("type") == "copy")
        fetch_tasks = sum(1 for t in tasks if t.get("type") == "fetch")
        copy_bytes = sum(int(t.get("length", 0)) for t in tasks if t.get("type") == "copy")
        fetch_bytes = sum(int(t.get("length", 0)) for t in tasks if t.get("type") == "fetch")
        fetch_pieces = sum(len(t.get("pieces", [])) for t in tasks if t.get("type") == "fetch")
        print_info(
            f"[cat_mpu_breakdown] phase=normalize elapsed_s={time.time() - phase_start:.6f} "
            f"tasks={len(tasks)} copy_tasks={copy_tasks} fetch_tasks={fetch_tasks} "
            f"fetch_pieces={fetch_pieces} copy_bytes={copy_bytes} fetch_bytes={fetch_bytes}"
        )
        if _env_bool("MPU_PLAN_TRACE"):
            limit = _env_int("MPU_PLAN_TRACE_LIMIT", 80)
            fetch_part_count = sum(1 for t in tasks if t.get("type") == "fetch")
            print_debug(
                f"[cat_mpu_fetch_plan] phase=normalized dest={dest_bucket}/{dest_key} "
                f"fetch_parts={fetch_part_count} fetch_pieces={fetch_pieces} "
                f"fetch_bytes={fetch_bytes}"
            )
            printed_fetch_parts = 0
            for part_idx, task in enumerate(tasks, start=1):
                if task.get("type") != "fetch":
                    continue
                if printed_fetch_parts >= limit:
                    break
                printed_fetch_parts += 1
                pieces = sorted(task.get("pieces", []), key=lambda p: int(p.get("logical_offset", 0)))
                kinds: Dict[str, int] = {}
                kind_bytes: Dict[str, int] = {}
                for piece in pieces:
                    kind = _source_kind(str(piece.get("source_path", "")))
                    piece_len = int(piece.get("length", 0))
                    kinds[kind] = kinds.get(kind, 0) + 1
                    kind_bytes[kind] = kind_bytes.get(kind, 0) + piece_len
                print_debug(
                    f"[cat_mpu_fetch_part] part={part_idx} length={int(task.get('length', 0))} "
                    f"pieces={len(pieces)} kinds={kinds} kind_bytes={kind_bytes} "
                    f"logical_contiguous={int(_pieces_are_logically_contiguous(pieces))} "
                    f"physical_contiguous={int(_pieces_are_physically_contiguous(pieces))}"
                )
                for piece_idx, piece in enumerate(pieces[:limit], start=1):
                    print_debug(
                        f"[cat_mpu_fetch_piece] part={part_idx} piece={piece_idx} "
                        f"kind={_source_kind(str(piece.get('source_path', '')))} "
                        f"logical={_fmt_range(int(piece.get('logical_offset', 0)), int(piece.get('length', 0)))} "
                        f"source_offset={int(piece.get('source_offset', 0))} "
                        f"source={_fmt_source(str(piece.get('source_path', '')))}"
                    )
                if len(pieces) > limit:
                    print_debug(
                        f"[cat_mpu_fetch_piece] part={part_idx} omitted={len(pieces) - limit} limit={limit}"
                    )
            if fetch_part_count > printed_fetch_parts:
                print_debug(
                    f"[cat_mpu_fetch_part] omitted={fetch_part_count - printed_fetch_parts} "
                    f"limit={limit}"
                )
        if not tasks:
            return {"UploadId": None, "Parts": []}

        def _split(path: str) -> Tuple[str, str]:
            b, k = path.split("/", 1)
            return b, k

        phase_start = time.time()
        mpu = await self.s3_client.create_multipart_upload(dest_bucket, dest_key)
        upload_id = mpu["UploadId"]
        print_info(
            f"[cat_mpu_breakdown] phase=create_mpu elapsed_s={time.time() - phase_start:.6f} "
            f"upload_id={upload_id}"
        )

        task_list: List[Tuple[int, Dict[str, Any]]] = [
            (i, t) for i, t in enumerate(tasks, start=1)
        ]
        part_sem = asyncio.Semaphore(max_concurrency)

        async def _run_one(part_num: int, t: Dict[str, Any], enqueued_at: float) -> Dict[str, Any]:
            async with part_sem:
                part_start = time.time()
                queue_wait_s = part_start - enqueued_at
                trace_part_timing = _env_bool("MPU_PLAN_TRACE")
                if trace_part_timing:
                    print_debug(
                        f"[cat_mpu_part_start] part={part_num} type={t.get('type')} "
                        f"queue_wait_s={queue_wait_s:.6f}"
                    )
                try:
                    if t["type"] == "copy":
                        copy_start = time.time()
                        src_b, src_k = _split(t["source_path"])
                        start = t["source_offset"]
                        end = t["source_offset"] + t["length"] - 1
                        res = await self.s3_client.upload_part_copy(
                            dest_bucket, dest_key, upload_id, part_num,
                            src_b, src_k, start, end
                        )
                        etag = None
                        if isinstance(res, dict):
                            etag = res.get("ETag")
                            if etag is None and isinstance(res.get("CopyPartResult"), dict):
                                etag = res["CopyPartResult"].get("ETag")
                        if not etag:
                            raise ValueError("upload_part_copy returned unexpected format without ETag")
                        copy_s = time.time() - copy_start
                        total_s = time.time() - part_start
                        if trace_part_timing:
                            print_debug(
                                f"[cat_mpu_part_timing] part={part_num} type=copy "
                                f"queue_wait_s={queue_wait_s:.6f} copy_s={copy_s:.6f} "
                                f"total_s={total_s:.6f} bytes={int(t.get('length', 0))}"
                            )
                            print_debug(
                                f"[cat_mpu_part_done] part={part_num} type=copy total_s={total_s:.6f}"
                            )
                        return {"ETag": etag, "PartNumber": part_num}

                    elif t["type"] == "fetch":
                        pieces = list(t["pieces"])
                        pieces.sort(key=lambda p: p["logical_offset"])

                        fetch_start = time.time()
                        body, coalesced_reads, fetch_timing = await self._fetch_pieces_coalesced(
                            pieces, piece_concurrency)
                        fetch_s = time.time() - fetch_start
                        if _env_bool("MPU_PLAN_TRACE"):
                            print_debug(
                                f"[cat_mpu_fetch_coalesce] part={part_num} "
                                f"pieces={len(pieces)} source_reads={coalesced_reads}"
                            )
                            print_debug(
                                f"[cat_mpu_fetch_timing] part={part_num} "
                                f"plan_s={fetch_timing['plan_s']:.6f} "
                                f"read_s={fetch_timing['read_s']:.6f} "
                                f"scatter_s={fetch_timing['scatter_s']:.6f} "
                                f"total_s={fetch_timing['total_s']:.6f}"
                            )

                        if "length" in t and len(body) != t["length"]:
                            raise ValueError(f"assembled body length {len(body)} != declared {t['length']} for part {part_num}")

                        upload_start = time.time()
                        res = await self.s3_client.upload_part(
                            dest_bucket, dest_key, upload_id, part_num, body
                        )
                        upload_s = time.time() - upload_start
                        etag = res.get("ETag") if isinstance(res, dict) else None
                        if not etag:
                            raise ValueError("upload_part returned unexpected format without ETag")
                        total_s = time.time() - part_start
                        if trace_part_timing:
                            print_debug(
                                f"[cat_mpu_part_timing] part={part_num} type=fetch "
                                f"queue_wait_s={queue_wait_s:.6f} fetch_s={fetch_s:.6f} "
                                f"upload_s={upload_s:.6f} total_s={total_s:.6f} "
                                f"bytes={len(body)} source_reads={coalesced_reads}"
                            )
                            print_debug(
                                f"[cat_mpu_part_done] part={part_num} type=fetch total_s={total_s:.6f}"
                            )
                        return {"ETag": etag, "PartNumber": part_num}

                    else:
                        raise ValueError(f"unknown task type: {t.get('type')}")
                except Exception as e:
                    print_error(
                        f"[s3_storage] part FAILED part={part_num} type={t.get('type')} "
                        f"length={t.get('length')} logical_offset={t.get('logical_offset')} "
                        f"source_path={t.get('source_path')} source_offset={t.get('source_offset')} "
                        f"pieces={len(t.get('pieces', [])) if isinstance(t.get('pieces'), list) else 0} "
                        f"error={type(e).__name__}: {e}"
                    )
                    raise

        results: List[Dict[str, Any]] = []
        try:
            phase_start = time.time()
            results = await asyncio.gather(
                *[_run_one(part_num, t, time.time()) for part_num, t in task_list],
                return_exceptions=False,
            )
            print_info(
                f"[cat_mpu_breakdown] phase=upload_parts elapsed_s={time.time() - phase_start:.6f} "
                f"parts={len(results)} copy_tasks={copy_tasks} fetch_tasks={fetch_tasks} "
                f"copy_bytes={copy_bytes} fetch_bytes={fetch_bytes}"
            )

            phase_start = time.time()
            results.sort(key=lambda x: x["PartNumber"])
            complete_res = await self.s3_client.complete_multipart_upload(
                dest_bucket, dest_key, upload_id, results)
            if not complete_res:
                raise RuntimeError(
                    f"complete_multipart_upload failed for {dest_bucket}/{dest_key} "
                    f"upload_id={upload_id} parts={len(results)}")
            print_info(
                f"[cat_mpu_breakdown] phase=complete_mpu elapsed_s={time.time() - phase_start:.6f} "
                f"parts={len(results)} upload_id={upload_id}"
            )
            print_info(
                f"[cat_mpu_breakdown] phase=done total_elapsed_s={time.time() - total_start:.6f} "
                f"parts={len(results)} upload_id={upload_id}"
            )
            return {"UploadId": upload_id, "Parts": results, "CompleteResult": complete_res}

        except Exception as e:
            print_error(
                f"[s3_storage] mpu_execute_plan FAILED dest=s3://{dest_bucket}/{dest_key} "
                f"upload_id={upload_id} parts={len(task_list)} error={type(e).__name__}: {e}"
            )
            traceback.print_exc()
            try:
                phase_start = time.time()
                await self.s3_client.abort_multipart_upload(dest_bucket, dest_key, upload_id)
                print_info(
                    f"[cat_mpu_breakdown] phase=abort_mpu elapsed_s={time.time() - phase_start:.6f} "
                    f"upload_id={upload_id}"
                )
            finally:
                raise

    # ---------- Simple sync path ----------
    async def sync_simple_execute(
        self,
        dest_bucket: str,
        dest_key: str,
        object_map,
        file_size: int,
        *,
        max_concurrency: int = 24,
    ) -> dict:
        """Download mapped segments, merge them locally, and upload the merged file."""
        def _split(path: str) -> Tuple[str, str]:
            b, k = path.split("/", 1)
            return b, k

        segments = object_map.segments
        if not segments:
            raise ValueError("object_map has no segments")

        temp_file = tempfile.NamedTemporaryFile(delete=False)
        temp_file_path = temp_file.name
        temp_file.close()

        try:
            download_start = time.time()
            download_sem = asyncio.Semaphore(max_concurrency)

            async def _download_segment(seg):
                async with download_sem:
                    start = seg.source_offset
                    end = seg.source_offset + seg.length - 1
                    data = await self.get_source_range(seg.source_path, start, end)
                    return seg.offset, data

            results = await asyncio.gather(*[_download_segment(seg) for seg in segments])
            download_time = time.time() - download_start
            print_info(f"sync_simple download_elapsed_s={download_time:.2f}")

            merge_start = time.time()
            results.sort(key=lambda x: x[0])
            with open(temp_file_path, 'wb') as f:
                current_offset = 0
                for offset, data in results:
                    if current_offset < offset:
                        gap_size = offset - current_offset
                        f.write(b'\x00' * gap_size)
                        current_offset = offset

                    expected_length = len(data)
                    if offset + expected_length > file_size:
                        raise ValueError(f"Segment at offset {offset} exceeds file_size {file_size}")

                    f.write(data)
                    current_offset += len(data)

                if current_offset < file_size:
                    gap_size = file_size - current_offset
                    f.write(b'\x00' * gap_size)

                f.flush()
                actual_size = os.path.getsize(temp_file_path)
                if actual_size != file_size:
                    raise ValueError(f"Merged file size {actual_size} != expected file_size {file_size}")
            merge_time = time.time() - merge_start
            print_info(f"sync_simple merge_elapsed_s={merge_time:.2f}")

            upload_start = time.time()
            await self.put_from_file(dest_bucket, dest_key, temp_file_path, file_size)
            upload_time = time.time() - upload_start
            print_info(f"sync_simple upload_elapsed_s={upload_time:.2f}")

            return {
                "bucket": dest_bucket,
                "key": dest_key,
                "file_size": file_size,
                "segments_count": len(segments)
            }

        finally:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)

