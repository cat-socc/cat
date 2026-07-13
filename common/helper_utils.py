
from collections import defaultdict
from typing import List, Tuple, Dict
import mmap
import os
from pathlib import Path
from datetime import datetime, timezone
from dateutil import parser
import aiofiles
from common.models import Range, PatchRangeRequest, DataRangeRequest, FilePathRequest, AtomicPutRequest

# === Helper: Split ranges across chunks ===
def split_patch_range(offset: int, length: int, chunk_size: int) -> List[Tuple[int, int, int]]:
    """
    Split a single patch range into parts mapped to chunk-local ranges.

    Returns:
        List of (chunk_index, inner_offset, length) tuples.
    """
    parts = []
    end = offset + length
    while offset < end:
        index = offset // chunk_size
        inner_offset = offset % chunk_size
        l = min(chunk_size - inner_offset, end - offset)
        parts.append((index, inner_offset, l))
        offset += l
    return parts


def get_utc_timestamp(): 
    """
    Get the current UTC timestamp in ISO format.
    """
    return datetime.now(timezone.utc).isoformat() + 'Z'

def parse_timestamp(ts): 
    """
    Parse a UTC timestamp string into a datetime object.
    
    Args:
        ts (str): Timestamp string in ISO format.
    
    Returns:
        datetime: Parsed datetime object.
    """
    return parser.isoparse(ts)

def make_data_provider_from_bytes(data: bytes):
    # 记录当前已经处理的数据长度
    processed_length = 0
    
    async def provider(range_info: Range):
        nonlocal processed_length
        # 从data中按顺序取出对应长度的数据
        start = processed_length
        end = start + range_info.length
        result = data[start:end]
        processed_length = end
        return result
    
    return provider

def make_data_provider_from_file(file_path: str, source_offsets_match_range_offsets: bool = False):
    # By default this matches bytes/delta providers: ranges are packed sequentially. When the source
    # is a full local cache file, each range's payload starts at range.offset in that file.
    processed_length = 0

    async def provider(range_info: Range):
        nonlocal processed_length
        start = range_info.offset if source_offsets_match_range_offsets else processed_length
        length = range_info.length
        async with aiofiles.open(file_path, 'rb') as f:
            await f.seek(start)
            chunk = await f.read(length)
        if not source_offsets_match_range_offsets:
            processed_length = start + len(chunk)
        return chunk
    return provider

def _coalesce_payload_fragments(
    fragments: List[Tuple[int, int, int]]
) -> List[Tuple[int, int, int]]:
    if not fragments:
        return []
    fragments.sort(key=lambda item: item[0])
    merged: List[Tuple[int, int, int]] = []
    for offset, length, payload_offset in fragments:
        if length <= 0:
            continue
        if merged:
            prev_offset, prev_length, prev_payload_offset = merged[-1]
            if (
                prev_offset + prev_length == offset
                and prev_payload_offset + prev_length == payload_offset
            ):
                merged[-1] = (prev_offset, prev_length + length, prev_payload_offset)
                continue
        merged.append((offset, length, payload_offset))
    return merged

def _packed_payload_sources(
    ranges: List[Range],
) -> Tuple[List[Tuple[Range, int]], bool, bool, int]:
    sources: List[Tuple[Range, int]] = []
    packed_cursor = 0
    sorted_non_overlapping = True
    last_start = None
    last_end = None

    for r in ranges:
        start = int(r.offset)
        length = int(r.length)
        end = start + length
        sources.append((r, packed_cursor))
        packed_cursor += length
        if length <= 0:
            continue
        if last_end is not None:
            if start < last_end or (last_start is not None and start < last_start):
                sorted_non_overlapping = False
        last_start = start
        last_end = end

    non_overlapping = True
    if not sorted_non_overlapping:
        ordered = sorted(
            (
                (int(r.offset), int(r.offset) + int(r.length))
                for r in ranges
                if int(r.length) > 0
            ),
            key=lambda item: item[0],
        )
        for (_, prev_end_sorted), (start, _) in zip(ordered, ordered[1:]):
            if start < prev_end_sorted:
                non_overlapping = False
                break
    return sources, sorted_non_overlapping, non_overlapping, packed_cursor

def _resolve_ordered_payload_ranges(
    ranges: List[Range],
) -> List[Tuple[Range, int]]:
    """
    Resolve packed range writes using write-order overwrite semantics.

    Returns final non-overlapping logical fragments as (range, payload_offset_in_original_data).
    Later writes overwrite earlier writes on overlap.
    """
    sources, sorted_non_overlapping, non_overlapping, packed_cursor = _packed_payload_sources(ranges)
    if sorted_non_overlapping:
        return sources
    if non_overlapping:
        return sorted(sources, key=lambda item: int(item[0].offset))

    fragments: List[Tuple[int, int, int]] = []
    packed_cursor = 0
    for r in ranges:
        new_start = int(r.offset)
        new_len = int(r.length)
        new_end = new_start + new_len
        new_payload_offset = packed_cursor
        packed_cursor += new_len
        if new_len <= 0:
            continue

        next_fragments: List[Tuple[int, int, int]] = []
        for old_start, old_len, old_payload_offset in fragments:
            old_end = old_start + old_len
            if old_end <= new_start or old_start >= new_end:
                next_fragments.append((old_start, old_len, old_payload_offset))
                continue
            if old_start < new_start:
                left_len = new_start - old_start
                next_fragments.append((old_start, left_len, old_payload_offset))
            if old_end > new_end:
                right_start = new_end
                right_len = old_end - new_end
                right_payload_offset = old_payload_offset + (right_start - old_start)
                next_fragments.append((right_start, right_len, right_payload_offset))

        next_fragments.append((new_start, new_len, new_payload_offset))
        fragments = _coalesce_payload_fragments(next_fragments)

    required = sum(int(r.length) for r in ranges)
    if packed_cursor != required:
        raise ValueError(f"packed cursor mismatch: cursor={packed_cursor} required={required}")
    return [(Range(offset=offset, length=length), payload_offset)
            for offset, length, payload_offset in fragments]

def _resolve_absolute_file_ranges(ranges: List[Range]) -> List[Range]:
    """
    Normalize ranges whose source offsets match their logical offsets.

    This path reads from a full local cache file that already contains the final bytes, so
    overlapping ranges can be collapsed to their union before writing the bundle.
    """
    intervals: List[Tuple[int, int]] = []
    for r in ranges:
        start = int(r.offset)
        end = start + int(r.length)
        if end > start:
            intervals.append((start, end))
    if not intervals:
        return []
    intervals.sort()
    merged: List[Tuple[int, int]] = []
    cur_start, cur_end = intervals[0]
    for start, end in intervals[1:]:
        if start <= cur_end:
            cur_end = max(cur_end, end)
        else:
            merged.append((cur_start, cur_end))
            cur_start, cur_end = start, end
    merged.append((cur_start, cur_end))
    return [Range(offset=start, length=end - start) for start, end in merged]

def build_bundled_payload_bytes(
    ranges: List[Range],
    data: bytes,
    dest_path: str,
) -> Tuple[int, List[Tuple[Range, int]]]:
    """
    Write packed range payload bytes into dest_path in logical-offset order.

    The incoming bytes are still interpreted in the original ranges order. This preserves CAT's
    packed-delta ABI while making the bundle layout friendlier to later MPU fetches.
    """
    if not ranges:
        return 0, []
    required = sum(int(r.length) for r in ranges)
    payload = memoryview(data)
    if payload.itemsize != 1:
        payload = payload.cast("B")
    if payload.nbytes < required:
        raise ValueError(
            f"bundle bytes payload too small: need {required} bytes, payload has {payload.nbytes}"
        )

    segments: List[Tuple[Range, int]] = []
    payload_offset = 0

    source_fragments, sorted_non_overlapping, non_overlapping, _ = _packed_payload_sources(ranges)
    if sorted_non_overlapping:
        with open(dest_path, "wb") as out:
            out.write(payload[:required])
        for r, source_payload_offset in source_fragments:
            if int(r.length) > 0:
                segments.append((r, source_payload_offset))
        return required, segments

    if non_overlapping:
        source_fragments = sorted(source_fragments, key=lambda item: int(item[0].offset))
    else:
        source_fragments = _resolve_ordered_payload_ranges(ranges)

    with open(dest_path, "wb") as out:
        for r, source_payload_offset in source_fragments:
            r_len = int(r.length)
            out.write(payload[source_payload_offset:source_payload_offset + r_len])
            segments.append((r, payload_offset))
            payload_offset += r_len
    return payload_offset, segments


def build_bundled_payload_file(
    file_path: str,
    ranges: List[Range],
    dest_path: str,
    *,
    source_offsets_match_range_offsets: bool = False,
) -> Tuple[int, List[Tuple[Range, int]]]:
    """
    Write all range payloads into dest_path as one contiguous bundle.

    Maps file_path once with mmap instead of per-range open/seek/read.
    Returns (total_bytes_written, [(range, payload_offset_in_bundle), ...]).
    """
    path = str(Path(file_path).expanduser().resolve())
    file_sz = os.path.getsize(path)
    if not ranges:
        return 0, []

    if source_offsets_match_range_offsets:
        required = max(int(r.offset) + int(r.length) for r in ranges)
    else:
        required = sum(int(r.length) for r in ranges)
    if file_sz < required:
        raise ValueError(
            f"bundle source file too small: need {required} bytes, file has {file_sz}: {path}"
        )

    segments: List[Tuple[Range, int]] = []
    payload_offset = 0

    if source_offsets_match_range_offsets:
        bundle_sources = [
            (r, int(r.offset))
            for r in _resolve_absolute_file_ranges(ranges)
        ]
    else:
        bundle_sources = _resolve_ordered_payload_ranges(ranges)

    with open(path, "rb") as src:
        with mmap.mmap(src.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            with open(dest_path, "wb") as out:
                for r, source_start in bundle_sources:
                    r_len = int(r.length)
                    start = int(source_start)
                    end = start + r_len
                    if end > file_sz:
                        raise ValueError(
                            f"bundle read out of bounds: offset={start} length={r_len} file_size={file_sz}"
                        )
                    # Slice mmap directly (bytes copy). Do not use memoryview slices: they pin
                    # the mmap buffer and trigger "cannot close exported pointers exist".
                    out.write(mm[start:end])
                    segments.append((r, payload_offset))
                    payload_offset += r_len
    return payload_offset, segments


def split_ranges(ranges: List[Range], chunk_size: int = 5 * 1024 * 1024) -> List[Range]:
    new_ranges = []
    for range in ranges:
        new_ranges.extend(split_range(range.offset, range.offset + range.length, chunk_size))
    return new_ranges
    
def split_range(start_offset: int, end_offset: int, chunk_size: int = 5 * 1024 * 1024) -> List[Range]:
        total_len = end_offset - start_offset
        ranges = []
        current_offset = start_offset
        remaining = total_len

        while remaining >= chunk_size:
            ranges.append(Range(offset=current_offset, length=chunk_size))
            current_offset += chunk_size
            remaining -= chunk_size
        
        if remaining > 0:
            if ranges:
                # 合并到上一个
                ranges[-1].length += remaining
            else:
                # 唯一一个range就不足chunk_size，也要保留
                ranges.append(Range(offset=current_offset, length=remaining))


        return ranges
