from typing import Any, Iterable, List, Optional, Tuple

from common.models import Range


WRITE = 1
TRUNCATE = 2


DeltaOp = Tuple[int, int, int, int]


def normalize_delta_op(op: Any) -> DeltaOp:
    return int(op[0]), int(op[1]), int(op[2]), int(op[3])


def coalesce_adjacent_write_ops(
    ops: Iterable[Any],
    max_merged_size: Optional[int] = None,
    *,
    skip_zero_length_writes: bool = True,
) -> List[DeltaOp]:
    """
    Merge adjacent WRITE ops when object ranges and source ranges are contiguous.

    TRUNCATE ops are never crossed, so operation ordering is preserved. When
    max_merged_size is set, merged WRITE chunks are capped at that size.
    """
    merged: List[DeltaOp] = []
    cur: Optional[DeltaOp] = None

    for op in ops:
        op_type, object_offset, length, delta_offset = normalize_delta_op(op)

        if op_type == WRITE and length <= 0 and skip_zero_length_writes:
            continue

        if op_type != WRITE or length <= 0:
            if cur is not None:
                merged.append(cur)
                cur = None
            merged.append((op_type, object_offset, length, delta_offset))
            continue

        if cur is not None:
            cur_type, cur_obj, cur_len, cur_delta = cur
            within_cap = (
                max_merged_size is None
                or cur_len + length <= max_merged_size
            )
            if (
                cur_type == WRITE
                and object_offset == cur_obj + cur_len
                and delta_offset == cur_delta + cur_len
                and within_cap
            ):
                cur = (WRITE, cur_obj, cur_len + length, cur_delta)
                continue

        if cur is not None:
            merged.append(cur)
        cur = (WRITE, object_offset, length, delta_offset)

    if cur is not None:
        merged.append(cur)
    return merged


def can_coalesce_absolute_write_intervals(ops: Iterable[Any]) -> bool:
    """
    Interval coalescing is only safe when source offsets match object offsets.
    """
    for op in ops:
        op_type, object_offset, length, delta_offset = normalize_delta_op(op)
        if op_type == TRUNCATE:
            return False
        if op_type == WRITE and length > 0 and object_offset != delta_offset:
            return False
    return True


def coalesce_absolute_write_intervals_by_object_offset(
    ops: Iterable[Any],
    max_merged_size: int = 64 * 1024 * 1024,
) -> List[DeltaOp]:
    """
    Sort WRITE ops by object offset and union overlapping/adjacent intervals.

    Returned WRITE ops use source_offset == object_offset. This is intended for
    full local-cache sources, not packed delta payload files.
    """
    ops_list = [normalize_delta_op(op) for op in ops]
    if not can_coalesce_absolute_write_intervals(ops_list):
        return ops_list

    intervals: List[Tuple[int, int]] = []
    passthrough: List[DeltaOp] = []
    for op_type, object_offset, length, delta_offset in ops_list:
        if op_type == WRITE and length > 0:
            intervals.append((object_offset, object_offset + length))
        else:
            passthrough.append((op_type, object_offset, length, delta_offset))

    if not intervals:
        return passthrough

    intervals.sort()
    merged: List[Tuple[int, int]] = []
    cur_start, cur_end = intervals[0]
    for start, end in intervals[1:]:
        if start <= cur_end:
            cur_end = max(cur_end, end)
            continue
        merged.append((cur_start, cur_end))
        cur_start, cur_end = start, end
    merged.append((cur_start, cur_end))

    coalesced: List[DeltaOp] = []
    for start, end in merged:
        chunk_start = start
        while chunk_start < end:
            chunk_len = min(max_merged_size, end - chunk_start)
            coalesced.append((WRITE, chunk_start, chunk_len, chunk_start))
            chunk_start += chunk_len
    return passthrough + coalesced


def sum_write_bytes(ops: Iterable[Any]) -> int:
    total = 0
    for op in ops:
        op_type, _, length, _ = normalize_delta_op(op)
        if op_type == WRITE and length > 0:
            total += length
    return total


def parse_cat_delta_ops(
    ops: Iterable[Any],
    *,
    skip_zero_length_writes: bool = True,
) -> Tuple[List[Range], List[int]]:
    """
    Split CAT delta ops into write ranges and truncate sizes.
    """
    write_ranges: List[Range] = []
    truncates: List[int] = []
    for op in ops:
        op_type, object_offset, length, _ = normalize_delta_op(op)
        if op_type == WRITE:
            if not skip_zero_length_writes or length > 0:
                write_ranges.append(Range(offset=object_offset, length=length))
        elif op_type == TRUNCATE:
            truncates.append(object_offset)
        else:
            raise RuntimeError(f"unknown CAT delta op: {op_type}")
    return write_ranges, truncates
