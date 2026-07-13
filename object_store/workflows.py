from typing import Any, Callable, List, Optional

from common.cat_delta_ops import parse_cat_delta_ops, sum_write_bytes
from common.models import Range
from object_store.objects_manager import ObjectsManager


LogFn = Callable[[str], None]


def _noop_log(_: str) -> None:
    pass


def _as_ranges(ranges: Optional[List[Range]]) -> List[Range]:
    return ranges if isinstance(ranges, list) else []


def _is_full_payload(logical_size: Optional[int], total_modified_size: Optional[int]) -> bool:
    return (
        logical_size is not None
        and total_modified_size is not None
        and int(total_modified_size) > 0
        and int(logical_size) == int(total_modified_size)
    )


async def atomic_put(
    objects_manager: ObjectsManager,
    bucket: str,
    key: str,
    *,
    ranges: Optional[List[Range]] = None,
    file_path: Optional[str] = None,
    file_size: Optional[int] = None,
    total_modified_size: Optional[int] = None,
    data: Optional[Any] = None,
    source_offsets_match_range_offsets: bool = False,
) -> bool:
    """
    Shared full-vs-range write routing used by HTTP and bridge adapters.
    """
    parsed_ranges = _as_ranges(ranges)

    if file_path and file_size is not None:
        file_size_i = int(file_size)
        if _is_full_payload(file_size_i, total_modified_size):
            return await objects_manager.write_full_object_from_file_path_to_snapshot_bucket(
                bucket, key, file_path, file_size_i
            )
        return await objects_manager.write_range_object_from_file_path(
            bucket,
            key,
            parsed_ranges,
            file_path,
            file_size_i,
            source_offsets_match_range_offsets=source_offsets_match_range_offsets,
        )

    if data is not None:
        data_bytes = bytes(data)
        if _is_full_payload(len(data_bytes), total_modified_size):
            return await objects_manager.write_full_object_to_snapshot_bucket(
                bucket, key, data_bytes
            )
        return await objects_manager.write_range_object(
            bucket, key, parsed_ranges, data_bytes
        )

    raise ValueError("must provide file_path+file_size or data")


async def object_sync(
    objects_manager: ObjectsManager,
    bucket: str,
    key: str,
    *,
    ranges: Optional[List[Range]] = None,
    file_path: Optional[str] = None,
    file_size: Optional[int] = None,
    data: Optional[Any] = None,
    source_offsets_match_range_offsets: bool = False,
) -> bool:
    """
    Shared object_sync routing. With local payload, stage local ranges and compact;
    without payload, compact existing shadow metadata.
    """
    parsed_ranges = _as_ranges(ranges)

    if file_path or data is not None:
        if not parsed_ranges:
            raise ValueError("local object sync requires ranges")
        if file_path:
            return await objects_manager.compact_file_with_local_ranges_from_file_path(
                bucket,
                key,
                parsed_ranges,
                file_path,
                file_size,
                source_offsets_match_range_offsets=source_offsets_match_range_offsets,
            )
        return await objects_manager.compact_file_with_local_ranges_from_bytes(
            bucket, key, parsed_ranges, bytes(data or b""), file_size=file_size
        )

    return await objects_manager.compact_file(bucket, key)


async def apply_delta_file(
    objects_manager: ObjectsManager,
    bucket: str,
    key: str,
    *,
    delta_path: str,
    logical_size: int,
    ops: List[Any],
    total_modified_size: int,
    source_offsets_match_range_offsets: bool = False,
    log: LogFn = _noop_log,
) -> bool:
    """
    Apply CAT delta ops whose WRITE payloads live in a file.
    """
    om = objects_manager.get_manager(bucket, key)
    write_ranges, truncates = parse_cat_delta_ops(ops)
    logical_size_i = int(logical_size)
    total_modified_i = int(total_modified_size)

    object_missing = await om.get_meta_if_exists(file_size=logical_size_i) is None
    if object_missing:
        log(f"path=BOOTSTRAP_FULL_OBJECT s3://{bucket}/{key} file={delta_path!r}")
        return await objects_manager.full_object_rewrite_from_delta_file(
            bucket, key, str(delta_path), logical_size_i, ops, total_modified_i
        )

    if write_ranges:
        full_object_from_delta = (
            not source_offsets_match_range_offsets
            and not truncates
            and _is_full_payload(logical_size_i, total_modified_i)
        )
        if full_object_from_delta:
            log(f"path=FULL_OBJECT_PUT s3://{bucket}/{key} file={delta_path!r}")
            ok = await objects_manager.write_full_object_from_file_path_to_snapshot_bucket(
                bucket, key, delta_path, logical_size_i
            )
        else:
            log(f"path=RANGE_PUT s3://{bucket}/{key} ranges={len(write_ranges)}")
            ok = await objects_manager.write_range_object_from_file_path(
                bucket,
                key,
                write_ranges,
                delta_path,
                logical_size_i,
                source_offsets_match_range_offsets=source_offsets_match_range_offsets,
            )
        if not ok:
            return False

    await apply_truncates(objects_manager, bucket, key, truncates, log=log)
    return True


async def apply_delta_bytes(
    objects_manager: ObjectsManager,
    bucket: str,
    key: str,
    *,
    delta_bytes: Any,
    logical_size: int,
    ops: List[Any],
    total_modified_size: int,
    log: LogFn = _noop_log,
) -> bool:
    """
    Apply CAT delta ops whose WRITE payloads are supplied as packed bytes.
    """
    payload = memoryview(delta_bytes)
    om = objects_manager.get_manager(bucket, key)
    write_ranges, truncates = parse_cat_delta_ops(ops)
    logical_size_i = int(logical_size)
    total_modified_i = int(total_modified_size)

    write_bytes = sum_write_bytes(ops)
    if payload.nbytes < write_bytes:
        raise RuntimeError(
            f"delta payload too small: payload={payload.nbytes} write_bytes={write_bytes}"
        )

    object_missing = await om.get_meta_if_exists(file_size=logical_size_i) is None
    if object_missing:
        full_object_from_delta = (
            write_ranges and not truncates and _is_full_payload(logical_size_i, total_modified_i)
        )
        if not full_object_from_delta:
            raise RuntimeError("cannot bootstrap sparse/missing object from delta bytes")
        return await objects_manager.write_full_object_to_snapshot_bucket(
            bucket, key, bytes(payload)
        )

    if write_ranges:
        full_object_from_delta = (
            not truncates and _is_full_payload(logical_size_i, total_modified_i)
        )
        if full_object_from_delta:
            ok = await objects_manager.write_full_object_to_snapshot_bucket(
                bucket, key, bytes(payload)
            )
        else:
            ok = await objects_manager.write_range_object(
                bucket, key, write_ranges, payload
            )
        if not ok:
            return False

    await apply_truncates(objects_manager, bucket, key, truncates, log=log)
    return True


async def apply_truncates(
    objects_manager: ObjectsManager,
    bucket: str,
    key: str,
    truncates: List[int],
    *,
    log: LogFn = _noop_log,
) -> None:
    if not truncates:
        return
    om = objects_manager.get_manager(bucket, key)
    for i, new_sz in enumerate(truncates):
        log(f"truncate[{i}] new file_size={new_sz}")
        mm = await om._get_meta()
        mm.file_size = int(new_sz)
        await om.save_meta()
