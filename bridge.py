import asyncio
import gc
import os
import sys
import time
from typing import Optional

# Allow direct imports when this module is loaded outside the app package.
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from common.cat_delta_ops import (
    coalesce_absolute_write_intervals_by_object_offset,
    coalesce_adjacent_write_ops,
    parse_cat_delta_ops,
    sum_write_bytes,
)
from common.logger import print_debug, print_info
from core.dependencies import get_object_manager
from object_store.workflows import (
    apply_delta_bytes,
    apply_delta_file,
    atomic_put,
)


def _bridge_debug(msg: str) -> None:
    print_debug(f"[bridge] {msg}")


def _bridge_info(msg: str) -> None:
    print_info(f"[bridge] {msg}")


# Reuse one event loop across bridge calls.
_bridge_loop: Optional[asyncio.AbstractEventLoop] = None
MAX_BRIDGE_MERGED_WRITE_SIZE = 64 * 1024 * 1024


def _join_default_executor_without_poisoning_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Wait for the loop's default ThreadPoolExecutor workers (run_in_executor(None, ...)).

    Do not call ``loop.shutdown_default_executor()`` on a reused loop. Python marks the default
    executor as permanently shut down, which breaks later calls that use ``run_in_executor(None)``.
    """
    ex = getattr(loop, "_default_executor", None)
    if ex is not None:
        ex.shutdown(wait=True)
        loop._default_executor = None  # type: ignore[attr-defined]


def _run_asyncio_bridge(coro):
    """Run *coro* on a long-lived loop and join default executor threads before returning."""
    global _bridge_loop
    if _bridge_loop is None or _bridge_loop.is_closed():
        _bridge_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_bridge_loop)
    ret = _bridge_loop.run_until_complete(coro)
    _join_default_executor_without_poisoning_loop(_bridge_loop)
    gc.collect()
    return ret


def warmup_bridge():
    """Initialize shared runtime state before the first real operation."""
    _bridge_info("warmup_bridge start")

    async def _run():
        await get_object_manager()
        return True

    try:
        ret = _run_asyncio_bridge(_run())
        _bridge_info("warmup_bridge done")
        return ret
    except BaseException as e:
        _bridge_info(f"warmup_bridge failed: {type(e).__name__}: {e}")
        raise


def rangeput_delta_bridge(
    bucket,
    key,
    delta_path,
    logical_size,
    ops,
    total_modified_size,
    arg1=None,
    arg2=None,
):
    """
    Apply CAT delta log to object store.

    Each op is (type, object_offset, length, delta_offset):
      type 1 (WRITE): payload at delta_path[delta_offset : delta_offset+length] -> object byte range
      type 2 (TRUNCATE): logical size = object_offset (length/delta_offset unused)

    Delta file layout defaults to WRITE payloads appended in op order. When arg1 is truthy, the
    delta_path is a full local cache file and each op's delta_offset is an absolute file offset.
    """
    bridge_start = time.time()
    source_offsets_match_range_offsets = bool(arg1)
    _bridge_info(
        f"rangeput_delta_bridge start bucket={bucket!r} key={key!r} "
        f"delta_path={delta_path!r} logical_size={logical_size} "
        f"total_modified_size={total_modified_size} op_count={len(ops)} "
        f"source_offsets_match_range_offsets={source_offsets_match_range_offsets}"
    )
    original_op_count = len(ops)
    original_write_bytes = sum_write_bytes(ops)
    ops = coalesce_adjacent_write_ops(
        ops,
        MAX_BRIDGE_MERGED_WRITE_SIZE,
        skip_zero_length_writes=False,
    )
    if source_offsets_match_range_offsets:
        adjacent_op_count = len(ops)
        ops = coalesce_absolute_write_intervals_by_object_offset(ops)
        _bridge_debug(
            f"rangeput_delta_bridge coalesced ops "
            f"original={original_op_count} adjacent={adjacent_op_count} interval={len(ops)} "
            f"write_bytes={original_write_bytes}->{sum_write_bytes(ops)}"
        )
    async def _run():
        _bridge_debug("await get_object_manager()")
        manager = await get_object_manager()
        _bridge_debug("get_object_manager() done")
        write_ranges, truncates = parse_cat_delta_ops(ops)

        logical_size_i = int(logical_size)
        total_modified_i = int(total_modified_size)

        _bridge_debug(
            f"parsed write_ranges={len(write_ranges)} truncates={len(truncates)} "
            f"logical_size_i={logical_size_i} total_modified_i={total_modified_i}"
        )

        ok = await apply_delta_file(
            manager,
            bucket,
            key,
            delta_path=str(delta_path),
            logical_size=logical_size_i,
            ops=ops,
            total_modified_size=total_modified_i,
            source_offsets_match_range_offsets=source_offsets_match_range_offsets,
            log=_bridge_debug,
        )
        if not ok:
            raise RuntimeError("apply_delta_file failed")

        _bridge_info(
            f"rangeput_delta_bridge done bucket={bucket!r} key={key!r} "
            f"logical_size={logical_size_i} uploaded_bytes={sum_write_bytes(ops)} "
            f"elapsed_s={time.time() - bridge_start:.6f}"
        )
        return True

    try:
        return _run_asyncio_bridge(_run())
    except BaseException as e:
        _bridge_info(f"rangeput_delta_bridge failed: {type(e).__name__}: {e}")
        raise


def full_object_put_bridge(bucket, key, file_path, logical_size):
    """Upload a complete local cache file as the new base object."""
    bridge_start = time.time()
    _bridge_info(
        f"full_object_put_bridge start bucket={bucket!r} key={key!r} "
        f"file_path={file_path!r} logical_size={logical_size}"
    )

    async def _run():
        manager = await get_object_manager()
        ok = await atomic_put(
            manager,
            bucket,
            key,
            file_path=str(file_path),
            file_size=int(logical_size),
            total_modified_size=int(logical_size),
        )
        _bridge_debug(
            f"full_object_put_bridge put ok={ok} bucket={bucket!r} key={key!r} "
            f"uploaded_bytes={int(logical_size)}"
        )
        if not ok:
            raise RuntimeError("write_full_object_from_file_path_to_snapshot_bucket failed")
        return True

    try:
        ret = _run_asyncio_bridge(_run())
        _bridge_info(
            f"full_object_put_bridge done bucket={bucket!r} key={key!r} "
            f"uploaded_bytes={int(logical_size)} elapsed_s={time.time() - bridge_start:.6f}"
        )
        return ret
    except BaseException as e:
        _bridge_info(f"full_object_put_bridge failed: {type(e).__name__}: {e}")
        raise


def rangeput_delta_bytes_bridge(
    bucket,
    key,
    delta_bytes,
    logical_size,
    ops,
    total_modified_size,
    arg1=None,
    arg2=None,
):
    """
    Apply CAT delta log to object store with WRITE payloads supplied directly as bytes.

    Each WRITE op's payload is packed sequentially in delta_bytes.
    """
    bridge_start = time.time()
    payload = memoryview(delta_bytes)
    _bridge_info(
        f"rangeput_delta_bytes_bridge start bucket={bucket!r} key={key!r} "
        f"payload_bytes={payload.nbytes} logical_size={logical_size} "
        f"total_modified_size={total_modified_size} op_count={len(ops)}"
    )
    original_op_count = len(ops)
    original_write_bytes = sum_write_bytes(ops)
    ops = coalesce_adjacent_write_ops(
        ops,
        MAX_BRIDGE_MERGED_WRITE_SIZE,
        skip_zero_length_writes=False,
    )
    _bridge_debug(
        f"rangeput_delta_bytes_bridge coalesced ops "
        f"original={original_op_count} adjacent={len(ops)} "
        f"write_bytes={original_write_bytes}->{sum_write_bytes(ops)}"
    )

    async def _run():
        manager = await get_object_manager()
        write_ranges, truncates = parse_cat_delta_ops(ops)
        logical_size_i = int(logical_size)
        total_modified_i = int(total_modified_size)
        _bridge_debug(
            f"parsed bytes write_ranges={len(write_ranges)} truncates={len(truncates)} "
            f"logical_size_i={logical_size_i} total_modified_i={total_modified_i}"
        )
        ok = await apply_delta_bytes(
            manager,
            bucket,
            key,
            delta_bytes=payload,
            logical_size=logical_size_i,
            ops=ops,
            total_modified_size=total_modified_i,
            log=_bridge_debug,
        )
        if not ok:
            raise RuntimeError("apply_delta_bytes failed")

        _bridge_info(
            f"rangeput_delta_bytes_bridge done bucket={bucket!r} key={key!r} "
            f"logical_size={logical_size_i} uploaded_bytes={sum_write_bytes(ops)} "
            f"elapsed_s={time.time() - bridge_start:.6f}"
        )
        return True

    try:
        return _run_asyncio_bridge(_run())
    except BaseException as e:
        _bridge_info(f"rangeput_delta_bytes_bridge failed: {type(e).__name__}: {e}")
        raise


def publish_local_to_cos_bridge(
    bucket,
    key,
    delta_path,
    logical_size,
    ops,
    total_modified_size,
    arg1=None,
    arg2=None,
):
    """
    Publish local CAT payloads directly into the primary object.

    Routing:
      - total_modified_size == 0: compact existing shadow metadata only
      - logical_size == total_modified_size: upload delta_path as a full base object
      - logical_size > total_modified_size: stage local WRITE ranges and run PTAS compact

    Each op is (type, object_offset, length, source_offset). For cat:// object sync the file is the
    full writable cache, so source_offset is the same as object_offset.
    """
    _bridge_info(
        f"publish_local_to_cos_bridge start bucket={bucket!r} key={key!r} "
        f"delta_path={delta_path!r} logical_size={logical_size} "
        f"total_modified_size={total_modified_size} op_count={len(ops)}"
    )
    original_op_count = len(ops)
    original_write_bytes = sum_write_bytes(ops)
    ops = coalesce_adjacent_write_ops(
        ops,
        MAX_BRIDGE_MERGED_WRITE_SIZE,
        skip_zero_length_writes=False,
    )
    adjacent_op_count = len(ops)
    ops = coalesce_absolute_write_intervals_by_object_offset(ops)
    _bridge_debug(
        f"publish_local_to_cos_bridge coalesced ops "
        f"original={original_op_count} adjacent={adjacent_op_count} interval={len(ops)} "
        f"write_bytes={original_write_bytes}->{sum_write_bytes(ops)}"
    )

    async def _run():
        manager = await get_object_manager()
        write_ranges, truncates = parse_cat_delta_ops(ops)

        logical_size_i = int(logical_size)
        total_modified_i = int(total_modified_size)
        if truncates:
            _bridge_debug(
                f"publish_local_to_cos_bridge saw {len(truncates)} truncate op(s); "
                f"using logical_size={logical_size_i} as target size"
            )

        _bridge_debug(
            f"publish_local_to_cos_bridge route inputs ranges={len(write_ranges)} "
            f"logical_size={logical_size_i} total_modified_size={total_modified_i}"
        )

        ok = await manager.publish_local_to_cos_from_file_path(
            bucket,
            key,
            write_ranges,
            str(delta_path),
            logical_size_i,
            total_modified_i,
            source_offsets_match_range_offsets=True,
            has_truncates=bool(truncates),
        )
        _bridge_info(f"publish_local_to_cos_bridge publish ok={ok}")
        if not ok:
            raise RuntimeError("publish_local_to_cos_from_file_path failed")
        return True

    try:
        return _run_asyncio_bridge(_run())
    except BaseException as e:
        _bridge_info(f"publish_local_to_cos_bridge failed: {type(e).__name__}: {e}")
        raise


async def read_object_range(bucket, key, offset, length):
    """
    Read [offset, offset+length) from the logical object (chunk/log merge in ObjectManager).

    Async callers: await read_object_range(...). Sync / embedded: read_object_range_bridge.
    """
    manager = await get_object_manager()
    return await manager.read_object_range(bucket, key, int(offset), int(length))


def read_object_range_bridge(bucket, key, offset, length):
    """
    Run read_object_range on the shared bridge event loop; returns bytes (sync API).
    """
    _bridge_info(
        f"read_object_range_bridge start bucket={bucket!r} key={key!r} "
        f"offset={offset} length={length}"
    )

    async def _run():
        data = await read_object_range(bucket, key, offset, length)
        _bridge_info(
            f"read_object_range_bridge done bucket={bucket!r} key={key!r} "
            f"got_bytes={len(data)}"
        )
        return data

    try:
        return _run_asyncio_bridge(_run())
    except BaseException as e:
        _bridge_info(f"read_object_range_bridge failed: {type(e).__name__}: {e}")
        raise


def head_object_bridge(bucket, key):
    """
    Return the CAT logical object size for bucket/key, or None when neither CAT metadata nor the
    base object exists. This intentionally bypasses ObjectManager.head_object(), which maps errors
    to size=0 and cannot distinguish a missing object from an empty object.
    """
    _bridge_info(f"head_object_bridge start bucket={bucket!r} key={key!r}")

    async def _run():
        manager = await get_object_manager()
        om = manager.get_manager(bucket, key)

        meta = await om.get_meta_if_exists()
        if meta is None:
            _bridge_debug(f"head_object_bridge miss bucket={bucket!r} key={key!r}")
            return None

        size = int(meta.file_size)
        _bridge_info(
            f"head_object_bridge hit bucket={bucket!r} key={key!r} size={size}"
        )
        return size

    try:
        return _run_asyncio_bridge(_run())
    except BaseException as e:
        _bridge_info(f"head_object_bridge failed: {type(e).__name__}: {e}")
        raise


def delete_object_bridge(bucket, key):
    """
    Delete the CAT logical object and its shadow metadata/data.
    """
    _bridge_info(f"delete_object_bridge start bucket={bucket!r} key={key!r}")

    async def _run():
        manager = await get_object_manager()
        await manager.delete_object_all(bucket, key)
        manager.managers.pop((bucket, key), None)
        _bridge_info(f"delete_object_bridge done bucket={bucket!r} key={key!r}")
        return True

    try:
        return _run_asyncio_bridge(_run())
    except BaseException as e:
        _bridge_info(f"delete_object_bridge failed: {type(e).__name__}: {e}")
        raise


def publish_compact_bridge(bucket, key):
    """
    Post-checkpoint: merge CAT shadow segments into the primary object (``compact_file``).

    Invoked from C++ after the main data file checkpoint sync (``ShadowFile::applyShadowPages``).
    """
    _bridge_info(f"publish_compact_bridge start bucket={bucket!r} key={key!r}")

    async def _run():
        manager = await get_object_manager()
        om = manager.get_manager(bucket, key)
        meta = await om.get_meta_if_exists()
        if meta is None or int(getattr(meta, "dirty_size", 0)) == 0:
            _bridge_debug(
                f"publish_compact_bridge no dirty meta; already compact bucket={bucket!r} key={key!r}"
            )
            return True
        ok = await manager.compact_file(bucket, key)
        _bridge_info(f"publish_compact_bridge compact_file ok={ok}")
        if not ok:
            raise RuntimeError("compact_file failed")
        return True

    try:
        return _run_asyncio_bridge(_run())
    except BaseException as e:
        _bridge_info(f"publish_compact_bridge failed: {type(e).__name__}: {e}")
        raise


def full_rewrite_delta_bridge(
    bucket,
    key,
    delta_path,
    logical_size,
    ops,
    total_modified_size,
    arg1=None,
    arg2=None,
):
    """
    Full-object rewrite (explicit download → local merge → full upload).

    Same parameters as rangeput_delta_bridge:
      - ops: list of (type, object_offset, length, delta_offset) — type 1 WRITE, type 2 TRUNCATE
      - delta_path: local file holding WRITE payloads in op order

    Flow:
      1) S3 download_object_to_file: primary object to a temp file (empty if object missing / size 0)
      2) apply_cat_delta_ops_to_local_file (common.local_file_utils): mmap delta + pwrite/ftruncate on base
      3) write_full_object_from_file_path_to_snapshot_bucket: upload merged file
    """
    _bridge_info(
        f"full_rewrite_delta_bridge start bucket={bucket!r} key={key!r} "
        f"delta_path={delta_path!r} logical_size={logical_size} "
        f"total_modified_size={total_modified_size} op_count={len(ops)}"
    )
    ops = coalesce_adjacent_write_ops(
        ops,
        MAX_BRIDGE_MERGED_WRITE_SIZE,
        skip_zero_length_writes=False,
    )
    async def _run():
        _bridge_debug("full_rewrite_delta_bridge: await get_object_manager()")
        manager = await get_object_manager()
        _bridge_debug("full_rewrite_delta_bridge: get_object_manager() done")
        ok = await manager.full_object_rewrite_from_delta_file(
            bucket,
            key,
            str(delta_path),
            int(logical_size),
            ops,
            int(total_modified_size),
        )
        _bridge_debug(f"full_rewrite_delta_bridge: full_object_rewrite_from_delta_file ok={ok}")
        if not ok:
            raise RuntimeError("full_object_rewrite_from_delta_file failed")
        _bridge_info(
            f"full_rewrite_delta_bridge done bucket={bucket!r} key={key!r} "
            f"logical_size={int(logical_size)}"
        )
        return True

    try:
        return _run_asyncio_bridge(_run())
    except BaseException as e:
        _bridge_info(f"full_rewrite_delta_bridge failed: {type(e).__name__}: {e}")
        raise
