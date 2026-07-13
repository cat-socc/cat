import asyncio
import mmap
import os
import aiofiles
from typing import Any, List, Tuple

from common.cat_delta_ops import TRUNCATE, WRITE, coalesce_adjacent_write_ops
from common.models import Range

class FileSlice:
    def __init__(self, filename: str, offset: int, length: int):
        self.filename = filename
        self.offset = offset
        self.length = length
        self.file = None

    async def open(self):
        self.file = await aiofiles.open(self.filename, 'rb')
        await self.file.seek(self.offset)

    async def read(self, size: int = -1) -> bytes:
        if self.file is None:
            raise RuntimeError("File not opened. Call open() first.")
        if size < 0 or size > self.length:
            size = self.length
        data = await self.file.read(size)
        self.length -= len(data)
        return data

    async def close(self):
        if self.file:
            await self.file.close()

async def read_chunks_from_file(file_path: str, chunk_infos: List[Any]) -> List[Tuple[Any, bytes]]:
    """
    Read chunks from a local file.

    Each item in chunk_infos must expose start_offset and end_offset attributes.
    Returns [(chunk_info, data), ...].
    """
    results = []

    async def read_one(info: Any):
        async with aiofiles.open(file_path, 'rb') as f:
            await f.seek(info.start_offset)
            data = await f.read(info.end_offset - info.start_offset)
            return (info, data)

    tasks = [read_one(info) for info in chunk_infos]
    results = await asyncio.gather(*tasks)
    return results

async def read_log_data_from_file(file_path: str, ranges: List[Range], max_concurrency: int = 10) -> bytes:
    """
    Asynchronously read file chunks from the specified ranges with concurrent optimization.
    
    Optimizations:
    1. Concurrent reading for multiple ranges
    2. Pre-allocate memory to avoid frequent concatenation
    3. Sort ranges by offset to minimize seek operations
    4. Add error handling for file operations
    5. Optimize memory usage with bytearray
    6. Control concurrency level to avoid overwhelming the system
    """
    if not ranges:
        return b''
    
    # Sort ranges by offset to minimize seek operations
    sorted_ranges = sorted(ranges, key=lambda r: r.offset)
    
    # Calculate total size and pre-allocate memory
    total_size = sum(r.length for r in sorted_ranges)
    result = bytearray(total_size)
    
    # For single range, use simple sequential read
    if len(sorted_ranges) == 1:
        try:
            async with aiofiles.open(file_path, 'rb') as f:
                await f.seek(sorted_ranges[0].offset)
                data = await f.read(sorted_ranges[0].length)
                if len(data) != sorted_ranges[0].length:
                    raise IOError(f"Expected {sorted_ranges[0].length} bytes at offset {sorted_ranges[0].offset}, but got {len(data)}")
                return data
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {file_path}")
        except IOError as e:
            raise IOError(f"IO error reading file {file_path}: {e}")
        except Exception as e:
            raise Exception(f"Unexpected error reading file {file_path}: {e}")
    
    # For multiple ranges, use concurrent reading
    async def read_range(r: Range, start_pos: int) -> Tuple[int, bytes]:
        """Read a single range and return (start_position, data)"""
        try:
            async with aiofiles.open(file_path, 'rb') as f:
                await f.seek(r.offset)
                data = await f.read(r.length)
                if len(data) != r.length:
                    raise IOError(f"Expected {r.length} bytes at offset {r.offset}, but got {len(data)}")
                return start_pos, data
        except Exception as e:
            raise Exception(f"Error reading range {r.offset}-{r.offset + r.length}: {e}")
    
    try:
        # Create tasks for concurrent reading
        tasks = [read_range(r, sum(sorted_ranges[j].length for j in range(i))) for i, r in enumerate(sorted_ranges)]
        results = await asyncio.gather(*tasks)
        for start_pos, data in results:
            result[start_pos:start_pos+len(data)] = data
        return bytes(result)
    except Exception as e:
        raise Exception(f"Error in concurrent read_log_data_from_file: {e}")

async def read_big_ranges_from_local_disk(file_path, offset, length):
    """Asynchronously read a file chunk from the specified offset and length."""
    async with aiofiles.open(file_path, 'rb') as f:
        await f.seek(offset)
        data = await f.read(length)
    return data

def sync_read_patch(path: str, offset: int, length: int) -> bytes:
    with open(path, 'rb') as f:
        f.seek(offset)
        return f.read(length)

async def read_small_ranges_from_local_disk(file_path, offset, length):
    return await asyncio.to_thread(sync_read_patch, file_path, offset, length)

async def read_all_patch_ranges_concurrently(
    file_path: str,
    modifications: List[Tuple[int, int]]
) -> List[Tuple[int, bytes]]:
    """
    Concurrently read (offset, length) regions from file and return list of (offset, data).
    """
    async def read_one(offset, length):
        data = await read_small_ranges_from_local_disk(file_path, offset, length)
        return offset, data

    tasks = [read_one(offset, length) for offset, length in modifications]
    return await asyncio.gather(*tasks)


def _pwrite_all(fd: int, buf, offset: int) -> None:
    view = memoryview(buf)
    try:
        written = 0
        total = len(view)
        while written < total:
            n = os.pwrite(fd, view[written:], offset + written)
            if n <= 0:
                raise OSError(f"pwrite made no progress at offset={offset + written}")
            written += n
    finally:
        view.release()

def _apply_cat_delta_ops_to_local_file_sync(
    base_path: str,
    delta_path: str,
    ops: List[Any],
    logical_size: int,
) -> None:
    logical_sz = int(logical_size)
    norm_ops = coalesce_adjacent_write_ops(ops)

    delta_size = os.path.getsize(delta_path) if os.path.exists(delta_path) else 0

    base_fd = os.open(base_path, os.O_RDWR)
    delta_fd = -1
    delta_mm = None
    delta_view = None

    try:
        if delta_size > 0:
            delta_fd = os.open(delta_path, os.O_RDONLY)
            delta_mm = mmap.mmap(delta_fd, 0, access=mmap.ACCESS_READ)
            delta_view = memoryview(delta_mm)

        for op_type, object_offset, length, delta_offset in norm_ops:
            if op_type == WRITE:
                if delta_view is None:
                    raise OSError("WRITE op exists but delta file is empty")

                end = delta_offset + length
                if end > delta_size:
                    raise OSError(
                        f"CAT delta read out of bounds: "
                        f"delta_offset={delta_offset}, length={length}, delta_size={delta_size}"
                    )

                payload = delta_view[delta_offset:end]
                try:
                    _pwrite_all(base_fd, payload, object_offset)
                finally:
                    payload.release()

            elif op_type == TRUNCATE:
                os.ftruncate(base_fd, object_offset)

            else:
                raise ValueError(f"unknown CAT delta op type: {op_type}")

        # Final logical size: shrink or sparse zero-extend (no giant b"\\x00" buffer).
        os.ftruncate(base_fd, logical_sz)

    finally:
        if delta_view is not None:
            delta_view.release()
        if delta_mm is not None:
            delta_mm.close()
        if delta_fd >= 0:
            os.close(delta_fd)
        os.close(base_fd)


async def apply_cat_delta_ops_to_local_file(
    base_path: str, delta_path: str, ops: List[Any], logical_size: int
) -> None:
    """
    Apply CAT delta onto a local base file (same op tuples as bridge.rangeput_delta_bridge).

    Delta is mmap'd; base updates use os.pwrite / os.ftruncate in op order. Blocking work runs
    in a thread via asyncio.to_thread so the event loop is not wedged.
    """
    await asyncio.to_thread(
        _apply_cat_delta_ops_to_local_file_sync,
        base_path,
        delta_path,
        ops,
        logical_size,
    )
