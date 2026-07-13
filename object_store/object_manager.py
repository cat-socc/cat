import asyncio
import io
import json
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

from botocore.exceptions import ClientError

from common.constants import MAX_CONCURRENCY
from common.helper_utils import (
    build_bundled_payload_bytes,
    build_bundled_payload_file,
    make_data_provider_from_bytes,
    make_data_provider_from_file,
)
from common.logger import print_info, print_debug, print_warning, print_error
from common.local_file_utils import apply_cat_delta_ops_to_local_file
from common.models import Range
from object_store.object_meta.object_meta_manager import ObjectMetaManager
from object_store.object_meta.segment import Segment
from object_store.S3Storage.s3_storage import S3Storage


def _objectstore_debug(msg: str) -> None:
    print_debug(msg)


def _segments_are_non_overlapping(segments: List[Segment]) -> bool:
    if len(segments) < 2:
        return True
    ordered = sorted(segments, key=lambda s: s.offset)
    return all(ordered[i - 1].end() <= ordered[i].offset for i in range(1, len(ordered)))


def _segments_are_sorted_non_overlapping(segments: List[Segment]) -> bool:
    return all(segments[i - 1].end() <= segments[i].offset for i in range(1, len(segments)))


def _update_object_map_many(object_map, segments: List[Segment]) -> str:
    if _segments_are_sorted_non_overlapping(segments):
        object_map.update_with_new_segments_bulk(segments, assume_sorted=True)
        return "bulk-sorted"
    if _segments_are_non_overlapping(segments):
        object_map.update_with_new_segments_bulk(segments)
        return "bulk"
    for seg in segments:
        object_map.update_with_new_segments(seg.offset, seg.length, [seg])
    return "sequential-overlap-fallback"


def split_path(path: str) -> tuple[str, str]:
    b, k = path.split("/", 1)
    return b, k


class ObjectManager:
    def __init__(self, s3_client, primary_bucket: str, primary_key: str, shadow_bucket: str, snapshot_interval: int = 300):
        self.s3_client = s3_client
        self.storage: S3Storage = S3Storage(s3_client)
        self.primary_bucket = primary_bucket
        self.primary_key = primary_key
        self.shadow_bucket = shadow_bucket
        self.snapshot_interval = snapshot_interval
        self._meta_manager: Optional[ObjectMetaManager] = None
        self._bucket_cache = set()

    ############ meta helpers ############
    def get_base_object_key(self) -> str:
        return f"{self.primary_bucket}/{self.primary_key}"

    def get_shadow_meta_object_key(self) -> str:
        return f"{self.primary_key}"

    def get_shadow_data_object_key(self) -> str:
        return f"{self.primary_key}-data"

    def shadow_meta_key(self) -> str:
        return self.get_shadow_meta_object_key()

    def shadow_data_prefix(self) -> str:
        return f"{self.get_shadow_data_object_key()}/"

    def _meta_s3_bucket_key(self):
        return self.shadow_bucket, self.get_shadow_meta_object_key()

    def _put_json(self, bucket: str, key: str, payload: dict):
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        self._ensure_bucket(bucket)
        self.s3_client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")

    def _get_json(self, bucket: str, key: str) -> Optional[dict]:
        try:
            obj = self.s3_client.get_object(Bucket=bucket, Key=key)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("NoSuchKey", "404", "NotFound"):
                return None
            raise
        data = obj["Body"].read()
        return json.loads(data.decode("utf-8"))

    def _head_object(self, bucket: str, key: str) -> Optional[dict]:
        try:
            return self.s3_client.head_object(Bucket=bucket, Key=key)
        except ClientError:
            return None

    ############ meta lifecycle ############
    async def _get_meta(self, create_new_meta: bool = False, file_size: int = 0) -> ObjectMetaManager:
        if self._meta_manager is not None:
            return self._meta_manager

        meta_key = self.shadow_meta_key()

        meta_json, base_head = await asyncio.gather(
            self.storage.get_json(self.shadow_bucket, meta_key),
            self.storage.head(self.primary_bucket, self.primary_key),
        )
        _objectstore_debug("metadata and base HEAD fetch completed")

        if meta_json:
            mm = ObjectMetaManager.from_dict(meta_json)
            mm.shadow_bucket = self.shadow_bucket
            mm.shadow_key = meta_key
            if base_head and base_head.get("ContentLength") is not None:
                mm.repair_primary_segments_to_base_size(int(base_head["ContentLength"]))
            self._meta_manager = mm
            return mm

        if base_head:
            size = int(file_size) if file_size and file_size > 0 else int(base_head.get("ContentLength", 0))
            if size < 0:
                raise FileNotFoundError(
                    f"cannot infer file size from base object: s3://{self.primary_bucket}/{self.primary_key}"
                )

            mm = ObjectMetaManager(
                primary_bucket=self.primary_bucket,
                primary_key=self.primary_key,
                file_size=size,
                base_size=int(base_head.get("ContentLength", 0)),
            )
            mm.shadow_bucket = self.shadow_bucket
            mm.shadow_key = meta_key
            self._meta_manager = mm
            return mm

        raise FileNotFoundError(
            f"metadata not found at s3://{self.shadow_bucket}/{meta_key} "
            f"and base object missing at s3://{self.primary_bucket}/{self.primary_key}"
        )

    async def get_meta_if_exists(self, file_size: int = 0) -> Optional[ObjectMetaManager]:
        try:
            return await self._get_meta(file_size=file_size)
        except FileNotFoundError:
            return None

    def reset_meta_cache_to_primary(self, file_size: int) -> None:
        mm = ObjectMetaManager(
            primary_bucket=self.primary_bucket,
            primary_key=self.primary_key,
            file_size=int(file_size),
        )
        mm.shadow_bucket = self.shadow_bucket
        mm.shadow_key = self.shadow_meta_key()
        mm.dirty_size = 0
        self._meta_manager = mm

    def clear_meta_cache(self) -> None:
        self._meta_manager = None

    async def save_meta(self):
        save_start = time.time()
        await self.storage.put_json(self.shadow_bucket, self.shadow_meta_key(), self._meta_manager.to_dict())
        _objectstore_debug(f"metadata saved elapsed_s={time.time() - save_start:.3f}")


    #########################################################
    # Write full object (snapshot format)
    #########################################################
    async def _write_full_object_to_snapshot_bucket(self, data_provider, data_size: int, operation_name: str) -> bool:
        total_start = time.time()
        try:
            data = data_provider() if callable(data_provider) else data_provider
            upload_start = time.time()
            if isinstance(data, str):
                await self.storage.put_from_file(self.primary_bucket, self.primary_key, data, data_size)
                source_kind = "file"
            else:
                await self.storage.put(self.primary_bucket, self.primary_key, data, data_size)
                source_kind = "bytes"
            upload_elapsed = time.time() - upload_start
            print_info(
                f"[cat_upload_breakdown] op={operation_name} phase=upload_primary "
                f"bucket={self.primary_bucket} key={self.primary_key} source={source_kind} "
                f"bytes={int(data_size)} elapsed_s={upload_elapsed:.6f}"
            )

            clean_start = time.time()
            await self._clean_shadow_bucket()
            print_info(
                f"[cat_upload_breakdown] op={operation_name} phase=clean_shadow "
                f"bucket={self.shadow_bucket} key_prefix={self.primary_key} "
                f"elapsed_s={time.time() - clean_start:.6f}"
            )
            self.reset_meta_cache_to_primary(data_size)
            print_info(
                f"[cat_upload_breakdown] op={operation_name} phase=done "
                f"bucket={self.primary_bucket} key={self.primary_key} bytes={int(data_size)} "
                f"total_elapsed_s={time.time() - total_start:.6f}"
            )
            return True
        except Exception as e:
            print_error(f"Error in {operation_name}: {e}")
            return False

    async def write_full_object_from_bytes_to_snapshot_bucket(self, data: bytes) -> bool:
        return await self._write_full_object_to_snapshot_bucket(
            lambda: data,
            len(data),
            "write_full_object_from_bytes_to_snapshot_bucket"
        )

    async def write_full_object_from_file_path_to_snapshot_bucket(self, file_path: str, file_size: int) -> bool:
        return await self._write_full_object_to_snapshot_bucket(
            lambda: file_path,
            file_size,
            "write_full_object_from_file_path_to_snapshot_bucket"
        )

    async def full_object_rewrite_from_delta_file(
        self, delta_path: str, logical_size: int, ops, total_modified_size: int
    ) -> bool:
        """
        Full-object rewrite: download primary object to a temp file via S3 download-to-file,
        merge the CAT delta log locally (same op layout as rangeput_delta_bridge), then
        upload the merged file with write_full_object_from_file_path_to_snapshot_bucket.
        """
        try:
            logical_size_i = int(logical_size)
            total_modified_i = int(total_modified_size)
            print_info(
                f"full_object_rewrite_from_delta_file start "
                f"s3://{self.primary_bucket}/{self.primary_key} "
                f"delta_path={delta_path!r} logical_size={logical_size_i} "
                f"total_modified_size={total_modified_i} op_count={len(ops)}"
            )

            tmp = tempfile.NamedTemporaryFile(delete=False)
            base_tmp_path = tmp.name
            tmp.close()

            total_start = time.time()
            _objectstore_debug("full_object_rewrite_from_delta_file started")
            try:
                head = await self.storage.head(self.primary_bucket, self.primary_key)
                content_len = 0
                if head is not None and head.get("ContentLength") is not None:
                    content_len = int(head["ContentLength"])
                if content_len > 0:
                    dl = await self.storage.download_object_to_file(
                        self.primary_bucket, self.primary_key, base_tmp_path
                    )
                    if dl != content_len:
                        print_warning(
                            f"download size mismatch: head={content_len} downloaded={dl} "
                            f"for s3://{self.primary_bucket}/{self.primary_key}"
                        )
                else:
                    with open(base_tmp_path, "wb"):
                        pass

                await apply_cat_delta_ops_to_local_file(
                    base_tmp_path, delta_path, ops, logical_size_i
                )
                merged_size = os.path.getsize(base_tmp_path)
                if merged_size != logical_size_i:
                    raise OSError(
                        f"merged file size {merged_size} != logical_size {logical_size_i}"
                    )

                ok = await self.write_full_object_from_file_path_to_snapshot_bucket(
                    base_tmp_path, logical_size_i
                )
                print_info(f"full_object_rewrite_from_delta_file done ok={ok}")
                return ok
            finally:
                print_debug(
                    f"full_object_rewrite_from_delta_file elapsed_s={time.time() - total_start:.3f}"
                )
                if os.path.exists(base_tmp_path):
                    os.unlink(base_tmp_path)
        except Exception as e:
            print_error(f"Error in full_object_rewrite_from_delta_file: {e}")
            return False

    #########################################################
    # Write range object
    #########################################################
    async def _write_range_object_generic(
        self, data_provider, ranges: List[Range], operation_name: str, file_size: Optional[int] = None,
        bundle_payload: bool = False,
        bundle_source_bytes: Optional[bytes] = None,
        bundle_source_file: Optional[str] = None,
        bundle_source_offsets_match_range_offsets: bool = False,
    ) -> bool:
        range_write_time = time.time()
        mm = await self._get_meta(file_size=int(file_size or 0))
        try:
            original_modified_size = sum(int(r.length) for r in ranges)
            if bundle_payload and len(ranges) > 1:
                bundle_key = (
                    f"{self.get_shadow_data_object_key()}/"
                    f"range_bundle_{ranges[0].offset}_{original_modified_size}_{time.time_ns()}"
                )
                tmp = tempfile.NamedTemporaryFile(delete=False)
                tmp_path = tmp.name
                tmp.close()
                uploaded_segments: List[Tuple[Range, Segment]] = []
                try:
                    if bundle_source_file:
                        bundle_build_start = time.time()
                        payload_offset, segment_offsets = await asyncio.to_thread(
                            build_bundled_payload_file,
                            bundle_source_file,
                            ranges,
                            tmp_path,
                            source_offsets_match_range_offsets=(
                                bundle_source_offsets_match_range_offsets
                            ),
                        )
                        uploaded_segments = [
                            (
                                r,
                                Segment(
                                    int(r.offset),
                                    int(r.length),
                                    f"{self.shadow_bucket}/{bundle_key}",
                                    off,
                                ),
                            )
                            for r, off in segment_offsets
                        ]
                        print_info(
                            f"bundled payload built: {len(ranges)} ranges/{payload_offset} bytes "
                            f"from {bundle_source_file} in {time.time() - bundle_build_start:.3f}s"
                        )
                    elif bundle_source_bytes is not None:
                        bundle_build_start = time.time()
                        payload_offset, segment_offsets = await asyncio.to_thread(
                            build_bundled_payload_bytes,
                            ranges,
                            bundle_source_bytes,
                            tmp_path,
                        )
                        uploaded_segments = [
                            (
                                r,
                                Segment(
                                    int(r.offset),
                                    int(r.length),
                                    f"{self.shadow_bucket}/{bundle_key}",
                                    off,
                                ),
                            )
                            for r, off in segment_offsets
                        ]
                        print_info(
                            f"bundled payload built: {len(ranges)} ranges/{payload_offset} bytes "
                            f"from bytes in logical order in {time.time() - bundle_build_start:.3f}s"
                        )
                    else:
                        payload_offset = 0
                        with open(tmp_path, "wb") as bundle_out:
                            for r in ranges:
                                r_len = int(r.length)
                                data = await data_provider(r)
                                if len(data) != r_len:
                                    raise ValueError(
                                        f"range payload length mismatch: offset={r.offset} "
                                        f"expected={r_len} got={len(data)}"
                                    )
                                bundle_out.write(data)
                                uploaded_segments.append((
                                    r,
                                    Segment(
                                        int(r.offset), r_len,
                                        f"{self.shadow_bucket}/{bundle_key}",
                                        payload_offset,
                                    ),
                                ))
                                payload_offset += r_len
                    upload_start = time.time()
                    await self.storage.put_from_file(
                        self.shadow_bucket, bundle_key, tmp_path, payload_offset
                    )
                    print_info(
                        f"[cat_upload_breakdown] op={operation_name} phase=upload_shadow_data "
                        f"bucket={self.shadow_bucket} key={bundle_key} ranges={len(ranges)} "
                        f"bytes={int(payload_offset)} bundled=1 elapsed_s="
                        f"{time.time() - upload_start:.6f}"
                    )
                finally:
                    if os.path.exists(tmp_path):
                        os.unlink(tmp_path)
                map_update_start = time.time()
                update_mode = _update_object_map_many(
                    mm.object_map, [seg for _, seg in uploaded_segments]
                )
                print_info(
                    f"object_map {update_mode} update: {len(uploaded_segments)} bundled segments "
                    f"in {time.time() - map_update_start:.3f}s"
                )
                print_info(
                    f"bundled range write: {len(ranges)} ranges/{original_modified_size} bytes "
                    f"-> 1 object s3://{self.shadow_bucket}/{bundle_key}"
                )
            else:
                uploaded: List[Tuple[Range, str]] = []
                tasks = []
                upload_keys = []
                for r in ranges:
                    key = f"{self.get_shadow_data_object_key()}/range_{r.offset}_{r.length}"
                    upload_keys.append(key)
                    data = await data_provider(r)
                    tasks.append(self.storage.put(self.shadow_bucket, key, data, r.length))
                upload_start = time.time()
                await asyncio.gather(*tasks)
                print_info(
                    f"[cat_upload_breakdown] op={operation_name} phase=upload_shadow_data "
                    f"bucket={self.shadow_bucket} key_prefix={self.get_shadow_data_object_key()} "
                    f"ranges={len(ranges)} bytes={int(original_modified_size)} bundled=0 "
                    f"elapsed_s={time.time() - upload_start:.6f}"
                )
                uploaded = [(r, key) for r, key in zip(ranges, upload_keys)]

                map_update_start = time.time()
                update_mode = _update_object_map_many(mm.object_map, [
                    Segment(r.offset, r.length, f"{self.shadow_bucket}/{key}", 0)
                    for r, key in uploaded
                ])
                print_info(
                    f"object_map {update_mode} update: {len(uploaded)} uploaded segments "
                    f"in {time.time() - map_update_start:.3f}s"
                )
            if file_size is not None:
                mm.file_size = int(file_size)
            mm.modified_ts = datetime.now(timezone.utc)
            mm.dirty_size += original_modified_size
            mm.version += 1

            meta_start = time.time()
            await self.save_meta()
            print_info(
                f"[cat_upload_breakdown] op={operation_name} phase=save_meta "
                f"bucket={self.shadow_bucket} key={self.shadow_meta_key()} "
                f"bytes={int(original_modified_size)} elapsed_s={time.time() - meta_start:.6f}"
            )
            _objectstore_debug(
                f"range write done elapsed_s={time.time() - range_write_time:.3f}"
            )
            print_info(
                f"[cat_upload_breakdown] op={operation_name} phase=done "
                f"primary_bucket={self.primary_bucket} primary_key={self.primary_key} "
                f"shadow_bucket={self.shadow_bucket} ranges={len(ranges)} "
                f"bytes={int(original_modified_size)} file_size={int(file_size or 0)} "
                f"total_elapsed_s={time.time() - range_write_time:.6f}"
            )

            return True
        except Exception as e:
            print_error(f"Error in {operation_name}: {e}")
            return False

    async def write_range_object_from_bytes(self, ranges: List[Range], data: bytes) -> bool:
        return await self._write_range_object_generic(
            # lambda log_manager: log_manager.append_patch_to_log_data(data),
            make_data_provider_from_bytes(data),
            ranges,
            "write_range_object_from_bytes",
            bundle_payload=True,
            bundle_source_bytes=data,
        )

    async def write_range_object_from_file_path(self, ranges: List[Range], file_path: str,
                                                file_size: int,
                                                source_offsets_match_range_offsets: bool = False) -> bool:
        return await self._write_range_object_generic(
            make_data_provider_from_file(file_path, source_offsets_match_range_offsets),
            ranges,
            "write_range_object_from_file_path",
            file_size=file_size,
            bundle_payload=True,
            bundle_source_file=file_path,
            bundle_source_offsets_match_range_offsets=source_offsets_match_range_offsets,
        )

    def _local_file_source_uri(self, file_path: str) -> str:
        return Path(file_path).expanduser().resolve().as_uri()

    async def _stage_local_range_segments(
        self, ranges: List[Range], file_path: str, file_size: Optional[int] = None,
        source_offsets_match_range_offsets: bool = False,
    ) -> None:
        """
        Add local range payloads to the object map without uploading them to shadow.
        By default the local file layout matches rangeput's payload layout: ranges are packed
        sequentially in the same order as the ranges list. Object-sync can pass the full local
        cache file instead, where each segment's source offset is the object offset.
        """
        mm = await self._get_meta(file_size=int(file_size or 0))
        source_uri = self._local_file_source_uri(file_path)
        required_payload_size = sum(int(r.length) for r in ranges)
        if source_offsets_match_range_offsets:
            required_payload_size = max(
                (int(r.offset) + int(r.length) for r in ranges),
                default=0,
            )
        actual_payload_size = os.path.getsize(file_path)
        if actual_payload_size < required_payload_size:
            raise ValueError(
                f"local payload file too small: need {required_payload_size}, got {actual_payload_size}"
            )
        payload_offset = 0
        total_modified = 0

        new_segments: List[Segment] = []
        for r in ranges:
            r_len = int(r.length)
            source_offset = int(r.offset) if source_offsets_match_range_offsets else payload_offset
            new_segments.append(Segment(int(r.offset), r_len, source_uri, source_offset))
            payload_offset += r_len
            total_modified += r_len
        map_update_start = time.time()
        update_mode = _update_object_map_many(mm.object_map, new_segments)
        print_info(
            f"object_map {update_mode} update: {len(new_segments)} local segments "
            f"in {time.time() - map_update_start:.3f}s"
        )

        if file_size is not None:
            mm.file_size = int(file_size)
        mm.modified_ts = datetime.now(timezone.utc)
        mm.dirty_size += total_modified
        mm.version += 1

    async def compact_file_with_local_ranges_from_file_path(
        self, ranges: List[Range], file_path: str, file_size: Optional[int] = None,
        source_offsets_match_range_offsets: bool = False,
    ) -> bool:
        mm = await self._get_meta(file_size=int(file_size or 0))
        old_segments = [
            Segment(s.offset, s.length, s.source_path, s.source_offset)
            for s in mm.object_map.segments
        ]
        old_file_size = mm.file_size
        old_modified_ts = mm.modified_ts
        old_dirty_size = mm.dirty_size
        old_version = mm.version

        def _restore_meta_state() -> None:
            mm.object_map.segments = old_segments
            mm.file_size = old_file_size
            mm.modified_ts = old_modified_ts
            mm.dirty_size = old_dirty_size
            mm.version = old_version

        try:
            await self._stage_local_range_segments(
                ranges, file_path, file_size=file_size,
                source_offsets_match_range_offsets=source_offsets_match_range_offsets,
            )
            success = await self.compact_file()
            if not success:
                _restore_meta_state()
            return success
        except Exception as e:
            _restore_meta_state()
            print_error(f"Error in compact_file_with_local_ranges_from_file_path: {e}")
            return False

    async def compact_file_with_local_ranges_from_bytes(
        self, ranges: List[Range], data: bytes, file_size: Optional[int] = None
    ) -> bool:
        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp_path = tmp.name
        try:
            required_payload_size = sum(int(r.length) for r in ranges)
            if len(data) != required_payload_size:
                raise ValueError(
                    f"data payload size {len(data)} != total range length {required_payload_size}"
                )
            tmp.write(data)
            tmp.close()
            return await self.compact_file_with_local_ranges_from_file_path(
                ranges, tmp_path, file_size=file_size
            )
        except Exception as e:
            print_error(f"Error in compact_file_with_local_ranges_from_bytes: {e}")
            return False
        finally:
            if not tmp.closed:
                tmp.close()
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    #########################################################
    # Read object range
    #########################################################
    async def read_object_range(self, offset: int, length: int) -> bytes:
        mm = await self._get_meta()
        plan = mm.object_map.plan_read(offset, length)  # [(source_path, src_off, len)]

        sem = asyncio.Semaphore(MAX_CONCURRENCY)

        async def _fetch_one(path: str, src_off: int, ln: int) -> bytes:
            async with sem:
                return await self.storage.get_source_range(path, src_off, src_off + ln - 1)

        if len(plan) == 1:
            path, src_off, ln = plan[0]
            return await _fetch_one(path, src_off, ln)

        parts = await asyncio.gather(*[
            _fetch_one(path, src_off, ln) for (path, src_off, ln) in plan
        ])
        return b''.join(parts)

    #########################################################
    # head and delete object
    #########################################################

    async def head_object(self) -> dict:
        """Return logical object metadata."""
        try:
            meta = await self._get_meta()
            result = {
                "size": getattr(meta, 'file_size', None),
                "etag": getattr(meta, 'snapshot_etag', None),
                "last_modified": getattr(meta, 'last_modified', None),
                "content_type": getattr(meta, 'content_type', "application/octet-stream"),
                "custom_metadata": getattr(meta, 'custom_metadata', {})
            }
        except Exception as e:
            print_error(f"Error in head_object: {e}")
            result = {
                "size": 0,
                "etag":None,
                "last_modified": None,
                "content_type": "application/octet-stream",
                "custom_metadata": {}
            }
        return result

    #########################################################
    # clean shadow bucket
    #########################################################
    async def delete_object_all(self):
        await self.storage.delete(self.primary_bucket, self.primary_key)
        await self._clean_shadow_bucket()
        self.clear_meta_cache()

    async def _clean_shadow_bucket(self):
        meta_key = self.get_shadow_meta_object_key()
        data_key = self.get_shadow_data_object_key()
        tasks = [
            self.storage.delete(self.shadow_bucket, meta_key),
            self.storage.delete_prefix(self.shadow_bucket, data_key)
        ]
        await asyncio.gather(*tasks)

    #########################################################
    # Snapshot
    #########################################################

    async def compact_file(self) -> bool:
        total_start = time.time()
        print_info(
            f"[cat_compact_breakdown] phase=start bucket={self.primary_bucket} "
            f"key={self.primary_key}"
        )
        try:
            phase_start = time.time()
            mm = await self._get_meta()
            print_info(
                f"[cat_compact_breakdown] phase=get_meta elapsed_s={time.time() - phase_start:.6f} "
                f"file_size={getattr(mm, 'file_size', 0)} dirty_size={getattr(mm, 'dirty_size', 0)} "
                f"segments={len(getattr(mm.object_map, 'segments', []))}"
            )

            phase_start = time.time()
            tasks = mm.plan_mpu_tasks()
            copy_tasks = sum(1 for t in tasks if t.get("type") == "copy")
            fetch_tasks = sum(1 for t in tasks if t.get("type") == "fetch")
            copy_bytes = sum(int(t.get("length", 0)) for t in tasks if t.get("type") == "copy")
            fetch_bytes = sum(int(t.get("length", 0)) for t in tasks if t.get("type") == "fetch")
            print_info(
                f"[cat_compact_breakdown] phase=plan_mpu elapsed_s={time.time() - phase_start:.6f} "
                f"tasks={len(tasks)} copy_tasks={copy_tasks} fetch_tasks={fetch_tasks} "
                f"copy_bytes={copy_bytes} fetch_bytes={fetch_bytes}"
            )
            if not tasks:
                print_info(
                    f"[cat_compact_breakdown] phase=done_no_tasks total_elapsed_s="
                    f"{time.time() - total_start:.6f}"
                )
                return True

            dest_bucket, dest_key = self.primary_bucket, self.primary_key
            phase_start = time.time()
            await self.storage.mpu_execute_plan(dest_bucket, dest_key, tasks)
            print_info(
                f"[cat_compact_breakdown] phase=mpu_execute elapsed_s={time.time() - phase_start:.6f}"
            )

            phase_start = time.time()
            await self._clean_shadow_bucket()
            print_info(
                f"[cat_compact_breakdown] phase=clean_shadow elapsed_s={time.time() - phase_start:.6f}"
            )

            new_segment = Segment(0, mm.file_size, f"{self.primary_bucket}/{self.primary_key}", 0)
            mm.object_map.segments = [new_segment]
            mm.dirty_size = 0
            mm.version += 1

            print_info(
                f"[cat_compact_breakdown] phase=done total_elapsed_s={time.time() - total_start:.6f}"
            )
            return True
        except Exception as e:
            print_error(f"Error in compact_file: {e}")
            raise

    async def sync_simple_execute(self) -> bool:
        """
        Download all mapped segments, merge them locally, and upload the result.
        """
        try:
            mm = await self._get_meta()
            dest_bucket, dest_key = self.primary_bucket, self.primary_key
            await self.storage.sync_simple_execute(
                dest_bucket,
                dest_key,
                mm.object_map,
                mm.file_size
            )

            await self._clean_shadow_bucket()

            new_segment = Segment(0, mm.file_size, f"{self.primary_bucket}/{self.primary_key}", 0)
            mm.object_map.segments = [new_segment]
            mm.dirty_size = 0
            mm.version += 1

            return True
        except Exception as e:
            print_error(f"Error in sync_simple_execute: {e}")
            return False


    def _start_snapshot_task(self):
        """Start the periodic snapshot task"""
        try:
            # Create a task that will run the periodic snapshot
            self._snapshot_task = asyncio.create_task(self._periodic_snapshot())
            print_info(f"Started periodic snapshot task for {self.bucket}/{self.key} with interval {self.snapshot_interval}s")
        except Exception as e:
            print_error(f"Failed to start snapshot task: {e}")

    async def _stop_snapshot_task(self):
        """Stop the periodic snapshot task"""
        if self._snapshot_task and not self._snapshot_task.done():
            self._snapshot_task.cancel()
            try:
                await self._snapshot_task
            except asyncio.CancelledError:
                pass
            print_info(f"Stopped snapshot task for {self.bucket}/{self.key}")

    async def _periodic_snapshot(self):
        """Periodic snapshot task that runs in the background"""
        while True:
            try:
                await asyncio.sleep(self.snapshot_interval)
                await self.compact_chunks()
            except asyncio.CancelledError:
                print_info(f"Snapshot task cancelled for {self.bucket}/{self.key}")
                break
            except Exception as e:
                print_error(f"Error in periodic snapshot for {self.bucket}/{self.key}: {e}")
                # Continue running even if there's an error
                continue
