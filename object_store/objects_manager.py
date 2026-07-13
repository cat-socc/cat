from typing import Dict, List, Optional, Tuple

from common.models import Range
from object_store.object_manager import ObjectManager
from s3_utils.s3_boto3 import S3Boto3

"""
Manage all object read/write, metadata, and log information
"""


class ObjectsManager:
    def __init__(self, s3_client: S3Boto3):
        self.managers: Dict[Tuple[str, str], ObjectManager] = {}
        self.s3_client = s3_client

    def get_manager(self, bucket: str, key: str) -> ObjectManager:
        """
        Get or create an ObjectMetaManager for the specified bucket and key.
        This method ensures that there is a manifest manager for the given bucket and key,
        initializing it if it does not already exist.
        """
        identifier = (bucket, key)
        if identifier not in self.managers:
            self.managers[identifier] = ObjectManager(
                s3_client=self.s3_client,
                primary_bucket=bucket,
                primary_key=key,
                shadow_bucket=f"{bucket}-shadow",
            )
        return self.managers[identifier]

    async def write_full_object_to_snapshot_bucket(self, bucket: str, key: str, data: bytes) -> bool:
        """
        Write a full object to snapshot bucket without chunking
        """
        object_manager = self.get_manager(bucket, key)
        return await object_manager.write_full_object_from_bytes_to_snapshot_bucket(data)

    async def write_full_object_from_file_path_to_snapshot_bucket(self, bucket: str, key: str, file_path: str, file_size: int) -> bool:
        """
        Write a full object to snapshot bucket from file path without chunking
        """
        object_manager = self.get_manager(bucket, key)
        return await object_manager.write_full_object_from_file_path_to_snapshot_bucket(file_path, file_size)

    async def write_range_object(self, bucket: str, key: str, ranges: List[Range], data: bytes) -> bool:
        """
        Write a range of object to S3, ranges: modified ranges, data: the data to write
        """
        object_manager = self.get_manager(bucket, key)
        return await object_manager.write_range_object_from_bytes(ranges, data)

    async def write_range_object_from_file_path(self, bucket: str, key: str, ranges: List[Range],
                                                file_path: str, file_size: int,
                                                source_offsets_match_range_offsets: bool = False) -> bool:
        """
        Write a range of object to S3 from a file path
        """
        object_manager = self.get_manager(bucket, key)
        return await object_manager.write_range_object_from_file_path(
            ranges, file_path, file_size,
            source_offsets_match_range_offsets=source_offsets_match_range_offsets,
        )

    async def full_object_rewrite_from_delta_file(
        self,
        bucket: str,
        key: str,
        delta_path: str,
        logical_size: int,
        ops,
        total_modified_size: int,
    ) -> bool:
        """
        Full-object rewrite: download primary → local merge with CAT delta ops → full upload.
        Op tuple layout matches rangeput_delta_bridge / apply_cat_delta_ops_to_local_file.
        """
        object_manager = self.get_manager(bucket, key)
        return await object_manager.full_object_rewrite_from_delta_file(
            delta_path, logical_size, ops, total_modified_size
        )

    async def read_object_range(self, bucket: str, key: str, offset: int, length: int) -> bytes:
        """
        Intelligently read object range data
        Decide whether to read data from chunk or log based on the range and object meta information
        """
        object_manager = self.get_manager(bucket, key)
        return await object_manager.read_object_range(offset, length)

    async def head_object(self, bucket: str, key: str) -> Optional[dict]:
        """Return logical object metadata."""
        object_manager = self.get_manager(bucket, key)
        return await object_manager.head_object()

    async def delete_object_all(self, bucket: str, key: str) -> bool:
        """
        Delete the object and all related metadata, chunk data, and snapshot data.
        """
        object_manager = self.get_manager(bucket, key)
        return await object_manager.delete_object_all()

    async def compact_file(self, bucket: str, key: str) -> bool:
        """
        Compact the file
        """
        object_manager = self.get_manager(bucket, key)
        return await object_manager.compact_file()

    async def compact_file_with_local_ranges_from_file_path(
        self, bucket: str, key: str, ranges: List[Range], file_path: str,
        file_size: Optional[int] = None, source_offsets_match_range_offsets: bool = False,
    ) -> bool:
        """
        Compact with range payloads that are already available in a local file.
        The file contains payload bytes packed in the same order as ranges.
        """
        object_manager = self.get_manager(bucket, key)
        return await object_manager.compact_file_with_local_ranges_from_file_path(
            ranges, file_path, file_size=file_size,
            source_offsets_match_range_offsets=source_offsets_match_range_offsets,
        )

    async def compact_file_with_local_ranges_from_bytes(
        self, bucket: str, key: str, ranges: List[Range], data: bytes, file_size: Optional[int] = None
    ) -> bool:
        """
        Compact with range payloads supplied as request bytes.
        """
        object_manager = self.get_manager(bucket, key)
        return await object_manager.compact_file_with_local_ranges_from_bytes(
            ranges, data, file_size=file_size
        )

    async def publish_local_to_cos_from_file_path(
        self,
        bucket: str,
        key: str,
        ranges: List[Range],
        file_path: str,
        logical_size: int,
        total_modified_size: int,
        source_offsets_match_range_offsets: bool = False,
        has_truncates: bool = False,
    ) -> bool:
        """
        Top-level publish path used by the CAT bridge.

        Route by payload shape:
        - no modified payload: compact existing shadow metadata into the base object
        - full-object payload: upload the local file directly to the base object
        - partial payload: stage local cache ranges into metadata, then compact
        """
        logical_size_i = int(logical_size)
        total_modified_i = int(total_modified_size)

        if total_modified_i == 0 and not has_truncates:
            return await self.compact_file(bucket, key)
        if total_modified_i == 0 and has_truncates:
            return await self.compact_file_with_local_ranges_from_file_path(
                bucket, key, ranges, file_path, logical_size_i,
                source_offsets_match_range_offsets=source_offsets_match_range_offsets,
            )
        if logical_size_i == total_modified_i:
            return await self.write_full_object_from_file_path_to_snapshot_bucket(
                bucket, key, file_path, logical_size_i
            )
        if logical_size_i > total_modified_i:
            return await self.compact_file_with_local_ranges_from_file_path(
                bucket, key, ranges, file_path, logical_size_i,
                source_offsets_match_range_offsets=source_offsets_match_range_offsets,
            )
        raise ValueError(
            f"invalid publish sizes: logical_size={logical_size_i}, "
            f"total_modified_size={total_modified_i}"
        )
