from requests import get
import boto3
import time
import os
import tempfile
import json
import asyncio
from boto3.s3.transfer import TransferConfig
from .base_executor import BaseExecutor
from typing import Tuple, List, Dict, Any, Optional

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from s3_utils.s3_boto3 import S3Boto3
from object_store.objects_manager import ObjectsManager
from common.models import Range
import os, shutil, tempfile, time, asyncio


class ObjectStoreExecutor(BaseExecutor):
    def __init__(self, bucket_name: str = 'datasize2'):
        super().__init__(bucket_name)
        self.prepare()
        self.semaphore = asyncio.Semaphore(10)

    def get_name(self):
        return "object_store"

    def prepare(self):
        super().prepare()
        self.objects_manager = ObjectsManager(self.s3_boto3)
    
    #########################################################
    # write functions
    #########################################################
    def write_file(self, file_path, file_size_mb):
        self.check_file_exists(file_path, file_size_mb)
        file_name = os.path.basename(file_path)
        # 直接用big_file_path上传
        start_time = time.perf_counter()
        success = asyncio.run(self.objects_manager.write_full_object_from_file_path_to_snapshot_bucket(
            self.bucket_name,
            file_name,
            file_path,
            file_size_mb * 1024 * 1024
        ))
        elapsed = time.perf_counter() - start_time
        if not success:
            raise Exception("Object Store write failed")
        throughput_mb_s = file_size_mb / elapsed
        return throughput_mb_s, elapsed

    def write_ranges_file(self, file_path, file_size_mb, modified_size_mb: int, ranges: List[Tuple[int, int]]):
        # self.write_file(file_path, file_size_mb)
        self.check_file_exists(file_path, file_size_mb)
        file_name = os.path.basename(file_path)
        ranges = [Range(offset=start, length=length) for start, length in ranges]
        start_time = time.perf_counter()
        success = asyncio.run(self.objects_manager.write_range_object_from_file_path(
            bucket=self.bucket_name,
            key=file_name,
            ranges=ranges,
            file_path=file_path,
            file_size=file_size_mb * 1024 * 1024
        ))
        elapsed = time.perf_counter() - start_time
        if not success:
            raise Exception("Object Store range write failed")
        om = self.objects_manager.get_manager(self.bucket_name, file_name)
        timings: Optional[Dict[str, Any]] = getattr(om, "last_range_write_timings", None)
        if timings:
            print(
                f"  ⏱️  分项: prefetch={timings.get('prefetch_wall_s', timings['fetch_meta_s']):.4f}s "
                f"(meta={timings['fetch_meta_s']:.4f}s, load={timings.get('load_range_data_s', 0):.4f}s), "
                f"upload_range={timings['upload_range_s']:.4f}s, "
                f"upload_meta={timings['upload_meta_s']:.4f}s"
            )
        throughput_mb_s = modified_size_mb / elapsed
        return throughput_mb_s, elapsed, timings

    
    #########################################################
    # read functions
    #########################################################

    def read_range_file(self, file_name, start_offset, end_offset):
        self.objects_manager = ObjectsManager(self.s3_boto3)
        length = end_offset - start_offset + 1
        if start_offset == 0 and end_offset == 0:
            length = 0
        start_time = time.perf_counter()
        data = asyncio.run(self.objects_manager.read_object_range(
            self.bucket_name, 
            file_name, 
            start_offset,  # start offset
            length  # length
        ))
        
        elapsed = time.perf_counter() - start_time
        
        if not data:
            raise Exception("Object Store range read failed")
        print(f"Object Store read range file {file_name} size: {len(data)}")
        range_size_mb = len(data) / 1024 / 1024
        throughput_mb_s = range_size_mb / elapsed
        return throughput_mb_s, elapsed
    
    #########################################################
    # compact functions
    #########################################################
    def compact_file_from_chunks(self, file_path, file_size_mb, modified_size_mb, ranges):
        self.objects_manager = ObjectsManager(self.s3_boto3)
        self.check_file_exists(file_path, file_size_mb)
        file_name = os.path.basename(file_path)
        # self.compact_chunks(file_path, file_size_mb, modified_size_mb, ranges)
        start_time = time.perf_counter()
        object_manager = self.objects_manager.get_manager(self.bucket_name, file_name)
        compact_success = asyncio.run(object_manager.compact_file())
        compact_time = time.perf_counter() - start_time
        throughput_mb_s = file_size_mb / compact_time
        print(f"Compact file {file_name} time: {compact_time}")
        return throughput_mb_s, compact_time
    
    def compact_file_from_logs(self, file_path, file_size_mb, modified_size_mb, ranges):
        self.objects_manager = ObjectsManager(self.s3_boto3)
        self.check_file_exists(file_path, file_size_mb)
        file_name = os.path.basename(file_path)
        object_manager = self.objects_manager.get_manager(self.bucket_name, file_name)   
        # # 1.write fll write
        # self.write_file(file_path, file_size_mb)
        # # 2.write log data
        # self.write_ranges_file(file_path, file_size_mb, modified_size_mb, ranges)
        # 3.compact file
        start_time = time.perf_counter()
        compact_success = asyncio.run(object_manager.compact_file())
        compact_time = time.perf_counter() - start_time
        throughput_mb_s = file_size_mb / compact_time
        print(f"Compact file {file_name} time: {compact_time}")
        return throughput_mb_s, compact_time

    def compact_chunks(self, file_path, file_size_mb, modified_size_mb, ranges):
        # self.objects_manager = ObjectsManager(self.s3_boto3)
        self.check_file_exists(file_path, file_size_mb)
        file_name = os.path.basename(file_path)
        object_manager = self.objects_manager.get_manager(self.bucket_name, file_name)   
        # 1.write fll write
        # self.write_file(file_path, file_size_mb)
        # # 2.write log data
        # self.write_ranges_file(file_path, file_size_mb, modified_size_mb, ranges)
        # 3.compact chunks
        start_time = time.perf_counter()
        compact_success = asyncio.run(object_manager.compact_chunks())
        compact_time = time.perf_counter() - start_time
        throughput_mb_s = file_size_mb / compact_time
        print(f"Compact chunks {file_name} time: {compact_time}")
        return throughput_mb_s, compact_time
    
    #########################################################
    # other functions
    #########################################################
    def state_file(self, file_name):
        """获取文件的元数据"""
        start_time = time.perf_counter()
        try:
            metadata = asyncio.run(self.objects_manager.head_object(self.bucket_name, file_name))
            elapsed = time.perf_counter() - start_time
            # 对于 head 操作，我们返回一个虚拟的吞吐量（基于操作时间）
            # 这里使用 1MB 作为基准来计算吞吐量
            virtual_size_mb = 1.0
            throughput_mb_s = virtual_size_mb / elapsed if elapsed > 0 else 0
            return throughput_mb_s, elapsed
        except Exception as e:
            elapsed = time.perf_counter() - start_time
            raise Exception(f"Object Store head failed: {e}")

    def update_metadata(self, file_name, metadata):
        """更新文件的元数据"""
        # Object Store不支持直接更新元数据，这里返回False
        print("Warning: Object Store does not support direct metadata updates")
        return False

    def delete_file(self, file_name):
        """删除文件"""
        start_time = time.perf_counter()
        try:
            success = asyncio.run(self.objects_manager.delete_object_all(self.bucket_name, file_name))
            elapsed = time.perf_counter() - start_time
            # 对于 delete 操作，我们返回一个虚拟的吞吐量（基于操作时间）
            # 这里使用 1MB 作为基准来计算吞吐量
            virtual_size_mb = 1.0
            throughput_mb_s = virtual_size_mb / elapsed if elapsed > 0 else 0
            return throughput_mb_s, elapsed
        except Exception as e:
            elapsed = time.perf_counter() - start_time
            raise Exception(f"Object Store delete failed: {e}")

    def cleanup(self, files_to_delete=None):
        """
        清理测试创建的文件
        
        Args:
            files_to_delete: 要删除的文件列表，如果为None则删除默认的测试文件
        """
        if files_to_delete is None:
            # 默认删除模式，删除常见的测试文件
            default_files = []
            for i in range(10):
                default_files.extend([
                    f"big_file_{i}.bin",
                    f"small_file_{i}.bin", 
                    f"range_file_{i}.bin",
                    f"{self.test_file_name}-{i}"
                ])
            files_to_delete = default_files
        
        for file_name in files_to_delete:
            try:
                self.delete_file(file_name)
            except Exception as e:
                print(f"Warning: Failed to delete {file_name}: {e}")
                pass 