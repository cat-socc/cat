import boto3
import time
import os
import tempfile
import json
import asyncio
from boto3.s3.transfer import TransferConfig
from .base_executor import BaseExecutor
import sys
import random
from typing import List, Tuple, Dict, Any
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from s3_utils.s3_boto3 import S3Boto3
from common.constants import *
from datetime import datetime, timezone
from common.helper_utils import split_ranges
from common.models import Range
from object_store.object_manager import split_path

class Boto3Executor(BaseExecutor):
    def __init__(self, bucket_name: str = 'datasize2'):
        super().__init__(bucket_name)
        self.prepare()
        self.semaphore = asyncio.Semaphore(10)

    def get_name(self):
        return "Boto3-S3"

    def prepare(self):
        super().prepare()

    def write_file(self, file_path, file_size_mb):
        self.check_file_exists(file_path, file_size_mb)
        file_name = os.path.basename(file_path)
        start_time = time.perf_counter()
        success = asyncio.run(self.s3_boto3.put_object_from_file(
            bucket=self.bucket_name,
            key=file_name,
            filename=file_path
        ))
        elapsed = time.perf_counter() - start_time
        if not success:
            raise Exception("Boto3 write failed")
        throughput_mb_s = file_size_mb / elapsed
        return throughput_mb_s, elapsed

    def write_ranges_file(self, file_path, file_size_mb, modified_size_mb: int, ranges: List[Tuple[int, int]]):
        # self.write_file(file_path, file_size_mb)
        self.check_file_exists(file_path, file_size_mb)
        start_time = time.perf_counter()
        success = asyncio.run(self.write_ranges_file_with_ranges(file_path, file_size_mb, ranges))
        if not success:
            raise Exception("Boto3 write ranges failed")
        elapsed = time.perf_counter() - start_time
        throughput_mb_s = modified_size_mb / elapsed
        return throughput_mb_s, elapsed
    
    async def write_ranges_file_with_ranges(
        self, 
        file_path, 
        file_size_mb, 
        ranges: List[Tuple[int, int]],
        *,
        max_concurrency: int = 24,      # 与 mpu_execute_plan 对齐：part 级并发
        piece_concurrency: int = 8      # 与 mpu_execute_plan 对齐：fetch 内部 piece 并发
        ) -> bool:
        """写入文件的指定范围到S3，复用 plan_mpu_tasks 逻辑"""
        file_name = os.path.basename(file_path)
        file_size = file_size_mb * 1024 * 1024
        
        print(f"Writing {len(ranges)} ranges to {file_name}")
        
        # 1. 创建 ObjectMetaManager 和 ObjectMap
        from object_store.object_meta.object_meta_manager import ObjectMetaManager
        from object_store.object_meta.segment import Segment
        
        # 初始化 meta manager，base object 指向 S3（bucket/key），range 段指向本地
        mm = ObjectMetaManager(
            primary_bucket=self.bucket_name,
            primary_key=file_name,
            file_size=file_size
        )
        
        # 2. 添加 range 写入的 segments（标记为本地路径）
        for offset, length in ranges:
            segment = Segment(
                offset=offset,
                length=length,
                source_path=f"local/{file_path}",
                source_offset=offset
            )
            mm.object_map.update_with_new_segments(offset, length, [segment])
        
        # 3. 生成 MPU 计划
        mpu_tasks = mm.plan_mpu_tasks()
        print(f"Generated {len(mpu_tasks)} MPU tasks")
        for i, task in enumerate(mpu_tasks):
            print(f"Task {i+1}: {task}")
        
        # 4. 创建 multipart upload
        upload_result = await self.s3_boto3.create_multipart_upload(
            self.bucket_name, 
            file_name
        )
        upload_id = upload_result['UploadId']
        # 4) 工具函数与并发控制
        def parse_source_path(path: str):
            if path.startswith("local/"):
                return ("local", None, path[len("local/"):], True)
            if "/" not in path:
                raise ValueError(f"Invalid source_path: {path}")
            bucket, key = path.split("/", 1)
            return ("s3", bucket, key, False)

        async def read_piece(piece: Dict[str, Any], base_logical: int) -> tuple[int, bytes]:
            """读取单个 piece；支持 local/ 与 S3；返回 (写回偏移, 数据)"""
            start_pos = piece["logical_offset"] - base_logical
            kind, bucket, key, is_local = parse_source_path(piece["source_path"])
            src_start = piece["source_offset"]
            src_end = src_start + piece["length"] - 1
            if is_local:
                # 本地读放线程池，避免阻塞事件循环
                def _read_local():
                    with open(key, "rb") as f:
                        f.seek(src_start)
                        return f.read(piece["length"])
                buf = await asyncio.to_thread(_read_local)
            else:
                # 统一走 self.get_object（你已改为 10MB 并发获取）
                buf = await self.get_object(bucket, key, src_start, src_end)
            return start_pos, buf

        part_sem = asyncio.Semaphore(max_concurrency)

        async def execute_copy_task(task: dict, pn: int) -> dict:
            async with part_sem:
                t0 = time.time()
                print(f"[{t0:.3f}] start copy pn={pn}")
                kind, bucket, key, is_local = parse_source_path(task["source_path"])
                start = task["source_offset"]
                end = start + task["length"] - 1
                if is_local:
                    # 本地读取再上传
                    def _read_local():
                        with open(file_path, "rb") as f:
                            f.seek(start)
                            return f.read(task["length"])
                    data = await asyncio.to_thread(_read_local)
                    res = await self.s3_boto3.upload_part(
                        bucket=self.bucket_name,
                        key=file_name,
                        upload_id=upload_id,
                        part_number=pn,
                        data=data
                    )
                    if res is None:
                        raise RuntimeError(f"Failed to upload part {pn}")
                    t1 = time.time()
                    print(f"[{t1:.3f}] done  copy pn={pn} dur={t1-t0:.3f}s")
                    return {"PartNumber": pn, "ETag": res["ETag"]}
                else:
                    # 服务端 Copy（UploadPartCopy）
                    res = await self.s3_boto3.upload_part_copy(
                        bucket=self.bucket_name,
                        key=file_name,
                        upload_id=upload_id,
                        part_number=pn,
                        source_bucket=bucket,
                        source_key=key,
                        source_start=start,
                        source_end=end
                    )
                    if res is None:
                        raise RuntimeError(f"Failed to upload part copy {pn}")
                    etag = None
                    if isinstance(res, dict):
                        etag = res.get("ETag")
                        if etag is None and isinstance(res.get("CopyPartResult"), dict):
                            etag = res["CopyPartResult"].get("ETag")
                    if not etag:
                        raise ValueError("upload_part_copy returned unexpected format without ETag")
                    t1 = time.time()
                    print(f"[{t1:.3f}] done  copy pn={pn} dur={t1-t0:.3f}s")
                    return {"PartNumber": pn, "ETag": etag}

        async def execute_fetch_task(task: dict, pn: int) -> dict:
            async with part_sem:
                total_len = task["length"]
                pieces = list(task["pieces"])
                if not pieces:
                    raise ValueError("fetch task has no pieces")
                # 对齐 mpu_execute_plan：先按 logical_offset 排序，基准取最小 logical_offset
                pieces.sort(key=lambda p: p["logical_offset"])
                base_logical = pieces[0]["logical_offset"]

                data = bytearray(total_len)
                piece_sem = asyncio.Semaphore(piece_concurrency)

                async def _one(p):
                    async with piece_sem:
                        pos, buf = await read_piece(p, base_logical)
                        data[pos:pos + len(buf)] = buf

                await asyncio.gather(*(_one(p) for p in pieces))

                # 安全检查（可选）
                if len(data) != total_len:
                    raise ValueError(f"assembled length {len(data)} != declared {total_len} for part {pn}")

                res = await self.s3_boto3.upload_part(
                    bucket=self.bucket_name,
                    key=file_name,
                    upload_id=upload_id,
                    part_number=pn,
                    data=bytes(data)
                )
                if res is None:
                    raise RuntimeError(f"Failed to upload part {pn}")
                return {"PartNumber": pn, "ETag": res["ETag"]}

        # 5) 预分配 PartNumber，并构建并发任务（与 mpu_execute_plan 一致）
        tasks_coros: List[asyncio.Task] = []
        part_number = 1
        for t in mpu_tasks:
            pn = part_number
            part_number += 1
            if t["type"] == "copy":
                tasks_coros.append(execute_copy_task(t, pn))
            elif t["type"] == "fetch":
                tasks_coros.append(execute_fetch_task(t, pn))
            else:
                raise ValueError(f"Unknown task type: {t['type']}")

        # 6) 并发执行所有 parts；异常则中止 MPU
        try:
            parts_result = await asyncio.gather(*tasks_coros)
        except Exception as e:
            print(f"Error during multipart upload: {e}")
            import traceback
            print(f"Traceback: {traceback.format_exc()}")
            await self.s3_boto3.abort_multipart_upload(self.bucket_name, file_name, upload_id)
            raise

        # 7) complete（按 PartNumber 排序）
        parts_result.sort(key=lambda x: x["PartNumber"])
        await self.s3_boto3.complete_multipart_upload(self.bucket_name, file_name, upload_id, parts_result)
        print(f"Successfully completed compact apply with {len(parts_result)} parts")
        return True
        # 解析 source_path，返回 (kind, bucket, key, is_local)
        # def parse_source_path(path: str):
        #     if path.startswith('local/'):
        #         return ('local', None, path[len('local/'):], True)
        #     # s3 路径格式 bucket/key
        #     if '/' not in path:
        #         raise ValueError(f"Invalid source_path: {path}")
        #     bucket, key = path.split('/', 1)
        #     return ('s3', bucket, key, False)
        
        # # 5. 执行 MPU 计划（显式 part_number，避免竞态）
        # tasks = []
        # part_number = 1
        
        # async def execute_copy_task(task: dict, pn: int) -> dict:
        #     t0 = time.time()
        #     print(f"[{t0:.3f}] start copy pn={pn}")
        #     kind, bucket, key, is_local = parse_source_path(task['source_path'])
        #     start = task['source_offset']
        #     end = start + task['length'] - 1
        #     if is_local:
        #         # 本地读取再上传
        #         with open(file_path, 'rb') as f:
        #             f.seek(start)
        #             data = f.read(task['length'])
        #         result = await self.s3_boto3.upload_part(
        #             bucket=self.bucket_name,
        #             key=file_name,
        #             upload_id=upload_id,
        #             part_number=pn,
        #             data=data
        #         )
        #         if result is None:
        #             raise Exception(f"Failed to upload part {pn}")
        #         return {'PartNumber': pn, 'ETag': result['ETag']}
        #     else:
        #         # S3 服务器端 copy
        #         res = await self.s3_boto3.upload_part_copy(
        #             bucket=self.bucket_name,
        #             key=file_name,
        #             upload_id=upload_id,
        #             part_number=pn,
        #             source_bucket=bucket,
        #             source_key=key,
        #             source_start=start,
        #             source_end=end
        #         )
        #         if res is None:
        #             raise Exception(f"Failed to upload part copy {pn}")
        #         # 兼容不同返回结构
        #         etag = None
        #         if isinstance(res, dict):
        #             etag = res.get('ETag')
        #             if etag is None and isinstance(res.get('CopyPartResult'), dict):
        #                 etag = res['CopyPartResult'].get('ETag')
        #         if etag is None:
        #             raise ValueError("upload_part_copy returned unexpected format without ETag")
        #         t1 = time.time()
        #         print(f"[{t1:.3f}] done  copy pn={pn} dur={t1-t0:.3f}s")
        #         return {'PartNumber': pn, 'ETag': etag}
        
        # async def execute_fetch_task(task: dict, pn: int) -> dict:
        #     # 目标缓冲
        #     total_len = task['length']
        #     data = bytearray(total_len)
        #     base_logical = task['pieces'][0]['logical_offset'] if task['pieces'] else 0
            
        #     async def fetch_piece(piece: dict) -> tuple[int, bytes]:
        #         start_pos = piece['logical_offset'] - base_logical
        #         kind, bucket, key, is_local = parse_source_path(piece['source_path'])
        #         src_start = piece['source_offset']
        #         src_end = src_start + piece['length'] - 1
        #         if is_local:
        #             local_path = key  # key 即去掉前缀后的本地路径
        #             with open(local_path, 'rb') as f:
        #                 f.seek(src_start)
        #                 buf = f.read(piece['length'])
        #             return start_pos, buf
        #         else:
        #             buf = await self.s3_boto3.get_object(bucket, key, src_start, src_end)
        #             return start_pos, buf
            
        #     # 并发抓取各 piece
        #     results = await asyncio.gather(*[fetch_piece(p) for p in task['pieces']])
        #     for pos, buf in results:
        #         data[pos:pos + len(buf)] = buf
            
        #     # 上传该 part
        #     res = await self.s3_boto3.upload_part(
        #         bucket=self.bucket_name,
        #         key=file_name,
        #         upload_id=upload_id,
        #         part_number=pn,
        #         data=bytes(data)
        #     )
        #     if res is None:
        #         raise Exception(f"Failed to upload part {pn}")
        #     return {'PartNumber': pn, 'ETag': res['ETag']}
        
        # # 逐个分配 part_number 并构建任务
        # for t in mpu_tasks:
        #     pn = part_number
        #     part_number += 1
        #     if t['type'] == 'copy':
        #         tasks.append(execute_copy_task(t, pn))
        #     elif t['type'] == 'fetch':
        #         tasks.append(execute_fetch_task(t, pn))
        #     else:
        #         raise ValueError(f"Unknown task type: {t['type']}")
        
        # # 7. 并发执行所有任务
        # try:
        #     parts_result = await asyncio.gather(*tasks)
        # except Exception as e:
        #     print(f"Error during multipart upload: {e}")
        #     import traceback
        #     print(f"Traceback: {traceback.format_exc()}")
        #     # 中止 multipart upload
        #     await self.s3_boto3.abort_multipart_upload(self.bucket_name, file_name, upload_id)
        #     raise e
        
        # # 8. 完成 multipart upload
        # await self.s3_boto3.complete_multipart_upload(
        #     self.bucket_name,
        #     file_name,
        #     upload_id,
        #     parts_result
        # )
        # print(f"Successfully completed compact apply with {len(parts_result)} parts")
        # return True
        
    def read_range_file(self, file_name, start_offset, end_offset):
        """从S3读取文件的指定范围（并发读取）"""
        start_time = time.perf_counter()
        if start_offset == 0 and end_offset == 0:
            # data = asyncio.run(self._read_range_file_async(
            #     file_name, None, None
            # ))
            data = asyncio.run(self.s3_boto3.get_object(self.bucket_name, file_name))
        else:
            # data = asyncio.run(self._read_range_file_async(
            #     file_name, start_offset, end_offset
            # ))
            data = asyncio.run(self.s3_boto3.get_object(self.bucket_name, file_name, start_offset, end_offset))
        
        elapsed = time.perf_counter() - start_time
        
        if not data:
            raise Exception("Boto3 range read failed")
        print(f"Boto3 read range file {file_name} size: {len(data)}")
        # assert len(data) == end_offset - start_offset + 1
        range_size_mb = len(data) / 1024 / 1024
        throughput_mb_s = range_size_mb / elapsed
        return throughput_mb_s, elapsed
    
    async def _read_range_file_async(self, file_name, start_offset, end_offset):
        """并发读取文件的指定范围"""
        if start_offset is None and end_offset is None:
            # 读取整个文件
            return await self.s3_boto3.boto3_client.get_object(
                self.bucket_name, 
                file_name
            )
        
        # 计算范围长度
        length = end_offset - start_offset + 1
        buf = bytearray(length)
        
        # 将范围分割成多个块进行并发读取
        # 使用合理的块大小（例如 10MB），可以根据需要调整
        chunk_size = 10 * 1024 * 1024  # 10MB per chunk
        num_chunks = (length + chunk_size - 1) // chunk_size
        
        # 发起并发请求
        tasks = []
        for i in range(num_chunks):
            chunk_start = start_offset + i * chunk_size
            chunk_end = min(end_offset, chunk_start + chunk_size - 1)
            tasks.append(self.s3_boto3.boto3_client.get_object(
                self.bucket_name,
                file_name,
                start=chunk_start,
                end=chunk_end
            ))
        
        parts = await asyncio.gather(*tasks)
        
        # 按顺序拷贝到目标缓冲
        cursor = 0
        for chunk in parts:
            chunk_len = len(chunk)
            buf[cursor:cursor + chunk_len] = chunk
            cursor += chunk_len
        
        return bytes(buf)

    def state_file(self, file_name):
        """获取文件的元数据"""
        start_time = time.perf_counter()
        try:
            metadata = self.s3_boto3.head_object(self.bucket_name, file_name)
            elapsed = time.perf_counter() - start_time
            # 对于 head 操作，我们返回一个虚拟的吞吐量（基于操作时间）
            # 这里使用 1MB 作为基准来计算吞吐量
            virtual_size_mb = 1.0
            throughput_mb_s = virtual_size_mb / elapsed if elapsed > 0 else 0
            return throughput_mb_s, elapsed
        except Exception as e:
            elapsed = time.perf_counter() - start_time
            raise Exception(f"Boto3 head failed: {e}")

    def update_metadata(self, file_name, metadata):
        """更新文件的元数据"""
        # Boto3不支持直接更新元数据，需要复制对象
        try:
            # 创建临时对象
            temp_key = f"{file_name}_temp"
            
            # 复制对象并设置新元数据
            copy_source = {'Bucket': self.bucket_name, 'Key': file_name}
            self.s3_client.copy_object(
                Bucket=self.bucket_name,
                Key=temp_key,
                CopySource=copy_source,
                Metadata=metadata,
                MetadataDirective='REPLACE'
            )
            
            # 删除原对象
            self.s3_boto3.delete_object(self.bucket_name, file_name)
            
            # 重命名临时对象
            self.s3_client.copy_object(
                Bucket=self.bucket_name,
                Key=file_name,
                CopySource={'Bucket': self.bucket_name, 'Key': temp_key}
            )
            
            # 删除临时对象
            self.s3_boto3.delete_object(self.bucket_name, temp_key)
            
            return True
        except Exception as e:
            raise Exception(f"Boto3 update metadata failed: {e}")

    def delete_file(self, file_name):
        """删除文件"""
        start_time = time.perf_counter()
        try:
            self.s3_boto3.delete_object(self.bucket_name, file_name)
            elapsed = time.perf_counter() - start_time
            # 对于 delete 操作，我们返回一个虚拟的吞吐量（基于操作时间）
            # 这里使用 1MB 作为基准来计算吞吐量
            virtual_size_mb = 1.0
            throughput_mb_s = virtual_size_mb / elapsed if elapsed > 0 else 0
            return throughput_mb_s, elapsed
        except Exception as e:
            elapsed = time.perf_counter() - start_time
            raise Exception(f"Boto3 delete failed: {e}")
