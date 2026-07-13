import os

import boto3
from io import BytesIO
import asyncio
from common.logger import print_info, print_debug, print_warning, print_error
from typing import Optional
from s3_utils.crt_client import CRTClient
from s3_utils.client import boto3_client
from boto3.s3.transfer import TransferConfig
from common.constants import *

class S3Boto3:
    def __init__(self, s3_client: boto3_client, transfer_config: TransferConfig):
        self.boto3_client = s3_client
        self.transfer_config = transfer_config
        # Same default chain + region as underlying boto3.client
        self.s3_resource = boto3.resource(
            "s3",
            region_name=s3_client.s3_client.meta.region_name,
        )
        self.crt_client = CRTClient(region=s3_client.s3_client.meta.region_name)
        self.crt_available = True
        self.max_concurrency=100
        
    #########################################################
    # Put object
    #########################################################
    async def put_object(self, bucket, key, data:bytes)->bool:
        if self.crt_available:
            return await self.crt_client.upload_bytes(bucket, key, data)
        else:
            return await self.boto3_client.put_object_boto3(bucket, key, data)
    
    async def put_object_from_file(self, bucket, key, filename)->bool:
        if self.crt_available:
            return await self.crt_client.upload_file(bucket, key, filename)
        else:
            return await self.boto3_client.put_object_from_file(bucket, key, filename)
    
    #########################################################
    # Get object
    #########################################################

    async def download_object_to_file(self, bucket: str, key: str, file_path: str) -> int:
        """
        Download the full S3 object to a local file (streaming / transfer manager).
        Returns the written file size in bytes.
        """
        loop = asyncio.get_running_loop()
        if self.crt_available:
            return await self.crt_client.download_object_to_file(bucket, key, file_path)

        def _boto_download() -> int:
            self.boto3_client.s3_client.download_file(bucket, key, file_path)
            return os.path.getsize(file_path)

        return await loop.run_in_executor(None, _boto_download)

    async def get_object(self, bucket, key, start=None, end=None)-> bytes:
        # if self.crt_available:
        #     if (end is None and start is  None):
        #         return await self.crt_client.get_whole_object(bucket, key)
        #     else: 
        #         return await self.crt_client.get_object(bucket, key, start, end)
        # else:
        #     if (end is None and start is  None):
        #         return await self.boto3_client.get_whole_object(bucket, key)
        #     else: 
        #         return await self.boto3_client.get_object(bucket, key, start, end)
        # 无范围：保持原逻辑（如需整文件也并发，可先实现 head 获取长度再走同样切片策略）
        if start is None and end is None:
            if self.crt_available:
                return await self.crt_client.get_whole_object(bucket, key)
            else:
                return await self.boto3_client.get_whole_object(bucket, key)

        # 参数校验
        if start is None or end is None or end < start:
            raise ValueError(f"Invalid range: start={start}, end={end}")

        total = end - start + 1
        if total <= 0:
            return b""

        # 预分配输出缓冲
        # out = bytearray(total)
        # sem = asyncio.Semaphore(self.max_concurrency)
        # chunk_size = (end - start + 1) // self.max_concurrency
        # async def fetch_one(i: int):
        #     s = start + i * chunk_size
        #     e = min(end, s + chunk_size - 1)
        #     async with sem:
        #         if self.crt_available:
        #             part = await self.crt_client.get_object(bucket, key, s, e)
        #         else:
        #             part = await self.boto3_client.get_object(bucket, key, s, e)
        #     off = s - start
        #     out[off:off + len(part)] = part

        # n_chunks = (total + chunk_size - 1) // chunk_size
        # await asyncio.gather(*(fetch_one(i) for i in range(n_chunks)))
        # return bytes(out)

        # Range 读取：不做二次切片并发，直接单次请求读取指定区间
        if self.crt_available:
            try:
                data = await self.crt_client.get_object(bucket, key, start, end)
            except Exception as e:
                print_error(
                    f"CRT range GET failed; falling back to boto3 for "
                    f"s3://{bucket}/{key} bytes={start}-{end}: {e}"
                )
                data = await self.boto3_client.get_object(bucket, key, start, end)
        else:
            data = await self.boto3_client.get_object(bucket, key, start, end)
        if len(data) != total:
            raise IOError(
                f"S3 range GET short read s3://{bucket}/{key} "
                f"bytes={start}-{end} expected={total} got={len(data)}"
            )
        return data

    #########################################################
    # Multipart upload
    #########################################################
    async def create_multipart_upload(self, bucket: str, key: str) -> Optional[dict]:
        return self.boto3_client.create_multipart_upload(bucket, key)

    async def upload_part(self, bucket: str, key: str, upload_id: str, part_number: int, data: bytes) -> Optional[dict]:
        return await self.boto3_client.upload_part(bucket, key, upload_id, part_number, data)

    async def complete_multipart_upload(self, bucket: str, key: str, upload_id: str, parts: list) -> Optional[dict]:
        return self.boto3_client.complete_multipart_upload(bucket, key, upload_id, parts)

    async def abort_multipart_upload(self, bucket: str, key: str, upload_id: str) -> bool:
        return self.boto3_client.abort_multipart_upload(bucket, key, upload_id)


    async def upload_part_copy(self, bucket: str, key: str, upload_id: str, part_number: int, 
                              source_bucket: str, source_key: str, source_start: int, source_end: int) -> Optional[dict]:
        return await self.boto3_client.upload_part_copy(bucket, key, upload_id, part_number, source_bucket, source_key, source_start, source_end)

    #########################################################
    # Delete object
    #########################################################
    async def delete_prefix(self, bucket, prefix):
        """
        Delete all objects in the given bucket with the specified prefix (i.e., a 'directory').
        """
        bucket_obj = self.s3_resource.Bucket(bucket)
        objects_to_delete = bucket_obj.objects.filter(Prefix=prefix)
        delete_list = [{'Key': obj.key} for obj in objects_to_delete]
        if not delete_list:
            return True
        # S3 delete_objects supports up to 1000 keys per call
        tasks = []
        for i in range(0, len(delete_list), 1000):
            chunk = delete_list[i:i+1000]
            # 使用 run_in_executor 来在线程池中执行同步的 delete_objects 操作
            def delete_chunk(chunk_data):
                return bucket_obj.delete_objects(Delete={'Objects': chunk_data})
            tasks.append(asyncio.to_thread(delete_chunk, chunk))
        await asyncio.gather(*tasks)
        return True
    
    def delete_object(self, bucket, key):
        try:
            self.boto3_client.s3_client.delete_object(Bucket=bucket, Key=key)
        except Exception as e:
            print_error(f"Error deleting object {bucket}/{key}: {e}")
            raise e
    
    #########################################################
    # metadata
    #########################################################
    def get_object_metadata(self, bucket, key):
        try:
            response = self.boto3_client.s3_client.head_object(Bucket=bucket, Key=key)
            return {
                'Size': response['ContentLength'],
                'ETag': response['ETag'],
                'LastModified': response['LastModified'],
                'ContentType': response['ContentType'],
                'Metadata': response.get('Metadata', {})
            }
        except self.boto3_client.s3_client.exceptions.ClientError as e:
            raise e

    async def get_object_size(self, bucket: str, key: str) -> Optional[int]:
        """Get the size of an object in bytes"""
        loop = asyncio.get_event_loop()
        def task():
            try:
                response = self.boto3_client.s3_client.head_object(Bucket=bucket, Key=key)
                return response.get('ContentLength')
            except Exception as e:
                print_error(f"Error getting object size: {e}")
                return None
        return await loop.run_in_executor(None, task)

    def head_object(self, bucket: str, key: str) -> Optional[dict]:
        """
        Get object metadata from S3 using head_object
        Returns dict with LastModified, ContentLength, ETag, etc.
        """
        try:
            response = self.boto3_client.s3_client.head_object(Bucket=bucket, Key=key)
            return {
                'LastModified': response.get('LastModified'),
                'ContentLength': response.get('ContentLength'),
                'ETag': response.get('ETag'),
                'ContentType': response.get('ContentType'),
                'Metadata': response.get('Metadata', {})
            }
        except self.boto3_client.s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"Object not found: {bucket}/{key}")
                return None
            else:
                print(f"Error in head_object for {bucket}/{key}: {e}")
                return None
        except Exception as e:
            print(f"Unexpected error in head_object for {bucket}/{key}: {e}")
            return None

    #########################################################
    # other functions
    #########################################################
    def copy_object(self, src_bucket, src_key, dst_bucket, dst_key, start=None, end=None):
        copy_source = {'Bucket': src_bucket, 'Key': src_key}
        if start is not None and end is not None:
            # 使用multipart upload的UploadPartCopy实现分片copy
            s3 = self.boto3_client.s3_client
            object_size = s3.head_object(Bucket=src_bucket, Key=src_key)['ContentLength']
            part_size = end - start + 1
            print_info(f"[S3Boto3] Multipart range copy: {src_bucket}/{src_key} [{start}-{end}] -> {dst_bucket}/{dst_key}")
            # 1. 创建multipart upload
            mpu = s3.create_multipart_upload(Bucket=dst_bucket, Key=dst_key)
            upload_id = mpu['UploadId']
            # 2. 只copy一个part
            part = s3.upload_part_copy(
                Bucket=dst_bucket,
                Key=dst_key,
                PartNumber=1,
                UploadId=upload_id,
                CopySource=copy_source,
                CopySourceRange=f'bytes={start}-{end}'
            )
            # 3. 完成multipart upload
            s3.complete_multipart_upload(
                Bucket=dst_bucket,
                Key=dst_key,
                UploadId=upload_id,
                MultipartUpload={'Parts': [{'ETag': part['CopyPartResult']['ETag'], 'PartNumber': 1}]}
            )
        else:
            print_info(f"[S3Boto3] Full copy: {src_bucket}/{src_key} -> {dst_bucket}/{dst_key}")
            self.boto3_client.s3_client.copy_object(
                Bucket=dst_bucket,
                Key=dst_key,
                CopySource=copy_source
            )


    def create_bucket(self, bucket: str) -> bool:
        """Create bucket, ignore if already exists"""
        try:
            self.boto3_client.s3_client.create_bucket(Bucket=bucket)
            print_info(f"Created bucket: {bucket}")
            return True
        except self.boto3_client.s3_client.exceptions.BucketAlreadyOwnedByYou:
            print_info(f"Bucket already exists and owned: {bucket}")
            return True
        except self.boto3_client.s3_client.exceptions.BucketAlreadyExists:
            print_info(f"Bucket already exists: {bucket}")
            return True
        except Exception as e:
            print_error(f"Error creating bucket {bucket}: {e}")
            return False
    
