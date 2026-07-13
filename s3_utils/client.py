import os

import boto3
from botocore.config import Config
from boto3.s3.transfer import TransferConfig
from common.constants import *
from io import BytesIO
import asyncio
import time
from common.logger import print_error, print_debug, print_info
from typing import Optional

class boto3_client:
    def __init__(self):
        """
        Initialize the S3 client with the configuer parameters.
        """
        self.boto_config = Config(
            max_pool_connections=MAX_POOL_CONNECTIONS,
            connect_timeout=CONNECT_TIMEOUT,
            read_timeout=  READ_TIMEOUT
        )

        self.transfer_config = TransferConfig(
            preferred_transfer_client='crt',
            max_concurrency=MAX_CONCURRENCY,
            multipart_threshold=MULTIPART_THRESHOLD,
            multipart_chunksize=MULTIPART_CHUNK_SIZE,
            use_threads=True,
            max_io_queue=1000,
            io_chunksize=262144
        )

        # Same as CRT: default credential chain (~/.aws/credentials, AWS_* env, instance role).
        # Do not pass static keys here so boto3 matches AwsCredentialsProvider.new_default_chain().
        region_name = (
            os.environ.get("AWS_DEFAULT_REGION")
            or os.environ.get("AWS_REGION")
            or AWS_REGION
        )
        self.s3_client = boto3.client(
            "s3",
            region_name=region_name,
            config=self.boto_config,
        )

    def get_client(self):
        return self.s3_client

    def get_transfer_config(self):
        return self.transfer_config
    
    async def get_object(self, bucket, key, start=None, end=None)->bytes:
        """Get object using boto3 with true concurrency"""
        loop = asyncio.get_running_loop()

        print_debug(f"boto3 get_object s3://{bucket}/{key} start={start} end={end}")
        if start is not None and end is not None:
            expected = end - start + 1

            def _call_range():
                return self.s3_client.get_object(
                    Bucket=bucket,
                    Key=key,
                    Range=f"bytes={start}-{end}"
                )['Body'].read()

            last_error = None
            for attempt in range(1, 4):
                try:
                    data = await loop.run_in_executor(None, _call_range)
                    if len(data) != expected:
                        raise IOError(
                            f"boto3 range GET short read s3://{bucket}/{key} "
                            f"bytes={start}-{end} expected={expected} got={len(data)}"
                        )
                    return data
                except Exception as e:
                    last_error = e
                    print_error(
                        f"boto3 get_object range error attempt={attempt} "
                        f"s3://{bucket}/{key} bytes={start}-{end}: {e}"
                    )
                    if attempt < 3:
                        await asyncio.sleep(0.1 * attempt)
            raise last_error
        else:
            try:
                return await self.get_whole_object(bucket, key)
            except Exception as e:
                print_error(f"boto3 get_object whole error s3://{bucket}/{key}: {e}")
                raise
    
    async def put_object_boto3(self, bucket, key, data:bytes)->bool:
        """Upload object using boto3 with true concurrency"""
        loop = asyncio.get_running_loop()

        def _call():
            return self.s3_client.upload_fileobj(
                Fileobj=BytesIO(data),
                Bucket=bucket,
                Key=key,
                Config=self.transfer_config
            )

        try:
            print_debug(f"boto3 put_object s3://{bucket}/{key} bytes={len(data)}")
            await loop.run_in_executor(None, _call)
            print_info(f"boto3 put_object done s3://{bucket}/{key} bytes={len(data)}")
            return True
        except Exception as e:
            print_error(f"boto3 put_object error: {e}")
            return False
    
    async def put_object_from_file(self, bucket, key, filename)->bool:
        """Upload object from file using boto3 with true concurrency"""
        loop = asyncio.get_running_loop()

        def _call():
            return self.s3_client.upload_file(
                Filename=filename,
                Bucket=bucket,
                Key=key,
                Config=self.transfer_config
            )

        try:
            print_debug(f"boto3 put_object_from_file s3://{bucket}/{key} file={filename}")
            await loop.run_in_executor(None, _call)
            return True
        except Exception as e:
            print_error(f"boto3 put_object_from_file error: {e}")
            return False
    
    async def get_whole_object(self, bucket, key)->bytes:
        """Get whole object using boto3 with true concurrency"""
        loop = asyncio.get_running_loop()

        def _call():
            buffer = BytesIO()
            self.s3_client.download_fileobj(
                Bucket=bucket, 
                Key=key, 
                Fileobj=buffer,
                Config=self.transfer_config
            )
            return buffer.getvalue()

        try:
            print_debug(f"boto3 get_whole_object s3://{bucket}/{key}")
            return await loop.run_in_executor(None, _call)
        except Exception as e:
            print_error(f"boto3 download_object error: {e}")
            return b''
    #########################################################
    # Multipart upload
    #########################################################
    def create_multipart_upload(self, bucket: str, key: str) -> Optional[dict]:
        try:
            response = self.s3_client.create_multipart_upload(Bucket=bucket, Key=key)
            return response
        except Exception as e:
            print_error(f"Error creating multipart upload: {e}")
            return None

    async def upload_part(self, bucket: str, key: str, upload_id: str, part_number: int, data: bytes) -> Optional[dict]:
        """Upload a part using UploadPart"""
        loop = asyncio.get_running_loop()

        def _call():
            return self.s3_client.upload_part(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=data
            )

        try:
            response = await loop.run_in_executor(None, _call)
            print_debug(f"Upload part response: {response}")
            return response
        except Exception as e:
            print_error(f"Error uploading part {part_number}: {e}")
            return None

    def complete_multipart_upload(self, bucket: str, key: str, upload_id: str, parts: list) -> Optional[dict]:
        print_debug(
            f"boto3 complete_multipart_upload s3://{bucket}/{key} "
            f"upload_id={upload_id} parts={len(parts)}"
        )
        try:
            return self.s3_client.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
        except Exception as e:
            print_error(f"Error completing multipart upload: {e}")
            raise

    def abort_multipart_upload(self, bucket: str, key: str, upload_id: str) -> bool:
        try:
            self.s3_client.abort_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id
            )
            return True
        except Exception as e:
            print_error(f"Error aborting multipart upload: {e}")
            return False    

    async def upload_part_copy(self, bucket: str, key: str, upload_id: str, part_number: int, 
                              source_bucket: str, source_key: str, source_start: int, source_end: int) -> Optional[dict]:
        """Upload a part by copying from another object using UploadPartCopy"""
        loop = asyncio.get_running_loop()
        copy_source = {'Bucket': source_bucket, 'Key': source_key}
        copy_source_range = f"bytes={source_start}-{source_end}"

        def _call():
            return self.s3_client.upload_part_copy(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_number,
                CopySource=copy_source,
                CopySourceRange=copy_source_range,
            )

        try:
            response = await loop.run_in_executor(None, _call)
            print_debug(f"Upload part copy response: {response}")
            return response
        except Exception as e:
            print_error(f"Error uploading part copy {part_number}: {e}")
            return None

def get_test_data(client):
    start_time = time.time()
    start = 0
    end = 100*1024*1024
    data = client.get_object("datasize3", "fio_test.0.0", start, end)
    print_info(f"boto3 get_object bytes={len(data)}")
    end_time = time.time()
    print_info(f"boto3 get_object elapsed_s={end_time - start_time}")

def get_whole_object_test(client:boto3_client):
    start_time = time.time()
    data = client.get_whole_object("datasize3", "fio_test.0.0")
    print_info(f"boto3 get_whole_object bytes={len(data)}")
    end_time = time.time()
    print_info(f"boto3 get_whole_object elapsed_s={end_time - start_time}")

if __name__ == "__main__":
    client = boto3_client()
    print_info("Starting S3 read test")
    try:
        get_whole_object_test(client)
    except Exception as e:
        print_error(f"S3 read test failed: {e}")
        print_error("Check AWS credentials and bucket configuration.")
