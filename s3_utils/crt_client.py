import os

from awscrt import io, auth, s3
from s3transfer.crt import CRTTransferManager, BotocoreCRTRequestSerializer
import botocore.session
from io import BytesIO
from awscrt.http import HttpRequest, HttpHeaders
import asyncio
from awscrt.s3 import S3RequestType
from boto3.s3.transfer import TransferConfig
# , S3UploadPart, S3ChecksumAlgorithm
from typing import Optional
from common.logger import print_debug, print_error, print_info
from common.constants import *
import time

class CRTClient:
    def __init__(self, region: Optional[str] = None):
        # Align with boto3_client: env first, then constants.AWS_REGION
        self.region = (
            region
            or os.environ.get("AWS_DEFAULT_REGION")
            or os.environ.get("AWS_REGION")
            or AWS_REGION
        )
        self._init_crt_components()

    def _init_crt_components(self):
        self.event_loop_group = io.EventLoopGroup()
        self.host_resolver = io.DefaultHostResolver(self.event_loop_group)
        self.client_bootstrap = io.ClientBootstrap(self.event_loop_group, self.host_resolver)

        self.credentials_provider = auth.AwsCredentialsProvider.new_default_chain(self.client_bootstrap)

        self.s3_client = s3.S3Client(
            bootstrap=self.client_bootstrap,
            region=self.region,
            credential_provider=self.credentials_provider,
            # part_size=PART_SIZE,
            multipart_upload_threshold=MULTIPART_THRESHOLD,
        )

        session = botocore.session.get_session()
        self.request_serializer = BotocoreCRTRequestSerializer(session)

        self.transfer_manager = CRTTransferManager(
            crt_s3_client=self.s3_client,
            crt_request_serializer=self.request_serializer
            # config=cfg
        )

    async def upload_file(self,  bucket: str, key: str, file_path: str)->bool:
        """Upload file using CRT with true concurrency"""
        loop = asyncio.get_running_loop()
        file_size = os.path.getsize(file_path)

        def _call():
            with open(file_path, 'rb') as f:
                future = self.transfer_manager.upload(
                    fileobj=f,
                    bucket=bucket,
                    key=key,
                    extra_args={},
                    subscribers={}
                )
                return future.result()

        try:
            upload_start = time.time()
            print_debug(
                f"CRT upload_file start s3://{bucket}/{key} "
                f"file={file_path} bytes={file_size}"
            )
            await loop.run_in_executor(None, _call)
            elapsed = time.time() - upload_start
            throughput = file_size / elapsed / 1024 / 1024 if elapsed > 0 else 0
            print_info(
                f"CRT upload_file done s3://{bucket}/{key} "
                f"bytes={file_size} elapsed={elapsed:.3f}s throughput={throughput:.2f}MiB/s"
            )
            print_debug("CRT upload_file complete")
            return True
        except Exception as e:
            print_error(f"Error uploading file {file_path} to {bucket}/{key}: {e}")
            return False

    async def upload_bytes(self, bucket: str, key: str, data: bytes)->bool:
        """Upload bytes using CRT with true concurrency"""
        loop = asyncio.get_running_loop()

        def _call():
            future = self.transfer_manager.upload(
                fileobj=BytesIO(data),
                bucket=bucket,
                key=key,
                extra_args={},
                subscribers={}
            )
            return future.result()

        try:
            print_debug(f"CRT upload_bytes start s3://{bucket}/{key} bytes={len(data)}")
            await loop.run_in_executor(None, _call)
            print_debug("CRT upload_bytes complete")
            return True
        except Exception as e:
            print_error(f"Error uploading object {bucket}/{key}: {e}")
            return False

    def shutdown(self):
        self.transfer_manager.shutdown()

    async def download_object_to_file(self, bucket: str, key: str, file_path: str) -> int:
        """Download full object to a local path (CRT transfer manager). Returns file size in bytes."""
        loop = asyncio.get_running_loop()

        def _call() -> int:
            with open(file_path, "wb") as f:
                future = self.transfer_manager.download(bucket, key, fileobj=f)
                future.result()
            return os.path.getsize(file_path)

        return await loop.run_in_executor(None, _call)

    async def get_whole_object(self, bucket: str, key: str) -> bytes:
        """Get whole object using CRT with true concurrency"""
        loop = asyncio.get_running_loop()

        def _call():
            buffer = BytesIO()
            future = self.transfer_manager.download(bucket, key, fileobj=buffer)
            future.result()
            return buffer.getvalue()

        try:    
            print_debug(f"CRT get_whole_object s3://{bucket}/{key}")
            result = await loop.run_in_executor(None, _call)
            print_info(f"CRT get_whole_object done s3://{bucket}/{key} bytes={len(result)}")
            return result
        except Exception as e:
            print_error(f"Error downloading object {bucket}/{key}: {e}")
            return b''
    
    async def get_object(self, bucket: str, key: str, start: int, end: int) -> bytes:
        range_header = f"bytes={start}-{end}"
        
        headers = HttpHeaders()
        headers.add("host", f"{bucket}.s3.{self.region}.amazonaws.com")
        headers.add("range", range_header)
        
        request = HttpRequest(
            method="GET",
            path=f"/{key}",
            headers=headers
        )

        body_parts = bytearray()
        loop = asyncio.get_event_loop()
        request_error = None

        def on_body(chunk: bytes, **kwargs):
            body_parts.extend(chunk)

        loop = asyncio.get_event_loop()
        done_event = asyncio.Event()

        def on_done(error=None, **kwargs):
            nonlocal request_error
            print_debug("CRT range GET completed")
            if error is None:
                print_debug("CRT range GET success")
            else:
                request_error = error
                print_error(f"CRT range GET error: {error}")
            try:
                if not loop.is_closed():
                    loop.call_soon_threadsafe(done_event.set)
                else:
                    done_event.set()
            except Exception as e:
                print_error(f"Warning: Error setting done event: {e}")
                try:
                    done_event.set()
                except:
                    pass

        
        try:
            future = self.s3_client.make_request(
                request=request,
                type=S3RequestType.GET_OBJECT,
                on_body=on_body,
                on_done=on_done
            )
            await done_event.wait()
            if request_error is not None:
                raise RuntimeError(
                    f"CRT range GET failed for s3://{bucket}/{key} {range_header}: {request_error}"
                )
            
            return bytes(body_parts)
        except Exception as e:
            print_error(f"Error downloading object {bucket}/{key}: {e}")
            import traceback
            print_error(f"Traceback: {traceback.format_exc()}")
            raise
    

async def main():
    crt_client = CRTClient()
    data = await crt_client.get_object("rawiotest", "Boto3-S3_test_file_4096MB", 0, 4294967295)  # 5MB
    print_info(f"Downloaded bytes={len(data)}")
    crt_client.shutdown()

def get_test_data():
    crt_client = CRTClient()
    start_time = time.time()
    start = 0
    end = 100*1024*1024
    data = asyncio.run(crt_client.get_object("datasize3", "fio_test.0.0", start, end))
    print_info(f"boto3 get_object bytes={len(data)}")
    end_time = time.time()
    print_info(f"boto3 get_object elapsed_s={end_time - start_time}")

def get_whole_object_test():
    crt_client = CRTClient()
    start_time = time.time()
    data = crt_client.get_whole_object("datasize3", "fio_test.0.0")
    print_info(f"boto3 get_whole_object bytes={len(data)}")
    end_time = time.time()
    print_info(f"boto3 get_whole_object elapsed_s={end_time - start_time}")

if __name__ == "__main__":
    get_whole_object_test()
