from typing import AsyncGenerator
from s3_utils.client import boto3_client
from s3_utils.s3_boto3 import S3Boto3
from object_store.objects_manager import ObjectsManager
from fastapi import Depends

# global instances
s3_boto_instance: S3Boto3 = None
objects_manager_instance: ObjectsManager = None

async def get_s3_boto() -> S3Boto3:
    global s3_boto_instance
    if s3_boto_instance is None:
        s3_raw = boto3_client()
        s3_boto_instance = S3Boto3(
            s3_client=s3_raw,
            transfer_config=s3_raw.get_transfer_config()
        )
    return s3_boto_instance

async def get_object_manager() -> ObjectsManager:
    global objects_manager_instance
    if objects_manager_instance is None:
        objects_manager_instance = ObjectsManager(await get_s3_boto())
    return objects_manager_instance
