# -*- coding: utf-8 -*-
import subprocess
import time
import os
import json
from abc import ABC, abstractmethod
from operator import add
import argparse
from typing import Tuple
import random
from typing import List
import numpy as np
import boto3
from botocore.config import Config
from boto3.s3.transfer import TransferConfig
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from s3_utils.s3_boto3 import S3Boto3
from common.constants import *

class BaseExecutor(ABC):
    """
    执行器的抽象基类。
    每个测试工具 (fio, aws, curl) 都应该有一个对应的实现。
    """
    def __init__(self, bucket_name: str = 'datasize2', mount_dir: str = '/tmp/fio_test'):
        self.bucket_name = bucket_name
        self.mount_dir = mount_dir
        self.created_files = []
        print(f"--- Initializing {self.get_name()} Executor ---")
    
    def check_file_exists(self, file_path, file_size_mb)->bool:
        create_file = False
        if os.path.exists(file_path):
            if os.path.getsize(file_path) == file_size_mb * 1024 * 1024:
                return True
            else:
                os.remove(file_path)
                create_file = True
        else:
            create_file = True
        if create_file:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'wb') as f:
                f.seek(file_size_mb * 1024 * 1024 - 1)
                f.write(b'\0')
                f.flush()
        self.created_files.append(file_path)
        return True

    @staticmethod
    def cleanup(self):
        for file_path in self.created_files:
            if os.path.exists(file_path):
                os.remove(file_path)
        self.created_files = []

    @abstractmethod
    def get_name(self):
        """返回执行器的名称。"""
        pass

    def prepare(self, bucket_name: str = 'datasize2'):
        # 配置AWS凭证和区域
        self.region = (
            os.environ.get("AWS_DEFAULT_REGION")
            or os.environ.get("AWS_REGION")
            or AWS_REGION
        )
        self.aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        
        # 创建自定义的boto3_client
        from s3_utils.client import boto3_client
        self.boto3_client_instance = boto3_client()
        
        # 配置传输参数
        self.transfer_config = TransferConfig(
            multipart_threshold=MULTIPART_THRESHOLD,
            max_concurrency=MAX_CONCURRENCY,
            multipart_chunksize=MULTIPART_CHUNK_SIZE,
            use_threads=True
        )
        
        # 初始化S3Boto3，使用自定义的boto3_client实例
        self.s3_boto3 = S3Boto3(self.boto3_client_instance, self.transfer_config)
        
        print(f"[{self.get_name()}] Bucket: {self.bucket_name}")
        print(f"[{self.get_name()}] Region: {self.region}")


    #########################################################
    # 写入文件
    #########################################################
    @abstractmethod
    def write_file(self, file_path, file_size_mb) -> Tuple[float, float]:
        """
        写入一个指定大小的文件。
        """
        pass

    @abstractmethod
    def write_ranges_file(self, file_path, file_size_mb, modified_size_mb: int, ranges: List[Tuple[int, int]]) -> Tuple[float, float]:
        """
        写入一个指定大小的文件的指定范围。
        """
        pass

    #########################################################
    # 读取文件
    #########################################################

    @abstractmethod
    def read_range_file(self, file_name, start_offset, end_offset):
        """
        读取一个指定大小的文件的指定范围。
        """
        pass
    
    #########################################################
    # metadata 
    #########################################################
    @abstractmethod
    def state_file(self, file_name):
        """
        获取一个指定大小的文件的元数据。
        """
        pass

    @abstractmethod
    def update_metadata(self, file_name, metadata):
        """
        更新一个指定大小的文件的元数据。
        """
        pass

    #########################################################
    # 删除文件
    #########################################################
    @abstractmethod
    def delete_file(self, file_name):
        """
        删除一个指定大小的文件。
        """
        pass
