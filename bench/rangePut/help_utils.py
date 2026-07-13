from typing import List, Dict
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from executors.boto3_executor import Boto3Executor
from executors.object_store_executor import ObjectStoreExecutor
import random
import numpy as np
from typing import Tuple


def generate_patch_ranges(file_size_bytes: int, patch_percent: float,
                         chunk_size: int = 64 * 1024 * 1024,  # 默认64MB
                         variability: Tuple[float, float] = (0.5, 2.0)) -> List[Tuple[int, int]]:
        """
        生成模拟 patch 范围，模拟真实workload：
        - patch大小分布为1KB、1MB、10MB、40MB
        - patch分布在不同chunk，chunk选择用zipfian分布模拟热点
        - patch偏移和大小都随机，且不会越界
        - patch_percent决定总patch字节数
        - 返回[(offset, length)]
        """
        total_patch_bytes = int(file_size_bytes * patch_percent)
        # 4kb 8kb 32kb 64kb 128kb meta data write
        # 1mb 2mb 4mb block write
        # write_sizes = [4*1024, 8*1024, 32*1024, 64*1024, 128*1024, 1*1024*1024, 2*1024*1024, 4*1024*1024]
        write_sizes = [1*1024*1024, 2*1024*1024, 4*1024*1024]
        num_chunks = (file_size_bytes + chunk_size - 1) // chunk_size
        ranges = []
        written = 0
        # 预估写入次数，避免死循环
        max_writes = max(10, total_patch_bytes // min(write_sizes))
        chunk_indices = np.random.zipf(a=2, size=max_writes) - 1
        chunk_indices = [i % num_chunks for i in chunk_indices]
        i = 0
        while written < total_patch_bytes:
            chunk_idx = chunk_indices[i % len(chunk_indices)]
            patch_len = random.choice(write_sizes)
            patch_len = min(patch_len, file_size_bytes - 1)
            chunk_start = chunk_idx * chunk_size
            chunk_end = min(chunk_start + chunk_size, file_size_bytes)
            if patch_len > (chunk_end - chunk_start):
                patch_len = chunk_end - chunk_start
            if patch_len <= 0:
                patch_len = min(write_sizes)
            chunk_offset = random.randint(0, max(0, chunk_end - chunk_start - patch_len))
            offset = chunk_start + chunk_offset
            # 防止越界
            if offset + patch_len > file_size_bytes:
                patch_len = file_size_bytes - offset
            if patch_len <= 0:
                patch_len = 1  # 保证patch长度始终大于0
            ranges.append((offset, patch_len))
            written += patch_len
            i += 1
        print(f"Generated {len(ranges)} ranges for {file_size_bytes} bytes file with {patch_percent} patch percent")
        print(ranges)
        return ranges


def generate_patch_ranges2(file_size_bytes: int, modified_size_mb: int,
                           chunk_size: int = 4 * 1024 * 1024) -> List[Tuple[int, int]]:
    """
    生成模拟 patch 范围，模拟真实workload：
    - patch大小为chunk size
    - patch分布在[0, file_size_bytes - chunk_size]，不会越界
    - patch偏移和大小都随机，不会重叠
    - 总修改大小约为 modified_size_mb（向上取整）
    - 返回[(offset, length)]
    """
    total_modified_bytes = modified_size_mb * 1024 * 1024
    max_offset = file_size_bytes - chunk_size
    num_patches = (total_modified_bytes + chunk_size - 1) // chunk_size  # 向上取整

    # 使用集合记录已有patch的起始offset，避免重复
    used_offsets = set()
    patch_ranges = []

    attempts = 0
    max_attempts = num_patches * 10  # 防止死循环

    while len(patch_ranges) < num_patches and attempts < max_attempts:
        offset = random.randint(0, max_offset)
        aligned_offset = offset - (offset % chunk_size)  # 对齐

        if aligned_offset not in used_offsets:
            used_offsets.add(aligned_offset)
            patch_ranges.append((aligned_offset, chunk_size))
        attempts += 1

    return patch_ranges


def get_executor(executor_type: str, bucket_name: str = 'datasize2'):
    if executor_type == 'boto3':
        return Boto3Executor(bucket_name)
    elif executor_type == 'object_store':
        return ObjectStoreExecutor(bucket_name)
    else:
        raise ValueError(
            f"Unknown executor type: {executor_type}. "
            "rangePut supports: boto3, object_store."
        )

# get average latency and throughput
def summary_results(results_list: List[Dict], test_type: str):
    """生成摘要结果，创建两个表格：平均延迟和平均吞吐量"""
    # 收集所有executor和test_name
    executors = set()
    test_names = set()
    
    for result in results_list:
        executors.add(result['executor'])
        for experiment_name, experiment in result['experiments'].items():
            test_names.add(experiment['test_type'])
    
    executors = sorted(list(executors))
    
    # 按照文件大小排序 test_names
    def extract_size(test_name):
        """从测试名称中提取文件大小（MB）"""
        import re
        # 匹配 read_file_4096MB_128MB 或 write_file_4096MB_128MB 格式（没有range关键字）
        match = re.search(r'(\d+)MB_(\d+)MB', test_name)
        if match:
            # 如果有两个数字，优先按文件大小排序，然后按范围大小排序
            file_size = int(match.group(1))
            range_size = int(match.group(2))
            return (file_size, range_size)
        
        # 匹配 write_file_1024MB_range_64MB 格式（有range关键字）
        match = re.search(r'(\d+)MB.*?range.*?(\d+)MB', test_name)
        if match:
            # 如果有两个数字，优先按文件大小排序，然后按范围大小排序
            file_size = int(match.group(1))
            range_size = int(match.group(2))
            return (file_size, range_size)
        
        # 匹配单个数字的格式，如 write_128MB
        match = re.search(r'(\d+)MB', test_name)
        if match:
            return (int(match.group(1)), 0)
        
        return (0, 0)
    
    test_names = sorted(list(test_names), key=extract_size)
    
    # 创建延迟表格
    latency_table = {}
    for executor in executors:
        latency_table[executor] = {}
        for test_name in test_names:
            latency_table[executor][test_name] = None
    
    # 创建吞吐量表格
    throughput_table = {}
    for executor in executors:
        throughput_table[executor] = {}
        for test_name in test_names:
            throughput_table[executor][test_name] = None
    
    # 填充数据
    for result in results_list:
        executor = result['executor']
        for experiment_name, experiment in result['experiments'].items():
            test_name = experiment['test_type']
            latency_table[executor][test_name] = experiment['avg_latency']
            throughput_table[executor][test_name] = experiment['avg_throughput']
    
    return latency_table, throughput_table, executors, test_names

def print_tables_to_file(latency_table: Dict, throughput_table: Dict, executors: List[str], test_names: List[str], filename: str = 'benchmark_results.txt'):
    """将表格打印到文件"""
    with open(filename, 'w') as f:
        # 打印延迟表格
        f.write("=" * 80 + "\n")
        f.write("AVERAGE LATENCY (seconds)\n")
        f.write("=" * 80 + "\n")
        
        # 表头
        f.write(f"{'Executor':<20}")
        for test_name in test_names:
            f.write(f" {test_name:<14}")  # 添加前导空格
        f.write("\n")
        
        # 分隔线
        f.write("-" * 80 + "\n")
        
        # 数据行
        for executor in executors:
            f.write(f"{executor:<20}")
            for test_name in test_names:
                value = latency_table[executor][test_name]
                if value is not None:
                    f.write(f" {value:<14.3f}")  # 添加前导空格
                else:
                    f.write(f" {'N/A':<14}")  # 添加前导空格
            f.write("\n")
        
        f.write("\n\n")
        
        # 打印吞吐量表格
        f.write("=" * 80 + "\n")
        f.write("AVERAGE THROUGHPUT (MB/s)\n")
        f.write("=" * 80 + "\n")
        
        # 表头
        f.write(f"{'Executor':<20}")
        for test_name in test_names:
            f.write(f" {test_name:<14}")  # 添加前导空格
        f.write("\n")
        
        # 分隔线
        f.write("-" * 80 + "\n")
        
        # 数据行
        for executor in executors:
            f.write(f"{executor:<20}")
            for test_name in test_names:
                value = throughput_table[executor][test_name]
                if value is not None:
                    f.write(f" {value:<14.2f}")  # 添加前导空格
                else:
                    f.write(f" {'N/A':<14}")  # 添加前导空格
            f.write("\n")
        
        f.write("\n" + "=" * 80 + "\n")
        f.write("Benchmark completed successfully!\n")
        f.write("=" * 80 + "\n")
    
    print(f"📊 结果表格已保存到: {filename}")
