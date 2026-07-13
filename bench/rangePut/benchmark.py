# -*- coding: utf-8 -*-
import sys
import os
import time
import json
import argparse
from typing import Dict, List, Tuple, Any, Callable
from datetime import datetime, timezone
import random

# 添加父目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from executors.base_executor import BaseExecutor
from help_utils import get_executor, summary_results, print_tables_to_file, generate_patch_ranges, generate_patch_ranges2


class Benchmark:
    
    def __init__(self, executor: BaseExecutor, file_path: str, iterations: int = 3, save_result_flag: int = 1):
        self.executor = executor
        self.iterations = iterations
        self.results = {}
        self.result_dir = 'res/details'
        self.summary_dir = 'res/summary'
        self.file_path = file_path
        self.save_result_flag = save_result_flag

    def cleanup(self):
        self.executor.cleanup()

    def _save_results(self, results: Dict):
        if self.save_result_flag != 1:
            return
        executor_name = results['executor']
        test_type = results['test_type']
        result_dir = self.result_dir
        filename = f"{executor_name}_{test_type}_{int(time.time())}.json"
        full_path = os.path.join(result_dir, filename)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\nResults saved to: {full_path}")
        return full_path

    def run_experiment_group(self, test_type: str, experiment_configs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        运行多组实验的通用模板
        
        Args:
            experiment_configs: 实验配置列表，每个配置包含：
                - test_name: 测试名称
                - test_func: 测试函数
                - params: 测试参数字典
                - description: 测试描述（可选）
            executor_method: 指定的executor方法名称
        
        Returns:
            包含所有实验结果的总结果字典
        """
        overall_start_time = datetime.now(timezone.utc)
        print(f"🚀 开始运行实验组，共 {len(experiment_configs)} 组实验")
        all_results = {
            'executor': self.executor.get_name(),
            'test_type': test_type,
            'overall_start_time': overall_start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'total_experiments': len(experiment_configs),
            'iterations_per_experiment': self.iterations,
            'experiments': {}
        }
        
        for i, config in enumerate(experiment_configs):
            test_name = config['test_name']
            test_func = config['test_func']
            params = config.get('params', {})
            description = config.get('description', f'Experiment {i+1}')
            
            print(f"\n{'='*60}")
            print(f"🧪 运行实验 {i+1}/{len(experiment_configs)}: {test_name}")
            print(f"📝 描述: {description}")
            print(f"⚙️  参数: {params}")
            print(f"{'='*60}")
            
            # 运行单个实验
            experiment_result = self._run_single_experiment(test_name, test_func, params)
            all_results['experiments'][test_name] = experiment_result
            
            print(f"✅ 实验 {test_name} 完成")
        
        overall_end_time = datetime.now(timezone.utc)
        overall_duration = overall_end_time - overall_start_time
        
        all_results['overall_end_time'] = overall_end_time.strftime('%Y-%m-%d %H:%M:%S')
        all_results['overall_duration_seconds'] = overall_duration.total_seconds()
        
        print(f"\n🎉 所有实验完成！")
        print(f"📊 总体结束时间: {overall_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏱️  总耗时: {overall_duration.total_seconds():.2f} 秒")
        
        # 保存详细结果
        self._save_results(all_results)
        return all_results

    def _run_single_experiment(self, test_name: str, test_func: Callable, params: Dict[str, Any], executor_method: str = None) -> Dict[str, Any]:
        """
        运行单个实验
        
        Args:
            test_name: 测试名称
            test_func: 测试函数
            params: 测试参数
            executor_method: 指定的executor方法名称
        
        Returns:
            实验结果字典
        """
        throughput_list = []
        latency_list = []
        test_details = {}
        for i in range(self.iterations):
            print(f"🔄 运行 {test_name} 测试，迭代 {i+1}/{self.iterations}...")
            
            try:
                # 调用测试函数（object_store range_write 可返回第三项：分项耗时）
                result = test_func(**params)
                if isinstance(result, tuple) and len(result) == 3:
                    throughput, latency, range_timings = result
                else:
                    throughput, latency = result
                    range_timings = None
                throughput_list.append(throughput)
                latency_list.append(latency)
                
                detail = {
                    'throughput_mbps': throughput,
                    'latency_s': latency,
                }
                if range_timings:
                    detail['range_write_timings'] = range_timings
                test_details[f'iteration_{i+1}'] = detail
                
                print(f"  ✅ 吞吐量: {throughput:.2f} MB/s, 延迟: {latency:.2f} s")
                
            except Exception as e:
                print(f"  ❌ 错误: {e}")
                throughput_list.append(0)
                latency_list.append(0)
                test_details[f'iteration_{i+1}'] = {
                    'throughput_mbps': 0,
                    'latency_s': 0,
                    'error': str(e)
                }
        
        # 计算统计信息
        valid_throughputs = [t for t in throughput_list if t > 0]
        valid_latencies = [l for l in latency_list if l > 0]
        
        avg_throughput = sum(valid_throughputs) / len(valid_throughputs) if valid_throughputs else 0
        avg_latency = sum(valid_latencies) / len(valid_latencies) if valid_latencies else 0
        min_throughput = min(throughput_list) if throughput_list else 0
        max_throughput = max(throughput_list) if throughput_list else 0
        min_latency = min(latency_list) if latency_list else 0
        max_latency = max(latency_list) if latency_list else 0
        
        result = {
            'test_type': test_name,
            'description': f'Experiment for {test_name}',
            'throughput_mbps': throughput_list,
            'latency_s': latency_list,
            'avg_throughput': avg_throughput,
            'avg_latency': avg_latency,
            'min_throughput': min_throughput,
            'max_throughput': max_throughput,
            'min_latency': min_latency,
            'max_latency': max_latency,
            'successful_iterations': len(valid_throughputs),
            'failed_iterations': len(throughput_list) - len(valid_throughputs),
            'test_details': test_details
        }
        return result

    # 创建实验配置
    def create_file_size_initial_write_experiment(self, file_sizes: List[int]) -> List[Dict[str, Any]]:
        """创建不同文件大小的写入实验配置"""
        configs = []
        for size in file_sizes:
            file_path = f"/tmp/{self.executor.get_name()}_test_file_{size}MB"
            configs.append({
                'test_name': f'write_file_{size}MB',
                'test_func': self.executor.write_file,
                'params': {'file_path': file_path, 'file_size_mb': size},
                'description': f'write file {size}MB test'
            })
        return configs
    
    def create_range_write_experiment(self, file_size: int, range_size_sets: List[int]) -> List[Dict[str, Any]]:
        """创建范围操作实验配置"""
        configs = []
        file_path = f"/tmp/{self.executor.get_name()}_test_file_{file_size}MB"
        for range_size in range_size_sets:
            random_start_offset = 0
            # random.randint(0, file_size * 1024 * 1024 - range_size * 1024 * 1024)
            configs.append({
                'test_name': f'write_file_{file_size}MB_range_{range_size}MB',
                'test_func': self.executor.write_ranges_file,
                    'params': {
                        'file_path': file_path,
                        'file_size_mb': file_size,
                        'modified_size_mb': range_size,
                        'ranges': [(random_start_offset, range_size*1024*1024)]
                    },
                    'description': f'write file {file_size}MB range {range_size}MB in {random_start_offset}-{random_start_offset+range_size*1024*1024} test'
                })
        return configs
    
    def create_range_write_in_files_experiment(self, file_size_sets: List[int], range_size:int) -> List[Dict[str, Any]]:
        """创建范围操作实验配置"""
        configs = []
        for file_size in file_size_sets:
            file_path = f"/tmp/{self.executor.get_name()}_test_file_{file_size}MB"
            random_start_offset =random.randint(0, file_size * 1024 * 1024 - range_size * 1024 * 1024)
            configs.append({
                'test_name': f'write_file_{file_size}MB_range_{range_size}MB',
                'test_func': self.executor.write_ranges_file,
                    'params': {
                        'file_path': file_path,
                        'file_size_mb': file_size,
                        'modified_size_mb': range_size,
                        'ranges': [(random_start_offset, range_size*1024*1024)]
                    },
                    'description': f'write file {file_size}MB range {range_size}MB in {random_start_offset}-{random_start_offset+range_size*1024*1024} test'
                })
        return configs


    def create_random_write_experiment(self, file_size: int, range_size_sets: List[int]) -> List[Dict[str, Any]]:
        """创建随机写入实验配置"""
        file_path = f"/tmp/{self.executor.get_name()}_test_file_{file_size}MB"
        configs = []
        for range_size_mb in range_size_sets:
            random_percent = range_size_mb / (file_size)
            ranges = generate_patch_ranges(file_size_bytes=file_size * 1024 * 1024, patch_percent=random_percent)
            modified_size_mb = file_size * random_percent
            configs.append({
                'test_name': f'write_file_{file_size}MB_random_{range_size_mb}MB',
                'test_func': self.executor.write_ranges_file,
                    'params': {
                        'file_path': file_path,
                        'file_size_mb': file_size,
                        'modified_size_mb': modified_size_mb,
                        'ranges': ranges
                    },
                    'description': f'write file {file_size}MB random {range_size_mb}MB test'
                })
        return configs
    
    def create_compact_experiment(self, file_size: int, range_size: int, compact_types: List[str]) -> List[Dict[str, Any]]:
        """创建压缩文件实验配置"""
        print(f"self.executor.get_name(): {self.executor.get_name()}")
        if self.executor.get_name() != 'object_store':
            return []
        configs = []
        file_path = f"/tmp/{self.executor.get_name()}_test_file_{file_size}MB"
        # random_percent = range_size / (file_size)
        # ranges = generate_patch_ranges(file_size_bytes=file_size * 1024 * 1024, patch_percent=random_percent)  
        modified_size_mb = range_size
        start_offset = random.randint(0, file_size * 1024 * 1024 - range_size * 1024 * 1024)
        length = range_size * 1024 * 1024  # range_size MB in bytes
        ranges = [(start_offset, length)]
        # 1. write data
        self.executor.write_file(file_path, file_size)
        # 2. write log data
        self.executor.write_ranges_file(file_path, file_size, modified_size_mb, ranges)
        # 3. compact file
        for compact_type in compact_types:
            if compact_type == 'compact_file_from_chunks':
                test_func = self.executor.compact_file_from_chunks
                test_name = f'{compact_type}_{file_size}MB_random_{range_size}MB'
                description = f'compact file {file_size}MB from chunks test'
            elif compact_type == 'compact_file_from_logs':
                test_func = self.executor.compact_file_from_logs
                test_name = f'{compact_type}_{file_size}MB_random_{range_size}MB'
                description = f'compact file {file_size}MB from logs test'
            elif compact_type == 'compact_chunks':
                test_func = self.executor.compact_chunks
                test_name = f'{compact_type}_{file_size}MB_random_{range_size}MB'
                description = f'compact chunks {file_size}MB test'
            else:
                raise ValueError(f"Unknown compact_type: {compact_type}")
            print(f"test_name: {test_name}, test_func: {test_func}, params: {ranges}, description: {description}")
            configs.append({
                'test_name': test_name,
                'test_func': test_func,
                'params': {
                    'file_path': file_path, 
                    'file_size_mb': file_size, 
                    'modified_size_mb': modified_size_mb,
                    'ranges': ranges
                },
                'description': description
            })
        return configs
    
    def create_compact_log_after_write_experiment2(self, file_size: int,  each_write_size_sets: List[int]) -> List[Dict[str, Any]]:
        """创建压缩文件实验配置"""
        print(f"self.executor.get_name(): {self.executor.get_name()}")
        if self.executor.get_name() != 'object_store':
            return []
        configs = []
        file_path = f"/tmp/{self.executor.get_name()}_test_file_{file_size}MB"
        # random_percent = range_size / (file_size)
        # ranges = generate_patch_ranges(file_size_bytes=file_size * 1024 * 1024, patch_percent=random_percent)  
        modified_size_mb = 256
        for each_write_size in each_write_size_sets:
            start_offset = random.randint(0, file_size * 1024 * 1024 - modified_size_mb * 1024 * 1024)
            ranges = generate_patch_ranges2(file_size_bytes=file_size * 1024 * 1024, modified_size_mb=modified_size_mb, chunk_size=each_write_size*1024*1024)  
            # print(f"ranges: {ranges}")
            # 2. write log data
            self.executor.write_ranges_file(file_path, file_size, modified_size_mb, ranges)
            # return
            # return
            # 3. compact file
            test_func = self.executor.compact_file_from_logs
            test_name = f'compact_file_from_logs_{file_size}MB_each_write_{each_write_size}MB'
            description = f'compact file {file_size}MB from logs test'
            print(f"test_name: {test_name}, test_func: {test_func}, params: {ranges}, description: {description}")
            configs.append({
                    'test_name': test_name,
                    'test_func': test_func,
                    'params': {
                        'file_path': file_path, 
                        'file_size_mb': file_size, 
                        'modified_size_mb': modified_size_mb,
                        'ranges': ranges
                    },
                    'description': description
                })
        return configs

    def create_compact_log_after_write_experiment(self, file_size: int,  each_write_size_sets: List[int]) -> List[Dict[str, Any]]:
        """创建压缩文件实验配置"""
        print(f"self.executor.get_name(): {self.executor.get_name()}")
        if self.executor.get_name() != 'object_store':
            return []
        configs = []
        file_path = f"/tmp/{self.executor.get_name()}_test_file_{file_size}MB"
        total_modified_size_mb = each_write_size_sets[0] 
        iterations = int(total_modified_size_mb / 5)
        print(f"writing {total_modified_size_mb}MB {iterations} times")
        for i in range(iterations): 
            start_offset = random.randint(0, file_size * 1024 * 1024 - 5 * 1024 * 1024)
            length = 5 * 1024 * 1024  # 5MB in bytes
            ranges = [(start_offset, length)]
            self.executor.write_ranges_file(file_path, file_size,5, ranges)
        # 3. compact file
        test_func = self.executor.compact_file_from_logs
        test_name = f'compact_file_from_logs_{file_size}MB_each_write_{total_modified_size_mb}MB'
        description = f'compact file {file_size}MB from logs test'
        print(f"test_name: {test_name}, test_func: {test_func}, params: {ranges}, description: {description}")
        configs.append({
                'test_name': test_name,
                'test_func': test_func,
                'params': {
                    'file_path': file_path, 
                    'file_size_mb': file_size, 
                    'modified_size_mb': total_modified_size_mb,
                    'ranges': ranges
                },
                'description': description
            })
        return configs

    def create_read_experiment(self, file_size_sets: List[int], range_size_sets: List[float]) -> List[Dict[str, Any]]:
        # 在s3上随机写入数据
        # 整个文件读取, range_size_sets[0] == 0 and len(range_size_sets) == 1
        if range_size_sets[0] == 0 and len(range_size_sets) == 1:
            configs = []
            for file_size in file_size_sets:
                file_path = f"/tmp/{self.executor.get_name()}_test_file_{file_size}MB"
                # self.executor.write_file(file_path, file_size)
                file_name = os.path.basename(file_path)
                configs.append({
                    'test_name': f'read_whole_file_{file_size}MB',
                    'test_func': self.executor.read_range_file,
                    'params': {
                        'file_name': file_name,
                        'start_offset': 0,
                        'end_offset': 0
                    },
                    'description': f'read whole file {file_size}MB test'
                })
        else:# 某个文件的range读取
            configs = []
            file_size = file_size_sets[0]
            file_path = f"/tmp/{self.executor.get_name()}_test_file_{file_size}MB"
            for range_size_mb in range_size_sets:
                # random_start_offset = 0
                # # random.randint(0, file_size * 1024 * 1024 - range_size_mb * 1024 * 1024)
                # random_end_offset = random_start_offset + range_size_mb * 1024 * 1024
                # self.executor.write_ranges_file(file_path, file_size, range_size_mb, [(random_start_offset, random_end_offset)])
                """创建读取实验配置"""
                if range_size_mb == 0:
                    start_offset = 0
                    end_offset = 0
                else:
                    start_offset = 0 #random.randint(0, file_size * 1024 * 1024 - range_size_mb * 1024 * 1024)
                    end_offset = start_offset + (range_size_mb * 1024 * 1024) - 1
                file_name = os.path.basename(file_path)
                configs.append({
                    'test_name': f'read_file_{file_size}MB_{range_size_mb}MB',
                    'test_func': self.executor.read_range_file,
                    'params': {
                        'file_name': file_name,
                        'start_offset': start_offset,
                        'end_offset': end_offset
                    },
                    'description': f'read file {file_size}MB range {range_size_mb}MB in {start_offset}-{end_offset} test'
                })
        return configs
    
    def create_head_experiment(self, file_size_sets: List[int]) -> List[Dict[str, Any]]:
        configs = []
        for file_size in file_size_sets:
            file_path = f"/tmp/{self.executor.get_name()}_test_file_{file_size}MB"
            file_name = os.path.basename(file_path)
            configs.append({
                'test_name': f'head_file_{file_size}MB',
                'test_func': self.executor.state_file,
                'params': {
                    'file_name': file_name,
                },
                'description': f'head file {file_size}MB test'
            })
        return configs

    def create_delete_experiment(self, file_size_sets: List[int]) -> List[Dict[str, Any]]:
        configs = []
        for file_size in file_size_sets:
            file_path = f"/tmp/{self.executor.get_name()}_test_file_{file_size}MB"
            file_name = os.path.basename(file_path)
            configs.append({
                'test_name': f'delete_file_{file_size}MB',
                'test_func': self.executor.delete_file,
                'params': {
                    'file_name': file_name,
                },
                'description': f'delete file {file_size}MB test'
            })
        return configs

def run_benchmark(executor: BaseExecutor, args: argparse.Namespace):
    benchmark = Benchmark(executor, file_path=args.file_path, iterations=args.iterations, save_result_flag=args.save_result_flag)
    benchmark.result_dir = args.result_dir
    benchmark.summary_dir = args.summary_dir
    if args.test_type == 'initial_write':
        experiment_configs = benchmark.create_file_size_initial_write_experiment(args.file_size_sets)
        results = benchmark.run_experiment_group('initial_write', experiment_configs)
    elif args.test_type == 'range_write':
        experiment_configs = benchmark.create_range_write_experiment(args.file_size_sets[0], args.range_size_sets)
        results = benchmark.run_experiment_group('range_write', experiment_configs)
    elif args.test_type == 'random_write':
        experiment_configs = benchmark.create_random_write_experiment(args.file_size_sets[0], args.range_size_sets)
        results = benchmark.run_experiment_group('random_write', experiment_configs)
    elif args.test_type == 'compact':
        print(f"compact_types: {args.compact_types}, random_percent: {args.random_percent_sets}, test_type: {args.test_type}")
        experiment_configs = benchmark.create_compact_experiment(args.file_size_sets[0], args.range_size_sets[0], args.compact_types)
        results = benchmark.run_experiment_group(args.test_type, experiment_configs)
    elif args.test_type == 'read_range':
        experiment_configs = benchmark.create_read_experiment(args.file_size_sets, args.range_size_sets)
        results = benchmark.run_experiment_group('read_range', experiment_configs)
    elif args.test_type == 'write_range_in_files':
        experiment_configs = benchmark.create_range_write_in_files_experiment(args.file_size_sets, args.range_size_sets[0])
        results = benchmark.run_experiment_group('write_range_in_files', experiment_configs)
    elif args.test_type == 'compact_log_after_write':
        experiment_configs = benchmark.create_compact_log_after_write_experiment(args.file_size_sets[0], args.range_size_sets)
        results = benchmark.run_experiment_group('compact_log_after_write', experiment_configs)
    elif args.test_type == 'head':
        experiment_configs = benchmark.create_head_experiment(args.file_size_sets)
        results = benchmark.run_experiment_group('head', experiment_configs)
    elif args.test_type == 'delete':
        experiment_configs = benchmark.create_delete_experiment(args.file_size_sets)
        results = benchmark.run_experiment_group('delete', experiment_configs)
    return results

def main():
    parser = argparse.ArgumentParser(description='Write Performance Benchmark')
    parser.add_argument('--executor-sets', nargs='+', default=['boto3', 'object_store'], choices=['boto3', 'object_store'],
                       help='Executor types supported by rangePut')
    parser.add_argument('--iterations', type=int, default=2, 
                       help='Number of iterations for each test')
    parser.add_argument('--test-type', type=str, default='range_write', choices=['initial_write', 'range_write', 'random_write', 'compact', 'read_range', 'write_range_in_files', 'compact_log_after_write', 'head', 'delete'],
                       help='Test type: initial_write, range_write, random_write, compact, read_range, write_range_in_files, compact_log_after_write')
    parser.add_argument('--compact-types', nargs='+', default=['compact_file_from_logs', 'compact_chunks', 'compact_file_from_chunks'],
                       help='Compact types: compact_file_from_chunks, compact_file_from_logs, compact_chunks')
    parser.add_argument('--file-size-sets', nargs='+', type=int, default=[128],
                       help='File sizes in MB for tests')
    parser.add_argument('--range-size-sets', nargs='+', type=int, default=[32],
                       help='Range sizes in MB for range write tests')
    parser.add_argument('--random-percent-sets', nargs='+', type=float, default=[0.1],
                       help='Random write percentage (0.0-1.0) for random tests')
    parser.add_argument('--bucket-name', type=str, default='datasize2',
                       help='Bucket name for tests')
    parser.add_argument('--file-path', type=str, default='/tmp/write_benchmark_file_test',
                       help='File path for tests')
    # 新增：结果目录参数
    parser.add_argument('--result-dir', type=str, default='res/details',
                       help='Directory to save detailed benchmark results')
    
    # 新增：摘要目录参数
    parser.add_argument('--summary-dir', type=str, default='res/summary',
                       help='Directory to save benchmark summary results')
    parser.add_argument('--save-result-flag', type=int, default=1,
                       help='Save result flag')
    
    args = parser.parse_args()
    results_list = []
    for executor_type in args.executor_sets:
        print(f"Running {executor_type} executor")
        executor = get_executor(executor_type, args.bucket_name)
        results = run_benchmark(executor, args)
        results_list.append(results)
    if args.save_result_flag == 1:
        latency_table, throughput_table, executors, test_names = summary_results(results_list, args.test_type)
        summary_filename = f"{args.test_type}_{int(time.time())}.txt"
        full_summary_path = os.path.join(args.summary_dir, summary_filename)
        os.makedirs(os.path.dirname(full_summary_path), exist_ok=True)
        print_tables_to_file(latency_table, throughput_table, executors, test_names, full_summary_path)

if __name__ == "__main__":
    main() 
