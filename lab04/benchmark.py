# benchmark.py
"""
性能基准测试脚本
对比原始版本和优化版本的执行时间和资源使用情况
"""

import os
import time
import psutil
from pathlib import Path
import sys
import importlib

sys.path.insert(0, str(Path(__file__).parent))


def get_process_info():
    """获取当前进程的资源使用情况"""
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return {
        "memory_mb": mem_info.rss / (1024 * 1024),
        "cpu_percent": process.cpu_percent(),
    }


def run_pipeline(module_name, output_suffix):
    """运行指定的流水线模块并记录性能指标"""
    # 动态导入模块
    module = importlib.import_module(module_name)
    PipelineConfig = module.PipelineConfig
    M1DataPipeline = module.M1DataPipeline

    # 配置参数
    config = PipelineConfig(
        data_dir=Path("../lab02/clean_data_partitioned"),
        output_path=Path(f"m1_final_clean_{output_suffix}.parquet"),
        bot_suspects_path=Path(f"bot_suspects_{output_suffix}.csv"),
        session_timeout_seconds=1800,
        hourly_threshold=100,
        min_active_hours=1,
    )

    print(f"\n{'=' * 60}")
    print(f"开始运行 {module_name}...")
    print(f"{'=' * 60}")

    # 记录初始状态
    start_time = time.time()
    start_mem = get_process_info()["memory_mb"]

    # 运行流水线
    pipeline = M1DataPipeline(config)
    pipeline.run_full_pipeline()

    # 记录结束状态
    end_time = time.time()
    end_mem = get_process_info()["memory_mb"]
    elapsed_time = end_time - start_time

    print(f"\n{'=' * 60}")
    print(f"{module_name} 执行完成")
    print(f"{'=' * 60}")
    print(f"  总耗时：{elapsed_time:.2f} 秒 ({elapsed_time / 60:.2f} 分钟)")
    print(f"  初始内存：{start_mem:.2f} MB")
    print(f"  结束内存：{end_mem:.2f} MB")
    print(f"  峰值内存：{max(start_mem, end_mem):.2f} MB")

    return {
        "time": elapsed_time,
        "start_memory_mb": start_mem,
        "end_memory_mb": end_mem,
    }


if __name__ == "__main__":
    print("=" * 60)
    print("性能基准测试")
    print("=" * 60)
    print(f"CPU 核心数：{psutil.cpu_count(logical=True)} (逻辑) / {psutil.cpu_count(logical=False)} (物理)")
    print(f"系统内存：{psutil.virtual_memory().total / (1024**3):.2f} GB")

    # 运行原始版本
    result_original = run_pipeline("m1_pipeline", "original")

    # 运行优化版本
    result_optimized = run_pipeline("m1_pipeline_2", "optimized")

    # 对比结果
    print("\n" + "=" * 60)
    print("性能对比结果")
    print("=" * 60)
    print(f"{'指标':<20} {'原始版本':>15} {'优化版本':>15} {'差异':>15}")
    print("-" * 60)

    # 时间对比
    time_diff = result_optimized["time"] - result_original["time"]
    time_pct = (time_diff / result_original["time"]) * 100
    print(
        f"{'耗时 (秒)':<20} {result_original['time']:>15.2f} "
        f"{result_optimized['time']:>15.2f} {time_diff:>+15.2f}"
    )

    # 内存对比
    mem_diff = result_optimized["end_memory_mb"] - result_original["end_memory_mb"]
    print(
        f"{'结束内存 (MB)':<20} {result_original['end_memory_mb']:>15.2f} "
        f"{result_optimized['end_memory_mb']:>15.2f} {mem_diff:>+15.2f}"
    )

    print("-" * 60)
    if time_diff < 0:
        print(f"✓ 优化版本快了 {abs(time_diff):.2f} 秒 ({abs(time_pct):.1f}%)")
    else:
        print(f"✗ 优化版本慢了 {time_diff:.2f} 秒 ({time_pct:.1f}%)")
    print("=" * 60)
