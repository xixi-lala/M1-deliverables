#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Polars Lazy API 精密去重脚本
功能：基于 user_id, item_id, behavior_type, timestamp 四维度去重
      剔除同一秒内、同一用户、针对同一商品、发生同一行为的完全重复记录
支持：亿级分区 Parquet 数据，按 behavior_type 字段分区
"""

import polars as pl
from pathlib import Path


def main():
    # ==================== 配置参数 ====================
    # 分区 Parquet 数据目录路径（按 behavior_type 字段分区）
    # 目录结构示例：
    #   clean_data_partitioned/
    #       behavior_type=1/
    #           part-0.parquet
    #           part-1.parquet
    #       behavior_type=2/
    #           part-0.parquet
    #       ...
    data_dir = Path("../lab02/clean_data_partitioned")
    
    # 去重键：四维度唯一性约束
    # - user_id: 用户 ID
    # - item_id: 商品 ID
    # - behavior_type: 行为类型（分区字段）
    # - timestamp: 时间戳（秒级精度）
    dedup_columns = ["user_id", "item_id", "behavior_type", "timestamp"]
    
    # ==================== 步骤 1: 懒加载读取分区 Parquet ====================
    # polars.scan_parquet 自动识别 Hive 风格分区目录
    # low_memory=True 降低内存占用，适合大数据集
    # glob=True 递归读取所有分区下的 parquet 文件
    lazy_df = pl.scan_parquet(
        data_dir / "**/*.parquet",
        glob=True,
        low_memory=True,
        # 启用分区投影优化，只读取需要的分区
        hive_partitioning=True,
    )
    
    # ==================== 步骤 2: 统计去重前总行数 ====================
    # collect() 触发实际计算，count() 返回总行数
    # 使用 engine="streaming" 模式处理亿级数据，降低内存峰值
    count_before = lazy_df.select(pl.len().alias("count")).collect(engine="streaming").item()
    print(f"【去重前】总行数：{count_before:,}")
    
    # ==================== 步骤 3: 执行精密去重 ====================
    # unique() 基于指定列组合进行去重
    # keep="first" 保留每组重复记录中的第一条
    # maintain_order=False 提升性能（不保证输出顺序）
    lazy_deduped = lazy_df.unique(
        subset=dedup_columns,
        keep="first",
        maintain_order=False,
    )
    
    # ==================== 步骤 4: 统计去重后总行数 ====================
    count_after = lazy_deduped.select(pl.len().alias("count")).collect(engine="streaming").item()
    print(f"【去重后】总行数：{count_after:,}")
    
    # ==================== 步骤 5: 计算去重统计信息 ====================
    duplicate_count = count_before - count_after
    duplicate_rate = (duplicate_count / count_before * 100) if count_before > 0 else 0
    
    print("\n" + "=" * 50)
    print("【去重统计报告】")
    print("=" * 50)
    print(f"去重维度：{dedup_columns}")
    print(f"重复记录数：{duplicate_count:,}")
    print(f"重复率：{duplicate_rate:.8f}%")
    print(f"保留记录数：{count_after:,}")
    print(f"保留率：{100 - duplicate_rate:.8f}%")
    print("=" * 50)
    
    # ==================== 步骤 6: （可选）输出去重后的数据 ====================
    # 如需保存结果，取消以下注释：
    # output_path = Path("clean_data_deduped")
    # lazy_deduped.sink_parquet(output_path, maintain_order=False)
    # print(f"\n去重后数据已保存至：{output_path}")
    
    return lazy_deduped


if __name__ == "__main__":
    # 设置 Polars 使用所有可用 CPU 核心
    pl.Config.set_streaming_chunk_size(100000)
    main()
