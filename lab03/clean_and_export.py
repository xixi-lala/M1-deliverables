#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Milestone 1 最终数据固化 - 实验三 任务 5
Polars Lazy API 流式计算，快速导出干净数据
"""

import polars as pl
from pathlib import Path


def main():
    # 路径配置
    base_dir = Path(__file__).parent
    data_dir = base_dir.parent / "lab02" / "clean_data_partitioned"
    bot_suspects_path = base_dir / "bot_suspects.csv"
    output_path = base_dir / "m1_final_clean.parquet"

    # ==================== 步骤 1: 懒加载读取分区数据 ====================
    lazy_df = pl.scan_parquet(
        data_dir / "**/*.parquet",
        glob=True,
        hive_partitioning=True,
    )

    # ==================== 步骤 2: 四维度去重 ====================
    deduped_df = lazy_df.unique(
        subset=["user_id", "item_id", "behavior_type", "timestamp"],
        keep="first",
    )

    # ==================== 步骤 3: 剔除机器人嫌疑账号 ====================
    # 读取嫌疑账号列表（只取 user_id 列）
    bot_users = pl.read_csv(bot_suspects_path).select("user_id").lazy()

    # anti join 剔除嫌疑账号
    clean_df = deduped_df.join(bot_users, on="user_id", how="anti")

    # ==================== 步骤 4: 导出为 Parquet ====================
    # 使用流式收集并写入（Snappy 压缩）
    clean_df.sink_parquet(output_path, compression="snappy")

    # ==================== 输出结果 ====================
    final_count = clean_df.select(pl.len()).collect().item()
    file_size_mb = output_path.stat().st_size / (1024 * 1024)

    print(f"最终数据行数：{final_count:,}")
    print(f"文件大小：{file_size_mb:.2f} MB")
    print(f"输出文件：{output_path}")


if __name__ == "__main__":
    main()
