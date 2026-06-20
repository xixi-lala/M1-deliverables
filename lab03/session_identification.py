#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
用户会话识别脚本 - 实验三 任务二
基于 Polars Lazy API + 窗口函数实现
功能：
  1. 按 user_id 分组
  2. 按 timestamp 升序排序
  3. 计算上一次行为时间，计算时间差（秒）
  4. 超过 30 分钟（1800 秒）则标记新会话
  5. 生成连续的 session_id，格式如 user_id + 序号
  6. 统计每个用户有多少个会话
  7. 输出前 20 行样例 + 总用户数 + 总会话数
"""

import polars as pl
from pathlib import Path


def main():
    # ==================== 配置参数 ====================
    # 分区 Parquet 数据目录路径（使用绝对路径）
    data_dir = Path(__file__).parent.parent / "lab02" / "clean_data_partitioned"
    
    # 会话超时阈值：30 分钟 = 1800 秒
    SESSION_TIMEOUT_SECONDS = 1800
    
    # ==================== 步骤 1: 懒加载读取分区 Parquet ====================
    # hive_partitioning=True 自动识别 behavior_type 分区
    lazy_df = pl.scan_parquet(
        data_dir / "**/*.parquet",
        glob=True,
        hive_partitioning=True,
    )
    
    # ==================== 步骤 2: 窗口函数计算会话边界 ====================
    # 定义窗口规范：按 user_id 分组，按 timestamp 升序排序
    window_spec = pl.col("timestamp").sort_by("timestamp").over("user_id")
    
    lazy_with_session = (
        lazy_df
        # 计算上一次行为时间（滞后一行）
        .with_columns(
            pl.col("timestamp")
            .sort_by("timestamp")
            .shift(1)
            .over("user_id")
            .alias("prev_timestamp")
        )
        # 计算与上一次行为的时间差（秒）
        .with_columns(
            (pl.col("timestamp") - pl.col("prev_timestamp")).alias("time_diff_seconds")
        )
        # 标记新会话：时间差超过阈值或为第一条记录（prev_timestamp 为 null）
        .with_columns(
            (
                (pl.col("time_diff_seconds").is_null()) |
                (pl.col("time_diff_seconds") > SESSION_TIMEOUT_SECONDS)
            ).alias("is_new_session")
        )
        # 生成会话组编号：对 is_new_session 累加，同一会话内编号相同
        .with_columns(
            pl.col("is_new_session")
            .cast(pl.UInt32)
            .cum_sum()
            .over("user_id")
            .alias("session_group")
        )
        # 生成 session_id：user_id + 会话序号（从 1 开始）
        .with_columns(
            (
                pl.col("user_id").cast(pl.Utf8) + "_" +
                (pl.col("session_group") + 1).cast(pl.Utf8)
            ).alias("session_id")
        )
    )
    
    # ==================== 步骤 3: 收集结果 ====================
    # 执行计算，获取带 session_id 的完整数据
    df_with_session = lazy_with_session.collect(engine="streaming")
    
    # ==================== 步骤 4: 输出前 20 行样例 ====================
    print("=" * 80)
    print("【前 20 行数据样例】")
    print("=" * 80)
    display_cols = ["user_id", "item_id", "behavior_type", "timestamp", 
                    "prev_timestamp", "time_diff_seconds", "session_id"]
    print(df_with_session.select(display_cols).head(20))
    
    # ==================== 步骤 5: 统计每个用户的会话数 ====================
    user_session_stats = (
        df_with_session
        .group_by("user_id")
        .agg(
            pl.col("session_id").n_unique().alias("session_count")
        )
        .sort("user_id")
    )
    
    # ==================== 步骤 6: 计算总体统计信息 ====================
    total_users = df_with_session["user_id"].n_unique()
    total_sessions = df_with_session["session_id"].n_unique()
    avg_sessions_per_user = total_sessions / total_users if total_users > 0 else 0
    
    # 会话数分布统计
    session_count_dist = (
        user_session_stats
        .group_by("session_count")
        .agg(pl.len().alias("user_count"))
        .sort("session_count")
    )
    
    # ==================== 步骤 7: 输出统计报告 ====================
    print("\n" + "=" * 80)
    print("【会话识别统计报告】")
    print("=" * 80)
    print(f"会话超时阈值：{SESSION_TIMEOUT_SECONDS} 秒（{SESSION_TIMEOUT_SECONDS / 60:.0f} 分钟）")
    print(f"总用户数：{total_users:,}")
    print(f"总会话数：{total_sessions:,}")
    print(f"人均会话数：{avg_sessions_per_user:.2f}")
    print("=" * 80)
    
    print("\n【每个用户的会话数统计（前 20 个用户）】")
    print(user_session_stats.head(20))
    
    print("\n【会话数分布统计】")
    print(session_count_dist)
    
    # ==================== 步骤 8: （可选）保存结果 ====================
    # 如需保存带 session_id 的数据，取消以下注释：
    # output_path = Path("data_with_sessions")
    # df_with_session.write_parquet(output_path / "sessions.parquet")
    # print(f"\n带会话标识的数据已保存至：{output_path}")
    
    return df_with_session, user_session_stats


if __name__ == "__main__":
    # 设置流式计算块大小，优化大内存数据处理
    pl.Config.set_streaming_chunk_size(100000)
    main()
