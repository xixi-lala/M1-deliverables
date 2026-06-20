#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
爬虫/机器人账号检测脚本
基于高频行为密度规则：正常用户单位时间内行为次数有限，机器人会高频点击

检测规则：
  1. 统计每个用户每小时的行为次数
  2. 找出单位时间内行为次数远超正常人类上限的用户
  3. 结合行为类型单一、无转化等辅助规则综合判定

判定标准：
  - 高度可疑：小时均行为 > 300 次 或 最大小时行为 > 500 次
  - 中度可疑：小时均行为 > 200 次 或 最大小时行为 > 300 次
"""

import polars as pl
from pathlib import Path


def detect_bot_by_hourly_frequency(
    data_dir: Path,
    hourly_threshold: int = 300,
    min_active_hours: int = 3,
) -> pl.DataFrame:
    """
    基于小时行为频率检测机器人账号

    Parameters
    ----------
    data_dir : Path
        分区 Parquet 数据目录路径
    hourly_threshold : int
        小时行为次数阈值，默认 300 次/小时
        正常用户约 20-60 次/小时
    min_active_hours : int
        最小活跃小时数，默认 3 小时
        排除偶然高频的用户

    Returns
    -------
    pl.DataFrame
        嫌疑账号列表，包含 user_id、行为频率统计等
    """
    # ==================== 步骤 1: 懒加载读取分区 Parquet ====================
    lazy_df = pl.scan_parquet(
        data_dir / "**/*.parquet",
        glob=True,
        hive_partitioning=True,
    )

    # ==================== 步骤 2: 按小时统计行为次数 ====================
    # 提取日期和小时，按用户 + 小时分组统计
    df_with_hour = (
        lazy_df
        .with_columns(
            pl.col("timestamp").dt.date().alias("date"),
            pl.col("timestamp").dt.hour().alias("hour"),
        )
    )

    # ==================== 步骤 3: 计算用户小时级行为统计 ====================
    user_hourly_stats = (
        df_with_hour
        .group_by("user_id", "date", "hour")
        .agg(
            pl.len().alias("actions_per_hour"),
        )
        .group_by("user_id")
        .agg(
            # 总行为次数
            pl.col("actions_per_hour").sum().alias("total_actions"),
            # 活跃小时数（有多少个小时有行为）
            pl.len().alias("active_hours"),
            # 平均每小时行为次数
            pl.col("actions_per_hour").mean().alias("avg_actions_per_hour"),
            # 最大单小时行为次数
            pl.col("actions_per_hour").max().alias("max_actions_per_hour"),
            # 最小单小时行为次数
            pl.col("actions_per_hour").min().alias("min_actions_per_hour"),
            # 标准差
            pl.col("actions_per_hour").std().alias("std_actions_per_hour"),
        )
    )

    # ==================== 步骤 4: 筛选嫌疑账号 ====================
    # 规则：最大小时行为 > 阈值 且 活跃小时数 >= 最小值
    bot_suspects = (
        user_hourly_stats
        .filter(
            (pl.col("max_actions_per_hour") >= hourly_threshold) &
            (pl.col("active_hours") >= min_active_hours)
        )
        .with_columns(
            # 嫌疑等级判定
            pl.when(
                (pl.col("max_actions_per_hour") > 500) |
                (pl.col("avg_actions_per_hour") > 300)
            )
            .then(pl.lit("高度可疑"))
            .when(
                (pl.col("max_actions_per_hour") > 300) |
                (pl.col("avg_actions_per_hour") > 200)
            )
            .then(pl.lit("中度可疑"))
            .otherwise(pl.lit("低度可疑"))
            .alias("suspect_level"),
        )
        .sort("max_actions_per_hour", descending=True)
    )

    return bot_suspects


def detect_bot_combined(
    data_dir: Path,
    hourly_threshold: int = 300,
    min_active_hours: int = 3,
) -> pl.DataFrame:
    """
    组合规则检测机器人账号：
    1. 高频行为密度（核心规则）
    2. 行为类型单一（辅助规则）
    3. 无转化高流量（辅助规则）

    Parameters
    ----------
    data_dir : Path
        分区 Parquet 数据目录路径
    hourly_threshold : int
        小时行为次数阈值
    min_active_hours : int
        最小活跃小时数

    Returns
    -------
    pl.DataFrame
        嫌疑账号列表，包含更多维度信息
    """
    # ==================== 步骤 1: 懒加载读取分区 Parquet ====================
    lazy_df = pl.scan_parquet(
        data_dir / "**/*.parquet",
        glob=True,
        hive_partitioning=True,
    )

    # ==================== 步骤 2: 按小时统计并计算用户行为统计 ====================
    # 时间戳是 Unix 秒，需要转换
    df_with_hour = (
        lazy_df
        .with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="s").dt.date().alias("date"),
            pl.from_epoch(pl.col("timestamp"), time_unit="s").dt.hour().alias("hour"),
        )
    )

    # 按用户 + 小时统计行为次数和行为类型
    user_hourly_detail = (
        df_with_hour
        .group_by("user_id", "date", "hour")
        .agg(
            pl.len().alias("actions_per_hour"),
            pl.col("behavior_type").n_unique().alias("behavior_type_count"),
            (pl.col("behavior_type") == "pv").sum().alias("pv_count"),
            (pl.col("behavior_type") == "cart").sum().alias("cart_count"),
            (pl.col("behavior_type") == "fav").sum().alias("fav_count"),
            (pl.col("behavior_type") == "buy").sum().alias("buy_count"),
        )
    )

    # ==================== 步骤 3: 聚合用户维度统计 ====================
    user_stats = (
        user_hourly_detail
        .group_by("user_id")
        .agg(
            # 行为频率统计
            pl.col("actions_per_hour").sum().alias("total_actions"),
            pl.len().alias("active_hours"),
            pl.col("actions_per_hour").mean().alias("avg_actions_per_hour"),
            pl.col("actions_per_hour").max().alias("max_actions_per_hour"),
            pl.col("actions_per_hour").min().alias("min_actions_per_hour"),
            pl.col("actions_per_hour").std().alias("std_actions_per_hour"),
            # 行为类型统计（汇总所有小时）
            pl.col("behavior_type_count").mean().alias("avg_behavior_type_count"),
            pl.col("pv_count").sum().alias("total_pv_count"),
            pl.col("cart_count").sum().alias("total_cart_count"),
            pl.col("fav_count").sum().alias("total_fav_count"),
            pl.col("buy_count").sum().alias("total_buy_count"),
        )
    )

    # ==================== 步骤 4: 多规则组合筛选 ====================
    bot_suspects = (
        user_stats
        .filter(
            # 核心规则：高频行为
            (pl.col("max_actions_per_hour") >= hourly_threshold) &
            (pl.col("active_hours") >= min_active_hours)
        )
        .with_columns(
            # 辅助规则标记
            # 规则 2: 行为类型单一（平均行为类型数 = 1）
            (pl.col("avg_behavior_type_count") == 1).alias("is_single_behavior"),
            # 规则 3: 无转化（购买次数 = 0）
            (pl.col("total_buy_count") == 0).alias("is_no_purchase"),
            # 规则 4: 只有 PV 无加购收藏
            ((pl.col("total_cart_count") == 0) & (pl.col("total_fav_count") == 0)).alias("is_pv_only"),
            # 规则 5: PV 占比 > 99%
            (pl.col("total_pv_count") / pl.col("total_actions") > 0.99).alias("is_pv_dominant"),
        )
        .with_columns(
            # 综合嫌疑等级
            pl.when(
                (pl.col("max_actions_per_hour") > 500) &
                (pl.col("is_pv_only"))
            )
            .then(pl.lit("高度可疑"))
            .when(
                (pl.col("max_actions_per_hour") > 300) &
                (pl.col("is_no_purchase"))
            )
            .then(pl.lit("中度可疑"))
            .otherwise(pl.lit("低度可疑"))
            .alias("suspect_level"),
        )
        .sort("max_actions_per_hour", descending=True)
    )

    return bot_suspects


def print_bot_report(bot_suspects: pl.DataFrame) -> None:
    """打印机器人检测报告"""
    print("\n" + "=" * 90)
    print(" " * 30 + "【机器人账号检测报告】")
    print("=" * 90)

    total_suspects = bot_suspects.height

    if total_suspects == 0:
        print("\n未检测到可疑账号")
        print("=" * 90)
        return

    # 按嫌疑等级统计
    suspect_summary = (
        bot_suspects
        .group_by("suspect_level")
        .agg(
            pl.len().alias("count"),
            pl.col("total_actions").mean().alias("avg_actions"),
            pl.col("max_actions_per_hour").mean().alias("avg_max_per_hour"),
        )
        .sort("suspect_level")
    )

    print("\n【嫌疑账号汇总】")
    print("-" * 90)
    print(f"总嫌疑账号数：{total_suspects:,}")
    print("-" * 90)
    print(suspect_summary)
    print("-" * 90)

    # 详细列表（前 20 条）
    print("\n【嫌疑账号详情】（前 20 条）")
    print("-" * 90)
    display_cols = [
        "user_id", "suspect_level", "total_actions", "active_hours",
        "avg_actions_per_hour", "max_actions_per_hour",
        "avg_behavior_type_count", "total_pv_count", "total_cart_count",
        "total_fav_count", "total_buy_count"
    ]

    # 检查列是否存在
    available_cols = [c for c in display_cols if c in bot_suspects.columns]
    print(bot_suspects.head(20).select(available_cols))
    print("-" * 90)

    # 统计信息
    print("\n【统计信息】")
    print("-" * 90)
    stats = bot_suspects.select([
        pl.col("total_actions").sum().alias("total_bot_actions"),
        pl.col("total_actions").mean().alias("avg_bot_actions"),
        pl.col("max_actions_per_hour").mean().alias("avg_max_per_hour"),
        pl.col("max_actions_per_hour").max().alias("max_max_per_hour"),
    ])
    print(stats)
    print("=" * 90)


def analyze_hourly_distribution(data_dir: Path) -> None:
    """分析小时行为频率分布，帮助确定合适阈值"""
    lazy_df = pl.scan_parquet(
        data_dir / "**/*.parquet",
        glob=True,
        hive_partitioning=True,
    )

    df_with_hour = (
        lazy_df
        .with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="s").dt.date().alias("date"),
            pl.from_epoch(pl.col("timestamp"), time_unit="s").dt.hour().alias("hour"),
        )
    )

    user_hourly_stats = (
        df_with_hour
        .group_by("user_id", "date", "hour")
        .agg(pl.len().alias("actions_per_hour"))
        .group_by("user_id")
        .agg(
            pl.len().alias("active_hours"),
            pl.col("actions_per_hour").max().alias("max_actions_per_hour"),
            pl.col("actions_per_hour").mean().alias("avg_actions_per_hour"),
        )
    ).collect(engine="streaming")

    # 统计不同阈值下的嫌疑账号数
    thresholds = [100, 200, 300, 500, 1000]
    min_hours_list = [1, 3, 5]

    print("\n【小时行为频率分布分析】")
    print("-" * 70)
    print(f"{'小时阈值':<12} {'最小活跃小时':<15} {'嫌疑账号数':<12}")
    print("-" * 70)

    for thresh in thresholds:
        for min_h in min_hours_list:
            count = user_hourly_stats.filter(
                (pl.col("max_actions_per_hour") >= thresh) &
                (pl.col("active_hours") >= min_h)
            ).height
            print(f"{thresh:<12} {min_h:<15} {count:<12,}")

    print("-" * 70)

    # 显示小时行为频率最高的前 20 个用户
    print("\n【小时行为频率最高的前 20 个用户】")
    print("-" * 70)
    top_20 = (
        user_hourly_stats
        .filter(pl.col("active_hours") >= 3)
        .sort("max_actions_per_hour", descending=True)
        .head(20)
    )
    print(top_20)
    print("-" * 70)


def main():
    # ==================== 配置参数 ====================
    data_dir = Path(__file__).parent.parent / "lab02" / "clean_data_partitioned"

    # 检测参数
    HOURLY_THRESHOLD = 100       # 小时行为次数阈值
    MIN_ACTIVE_HOURS = 1         # 最小活跃小时数

    print(f"\n检测参数:")
    print(f"  - 小时行为次数阈值：{HOURLY_THRESHOLD} 次/小时")
    print(f"  - 最小活跃小时数：{MIN_ACTIVE_HOURS} 小时")

    # ==================== 先分析分布 ====================
    analyze_hourly_distribution(data_dir)

    # ==================== 执行检测 ====================
    # 使用组合规则检测（基于高频行为密度）
    bot_suspects = detect_bot_combined(
        data_dir=data_dir,
        hourly_threshold=HOURLY_THRESHOLD,
        min_active_hours=MIN_ACTIVE_HOURS,
    ).collect(engine="streaming")

    # ==================== 输出报告 ====================
    print_bot_report(bot_suspects)

    # ==================== 保存结果 ====================
    output_path = Path(__file__).parent / "bot_suspects.csv"
    bot_suspects.write_csv(output_path)
    print(f"\n嫌疑账号列表已保存至：{output_path}")

    return bot_suspects


if __name__ == "__main__":
    pl.Config.set_streaming_chunk_size(100000)
    main()