#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
电商转化漏斗分析脚本 - 实验三 任务三
基于 Polars Lazy API 实现标准电商漏斗：PV 浏览 → 加购/收藏 → 购买

漏斗路径（3 步）：
  Step 1: 有浏览行为的用户
  Step 2: 在 Step1 用户中，有过加购或收藏的用户
  Step 3: 在 Step2 用户中，有过购买的用户

功能：
  1. 统计各行为总次数
  2. 计算转化到下一步的数量（按漏斗链路过滤）
  3. 计算转化率、流失率
  4. 输出清晰的漏斗报表
"""

import polars as pl
from pathlib import Path


def main():
    # ==================== 配置参数 ====================
    # 分区 Parquet 数据目录路径（使用绝对路径）
    data_dir = Path(__file__).parent.parent / "lab02" / "clean_data_partitioned"

    # 行为类型映射（实际数据为字符串：pv, cart, fav, buy）
    BEHAVIOR_MAP = {
        "pv": "pv",      # 浏览
        "cart": "cart",  # 加购
        "fav": "fav",    # 收藏
        "buy": "buy",    # 购买
    }

    # ==================== 步骤 1: 懒加载读取分区 Parquet ====================
    lazy_df = pl.scan_parquet(
        data_dir / "**/*.parquet",
        glob=True,
        hive_partitioning=True,
    )

    # ==================== 步骤 2: 统计各行为总次数 ====================
    behavior_counts = (
        lazy_df
        .group_by("behavior_type")
        .agg(pl.len().alias("count"))
        .sort("behavior_type")
        .collect(engine="streaming")
    )

    # ==================== 步骤 3: 构建用户行为宽表 ====================
    user_behavior_pivot = (
        lazy_df
        .group_by("user_id")
        .agg(
            (pl.col("behavior_type") == BEHAVIOR_MAP["pv"]).sum().alias("pv_count"),
            (pl.col("behavior_type") == BEHAVIOR_MAP["cart"]).sum().alias("cart_count"),
            (pl.col("behavior_type") == BEHAVIOR_MAP["fav"]).sum().alias("fav_count"),
            (pl.col("behavior_type") == BEHAVIOR_MAP["buy"]).sum().alias("buy_count"),
        )
        .collect(engine="streaming")
    )

    # ==================== 步骤 4: 标记用户是否有各行为 ====================
    user_behavior_flag = (
        user_behavior_pivot
        .with_columns(
            (pl.col("pv_count") > 0).alias("has_pv"),
            (pl.col("cart_count") > 0).alias("has_cart"),
            (pl.col("fav_count") > 0).alias("has_fav"),
            (pl.col("buy_count") > 0).alias("has_buy"),
        )
    )

    # ==================== 步骤 5: 按漏斗链路过滤用户 ====================
    # Step 1: 有 PV 的用户
    step1_df = user_behavior_flag.filter(pl.col("has_pv"))

    # Step 2: 在 Step1 用户中，有 Cart 或 Fav 的用户
    step2_df = step1_df.filter(pl.col("has_cart") | pl.col("has_fav"))

    # Step 3: 在 Step2 用户中，有 Buy 的用户
    step3_df = step2_df.filter(pl.col("has_buy"))

    # ==================== 步骤 6: 计算漏斗各步骤用户数 ====================
    step1_users = step1_df.height
    step2_users = step2_df.height
    step3_users = step3_df.height

    # ==================== 步骤 7: 计算转化率与流失率 ====================
    # 阶段转化率：下一步人数 / 当前步人数
    step1_to_step2_rate = (step2_users / step1_users * 100) if step1_users > 0 else 0
    step2_to_step3_rate = (step3_users / step2_users * 100) if step2_users > 0 else 0

    # 整体转化率：各步相对于 Step1 的转化率
    step1_overall_rate = 100.00
    step2_overall_rate = step1_to_step2_rate
    step3_overall_rate = (step3_users / step1_users * 100) if step1_users > 0 else 0

    # ==================== 步骤 8: 输出漏斗报表 ====================
    print("\n" + "=" * 80)
    print(" " * 25 + "【电商转化漏斗分析报告】")
    print("=" * 80)

    # --- 用户转化漏斗 ---
    print("\n【用户转化漏斗】")
    print("-" * 80)
    print(f"{'步骤':<10} {'行为类型':<16} {'用户数':>12} {'阶段转化率':>12} {'整体转化率':>12}")
    print("-" * 80)
    print(f"{'Step 1':<10} {'PV 浏览':<16} {step1_users:>12,} {'-':>12} {step1_overall_rate:>11.2f}%")
    print(f"{'Step 2':<10} {'加购或收藏':<16} {step2_users:>12,} {step1_to_step2_rate:>11.2f}% {step2_overall_rate:>11.2f}%")
    print(f"{'Step 3':<10} {'购买':<16} {step3_users:>12,} {step2_to_step3_rate:>11.2f}% {step3_overall_rate:>11.2f}%")
    print("-" * 80)

    # --- 总体转化摘要 ---
    print("\n【总体转化摘要】")
    print("-" * 80)
    total_users = user_behavior_pivot.height
    overall_conversion_rate = step3_overall_rate
    overall_dropoff_rate = 100 - overall_conversion_rate
    print(f"总用户数：{total_users:,}")
    print(f"Step 1 浏览用户数：{step1_users:,}")
    print(f"Step 2 加购或收藏用户数：{step2_users:,}")
    print(f"Step 3 最终购买用户数：{step3_users:,}")
    print(f"整体转化率 (PV→Buy): {overall_conversion_rate:.2f}%")
    print(f"整体流失率：{overall_dropoff_rate:.2f}%")
    print("=" * 80)

    # ==================== 步骤 9: 输出行为类型分布详情 ====================
    print("\n【行为类型分布详情】")
    print("-" * 80)
    print(behavior_counts)
    print("=" * 80)

    return {
        "behavior_counts": behavior_counts,
        "user_funnel": user_behavior_flag,
        "funnel_summary": {
            "step1_users": step1_users,
            "step2_users": step2_users,
            "step3_users": step3_users,
            "step1_to_step2_rate": step1_to_step2_rate,
            "step2_to_step3_rate": step2_to_step3_rate,
            "overall_conversion_rate": overall_conversion_rate,
        }
    }


if __name__ == "__main__":
    # 设置流式计算块大小，优化大内存数据处理
    pl.Config.set_streaming_chunk_size(100000)
    main()
