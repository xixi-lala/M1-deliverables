#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Milestone 1 数据处理流水线
基于 Polars Lazy API 的工程化数据管道
功能：数据读取 → 精密去重 → 会话识别 → 机器人检测 → 漏斗分析 → 数据导出
"""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import polars as pl

# ==================== 日志配置 ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    """流水线配置参数

    Attributes
    ----------
    data_dir : Path
        分区 Parquet 数据目录路径
    output_path : Path
        输出 Parquet 文件路径
    bot_suspects_path : Path
        机器人嫌疑账号列表 CSV 路径
    session_timeout_seconds : int
        会话超时阈值（秒），默认 1800（30 分钟）
    hourly_threshold : int
        小时行为次数阈值，默认 100
    min_active_hours : int
        最小活跃小时数，默认 1
    streaming_chunk_size : int
        流式计算块大小，默认 100000
    """

    data_dir: Path
    output_path: Path
    bot_suspects_path: Path
    session_timeout_seconds: int = 1800
    hourly_threshold: int = 100
    min_active_hours: int = 1
    streaming_chunk_size: int = 100000


class M1DataPipeline:
    """Milestone 1 数据处理流水线

    处理阶段：
        1. extract()   - 懒加载读取分区 Parquet 数据
        2. transform() - 去重 → 会话识别 → 机器人检测 → 剔除机器人
        3. load()      - 导出干净数据（含 session_id） + 漏斗分析报表

    Examples
    --------
    >>> config = PipelineConfig(
    ...     data_dir=Path("data"),
    ...     output_path=Path("output.parquet"),
    ...     bot_suspects_path=Path("bots.csv"),
    ... )
    >>> pipeline = M1DataPipeline(config)
    >>> results = pipeline.run_full_pipeline()
    """
    def __init__(self, config: PipelineConfig) -> None:
        """初始化数据流水线

        Parameters
        ----------
        config : PipelineConfig
            流水线配置参数
        """
        self.config: PipelineConfig = config
        self.lazy_df: Optional[pl.LazyFrame] = None
        self.deduped_df: Optional[pl.LazyFrame] = None
        self.session_lazy_df: Optional[pl.LazyFrame] = None
        self.bot_suspects: Optional[pl.DataFrame] = None
        self.clean_df: Optional[pl.LazyFrame] = None
        self.funnel_results: Optional[Dict[str, Any]] = None

        # 设置 Polars 流式计算块大小
        pl.Config.set_streaming_chunk_size(config.streaming_chunk_size)
    # ==================== Extract 阶段 ====================
    def extract(self) -> pl.LazyFrame:
        """懒加载读取分区 Parquet 数据

        使用 Polars scan_parquet 懒加载数据，自动识别 Hive 风格分区。

        Returns
        -------
        pl.LazyFrame
            懒加载的 DataFrame

        Raises
        ------
        FileNotFoundError
            数据目录不存在时抛出
        """
        logger.info("=" * 60)
        logger.info("【Extract 阶段】开始加载分区 Parquet 数据")
        logger.info("=" * 60)

        try:
            data_dir: Path = self.config.data_dir
            if not data_dir.exists():
                raise FileNotFoundError(f"数据目录不存在：{data_dir}")

            # 统计文件数量
            parquet_files: list[Path] = list(data_dir.glob("**/*.parquet"))
            logger.info(f"发现 {len(parquet_files)} 个 Parquet 文件")

            # 懒加载读取数据
            self.lazy_df = pl.scan_parquet(
                data_dir / "**/*.parquet",
                glob=True,
                hive_partitioning=True,
                low_memory=True,
            )

            # 统计行数
            row_count: int = (
                self.lazy_df.select(pl.len().alias("count"))
                .collect(engine="streaming")
                .item()
            )
            logger.info(f"成功加载数据，总行数：{row_count:,}")

            return self.lazy_df

        except Exception as e:
            logger.error(f"【Extract 阶段】失败：{e}")
            raise
    # ==================== Transform 阶段 ====================
    def transform(self) -> pl.LazyFrame:
        """数据转换（去重 → 会话识别 → 机器人检测 → 剔除机器人）

        Returns
        -------
        pl.LazyFrame
            去重、带 session_id、剔除机器人后的干净数据

        Raises
        ------
        ValueError
            未先执行 extract() 时抛出
        """
        logger.info("=" * 60)
        logger.info("【Transform 阶段】开始数据转换")
        logger.info("=" * 60)

        try:
            if self.lazy_df is None:
                raise ValueError("必须先执行 extract() 方法")

            # 2.1 精密去重
            self._deduplicate()

            # 2.2 会话识别：生成 session_id
            self._identify_sessions()

            # 2.3 机器人检测
            self._detect_bots()

            # 2.4 剔除机器人账号（在带 session_id 的数据上执行）
            self._remove_bot_accounts()

            logger.info("【Transform 阶段】数据转换完成")
            return self.clean_df

        except Exception as e:
            logger.error(f"【Transform 阶段】失败：{e}")
            raise
    def _deduplicate(self) -> None:
        """基于四维度精密去重

        去重键：user_id, item_id, behavior_type, timestamp
        使用 Polars unique() 方法，保留每组第一条记录。
        """
        logger.info("-" * 50)
        logger.info("【去重】开始四维度精密去重")

        try:
            dedup_columns: list[str] = [
                "user_id",
                "item_id",
                "behavior_type",
                "timestamp",
            ]

            # 统计去重前行数
            count_before: int = (
                self.lazy_df.select(pl.len().alias("count"))
                .collect(engine="streaming")
                .item()
            )
            logger.info(f"去重前行数：{count_before:,}")

            # 执行去重
            self.deduped_df = self.lazy_df.unique(
                subset=dedup_columns,
                keep="first",
                maintain_order=False,
            )

            # 统计去重后行数
            count_after: int = (
                self.deduped_df.select(pl.len().alias("count"))
                .collect(engine="streaming")
                .item()
            )
            duplicate_count: int = count_before - count_after
            duplicate_rate: float = (
                (duplicate_count / count_before * 100) if count_before > 0 else 0
            )

            logger.info(f"去重后行数：{count_after:,}")
            logger.info(
                f"去除重复记录：{duplicate_count:,} ({duplicate_rate:.4f}%)"
            )

        except Exception as e:
            logger.error(f"【去重】失败：{e}")
            raise
    def _identify_sessions(self) -> None:
        """会话识别 - 基于时间窗口生成 session_id

        逻辑：
            1. 按 user_id 分组，按 timestamp 升序排序
            2. 计算与上一次行为的时间差
            3. 时间差 > 1800 秒或为首条记录则标记新会话
            4. 累加标记生成会话组编号
            5. 生成 session_id = user_id_序号

        Raises
        ------
        ValueError
            未先执行去重操作时抛出
        """
        logger.info("-" * 50)
        logger.info("【会话识别】开始识别用户会话")

        try:
            if self.deduped_df is None:
                raise ValueError("必须先执行去重操作")

            timeout: int = self.config.session_timeout_seconds

            # 窗口函数计算会话边界
            self.session_lazy_df = (
                self.deduped_df
                # 计算上一次行为时间（滞后一行）
                .with_columns(
                    pl.col("timestamp")
                    .sort_by("timestamp")
                    .shift(1)
                    .over("user_id")
                    .alias("prev_timestamp")
                )
                # 计算时间差
                .with_columns(
                    (pl.col("timestamp") - pl.col("prev_timestamp")).alias(
                        "time_diff_seconds"
                    )
                )
                # 标记新会话
                .with_columns(
                    (
                        pl.col("time_diff_seconds").is_null()
                        | (pl.col("time_diff_seconds") > timeout)
                    ).alias("is_new_session")
                )
                # 生成会话组编号
                .with_columns(
                    pl.col("is_new_session")
                    .cast(pl.UInt32)
                    .cum_sum()
                    .over("user_id")
                    .alias("session_group")
                )
                # 生成 session_id
                .with_columns(
                    (
                        pl.col("user_id").cast(pl.Utf8)
                        + "_"
                        + (pl.col("session_group") + 1).cast(pl.Utf8)
                    ).alias("session_id")
                )
                # 删除中间列
                .drop(
                    [
                        "prev_timestamp",
                        "time_diff_seconds",
                        "is_new_session",
                        "session_group",
                    ]
                )
            )

            # 统计会话信息
            sample_count: int = (
                self.session_lazy_df.select(pl.len().alias("count"))
                .collect(engine="streaming")
                .item()
            )
            logger.info(
                f"会话识别完成，总记录数：{sample_count:,}（已添加 session_id 列）"
            )

        except Exception as e:
            logger.error(f"【会话识别】失败：{e}")
            raise
    def _detect_bots(self) -> None:
        """基于小时行为频率检测机器人账号

        逻辑：
            1. 优先尝试读取已有的 bot_suspects.csv
            2. 如果文件不存在或读取后为空（嫌疑数为 0），则重新执行检测
            3. 保存检测结果到 bot_suspects.csv
        """
        logger.info("-" * 50)
        logger.info("【机器人检测】开始检测嫌疑账号")

        try:
            bot_suspects_path: Path = self.config.bot_suspects_path
            should_run_detection: bool = True

            # 尝试读取已有的 bot_suspects.csv
            if bot_suspects_path.exists():
                logger.info(f"检测到已有机器人列表：{bot_suspects_path}")
                try:
                    self.bot_suspects = pl.read_csv(bot_suspects_path)

                    # 检查是否为空（只有表头没有数据，或完全空）
                    if self.bot_suspects.height == 0:
                        logger.warning(
                            "已有 bot_suspects.csv 为空（0 行），将重新执行检测..."
                        )
                        self.bot_suspects = None
                    else:
                        suspect_count: int = self.bot_suspects.height
                        logger.info(f"读取到 {suspect_count:,} 个嫌疑账号")
                        should_run_detection = False

                except Exception as read_err:
                    logger.warning(
                        f"读取 bot_suspects.csv 失败：{read_err}，"
                        "将重新执行检测..."
                    )
                    self.bot_suspects = None
            else:
                logger.info(
                    f"未找到 bot_suspects.csv（路径：{bot_suspects_path}），"
                    "将执行检测..."
                )

            # 如果需要检测，则执行机器人检测逻辑
            if should_run_detection or self.bot_suspects is None:
                logger.info("开始执行机器人检测...")
                self.bot_suspects = self._run_bot_detection()

                # 保存检测结果（无论是否为空都保存）
                if self.bot_suspects.height > 0:
                    self.bot_suspects.write_csv(bot_suspects_path)
                    logger.info(
                        f"机器人检测结果（{self.bot_suspects.height:,} 个嫌疑账号）"
                        f"已保存至：{bot_suspects_path}"
                    )
                else:
                    # 即使为空也保存文件（包含表头），避免下次重复检测
                    self.bot_suspects.write_csv(bot_suspects_path)
                    logger.warning(
                        f"未检测到嫌疑账号，已保存空结果至：{bot_suspects_path}"
                    )

            suspect_count = self.bot_suspects.height
            logger.info(f"最终嫌疑账号数：{suspect_count:,}")

        except Exception as e:
            logger.error(f"【机器人检测】失败：{e}")
            raise
    def _run_bot_detection(self) -> pl.DataFrame:
        """执行机器人检测逻辑（基于小时行为频率）

        判定标准：
            - 高度可疑：小时最大行为 > 500 次 且 行为类型单一（只有 PV）
            - 中度可疑：小时最大行为 > 300 次 且 无购买行为
            - 低度可疑：其他符合基础筛选条件的账号

        Returns
        -------
        pl.DataFrame
            嫌疑账号列表，包含用户 ID、行为统计和嫌疑等级
        """
        logger.info("  → 正在分析用户小时行为频率...")

        # 使用原始 lazy_df 进行检测
        lazy_df: pl.LazyFrame = self.lazy_df

        # 提取日期和小时
        df_with_hour: pl.LazyFrame = lazy_df.with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="s")
            .dt.date()
            .alias("date"),
            pl.from_epoch(pl.col("timestamp"), time_unit="s")
            .dt.hour()
            .alias("hour"),
        )

        # 按用户 + 小时统计行为次数和行为类型分布
        user_hourly_detail: pl.LazyFrame = (
            df_with_hour.group_by("user_id", "date", "hour")
            .agg(
                pl.len().alias("actions_per_hour"),
                pl.col("behavior_type")
                .n_unique()
                .alias("behavior_type_count"),
                (pl.col("behavior_type") == "pv").sum().alias("pv_count"),
                (pl.col("behavior_type") == "cart")
                .sum()
                .alias("cart_count"),
                (pl.col("behavior_type") == "fav").sum().alias("fav_count"),
                (pl.col("behavior_type") == "buy").sum().alias("buy_count"),
            )
        )

        # 聚合用户维度统计
        logger.info("  → 正在聚合用户维度统计...")
        user_stats: pl.LazyFrame = (
            user_hourly_detail.group_by("user_id")
            .agg(
                pl.col("actions_per_hour").sum().alias("total_actions"),
                pl.len().alias("active_hours"),
                pl.col("actions_per_hour")
                .mean()
                .alias("avg_actions_per_hour"),
                pl.col("actions_per_hour")
                .max()
                .alias("max_actions_per_hour"),
                pl.col("actions_per_hour")
                .min()
                .alias("min_actions_per_hour"),
                pl.col("actions_per_hour")
                .std()
                .alias("std_actions_per_hour"),
                pl.col("behavior_type_count")
                .mean()
                .alias("avg_behavior_type_count"),
                pl.col("pv_count").sum().alias("total_pv_count"),
                pl.col("cart_count").sum().alias("total_cart_count"),
                pl.col("fav_count").sum().alias("total_fav_count"),
                pl.col("buy_count").sum().alias("total_buy_count"),
            )
        )

        # 多规则组合筛选
        config: PipelineConfig = self.config
        logger.info(
            f"  → 筛选条件：max_actions_per_hour >= {config.hourly_threshold}, "
            f"active_hours >= {config.min_active_hours}"
        )

        bot_suspects: pl.DataFrame = (
            user_stats.filter(
                (pl.col("max_actions_per_hour") >= config.hourly_threshold)
                & (pl.col("active_hours") >= config.min_active_hours)
            )
            .with_columns(
                # 辅助规则标记
                (pl.col("avg_behavior_type_count") == 1).alias(
                    "is_single_behavior"
                ),
                (pl.col("total_buy_count") == 0).alias("is_no_purchase"),
                (
                    (pl.col("total_cart_count") == 0)
                    & (pl.col("total_fav_count") == 0)
                ).alias("is_pv_only"),
                (
                    pl.col("total_pv_count") / pl.col("total_actions") > 0.99
                ).alias("is_pv_dominant"),
            )
            .with_columns(
                # 综合嫌疑等级
                pl.when(
                    (pl.col("max_actions_per_hour") > 500)
                    & (pl.col("is_pv_only"))
                )
                .then(pl.lit("高度可疑"))
                .when(
                    (pl.col("max_actions_per_hour") > 300)
                    & (pl.col("is_no_purchase"))
                )
                .then(pl.lit("中度可疑"))
                .otherwise(pl.lit("低度可疑"))
                .alias("suspect_level"),
            )
            .sort("max_actions_per_hour", descending=True)
            .collect(engine="streaming")
        )

        logger.info(
            f"  → 检测完成，共发现 {bot_suspects.height:,} 个嫌疑账号"
        )
        return bot_suspects
    def _remove_bot_accounts(self) -> None:
        """使用 anti join 剔除机器人账号

        注意：此时数据已包含 session_id 列。
        统一 user_id 类型，避免 join 时类型不匹配。

        Raises
        ------
        ValueError
            未先执行会话识别或机器人检测时抛出
        """
        logger.info("-" * 50)
        logger.info("【剔除机器人】开始移除嫌疑账号")

        try:
            if self.session_lazy_df is None:
                raise ValueError("必须先执行会话识别操作")
            if self.bot_suspects is None:
                raise ValueError("必须先执行机器人检测")

            # 如果嫌疑账号为空，跳过剔除步骤
            if self.bot_suspects.height == 0:
                logger.info("嫌疑账号列表为空，跳过剔除步骤")
                self.clean_df = self.session_lazy_df
                return

            # 获取原始数据中 user_id 的类型
            session_user_id_dtype: pl.DataType = self.session_lazy_df.schema[
                "user_id"
            ]
            logger.info(f"  → 原始数据 user_id 类型：{session_user_id_dtype}")

            # 提取嫌疑账号 user_id，并统一类型
            bot_users: pl.LazyFrame = (
                self.bot_suspects.select(
                    pl.col("user_id").cast(session_user_id_dtype)
                )
                .lazy()
            )
            logger.info(
                f"  → 嫌疑账号 user_id 已转换为：{session_user_id_dtype}"
            )

            # 统计剔除前行数
            count_before: int = (
                self.session_lazy_df.select(pl.len().alias("count"))
                .collect(engine="streaming")
                .item()
            )

            # anti join 剔除
            self.clean_df = self.session_lazy_df.join(
                bot_users, on="user_id", how="anti"
            )

            count_after: int = (
                self.clean_df.select(pl.len().alias("count"))
                .collect(engine="streaming")
                .item()
            )
            removed_count: int = count_before - count_after

            logger.info(f"剔除前记录数：{count_before:,}")
            logger.info(f"剔除后记录数：{count_after:,}")
            if count_before > 0:
                logger.info(
                    f"剔除机器人相关记录：{removed_count:,} "
                    f"({removed_count / count_before * 100:.2f}%)"
                )
            else:
                logger.info("剔除机器人相关记录：0")

            # 验证 session_id 列是否存在
            if "session_id" not in self.clean_df.columns:
                logger.warning("⚠ 警告：输出数据中缺少 session_id 列！")
            else:
                logger.info("✓ 验证通过：输出数据包含 session_id 列")

        except Exception as e:
            logger.error(f"【剔除机器人】失败：{e}")
            raise
    # ==================== Load 阶段 ====================
    def load(self) -> Dict[str, Any]:
        """数据导出 + 漏斗分析

        Returns
        -------
        Dict[str, Any]
            漏斗分析结果，包含行为统计和各步骤用户数及转化率

        Raises
        ------
        ValueError
            未先执行 transform() 时抛出
        """
        logger.info("=" * 60)
        logger.info("【Load 阶段】开始数据导出和漏斗分析")
        logger.info("=" * 60)

        try:
            if self.clean_df is None:
                raise ValueError("必须先执行 transform() 方法")

            # 3.1 导出干净数据
            self._export_clean_data()

            # 3.2 执行漏斗分析
            self.funnel_results = self._analyze_funnel()

            return self.funnel_results

        except Exception as e:
            logger.error(f"【Load 阶段】失败：{e}")
            raise
    def _export_clean_data(self) -> None:
        """导出干净数据为 Parquet（包含 session_id 列）

        使用 Snappy 压缩，确保输出目录存在。
        """
        logger.info("-" * 50)
        logger.info(f"【数据导出】目标路径：{self.config.output_path}")

        try:
            output_path: Path = self.config.output_path

            # 确保输出目录存在
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # 流式写入
            self.clean_df.sink_parquet(output_path, compression="snappy")

            # 获取文件信息
            final_count: int = self.clean_df.select(pl.len()).collect().item()
            file_size_mb: float = output_path.stat().st_size / (1024 * 1024)

            logger.info(f"导出完成：{final_count:,} 行，{file_size_mb:.2f} MB")

            # 验证列
            columns: list[str] = self.clean_df.columns
            logger.info(f"输出列：{columns}")

        except Exception as e:
            logger.error(f"【数据导出】失败：{e}")
            raise
    def _analyze_funnel(self) -> Dict[str, Any]:
        """电商转化漏斗分析

        漏斗路径：PV 浏览 → 加购/收藏 → 购买

        Returns
        -------
        Dict[str, Any]
            漏斗分析结果，包含：
            - behavior_counts : 各行为类型统计
            - step1_users : 浏览用户数
            - step2_users : 加购或收藏用户数
            - step3_users : 购买用户数
            - step1_to_step2_rate : Step1→Step2 转化率
            - step2_to_step3_rate : Step2→Step3 转化率
            - overall_conversion_rate : 整体转化率
        """
        logger.info("-" * 50)
        logger.info("【漏斗分析】开始计算转化漏斗")

        try:
            # 从导出的 Parquet 读取数据
            funnel_df: pl.LazyFrame = pl.scan_parquet(self.config.output_path)

            # 统计各行为总次数
            behavior_counts: pl.DataFrame = (
                funnel_df.group_by("behavior_type")
                .agg(pl.len().alias("count"))
                .sort("behavior_type")
                .collect(engine="streaming")
            )

            # 构建用户行为宽表
            user_behavior_pivot: pl.DataFrame = (
                funnel_df.group_by("user_id")
                .agg(
                    (pl.col("behavior_type") == "pv")
                    .sum()
                    .alias("pv_count"),
                    (pl.col("behavior_type") == "cart")
                    .sum()
                    .alias("cart_count"),
                    (pl.col("behavior_type") == "fav")
                    .sum()
                    .alias("fav_count"),
                    (pl.col("behavior_type") == "buy")
                    .sum()
                    .alias("buy_count"),
                )
                .collect(engine="streaming")
            )

            # 标记用户是否有各行为
            user_behavior_flag: pl.DataFrame = user_behavior_pivot.with_columns(
                (pl.col("pv_count") > 0).alias("has_pv"),
                (pl.col("cart_count") > 0).alias("has_cart"),
                (pl.col("fav_count") > 0).alias("has_fav"),
                (pl.col("buy_count") > 0).alias("has_buy"),
            )

            # 漏斗链路过滤
            step1_df: pl.DataFrame = user_behavior_flag.filter(
                pl.col("has_pv")
            )
            step2_df: pl.DataFrame = step1_df.filter(
                pl.col("has_cart") | pl.col("has_fav")
            )
            step3_df: pl.DataFrame = step2_df.filter(pl.col("has_buy"))

            # 计算用户数
            step1_users: int = step1_df.height
            step2_users: int = step2_df.height
            step3_users: int = step3_df.height

            # 计算转化率
            step1_to_step2_rate: float = (
                (step2_users / step1_users * 100) if step1_users > 0 else 0
            )
            step2_to_step3_rate: float = (
                (step3_users / step2_users * 100) if step2_users > 0 else 0
            )
            step3_overall_rate: float = (
                (step3_users / step1_users * 100) if step1_users > 0 else 0
            )

            # 打印漏斗报表
            self._print_funnel_report(
                behavior_counts=behavior_counts,
                step1_users=step1_users,
                step2_users=step2_users,
                step3_users=step3_users,
                step1_to_step2_rate=step1_to_step2_rate,
                step2_to_step3_rate=step2_to_step3_rate,
                overall_conversion_rate=step3_overall_rate,
                total_users=user_behavior_pivot.height,
            )

            return {
                "behavior_counts": behavior_counts,
                "step1_users": step1_users,
                "step2_users": step2_users,
                "step3_users": step3_users,
                "step1_to_step2_rate": step1_to_step2_rate,
                "step2_to_step3_rate": step2_to_step3_rate,
                "overall_conversion_rate": step3_overall_rate,
            }

        except Exception as e:
            logger.error(f"【漏斗分析】失败：{e}")
            raise
    @staticmethod
    def _print_funnel_report(
        behavior_counts: pl.DataFrame,
        step1_users: int,
        step2_users: int,
        step3_users: int,
        step1_to_step2_rate: float,
        step2_to_step3_rate: float,
        overall_conversion_rate: float,
        total_users: int,
    ) -> None:
        """打印漏斗分析报表

        Parameters
        ----------
        behavior_counts : pl.DataFrame
            各行为类型统计
        step1_users : int
            浏览用户数
        step2_users : int
            加购或收藏用户数
        step3_users : int
            购买用户数
        step1_to_step2_rate : float
            Step1→Step2 转化率（%）
        step2_to_step3_rate : float
            Step2→Step3 转化率（%）
        overall_conversion_rate : float
            整体转化率（%）
        total_users : int
            总用户数
        """
        logger.info("-" * 60)
        logger.info("【电商转化漏斗分析报告】")
        logger.info("-" * 60)

        # 用户转化漏斗
        logger.info("【用户转化漏斗】")
        logger.info(
            f"{'步骤':<10} {'行为类型':<16} "
            f"{'用户数':>12} {'阶段转化率':>12} {'整体转化率':>12}"
        )
        logger.info("-" * 60)
        logger.info(
            f"{'Step 1':<10} {'PV 浏览':<16} "
            f"{step1_users:>12,} {'-':>12} {100.00:>11.2f}%"
        )
        logger.info(
            f"{'Step 2':<10} {'加购或收藏':<16} "
            f"{step2_users:>12,} "
            f"{step1_to_step2_rate:>11.2f}% {step1_to_step2_rate:>11.2f}%"
        )
        logger.info(
            f"{'Step 3':<10} {'购买':<16} "
            f"{step3_users:>12,} "
            f"{step2_to_step3_rate:>11.2f}% "
            f"{overall_conversion_rate:>11.2f}%"
        )
        logger.info("-" * 60)

        # 总体转化摘要
        overall_dropoff_rate: float = 100 - overall_conversion_rate
        logger.info("【总体转化摘要】")
        logger.info(f"总用户数：{total_users:,}")
        logger.info(f"Step 1 浏览用户数：{step1_users:,}")
        logger.info(f"Step 2 加购或收藏用户数：{step2_users:,}")
        logger.info(f"Step 3 最终购买用户数：{step3_users:,}")
        logger.info(
            f"整体转化率 (PV→Buy): {overall_conversion_rate:.2f}%"
        )
        logger.info(f"整体流失率：{overall_dropoff_rate:.2f}%")
        logger.info("-" * 60)

        # 行为类型分布
        logger.info("【行为类型分布详情】")
        logger.info(f"\n{behavior_counts}")
        logger.info("-" * 60)
    # ==================== 便捷方法 ====================
    def run_full_pipeline(self) -> Dict[str, Any]:
        """一键运行完整流水线

        Returns
        -------
        Dict[str, Any]
            漏斗分析结果
        """
        logger.info("=" * 60)
        logger.info("开始运行 Milestone 1 完整数据处理流水线")
        logger.info("=" * 60)

        try:
            # Extract
            self.extract()

            # Transform
            self.transform()

            # Load
            results: Dict[str, Any] = self.load()

            logger.info("=" * 60)
            logger.info("【流水线完成】所有处理阶段执行成功")
            logger.info("=" * 60)

            return results

        except Exception as e:
            logger.error(f"【流水线失败】{e}")
            raise
