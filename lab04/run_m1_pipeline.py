#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Milestone 1 数据处理流水线 - 主入口
一键运行全流程：extract → transform → load

用法：
    python run_m1_pipeline.py                     # 使用默认配置
    python run_m1_pipeline.py --help              # 查看帮助
"""

import sys
import logging
from pathlib import Path
# 确保可以导入 m1_pipeline 模块
sys.path.insert(0, str(Path(__file__).parent))
from m1_pipeline import M1DataPipeline, PipelineConfig
# ==================== 日志配置 ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
def main():
    """主入口函数"""
    logger.info("=" * 70)
    logger.info(" " * 15 + "Milestone 1 数据处理流水线")
    logger.info("=" * 70)
    # ==================== 路径配置 ====================
    base_dir = Path(__file__).parent
    data_dir = base_dir.parent / "lab02" / "clean_data_partitioned"
    output_path = base_dir / "m1_final_clean.parquet"
    bot_suspects_path = base_dir / "bot_suspects.csv"
    # ==================== 检测参数配置 ====================
    config = PipelineConfig(
        data_dir=data_dir,
        output_path=output_path,
        bot_suspects_path=bot_suspects_path,
        session_timeout_seconds=1800,   # 会话超时：30 分钟
        hourly_threshold=100,           # 小时行为次数阈值
        min_active_hours=1,             # 最小活跃小时数
        streaming_chunk_size=100000,    # 流式计算块大小
    )
    # ==================== 打印配置信息 ====================
    logger.info("\n【配置信息】")
    logger.info(f"  数据目录：{config.data_dir}")
    logger.info(f"  输出文件：{config.output_path}")
    logger.info(f"  机器人列表：{config.bot_suspects_path}")
    logger.info(f"  会话超时阈值：{config.session_timeout_seconds} 秒（{config.session_timeout_seconds // 60} 分钟）")
    logger.info(f"  小时行为阈值：{config.hourly_threshold} 次/小时")
    logger.info(f"  最小活跃小时数：{config.min_active_hours} 小时")
    logger.info("")
    # ==================== 验证输入路径 ====================
    if not config.data_dir.exists():
        logger.error(f"数据目录不存在：{config.data_dir}")
        logger.error("请确保 lab02/clean_data_partitioned 目录中有分区 Parquet 数据")
        sys.exit(1)
    # 统计文件数量
    parquet_files = list(config.data_dir.glob("**/*.parquet"))
    if len(parquet_files) == 0:
        logger.error(f"数据目录中没有找到 Parquet 文件：{config.data_dir}")
        sys.exit(1)
    logger.info(f"发现 {len(parquet_files)} 个 Parquet 文件")
    # ==================== 运行流水线 ====================
    try:
        # 创建流水线实例
        pipeline = M1DataPipeline(config)
        # 一键运行全流程
        results = pipeline.run_full_pipeline()
        # ==================== 输出最终结果 ====================
        logger.info("\n" + "=" * 70)
        logger.info("【流水线执行结果】")
        logger.info("=" * 70)
        logger.info(f"输出文件：{config.output_path}")
        logger.info(f"输出大小：{config.output_path.stat().st_size / (1024 * 1024):.2f} MB")

        if config.bot_suspects_path.exists():
            logger.info(f"机器人列表：{config.bot_suspects_path}")

        logger.info("=" * 70)

        return results
    except Exception as e:
        logger.error(f"\n流水线执行失败：{e}")
        logger.exception("详细错误信息：")
        sys.exit(1)
if __name__ == "__main__":
    main()
