# -*- coding: utf-8 -*-
"""
实验八 M2 统一流处理入口（含混沌容错测试）
严格按任务书要求：
  任务1：argparse CLI + 死信降级 (dead_letter.log)
  任务2：--chaos 启用异常注入 + 高负载背压测试
"""

import argparse
import csv
import json
import logging
import queue
import random
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

import joblib
import numpy as np
import pandas as pd

import warnings
warnings.filterwarnings("ignore", category=UserWarning)

# ===================== 默认配置 =====================
DEFAULT_DATASET = r"D:\MyProjects\DataAnalysis\lab02\UserBehavior.csv"
DEFAULT_MODEL   = r"D:\MyProjects\DataAnalysis\lab07\model.pkl"
DEFAULT_OUTPUT  = "scored_events.csv"
DEAD_LETTER_LOG = "dead_letter.log"

CSV_COLUMNS = ["user_id", "item_id", "category_id", "behavior_type", "timestamp"]
MODEL_FEATURES = ["category_id", "hour", "dayofweek"]

# 正常数据模拟参数（混沌模式用）
USER_ID_RANGE = (1, 100000)
ITEM_ID_RANGE = (1, 50000)
CATEGORY_ID_RANGE = (1, 5000000)
TIMESTAMP_RANGE = (1511539200, 1512316799)  # 2017.11.25 ~ 2017.12.3
BEHAVIOR_TYPES = ["pv", "cart", "fav", "buy"]

# ===================== 命令行参数解析 =====================
def parse_args():
    parser = argparse.ArgumentParser(
        description="流处理在线推理管道 - M2 统一入口",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--qps", type=int, default=20,
                        help="生产者速率 (条/秒)")
    parser.add_argument("--queue-limit", type=int, default=200,
                        help="队列最大容量 (0=无限)")
    parser.add_argument("--duration", type=int, default=30,
                        help="实验运行时长 (秒)")
    parser.add_argument("--dataset", type=str, default=DEFAULT_DATASET,
                        help="输入 CSV 数据集路径")
    parser.add_argument("--model", type=str, default=DEFAULT_MODEL,
                        help="训练好的模型文件路径")
    parser.add_argument("--output", type=str, default=DEFAULT_OUTPUT,
                        help="打标结果输出 CSV")
    parser.add_argument("--consumer-workers", type=int, default=1,
                        help="消费者线程数 (当前仅支持1)")
    # 混沌测试参数
    parser.add_argument("--chaos", action="store_true",
                        help="启用混沌测试模式 (注入异常数据)")
    parser.add_argument("--dirty-ratio", type=float, default=0.01,
                        help="混沌模式下异常数据注入比例 (默认 1%%)")
    return parser.parse_args()

# ===================== 日志配置 =====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("M2_Pipeline")

# ===================== 死信日志 =====================
def write_to_dead_letter(original_event: Dict, error_message: str):
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "error": error_message,
        "original_data": original_event
    }
    with open(DEAD_LETTER_LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

# ===================== 1. 全局初始化：加载模型 =====================
def load_model(model_path: str):
    logger.info("=" * 60)
    logger.info("步骤 1/3：全局初始化 - 加载模型")
    logger.info("=" * 60)
    logger.info(f"模型路径: {model_path}")
    try:
        model = joblib.load(model_path)
        logger.info("✅ 模型加载成功！")
        return model
    except Exception as e:
        logger.error(f"❌ 模型加载失败: {e}")
        raise

# ===================== 2. 正常生产者：从 CSV 读取 =====================
class DatasetProducer(threading.Thread):
    def __init__(self, q: queue.Queue, stop_event: threading.Event,
                 dataset_path: str, qps: int):
        super().__init__(name="Producer")
        self.q = q
        self.stop_event = stop_event
        self.dataset_path = dataset_path
        self.qps = qps
        self.count = 0

    def run(self):
        logger.info(f"[{self.name}] 启动，QPS={self.qps}，数据源=CSV")
        base_delay = 1.0 / self.qps
        try:
            with open(self.dataset_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f, fieldnames=CSV_COLUMNS)
                for row in reader:
                    if self.stop_event.is_set():
                        break
                    self.q.put(row, block=True)
                    self.count += 1
                    time.sleep(base_delay)
        except Exception as e:
            logger.error(f"❌ 生产者异常: {e}")
        finally:
            logger.info(f"[{self.name}] 停止，总发送: {self.count} 条")

# ===================== 3. 混沌生产者：模拟生成 + 异常注入 =====================
class ChaosProducer(threading.Thread):
    def __init__(self, q: queue.Queue, stop_event: threading.Event,
                 qps: int, dirty_ratio: float):
        super().__init__(name="ChaosProducer")
        self.q = q
        self.stop_event = stop_event
        self.qps = qps
        self.dirty_ratio = dirty_ratio
        self.total = 0
        self.dirty_count = 0

    def _generate_normal(self) -> Dict:
        return {
            "user_id": str(random.randint(*USER_ID_RANGE)),
            "item_id": str(random.randint(*ITEM_ID_RANGE)),
            "category_id": str(random.randint(*CATEGORY_ID_RANGE)),
            "behavior_type": random.choice(BEHAVIOR_TYPES),
            "timestamp": str(random.randint(*TIMESTAMP_RANGE)),
        }

    def _generate_dirty(self) -> Dict:
        base = self._generate_normal()
        dirty_type = random.choice(["missing_field", "type_error", "malformed_timestamp"])
        if dirty_type == "missing_field":
            field = random.choice(list(base.keys()))
            del base[field]
        elif dirty_type == "type_error":
            base["category_id"] = "not_a_number"
        elif dirty_type == "malformed_timestamp":
            base["timestamp"] = "-999999999"
        return base

    def run(self):
        logger.info(f"🐉 [{self.name}] 混沌模式启动！QPS={self.qps}, "
                    f"异常比例={self.dirty_ratio*100:.1f}%")
        base_delay = 1.0 / self.qps

        while not self.stop_event.is_set():
            try:
                if random.random() < self.dirty_ratio:
                    event = self._generate_dirty()
                    self.dirty_count += 1
                else:
                    event = self._generate_normal()

                self.q.put(event, block=True, timeout=0.1)
                self.total += 1

                if self.total % 500 == 0:
                    qsize = self.q.qsize()
                    logger.info(f"📦 已生成 {self.total} 条 | 异常 {self.dirty_count} 条 | "
                                f"队列深度 {qsize}")

                time.sleep(base_delay)
            except queue.Full:
                time.sleep(0.01)
                continue
            except Exception as e:
                logger.error(f"❌ 混沌生产者异常: {e}")
                break

        logger.info(f"🐉 [{self.name}] 停止: 共生产 {self.total} 条, 注入异常 {self.dirty_count} 条")

# ===================== 4. 消费者：在线推理 + 死信处理 =====================
class ScoringConsumer(threading.Thread):
    def __init__(self, consumer_id: int, q: queue.Queue,
                 stop_event: threading.Event, model, output_path: str):
        super().__init__(name=f"Consumer-{consumer_id}")
        self.consumer_id = consumer_id
        self.q = q
        self.stop_event = stop_event
        self.model = model
        self.output_path = output_path
        self.total_processed = 0
        self.total_failed = 0
        self._init_output_csv()

    def _init_output_csv(self):
        header = CSV_COLUMNS + ["predicted_label", "buy_probability", "error"]
        with open(self.output_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()

    def _extract_features(self, event: Dict) -> np.ndarray:
        category_id = int(event["category_id"])
        ts = int(event["timestamp"])
        ts_datetime = pd.Timestamp(ts, unit='s')
        hour = ts_datetime.hour
        dayofweek = ts_datetime.dayofweek
        return np.array([[category_id, hour, dayofweek]])

    def _append_to_csv(self, event: Dict):
        header = CSV_COLUMNS + ["predicted_label", "buy_probability", "error"]
        with open(self.output_path, "a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            row = {k: event.get(k, "") for k in header}
            writer.writerow(row)

    def run(self):
        logger.info(f"[{self.name}] 启动，在线推理中...")
        print_cnt = 0
        while not self.stop_event.is_set() or not self.q.empty():
            try:
                event = self.q.get(timeout=0.5)
                self.total_processed += 1
                event["predicted_label"] = -1
                event["buy_probability"] = -1.0
                event["error"] = ""

                # ============ 核心容错区 ============
                try:
                    features = self._extract_features(event)
                    event["predicted_label"] = int(self.model.predict(features)[0])
                    event["buy_probability"] = float(self.model.predict_proba(features)[0][1])
                except Exception as e:
                    self.total_failed += 1
                    event["error"] = str(e)[:50]
                    write_to_dead_letter(event, str(e))
                    # 不打印每条死信，避免刷屏
                    if self.total_failed <= 10 or self.total_failed % 50 == 0:
                        logger.warning(f"⚠️ 死信 #{self.total_failed}: {str(e)[:80]}")
                # ====================================

                self._append_to_csv(event)
                if print_cnt < 5:
                    status = "✅" if event["error"] == "" else "❌死信"
                    logger.info(f"  预览: UID={event.get('user_id','?')[:10]} | "
                                f"pred={event['predicted_label']} | prob={event['buy_probability']:.4f} | {status}")
                    print_cnt += 1
                self.q.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"❌ 消费者外层异常: {e}")
                break
        logger.info(f"[{self.name}] 停止: 处理 {self.total_processed}, 失败(死信) {self.total_failed}")

# ===================== 5. 主入口 =====================
def main():
    args = parse_args()

    if args.queue_limit <= 0:
        args.queue_limit = 0

    logger.info("=" * 60)
    logger.info("🚀 M2 流处理管道启动")
    logger.info(f"   模式: {'混沌测试' if args.chaos else '正常'}")
    logger.info(f"   QPS: {args.qps} | 队列上限: {args.queue_limit} | 时长: {args.duration}s")
    if args.chaos:
        logger.info(f"   异常注入比例: {args.dirty_ratio*100:.1f}%")
    logger.info("=" * 60)

    # 加载模型
    model = load_model(args.model)

    # 初始化队列和停止事件
    q = queue.Queue(maxsize=args.queue_limit) if args.queue_limit > 0 else queue.Queue()
    stop_event = threading.Event()

    def signal_handler(sig, frame):
        logger.info("\n⏹️  收到停止信号，正在优雅关闭...")
        stop_event.set()
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # 启动消费者
    consumers = []
    for i in range(args.consumer_workers):
        consumer = ScoringConsumer(i+1, q, stop_event, model, args.output)
        consumers.append(consumer)
        consumer.start()

    # 启动生产者（正常 or 混沌）
    if args.chaos:
        producer = ChaosProducer(q, stop_event, args.qps, args.dirty_ratio)
    else:
        producer = DatasetProducer(q, stop_event, args.dataset, args.qps)
    producer.start()

    # 等待实验时长
    try:
        stop_event.wait(timeout=args.duration)
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()

    # 等待线程结束
    producer.join()
    for c in consumers:
        c.join(timeout=5.0)

    # 最终统计
    logger.info("=" * 60)
    logger.info("📊 最终统计")
    logger.info(f"   生产者: 共发送 {producer.total if args.chaos else producer.count} 条")
    if args.chaos:
        logger.info(f"   异常注入: {producer.dirty_count} 条 ({producer.dirty_count/max(producer.total,1)*100:.2f}%)")
    total_processed = sum(c.total_processed for c in consumers)
    total_failed = sum(c.total_failed for c in consumers)
    logger.info(f"   消费者处理: {total_processed} 条 | 失败(死信): {total_failed} 条")
    logger.info(f"   最终队列残留: {q.qsize()} 条")
    logger.info(f"   📁 结果文件: {Path(args.output).absolute()}")
    logger.info(f"   📁 死信日志: {Path(DEAD_LETTER_LOG).absolute()}")
    logger.info("👋 管道已关闭")

if __name__ == "__main__":
    main()