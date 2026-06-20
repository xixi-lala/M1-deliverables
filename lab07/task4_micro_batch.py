# -*- coding: utf-8 -*-
"""
实验七 任务4：Micro-Batch 批量推理优化
功能：
1. 双触发机制：攒满 BATCH_SIZE 条 或 超时 BATCH_TIMEOUT 秒强制推理
2. 批量特征提取与推理，显著提升吞吐量
3. 支持切换模式：逐条推理(B=1) vs 批量推理(B=50)，方便对比
"""

import time
import threading
import queue
import csv
import pandas as pd
import numpy as np
import joblib
from pathlib import Path
from typing import Dict, List

# 全局抑制 sklearn 警告
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

# ===================== 配置区域（关键参数在此调整） =====================
# 路径配置
DATASET_PATH = r"D:\MyProjects\DataAnalysis\lab02\UserBehavior.csv"  # 请修改为你的路径
MODEL_PATH = r"D:\MyProjects\DataAnalysis\lab07\model.pkl"
OUTPUT_CSV_PATH = "scored_events_batch.csv"

# 【核心】Micro-Batch 配置
# 对比实验：
#   方案1（逐条推理）：BATCH_SIZE = 1
#   方案2（批量推理）：BATCH_SIZE = 50
BATCH_SIZE = 50          # 批量大小：50条/批
BATCH_TIMEOUT = 0.5      # 超时兜底：0.5秒后即使没攒满也强制推理

# 实验控制
CSV_COLUMNS = ["user_id", "item_id", "category_id", "behavior_type", "timestamp"]
MODEL_FEATURES = ["category_id", "hour", "dayofweek"]
QUEUE_MAXSIZE = 500
TOTAL_DATA_TO_PROCESS = 1000  # 任务书要求：处理1000条数据用于对比

# ===================== 1. 全局初始化：加载模型 =====================
def load_prediction_model():
    print("=" * 80)
    print("步骤 1/4：全局初始化 - 加载模型")
    print("=" * 80)
    print(f"📥 正在加载模型：{MODEL_PATH}")
    try:
        model = joblib.load(MODEL_PATH)
        print("✅ 模型加载成功！")
        return model
    except Exception as e:
        raise RuntimeError(f"模型加载失败：{str(e)}")

# ===================== 2. 生产者：流式读取数据（控制总量1000条） =====================
class DatasetProducer(threading.Thread):
    def __init__(self, q: queue.Queue, stop_event: threading.Event, total_count: int = TOTAL_DATA_TO_PROCESS):
        super().__init__(name="Producer")
        self.q = q
        self.stop_event = stop_event
        self.total_count = total_count
        self.count = 0
        
    def run(self):
        print(f"\n🚀 [{self.name}] 启动：计划读取 {self.total_count} 条数据")
        try:
            with open(DATASET_PATH, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f, fieldnames=CSV_COLUMNS)
                for row in reader:
                    if self.stop_event.is_set() or self.count >= self.total_count:
                        break
                    self.q.put(row, block=True)
                    self.count += 1
        except Exception as e:
            print(f"❌ 生产者异常：{str(e)}")
        finally:
            print(f"\n🏁 [{self.name}] 停止，总发送：{self.count} 条")

# ===================== 3. 【核心】Micro-Batch 消费者 =====================
class MicroBatchConsumer(threading.Thread):
    def __init__(self, consumer_id, q: queue.Queue, stop_event: threading.Event, model):
        super().__init__(name=f"Consumer-{consumer_id}")
        self.consumer_id = consumer_id
        self.q = q
        self.stop_event = stop_event
        self.model = model
        
        # Micro-Batch 核心变量
        self.buffer: List[Dict] = []  # 数据缓冲区
        self.last_flush_time = time.time()  # 上次批量推理的时间
        
        # 统计变量
        self.total_processed = 0
        self.start_time = None
        self.end_time = None
        
        # 初始化输出文件
        self._init_output_csv()
        
    def _init_output_csv(self):
        header = CSV_COLUMNS + ["predicted_label", "buy_probability", "error"]
        with open(OUTPUT_CSV_PATH, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            
    def _extract_features_batch(self, events: List[Dict]) -> np.ndarray:
        """【批量特征提取】一次性处理整个 buffer，效率更高"""
        features_list = []
        for event in events:
            category_id = int(event["category_id"])
            ts = int(event["timestamp"])
            ts_datetime = pd.Timestamp(ts, unit='s')
            hour = ts_datetime.hour
            dayofweek = ts_datetime.dayofweek
            features_list.append([category_id, hour, dayofweek])
        return np.array(features_list)
        
    def _flush_buffer(self):
        """【核心】批量推理：将 buffer 中的数据一次性送入模型"""
        if not self.buffer:
            return
            
        batch_size = len(self.buffer)
        # print(f"🔄 触发批量推理：Buffer大小 = {batch_size}")  # 可选：打印调试
        
        try:
            # 1. 批量特征提取
            batch_features = self._extract_features_batch(self.buffer)
            
            # 2. 【核心】批量推理（只调用两次 model，而不是 BATCH_SIZE 次！）
            preds = self.model.predict(batch_features)
            probs = self.model.predict_proba(batch_features)
            
            # 3. 结果回流：将预测结果填回每个 event
            for i, event in enumerate(self.buffer):
                event["predicted_label"] = int(preds[i])
                event["buy_probability"] = float(probs[i][1])
                event["error"] = ""
                
            # 4. 批量持久化写入
            self._append_batch_to_csv(self.buffer)
            
            # 5. 更新统计
            self.total_processed += batch_size
            
        except Exception as e:
            # 单批失败：标记所有为失败
            for event in self.buffer:
                event["predicted_label"] = -1
                event["buy_probability"] = -1.0
                event["error"] = str(e)[:50]
            self._append_batch_to_csv(self.buffer)
            
        finally:
            # 清空 buffer，重置时间
            self.buffer.clear()
            self.last_flush_time = time.time()
            
    def _append_batch_to_csv(self, events: List[Dict]):
        """批量写入CSV，减少磁盘I/O次数"""
        header = CSV_COLUMNS + ["predicted_label", "buy_probability", "error"]
        with open(OUTPUT_CSV_PATH, "a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            for event in events:
                row = {k: event.get(k, "") for k in header}
                writer.writerow(row)
            
    def run(self):
        print(f"\n👷 [{self.name}] 启动：Micro-Batch 模式 (BATCH_SIZE={BATCH_SIZE}, TIMEOUT={BATCH_TIMEOUT}s)")
        print("=" * 100)
        print(f"{'进度':<10} | {'Buffer大小':<12} | {'触发原因':<20} | {'累计处理':<10}")
        print("-" * 100)
        
        self.start_time = time.time()  # 记录开始时间
        
        while not self.stop_event.is_set() or not self.q.empty() or self.buffer:
            try:
                # 【任务书要求】queue.get(timeout=0.1)：非阻塞等待，避免卡死
                event = self.q.get(timeout=0.1)
                self.buffer.append(event)
                self.q.task_done()
                
            except queue.Empty:
                # 【任务书要求】捕获 queue.Empty 后 pass，继续检查超时
                pass
            
            # ==========================================
            # 【核心】双触发条件推理
            # 条件1：len(buffer) >= BATCH_SIZE (攒满了)
            # 条件2：(buffer 非空) and (距离上次刷新超过 BATCH_TIMEOUT) (超时兜底)
            # ==========================================
            if len(self.buffer) >= BATCH_SIZE or (self.buffer and time.time() - self.last_flush_time > BATCH_TIMEOUT):
                # 判断触发原因（用于打印）
                trigger_reason = "攒满Batch" if len(self.buffer) >= BATCH_SIZE else "超时兜底"
                
                # 执行批量推理
                self._flush_buffer()
                
                # 打印进度（每100条打一次）
                if self.total_processed % 100 == 0 or self.total_processed >= TOTAL_DATA_TO_PROCESS:
                    print(f"{self.total_processed:<10} | {len(self.buffer):<12} | {trigger_reason:<20} | {self.total_processed:<10}")
                
                # 【实验控制】处理够1000条就停止
                if self.total_processed >= TOTAL_DATA_TO_PROCESS:
                    self.stop_event.set()
                    break
        
        self.end_time = time.time()  # 记录结束时间
        print("=" * 100)
        print(f"🏁 [{self.name}] 停止，总处理：{self.total_processed} 条")

# ===================== 4. 主程序 =====================
if __name__ == "__main__":
    print("\n" + "=" * 80)
    print(f"实验七 任务4：Micro-Batch 批量推理优化 (BATCH_SIZE={BATCH_SIZE})")
    print("=" * 80)
    
    # 1. 加载模型
    try:
        model = load_prediction_model()
    except Exception as e:
        print(f"❌ 初始化失败：{str(e)}")
        exit(1)
    
    # 2. 初始化组件
    q = queue.Queue(maxsize=QUEUE_MAXSIZE)
    stop_event = threading.Event()
    
    producer = DatasetProducer(q, stop_event)
    consumer = MicroBatchConsumer(1, q, stop_event, model)
    
    # 3. 启动实验
    print(f"\n🚀 启动 Micro-Batch 实验，目标处理：{TOTAL_DATA_TO_PROCESS} 条数据")
    producer.start()
    consumer.start()
    
    # 等待结束
    producer.join()
    consumer.join()
    
    # 4. 【核心】吞吐量统计（用于填实验报告对比表）
    print("\n" + "🎉 " + "=" * 80)
    print("任务4 吞吐量对比统计")
    print("=" * 80)
    
    total_time = consumer.end_time - consumer.start_time
    throughput = consumer.total_processed / total_time if total_time > 0 else 0
    
    print(f"📊 实验配置：")
    print(f"   BATCH_SIZE = {BATCH_SIZE}")
    print(f"   BATCH_TIMEOUT = {BATCH_TIMEOUT} 秒")
    print(f"\n📊 性能结果：")
    print(f"   总处理数据量：{consumer.total_processed} 条")
    print(f"   总耗时：{total_time:.4f} 秒")
    print(f"   吞吐量：{throughput:.2f} 条/秒")
    
    # 【任务书要求】对比表提示
    print("\n" + "-" * 80)
    print("💡 实验报告对比表填写提示：")
    print("   1. 先运行 BATCH_SIZE = 1，记录『逐条推理』数据")
    print("   2. 再运行 BATCH_SIZE = 50，记录『Micro-Batch』数据")
    print("-" * 80)
    
    print("=" * 80)
    print("\n✅ 任务4完成！")
    print(f"📦 打标结果：{Path(OUTPUT_CSV_PATH).absolute()}")