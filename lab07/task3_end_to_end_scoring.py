# -*- coding: utf-8 -*-
"""
实验七 任务三：流式推理管道（用户行为购买预测）
修复内容：
1. 解决 NameError: name 'Path' is not defined
2. 全局抑制 sklearn 警告，彻底解决刷屏
3. 特征提取返回 numpy 数组，与训练时完全一致
"""
import time
import threading
import queue
import csv
import pandas as pd
import numpy as np
import joblib
from pathlib import Path  # 修复1：导入Path
from typing import Dict

# 修复2：全局抑制 sklearn 警告
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

# ===================== 配置参数 =====================
DATASET_PATH = r"D:\MyProjects\DataAnalysis\lab02\UserBehavior.csv"
MODEL_PATH = r"D:\MyProjects\DataAnalysis\lab07\model.pkl"
OUTPUT_CSV_PATH = "scored_events.csv"

CSV_COLUMNS = ["user_id", "item_id", "category_id", "behavior_type", "timestamp"]
MODEL_FEATURES = ["category_id", "hour", "dayofweek"]  

QUEUE_MAXSIZE = 200
EXPERIMENT_DURATION = 30
PRODUCER_RATE = 20

# ===================== 1. 加载训练好的预测模型 =====================
def load_prediction_model():
    print("=" * 80)
    print("步骤 1/3：全局初始化 - 加载模型")
    print("=" * 80)
    print(f"📥 正在加载模型：{MODEL_PATH}")
    try:
        model = joblib.load(MODEL_PATH)
        print("✅ 模型加载成功！")
        return model
    except Exception as e:
        raise RuntimeError(f"模型加载失败：{str(e)}")

# ===================== 2. 流式生产者 =====================
class DatasetProducer(threading.Thread):
    def __init__(self, q: queue.Queue, stop_event: threading.Event, producer_rate: int = PRODUCER_RATE):
        super().__init__(name="Producer")
        self.q = q
        self.stop_event = stop_event
        self.producer_rate = producer_rate
        self.count = 0
        
    def run(self):
        print(f"\n🚀 [{self.name}] 启动：开始流式读取数据集")
        base_delay = 1.0 / self.producer_rate
        
        try:
            with open(DATASET_PATH, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f, fieldnames=CSV_COLUMNS)
                for row in reader:
                    if self.stop_event.is_set():
                        break
                    self.q.put(row, block=True)
                    self.count += 1
                    time.sleep(base_delay)
        except Exception as e:
            print(f"❌ 生产者异常：{str(e)}")
        finally:
            print(f"\n🏁 [{self.name}] 停止，总发送：{self.count} 条")

# ===================== 3. 流式消费者 =====================
class ScoringConsumer(threading.Thread):
    def __init__(self, consumer_id, q: queue.Queue, stop_event: threading.Event, model):
        super().__init__(name=f"Consumer-{consumer_id}")
        self.consumer_id = consumer_id
        self.q = q
        self.stop_event = stop_event
        self.model = model
        self.total_processed = 0
        self.total_failed = 0
        self.predicted_positive = 0
        self.total_buy_prob = 0.0
        self._init_output_csv()
        
    def _init_output_csv(self):
        header = CSV_COLUMNS + ["predicted_label", "buy_probability", "error"]
        with open(OUTPUT_CSV_PATH, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            
    def _extract_features(self, event: Dict) -> np.ndarray:
        """修复3：返回 numpy 数组，不带特征名，与训练完全一致"""
        category_id = int(event["category_id"])
        ts = int(event["timestamp"])
        ts_datetime = pd.Timestamp(ts, unit='s')
        hour = ts_datetime.hour
        dayofweek = ts_datetime.dayofweek
        return np.array([[category_id, hour, dayofweek]])
        
    def _append_to_csv(self, event: Dict):
        header = CSV_COLUMNS + ["predicted_label", "buy_probability", "error"]
        with open(OUTPUT_CSV_PATH, "a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            row = {k: event.get(k, "") for k in header}
            writer.writerow(row)
            
    def run(self):
        print(f"\n👷 [{self.name}] 启动：开始在线推理")
        print("=" * 120)
        print(f"{'原始数据(预览)':<50} | {'预测标签':<10} | {'购买概率':<10} | {'状态':<10}")
        print("-" * 120)
        
        print_limit = 10
        print_count = 0
        
        while not self.stop_event.is_set() or not self.q.empty():
            try:
                event = self.q.get(timeout=0.5)
                self.total_processed += 1
                
                event["predicted_label"] = -1
                event["buy_probability"] = -1.0
                event["error"] = ""
                
                try:
                    features = self._extract_features(event)
                    predicted_label = int(self.model.predict(features)[0])
                    buy_probability = float(self.model.predict_proba(features)[0][1])
                    event["predicted_label"] = predicted_label
                    event["buy_probability"] = round(buy_probability, 4)
                    if predicted_label == 1:
                        self.predicted_positive += 1
                    self.total_buy_prob += buy_probability
                except Exception as e:
                    self.total_failed += 1
                    event["error"] = str(e)[:50]
                
                if print_count < print_limit:
                    preview = f"UID:{event['user_id'][:8]}, CID:{event['category_id'][:8]}, Type:{event['behavior_type']}"
                    status = "✅成功" if event["error"] == "" else "❌失败"
                    print(f"{preview:<50} | {event['predicted_label']:<10} | {event['buy_probability']:<10.4f} | {status:<10}")
                    print_count += 1
                elif print_count == print_limit:
                    print("... 后续数据不再打印（已保存至 scored_events.csv） ...")
                    print_count += 1
                
                self._append_to_csv(event)
                self.q.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                print(f"\n❌ 消费者全局异常：{str(e)}")
                break
        
        print("=" * 120)
        print(f"🏁 [{self.name}] 停止，总处理：{self.total_processed}，失败：{self.total_failed}")

# ===================== 4. 主程序 =====================
if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("实验七 任务3：端到端打标流水线（一键修复版）")
    print("=" * 80)
    
    try:
        model = load_prediction_model()
    except Exception as e:
        print(f"❌ 初始化失败：{str(e)}")
        exit(1)
    
    q = queue.Queue(maxsize=QUEUE_MAXSIZE)
    stop_event = threading.Event()
    
    producer = DatasetProducer(q, stop_event)
    consumer = ScoringConsumer(1, q, stop_event, model)
    
    print(f"\n🚀 启动端到端打标流水线，运行时长：{EXPERIMENT_DURATION} 秒")
    producer.start()
    consumer.start()
    
    try:
        time.sleep(EXPERIMENT_DURATION)
    except KeyboardInterrupt:
        print("\n⏹️  用户手动停止")
    
    print("\n" + "=" * 80)
    print("🛑 正在停止...")
    stop_event.set()
    
    producer.join()
    consumer.join(timeout=2.0)
    
    print("\n" + "🎉 " + "=" * 80)
    print("任务3 运行结果统计（实验报告用）")
    print("=" * 80)
    
    if Path(OUTPUT_CSV_PATH).exists():
        df_result = pd.read_csv(OUTPUT_CSV_PATH)
        print(f"📊 1. 总打标记录数：{len(df_result)}")
        print(f"📊 2. 正样本（预测为购买）占比：{(df_result['predicted_label'] == 1).mean()*100:.2f}%")
        print(f"📊 3. 平均购买概率：{df_result[df_result['buy_probability'] >= 0]['buy_probability'].mean():.4f}")
        print(f"📊 4. 推理失败记录数：{(df_result['predicted_label'] == -1).sum()}")
        print("\n📝 5. scored_events.csv 前10行：")
        print("-" * 80)
        print(df_result.head(10).to_string(index=False))
        print("-" * 80)
    
    print("=" * 80)
    print("\n✅ 任务3完成！")
    print(f"📦 打标结果文件：{Path(OUTPUT_CSV_PATH).absolute()}")