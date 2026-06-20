# -*- coding: utf-8 -*-
"""
实验六 任务4：流式特征预处理预演（最终修复版）
修复点：
1. 正确处理无表头的UserBehavior.csv，手动指定列名
2. 解决全NaN特征导致的拟合失败问题
3. 保留所有任务书要求的功能
"""
import time
import threading
import queue
import csv
import pandas as pd
import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
import matplotlib.pyplot as plt

plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

# ===================== 配置参数 =====================
DATASET_PATH = r"D:\MyProjects\DataAnalysis\lab02\UserBehavior.csv"
# 实验二明确的数据集列顺序（无表头）
CSV_COLUMNS = ["user_id", "item_id", "category_id", "behavior_type", "timestamp"]
# 提取的三个连续型数值特征
NUMERIC_FEATURES = ["category_id", "timestamp", "user_id"]
OFFLINE_FIT_SAMPLES = 1000
QUEUE_MAXSIZE = 200
EXPERIMENT_DURATION = 15

# ===================== 1. 离线阶段：从真实数据集拟合预处理Pipeline =====================
def offline_fit_pipeline():
    print("🔹 任务4 - 离线阶段：从真实数据集拟合预处理Pipeline...")
    print(f"📂 读取数据集：{DATASET_PATH}")
    print(f"📊 使用前 {OFFLINE_FIT_SAMPLES} 行进行离线拟合")
    
    history_data = []
    with open(DATASET_PATH, "r", encoding="utf-8") as f:
        # 【关键修复】手动指定列名，因为数据集无表头
        reader = csv.DictReader(f, fieldnames=CSV_COLUMNS)
        for i, row in enumerate(reader):
            if i >= OFFLINE_FIT_SAMPLES:
                break
            sample = []
            for feat in NUMERIC_FEATURES:
                try:
                    sample.append(float(row[feat]))
                except (ValueError, KeyError):
                    sample.append(np.nan)
            history_data.append(sample)
    
    # 构建预处理Pipeline
    preprocess_pipe = Pipeline([
        ('imputer', SimpleImputer(strategy='median')),
        ('scaler', StandardScaler())
    ])
    
    X_train = np.array(history_data)
    preprocess_pipe.fit(X_train)
    
    # 打印拟合结果
    print("\n✅ 离线拟合完成")
    print(f"特征名称：{NUMERIC_FEATURES}")
    print(f"填充的中位数：{preprocess_pipe.named_steps['imputer'].statistics_}")
    print(f"标准化均值：{preprocess_pipe.named_steps['scaler'].mean_}")
    print(f"标准化标准差：{preprocess_pipe.named_steps['scaler'].scale_}")
    
    return preprocess_pipe

# ===================== 2. 流式生产者：逐行读取真实数据集 =====================
class DatasetProducer(threading.Thread):
    def __init__(self, q, stop_event, producer_rate=20):
        super().__init__()
        self.q = q
        self.stop_event = stop_event
        self.producer_rate = producer_rate
        self.count = 0
    
    def run(self):
        print("\n🔹 生产者启动：开始流式读取数据集...")
        base_delay = 1.0 / self.producer_rate
        
        with open(DATASET_PATH, "r", encoding="utf-8") as f:
            # 【关键修复】同样手动指定列名
            reader = csv.DictReader(f, fieldnames=CSV_COLUMNS)
            for row in reader:
                if self.stop_event.is_set():
                    break
                self.q.put(row)
                self.count += 1
                time.sleep(base_delay)
        
        print(f"\n✅ 生产者停止，总发送：{self.count} 条")

# ===================== 3. 流式消费者：在线特征预处理 =====================
class StreamConsumer(threading.Thread):
    def __init__(self, q, stop_event, preprocess_pipe):
        super().__init__()
        self.q = q
        self.stop_event = stop_event
        self.pipe = preprocess_pipe
        self.raw_data = []
        self.scale_data = []
    
    def run(self):
        print("🔹 消费者启动：开始在线特征预处理...")
        print("="*80)
        print(f"{'原始值':<40} | {'标准化后值':<40}")
        print("="*80)
        
        processed_count = 0
        while not self.stop_event.is_set() or not self.q.empty():
            try:
                event = self.q.get(timeout=1)
                
                # 提取数值特征
                raw_feat = []
                for feat in NUMERIC_FEATURES:
                    try:
                        raw_feat.append(float(event[feat]))
                    except (ValueError, KeyError):
                        raw_feat.append(np.nan)
                raw_feat = np.array([raw_feat])
                
                # 在线标准化
                scaled_feat = self.pipe.transform(raw_feat)
                
                # 保存数据
                self.raw_data.append(raw_feat[0])
                self.scale_data.append(scaled_feat[0])
                
                # 每100条打印一次对比
                processed_count += 1
                if processed_count % 100 == 0:
                    raw_str = ", ".join([f"{x:.2f}" for x in raw_feat[0]])
                    scaled_str = ", ".join([f"{x:.4f}" for x in scaled_feat[0]])
                    print(f"{raw_str:<40} | {scaled_str:<40}")
                
                self.q.task_done()
                
            except queue.Empty:
                continue
        
        print("="*80)
        print(f"✅ 消费者停止，总处理：{processed_count} 条")

# ===================== 4. 主程序 =====================
if __name__ == "__main__":
    # 1. 离线拟合
    preprocess_pipe = offline_fit_pipeline()
    
    # 2. 初始化组件
    q = queue.Queue(maxsize=QUEUE_MAXSIZE)
    stop_event = threading.Event()
    
    producer = DatasetProducer(q, stop_event, producer_rate=20)
    consumer = StreamConsumer(q, stop_event, preprocess_pipe)
    
    # 3. 启动实验
    print(f"\n🚀 启动流式预处理实验，运行时长：{EXPERIMENT_DURATION} 秒")
    producer.start()
    consumer.start()
    
    time.sleep(EXPERIMENT_DURATION)
    stop_event.set()
    
    # 等待线程结束
    producer.join()
    consumer.join()
    
    # 4. 保存结果
    raw_df = pd.DataFrame(consumer.raw_data, columns=NUMERIC_FEATURES)
    scale_df = pd.DataFrame(consumer.scale_data, columns=[f"{f}_scaled" for f in NUMERIC_FEATURES])
    
    raw_df.to_csv("task4_raw_features.csv", index=False)
    scale_df.to_csv("task4_scaled_features.csv", index=False)
    
    # 5. 统计漂移检测
    print("\n📊 统计漂移检测结果")
    print("-"*50)
    online_mean = np.mean(consumer.scale_data, axis=0)
    online_std = np.std(consumer.scale_data, axis=0)
    
    for i, feat in enumerate(NUMERIC_FEATURES):
        print(f"{feat}:")
        print(f"  理论均值：0.0000 | 实测均值：{online_mean[i]:.4f} | 偏差：{abs(online_mean[i]):.4f}")
        print(f"  理论标准差：1.0000 | 实测标准差：{online_std[i]:.4f} | 偏差：{abs(online_std[i]-1.0):.4f}")
        print()
    
    print("\n🎉 任务4运行成功！")
    print("📊 原始特征：task4_raw_features.csv")
    print("📊 标准化特征：task4_scaled_features.csv")