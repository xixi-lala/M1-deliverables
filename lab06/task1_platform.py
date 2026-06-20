# -*- coding: utf-8 -*-
"""
实验六 任务1：可配置的Producer-Consumer实验平台
"""
import time
import random
import json
import uuid
import threading
import queue
import csv
from datetime import datetime, timezone
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Dict, Any

# ==================== 1. 实验配置类 ====================
@dataclass
class ExperimentConfig:
    """实验参数配置：所有可调参数都在这里"""
    # 生产速率 (λ): 平均每秒生成多少条数据
    producer_rate: float = 10.0          
    # 单条数据处理耗时 (t): 消费者处理一条数据需要多少秒
    consumer_process_time: float = 0.2    
    # 消费者线程数量 (n)
    n_consumers: int = 1                  
    # 队列最大容量 (-1 表示无限队列)
    queue_max_size: int = -1               
    # 实验总运行时长 (秒)
    experiment_duration: float = 15.0      
    # 监控采样间隔 (秒)
    monitor_interval: float = 0.5          
    # 是否启用背压 (任务2用，任务1先设为False)
    enable_backpressure: bool = False      
    
    # 输出文件路径
    metrics_output_path: Path = Path("experiment_metrics.csv")

# ==================== 2. 数据生成逻辑 (复用实验五) ====================
USER_ID_RANGE = (1, 100_000)
ITEM_ID_RANGE = (1, 50_000)
BEHAVIOR_TYPES = ["view", "cart", "purchase"]
BEHAVIOR_WEIGHTS = [76, 19, 5]

def generate_event() -> dict:
    """生成单条电商行为日志（复用实验五逻辑）"""
    event_id = str(uuid.uuid4())
    event_time = datetime.now(timezone.utc).isoformat()
    user_id = random.randint(*USER_ID_RANGE)
    item_id = random.randint(*ITEM_ID_RANGE)
    behavior_type = random.choices(BEHAVIOR_TYPES, BEHAVIOR_WEIGHTS, k=1)[0]
    
    return {
        "event_id": event_id,
        "event_time": event_time,
        "user_id": user_id,
        "item_id": item_id,
        "behavior_type": behavior_type,
    }

# ==================== 3. 生产者线程 (Producer) ====================
class ProducerThread(threading.Thread):
    def __init__(self, config: ExperimentConfig, output_queue: queue.Queue, stop_event: threading.Event):
        super().__init__(name="Producer")
        self.config = config
        self.output_queue = output_queue
        self.stop_event = stop_event
        self.total_produced = 0 # 统计生产总数
        
    def run(self):
        print(f"🚀 [{self.name}] 启动，生产速率: {self.config.producer_rate} 条/秒")
        base_delay = 1.0 / self.config.producer_rate
        
        while not self.stop_event.is_set():
            try:
                # 1. 生成事件
                event = generate_event()
                
                # 2. 放入队列 (如果队列满了，这里会阻塞，这就是天然的背压)
                self.output_queue.put(event, block=True, timeout=0.1)
                self.total_produced += 1
                
                # 3. 控制生产速率
                time.sleep(base_delay)
                
            except queue.Full:
                continue # 队列满了，重试
            except Exception as e:
                print(f"❌ [{self.name}] 错误: {e}")
        
        print(f"🏁 [{self.name}] 停止，共生产: {self.total_produced} 条")

# ==================== 4. 消费者线程 (Consumer) ====================
class ConsumerThread(threading.Thread):
    def __init__(self, consumer_id: int, config: ExperimentConfig, input_queue: queue.Queue, stop_event: threading.Event):
        super().__init__(name=f"Consumer-{consumer_id}")
        self.consumer_id = consumer_id
        self.config = config
        self.input_queue = input_queue
        self.stop_event = stop_event
        self.total_consumed = 0 # 统计消费总数
        
    def run(self):
        print(f"👷 [{self.name}] 启动，单条处理耗时: {self.config.consumer_process_time} 秒")
        
        while not self.stop_event.is_set() or not self.input_queue.empty():
            try:
                # 1. 从队列拿数据 (timeout避免一直等导致无法退出)
                event = self.input_queue.get(block=True, timeout=0.1)
                
                # 2. 模拟处理耗时 (核心！这里模拟业务逻辑)
                time.sleep(self.config.consumer_process_time)
                
                # 3. 标记任务完成 (非常重要！queue.join()需要这个)
                self.input_queue.task_done()
                self.total_consumed += 1
                
            except queue.Empty:
                continue # 队列空了，继续检查是否要停止
            except Exception as e:
                print(f"❌ [{self.name}] 错误: {e}")
        
        print(f"🏁 [{self.name}] 停止，共消费: {self.total_consumed} 条")
    
# ==================== 5. 监控线程 (Metrics Collector) ====================
class MonitorThread(threading.Thread):
    def __init__(self, config: ExperimentConfig, target_queue: queue.Queue, stop_event: threading.Event):
        super().__init__(name="Monitor", daemon=True) # daemon=True，主线程结束它自动结束
        self.config = config
        self.target_queue = target_queue
        self.stop_event = stop_event
        self.start_time = time.time()
        self.csv_writer = None
        self.csv_file = None
        
    def _init_csv(self):
        """初始化CSV文件，写入表头"""
        file_exists = self.config.metrics_output_path.exists()
        self.csv_file = open(self.config.metrics_output_path, 'a', newline='', encoding='utf-8')
        fieldnames = [
            'timestamp', 'elapsed_sec', 'queue_depth', 'max_capacity', 
            'load_pct', 'producer_rate', 'consumer_time', 'n_consumers', 'backpressure_on'
        ]
        self.csv_writer = csv.DictWriter(self.csv_file, fieldnames=fieldnames)
        if not file_exists:
            self.csv_writer.writeheader()
            
    def run(self):
        print(f"📊 [{self.name}] 启动，采样间隔: {self.config.monitor_interval} 秒")
        self._init_csv()
        
        while not self.stop_event.is_set():
            try:
                # 1. 采集指标
                now = time.time()
                elapsed = now - self.start_time
                q_depth = self.target_queue.qsize()
                q_max = self.config.queue_max_size if self.config.queue_max_size > 0 else -1
                load_pct = q_depth / q_max if q_max > 0 else 0.0
                
                # 2. 写入CSV
                row = {
                    'timestamp': datetime.now().isoformat(),
                    'elapsed_sec': round(elapsed, 2),
                    'queue_depth': q_depth,
                    'max_capacity': q_max,
                    'load_pct': round(load_pct, 4),
                    'producer_rate': self.config.producer_rate,
                    'consumer_time': self.config.consumer_process_time,
                    'n_consumers': self.config.n_consumers,
                    'backpressure_on': self.config.enable_backpressure
                }
                self.csv_writer.writerow(row)
                self.csv_file.flush() # 立即写入磁盘
                
                # 3. 打印实时状态 (可选，方便观察)
                print(f"\r📊 队列深度: {q_depth} | 已运行: {elapsed:.1f}秒", end="")
                
                # 4. 等待下一次采样
                time.sleep(self.config.monitor_interval)
                
            except Exception as e:
                print(f"\n❌ [{self.name}] 错误: {e}")
        
        if self.csv_file:
            self.csv_file.close()
        print(f"\n🏁 [{self.name}] 停止，数据已写入 {self.config.metrics_output_path}")

# ==================== 6. 主实验运行框架 ====================
def run_experiment(config: ExperimentConfig):
    """运行一组完整的实验"""
    print("=" * 60)
    print("🧪 开始实验")
    print(f"   参数: λ={config.producer_rate}, t={config.consumer_process_time}, n={config.n_consumers}")
    print("=" * 60)
    
    # 1. 初始化队列
    if config.queue_max_size > 0:
        q = queue.Queue(maxsize=config.queue_max_size)
    else:
        q = queue.Queue() # 无限队列
    
    # 2. 停止事件 (用于优雅停止所有线程)
    stop_event = threading.Event()
    
    # 3. 创建并启动监控线程
    monitor = MonitorThread(config, q, stop_event)
    monitor.start()
    
    # 4. 创建并启动消费者线程
    consumers = []
    for i in range(config.n_consumers):
        consumer = ConsumerThread(i+1, config, q, stop_event)
        consumers.append(consumer)
        consumer.start()
    
    # 5. 短暂休眠，确保消费者先启动
    time.sleep(0.5)
    
    # 6. 创建并启动生产者线程
    producer = ProducerThread(config, q, stop_event)
    producer.start()
    
    # 7. 主线程等待实验时长
    try:
        time.sleep(config.experiment_duration)
    except KeyboardInterrupt:
        print("\n⏹️  用户手动停止实验")
    
    # 8. 停止所有线程
    print("\n" + "=" * 60)
    print("🛑 正在停止实验...")
    stop_event.set() # 发送停止信号
    
    # 9. 等待生产者结束
    producer.join()
    
    # 10. 等待队列里的剩余数据消费完 (可选，或者不等直接停止)
    # q.join() 
    
    # 11. 等待消费者结束
    for consumer in consumers:
        consumer.join(timeout=2.0) # 最多等2秒
    
    # 12. 等待监控结束
    monitor.join(timeout=1.0)
    
    # 13. 打印最终统计
    print("=" * 60)
    print("📈 实验结束统计")
    print(f"   生产总数: {producer.total_produced}")
    total_consumed = sum(c.total_consumed for c in consumers)
    print(f"   消费总数: {total_consumed}")
    print(f"   最终队列深度: {q.qsize()}")
    print("=" * 60)

# ==================== 7. 程序入口 ====================
if __name__ == "__main__":
    # 测试运行：使用 A1 组参数先试试
    # A1: λ=10, t=0.2, n=1
    test_config = ExperimentConfig(
        producer_rate=10.0,
        consumer_process_time=0.2,
        n_consumers=1,
        queue_max_size=-1, # 无限队列
        experiment_duration=15.0,
        enable_backpressure=False
    )
    
    # 清理旧的CSV文件 
    if test_config.metrics_output_path.exists():
        test_config.metrics_output_path.unlink()
    
    run_experiment(test_config)