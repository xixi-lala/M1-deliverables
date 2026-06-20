# -*- coding: utf-8 -*-
"""
实验六 任务2：背压机制实现与对比实验
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

# ==================== 1. 实验配置类（新增背压参数） ====================
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
    queue_max_size: int = 100             
    # 实验总运行时长 (秒)
    experiment_duration: float = 20.0      
    # 监控采样间隔 (秒)
    monitor_interval: float = 0.5          
    # 是否启用背压
    enable_backpressure: bool = True       
    
    # 背压参数
    backpressure_high_threshold: float = 0.85  # 高水位线：85%触发背压
    backpressure_low_threshold: float = 0.30   # 低水位线：30%解除背压
    base_delay: float = 0.01                    # 基础发送间隔
    max_delay: float = 2.0                      # 最大退避间隔
    
    # 输出文件路径
    metrics_output_path: Path = Path("backpressure_metrics.csv")

# ==================== 2. 数据生成逻辑 (复用) ====================
USER_ID_RANGE = (1, 100_000)
ITEM_ID_RANGE = (1, 50_000)
BEHAVIOR_TYPES = ["view", "cart", "purchase"]
BEHAVIOR_WEIGHTS = [76, 19, 5]

def generate_event() -> dict:
    """生成单条电商行为日志"""
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

# ==================== 3. 生产者线程（新增背压指数退避逻辑） ====================
class ProducerThread(threading.Thread):
    def __init__(self, config: ExperimentConfig, output_queue: queue.Queue, 
                 stop_event: threading.Event, backpressure_state: threading.Event):
        super().__init__(name="Producer")
        self.config = config
        self.output_queue = output_queue
        self.stop_event = stop_event
        self.backpressure_state = backpressure_state  # 背压状态共享
        self.total_produced = 0
        
    def run(self):
        print(f"🚀 [{self.name}] 启动，生产速率: {self.config.producer_rate} 条/秒")
        base_delay = 1.0 / self.config.producer_rate
        current_delay = base_delay
        
        while not self.stop_event.is_set():
            try:
                # 1. 背压控制：指数退避
                if self.config.enable_backpressure:
                    if self.backpressure_state.is_set():
                        # 背压激活：发送间隔逐轮翻倍
                        current_delay = min(current_delay * 2, self.config.max_delay)
                        print(f"\r⚠️  [背压激活] 生产间隔调整为: {current_delay:.3f}秒", end="")
                    else:
                        # 背压解除：逐步恢复原速
                        current_delay = max(current_delay / 2, base_delay)
                
                # 2. 生成事件
                event = generate_event()
                
                # 3. 放入队列 (如果队列满了，这里会阻塞，这是天然的背压)
                self.output_queue.put(event, block=True, timeout=0.1)
                self.total_produced += 1
                
                # 4. 控制生产速率
                time.sleep(current_delay)
                
            except queue.Full:
                continue
            except Exception as e:
                print(f"\n❌ [{self.name}] 错误: {e}")
        
        print(f"\n🏁 [{self.name}] 停止，共生产: {self.total_produced} 条")

# ==================== 4. 消费者线程 (复用) ====================
class ConsumerThread(threading.Thread):
    def __init__(self, consumer_id: int, config: ExperimentConfig, input_queue: queue.Queue, stop_event: threading.Event):
        super().__init__(name=f"Consumer-{consumer_id}")
        self.consumer_id = consumer_id
        self.config = config
        self.input_queue = input_queue
        self.stop_event = stop_event
        self.total_consumed = 0
        
    def run(self):
        print(f"👷 [{self.name}] 启动，单条处理耗时: {self.config.consumer_process_time} 秒")
        
        while not self.stop_event.is_set() or not self.input_queue.empty():
            try:
                event = self.input_queue.get(block=True, timeout=0.1)
                time.sleep(self.config.consumer_process_time)
                self.input_queue.task_done()
                self.total_consumed += 1
            except queue.Empty:
                continue
            except Exception as e:
                print(f"\n❌ [{self.name}] 错误: {e}")
        
        print(f"\n🏁 [{self.name}] 停止，共消费: {self.total_consumed} 条")

# ==================== 5. 监控线程（新增水位线探针） ====================
class MonitorThread(threading.Thread):
    def __init__(self, config: ExperimentConfig, target_queue: queue.Queue, 
                 stop_event: threading.Event, backpressure_state: threading.Event):
        super().__init__(name="Monitor", daemon=True)
        self.config = config
        self.target_queue = target_queue
        self.stop_event = stop_event
        self.backpressure_state = backpressure_state
        self.start_time = time.time()
        self.csv_writer = None
        self.csv_file = None
        
    def _init_csv(self):
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
                
                # 2. 水位线探针与背压控制
                if self.config.enable_backpressure and q_max > 0:
                    if load_pct >= self.config.backpressure_high_threshold:
                        if not self.backpressure_state.is_set():
                            self.backpressure_state.set()
                            print(f"\n⚠️  触发背压: 队列载荷 {load_pct*100:.1f}% >= {self.config.backpressure_high_threshold*100:.0f}%")
                    elif load_pct <= self.config.backpressure_low_threshold:
                        if self.backpressure_state.is_set():
                            self.backpressure_state.clear()
                            print(f"\n✅ 压力缓解: 队列载荷 {load_pct*100:.1f}% <= {self.config.backpressure_low_threshold*100:.0f}%")
                
                # 3. 写入CSV
                row = {
                    'timestamp': datetime.now().isoformat(),
                    'elapsed_sec': round(elapsed, 2),
                    'queue_depth': q_depth,
                    'max_capacity': q_max,
                    'load_pct': round(load_pct, 4),
                    'producer_rate': self.config.producer_rate,
                    'consumer_time': self.config.consumer_process_time,
                    'n_consumers': self.config.n_consumers,
                    'backpressure_on': self.backpressure_state.is_set()
                }
                self.csv_writer.writerow(row)
                self.csv_file.flush()
                
                # 4. 打印实时状态
                bp_status = "🔴背压中" if self.backpressure_state.is_set() else "🟢正常"
                print(f"\r📊 队列: {q_depth}/{q_max} ({load_pct*100:.1f}%) | {bp_status} | 已运行: {elapsed:.1f}秒", end="")
                
                time.sleep(self.config.monitor_interval)
                
            except Exception as e:
                print(f"\n❌ [{self.name}] 错误: {e}")
        
        if self.csv_file:
            self.csv_file.close()
        print(f"\n🏁 [{self.name}] 停止，数据已写入 {self.config.metrics_output_path}")

# ==================== 6. 主实验运行框架 ====================
def run_experiment(config: ExperimentConfig, exp_label: str = ""):
    """运行一组完整的实验"""
    print("\n" + "=" * 60)
    print(f"🧪 开始实验 {exp_label}")
    print(f"   参数: λ={config.producer_rate}, t={config.consumer_process_time}, n={config.n_consumers}")
    print(f"   背压: {'启用' if config.enable_backpressure else '禁用'} | 队列容量: {config.queue_max_size}")
    print("=" * 60)
    
    # 1. 初始化队列
    if config.queue_max_size > 0:
        q = queue.Queue(maxsize=config.queue_max_size)
    else:
        q = queue.Queue()
    
    # 2. 停止事件和背压状态
    stop_event = threading.Event()
    backpressure_state = threading.Event()
    
    # 3. 创建并启动监控线程
    monitor = MonitorThread(config, q, stop_event, backpressure_state)
    monitor.start()
    
    # 4. 创建并启动消费者线程
    consumers = []
    for i in range(config.n_consumers):
        consumer = ConsumerThread(i+1, config, q, stop_event)
        consumers.append(consumer)
        consumer.start()
    
    time.sleep(0.5)
    
    # 5. 创建并启动生产者线程
    producer = ProducerThread(config, q, stop_event, backpressure_state)
    producer.start()
    
    # 6. 主线程等待实验时长
    try:
        time.sleep(config.experiment_duration)
    except KeyboardInterrupt:
        print("\n⏹️  用户手动停止实验")
    
    # 7. 停止所有线程
    print("\n" + "=" * 60)
    print("🛑 正在停止实验...")
    stop_event.set()
    backpressure_state.clear()
    
    producer.join()
    for consumer in consumers:
        consumer.join(timeout=2.0)
    monitor.join(timeout=1.0)
    
    # 8. 打印最终统计
    print("=" * 60)
    print("📈 实验结束统计")
    print(f"   生产总数: {producer.total_produced}")
    total_consumed = sum(c.total_consumed for c in consumers)
    print(f"   消费总数: {total_consumed}")
    print(f"   最终队列深度: {q.qsize()}")
    print("=" * 60)

# ==================== 7. 程序入口：背压对比实验 ====================
if __name__ == "__main__":
    # 选择A1组参数（之前溢出的组）做背压对比
    target_lambda = 10.0
    target_t = 0.2
    target_n = 1
    
    # 清理旧文件
    bp_file = Path("backpressure_metrics.csv")
    if bp_file.exists():
        bp_file.unlink()
    
    # 实验1：启用背压
    print("\n" + "#" * 60)
    print("# 第一组：启用背压 (有界队列 + 指数退避)")
    print("#" * 60)
    config_with_bp = ExperimentConfig(
        producer_rate=target_lambda,
        consumer_process_time=target_t,
        n_consumers=target_n,
        queue_max_size=100,
        enable_backpressure=True,
        experiment_duration=40.0,  # 延长到40秒
        metrics_output_path=bp_file
    )
    run_experiment(config_with_bp, exp_label="(启用背压)")
    
    print("\n\n⏳ 等待5秒后进行第二组实验...")
    time.sleep(5)
    
    # 实验2：禁用背压（无限队列，作为对照）
    print("\n" + "#" * 60)
    print("# 第二组：禁用背压 (无限队列，对照)")
    print("#" * 60)
    config_no_bp = ExperimentConfig(
        producer_rate=target_lambda,
        consumer_process_time=target_t,
        n_consumers=target_n,
        queue_max_size=-1,
        enable_backpressure=False,
        experiment_duration=40.0,  # 同样40秒
        metrics_output_path=bp_file
    )
    run_experiment(config_no_bp, exp_label="(禁用背压)")
    
    print("\n\n🎉 背压对比实验完成！")
    print(f"📊 数据已保存至: {bp_file.absolute()}")