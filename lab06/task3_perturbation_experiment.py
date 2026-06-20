# -*- coding: utf-8 -*-
"""
实验六 任务3：流量扰动实验（均匀抖动 + 周期性突发）
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

# ==================== 1. 实验配置（新增扰动参数） ====================
@dataclass
class ExperimentConfig:
    producer_rate: float = 4.0          # 基线生产速率
    consumer_process_time: float = 0.2   # 单条处理耗时
    n_consumers: int = 1                 # 消费者数
    queue_max_size: int = 500            # 队列容量
    experiment_duration: float = 30.0    # 实验时长
    monitor_interval: float = 0.5
    
    # 扰动模型参数
    perturbation_mode: str = "none"      # none/jitter/burst
    jitter_factor: float = 0.9           # 抖动幅度
    burst_interval: float = 4.0          # 突发周期
    burst_duration: float = 1.0          # 突发时长
    burst_multiplier: float = 2.0        # 突发倍率
    
    metrics_output_path: Path = Path("perturbation_metrics.csv")

# ==================== 2. 数据生成 ====================
def generate_event():
    return {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "user_id": random.randint(1, 100000),
        "item_id": random.randint(1, 50000),
        "behavior_type": random.choice(["view","cart","purchase"])
    }

# ==================== 3. 生产者（支持扰动） ====================
class ProducerThread(threading.Thread):
    def __init__(self, config, output_queue, stop_event):
        super().__init__(name="Producer")
        self.config = config
        self.output_queue = output_queue
        self.stop_event = stop_event
        self.total_produced = 0

    def run(self):
        base_delay = 1.0 / self.config.producer_rate
        start_time = time.time()
        while not self.stop_event.is_set():
            try:
                # 【扰动模型A：均匀随机抖动】
                if self.config.perturbation_mode == "jitter":
                    delay = base_delay * random.uniform(1-self.config.jitter_factor, 1+self.config.jitter_factor)
                # 【扰动模型B：周期性突发脉冲】
                elif self.config.perturbation_mode in ["burst_mild", "burst_heavy"]:
                    elapsed = time.time() - start_time
                    cycle_pos = elapsed % self.config.burst_interval
                    in_burst = cycle_pos < self.config.burst_duration
                    current_rate = self.config.producer_rate * (self.config.burst_multiplier if in_burst else 1.0)
                    delay = 1.0 / current_rate
                # 无扰动
                else:
                    delay = base_delay

                event = generate_event()
                self.output_queue.put(event, block=True, timeout=0.1)
                self.total_produced += 1
                time.sleep(delay)
            except queue.Full:
                continue
            except Exception as e:
                print(f"生产者错误：{e}")
        print(f"生产者停止，总产量：{self.total_produced}")

# ==================== 4. 消费者 ====================
class ConsumerThread(threading.Thread):
    def __init__(self, cid, config, input_queue, stop_event):
        super().__init__(name=f"Consumer-{cid}")
        self.config = config
        self.input_queue = input_queue
        self.stop_event = stop_event
        self.total_consumed = 0

    def run(self):
        while not self.stop_event.is_set() or not self.input_queue.empty():
            try:
                self.input_queue.get(block=True, timeout=0.1)
                time.sleep(self.config.consumer_process_time)
                self.input_queue.task_done()
                self.total_consumed += 1
            except queue.Empty:
                continue
        print(f"{self.name}停止，总消费：{self.total_consumed}")

# ==================== 5. 监控线程 ====================
class MonitorThread(threading.Thread):
    def __init__(self, config, target_queue, stop_event):
        super().__init__(name="Monitor", daemon=True)
        self.config = config
        self.target_queue = target_queue
        self.stop_event = stop_event
        self.start_time = time.time()
        self.writer = None

    def _init_csv(self):
        exists = self.config.metrics_output_path.exists()
        self.f = open(self.config.metrics_output_path, 'a', newline='', encoding='utf-8')
        fieldnames = ["timestamp","elapsed_sec","queue_depth","perturbation_mode"]
        self.writer = csv.DictWriter(self.f, fieldnames=fieldnames)
        if not exists: self.writer.writeheader()

    def run(self):
        self._init_csv()
        while not self.stop_event.is_set():
            now = time.time()
            elapsed = round(now - self.start_time, 2)
            depth = self.target_queue.qsize()
            self.writer.writerow({
                "timestamp": datetime.now().isoformat(),
                "elapsed_sec": elapsed,
                "queue_depth": depth,
                "perturbation_mode": self.config.perturbation_mode
            })
            self.f.flush()
            print(f"\r队列深度：{depth} | 扰动：{self.config.perturbation_mode}", end="")
            time.sleep(self.config.monitor_interval)
        self.f.close()

# ==================== 6. 实验运行 ====================
def run_perturbation_exp(mode, **kwargs):
    cfg = ExperimentConfig(
        producer_rate=4.0,
        consumer_process_time=0.2,
        n_consumers=1,
        perturbation_mode=mode,
        **kwargs
    )
    # 清理
    if Path(cfg.metrics_output_path).exists() and mode == "none":
        Path(cfg.metrics_output_path).unlink()
    
    q = queue.Queue(maxsize=cfg.queue_max_size)
    stop_event = threading.Event()
    monitor = MonitorThread(cfg, q, stop_event)
    consumers = [ConsumerThread(i, cfg, q, stop_event) for i in range(cfg.n_consumers)]
    producer = ProducerThread(cfg, q, stop_event)

    print(f"\n=== 启动实验：扰动={mode} ===")
    monitor.start()
    for c in consumers: c.start()
    time.sleep(0.5)
    producer.start()
    time.sleep(cfg.experiment_duration)
    
    stop_event.set()
    producer.join()
    for c in consumers: c.join(timeout=2)
    monitor.join(timeout=1)
    print(f"\n=== 实验结束：扰动={mode} ===")

# ==================== 7. 运行4组实验 ====================
if __name__ == "__main__":
    print("任务3：流量扰动实验（4组）")
    # 1. 基线（无扰动）
    run_perturbation_exp("none")
    time.sleep(2)
    # 2. 均匀随机抖动
    run_perturbation_exp("jitter", jitter_factor=0.9)
    time.sleep(2)
    # 3. 温和突发
    run_perturbation_exp("burst_mild", burst_multiplier=2.0, burst_duration=1.0)
    time.sleep(2)
    # 4. 激烈突发
    run_perturbation_exp("burst_heavy", burst_multiplier=4.0, burst_duration=1.5)
    print("\n✅ 全部4组扰动实验完成！")