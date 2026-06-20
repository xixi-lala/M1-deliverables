"""
电商行为日志数据模拟器 (Producer)
每秒生成 10-50 条 JSON 格式的电商行为日志，追加写入 streaming_logs.jsonl
"""

import json
import uuid
import time
import random
import numpy as np
from datetime import datetime, timezone, timedelta
from pathlib import Path

# 配置参数
OUTPUT_FILE = "streaming_logs.jsonl"
USER_ID_RANGE = (1, 100_000)
ITEM_ID_RANGE = (1, 50_000)
SESSION_TIMEOUT_MINUTES = 30

# 爆款商品配置：前 50 个商品占 80% 流量，其余 49950 个商品共享 20%
HOT_ITEM_COUNT = 50
ZIPF_ALPHA = 1.5  # Zipf 分布参数，越大越集中
# 预计算爆款商品权重分布（内部归一化到 1，由 generate_item_id 控制 80/20 分配）
_hot_items = np.arange(1, HOT_ITEM_COUNT + 1)
_hot_weights = _hot_items ** (-ZIPF_ALPHA)
_hot_weights = _hot_weights / _hot_weights.sum()

_cold_items = np.arange(HOT_ITEM_COUNT + 1, ITEM_ID_RANGE[1] + 1)
_cold_weights = np.ones(len(_cold_items)) / len(_cold_items)

# 行为类型及其概率权重 (view: 70%, cart: 20%, purchase: 10%)
BEHAVIOR_WEIGHTS = {
    "view": 76,
    "cart": 19,
    "purchase": 5,
}
BEHAVIOR_TYPES = list(BEHAVIOR_WEIGHTS.keys())
BEHAVIOR_PROBABILITIES = list(BEHAVIOR_WEIGHTS.values())

# 会话跟踪 {user_id: {"session_id": str, "last_active": datetime}}
session_tracker = {}


def generate_session_id(user_id: int) -> str:
    """生成或复用 session_id，超时 30 分钟重置"""
    now = datetime.now(timezone.utc)
    
    if user_id in session_tracker:
        last_active = session_tracker[user_id]["last_active"]
        elapsed = (now - last_active).total_seconds() / 60
        
        if elapsed < SESSION_TIMEOUT_MINUTES:
            # 会话未超时，复用
            session_tracker[user_id]["last_active"] = now
            return session_tracker[user_id]["session_id"]
        else:
            # 会话超时，重置
            seq = int(time.time() * 1000) % 100000
            new_session_id = f"{user_id}_{seq}"
            session_tracker[user_id] = {
                "session_id": new_session_id,
                "last_active": now,
            }
            return new_session_id
    
    # 新会话
    seq = int(time.time() * 1000) % 100000
    session_id = f"{user_id}_{seq}"
    session_tracker[user_id] = {
        "session_id": session_id,
        "last_active": now,
    }
    return session_id


def generate_item_id() -> int:
    """使用 Zipf 分布生成偏向爆款商品的 item_id"""
    if random.random() < 0.8:
        # 80% 概率选择爆款商品
        return int(np.random.choice(_hot_items, p=_hot_weights))
    else:
        # 20% 概率选择冷门商品
        return int(np.random.choice(_cold_items, p=_cold_weights))


def generate_event() -> dict:
    """生成单条电商行为日志"""
    event_id = str(uuid.uuid4())
    event_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%z")
    # 修正时区格式 (Python 的 %z 输出 +0000，需要转为 +00:00)
    event_time = event_time[:-2] + ":" + event_time[-2:]
    
    user_id = random.randint(*USER_ID_RANGE)
    item_id = generate_item_id()
    behavior_type = random.choices(BEHAVIOR_TYPES, BEHAVIOR_PROBABILITIES, k=1)[0]
    session_id = generate_session_id(user_id)
    
    return {
        "event_id": event_id,
        "event_time": event_time,
        "user_id": user_id,
        "item_id": item_id,
        "behavior_type": behavior_type,
        "session_id": session_id,
    }


def main():
    # 定义输出文件路径，使用Path对象处理路径
    output_path = Path(OUTPUT_FILE)
    # 打印程序启动信息
    print(f"🚀 数据模拟器已启动")
    print(f"📁 输出文件: {output_path.absolute()}")
    print(f"⚙️  每秒生成 10-50 条日志")
    print(f"📊 行为权重: {BEHAVIOR_WEIGHTS}")
    print(f"⏸️  按 Ctrl+C 优雅停止\n")
    
    # 初始化事件计数器
    total_events = 0
    try:
        # 以追加模式打开文件，使用utf-8编码
        with open(output_path, "a", encoding="utf-8") as f:
            # 无限循环生成事件
            while True:
                # 随机生成 10-50 条
                num_events = random.randint(10, 50)
                
                for _ in range(num_events):
                    event = generate_event()
                    f.write(json.dumps(event, ensure_ascii=False) + "\n")
                    total_events += 1
                
                f.flush()  # 确保数据写入磁盘
                print(f"✅ 已生成 {num_events} 条 | 累计: {total_events} 条", end="\r")
                
                time.sleep(1)
                
    except KeyboardInterrupt:
        print(f"\n\n🛑 收到停止信号，正在清理...")
        print(f"📊 总共生成 {total_events} 条日志")
        print(f"💾 数据已保存至: {output_path.absolute()}")
    except Exception as e:
        print(f"\n❌ 发生异常: {e}")
        raise
    finally:
        print("👋 数据模拟器已退出")


if __name__ == "__main__":
    main()
