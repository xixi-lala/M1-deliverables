# -*- coding: utf-8 -*-
"""
生成百万级模拟日志数据的脚本
功能：生成 100 万条包含脏数据的日志记录，保存为 CSV 文件
"""

import subprocess
import sys
import os
import random
import json
import time
from datetime import datetime, timedelta

# ==================== 自动安装依赖 ====================
def install_package(package_name):
    """自动安装指定的 Python 包"""
    try:
        __import__(package_name)
        print(f"✓ {package_name} 已安装")
    except ImportError:
        print(f"正在安装 {package_name}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package_name, "-q"])
        print(f"✓ {package_name} 安装完成")

# 安装所需依赖
install_package("faker")
install_package("polars")
install_package("tqdm")

from faker import Faker
from tqdm import tqdm
import polars as pl

# ==================== 配置参数 ====================
TOTAL_RECORDS = 1_000_000  # 总记录数：100 万
OUTPUT_FILE = "large_data.csv"
OUTPUT_ENCODING = "utf-8"

# 脏数据注入比例
DIRTY_RATIO_EVENT_ID = 0.005      # event_id 空字符串比例
DIRTY_RATIO_USER_ID = 0.01        # user_id 默认值比例
DIRTY_RATIO_TIMESTAMP = 0.01      # 时间戳混合格式比例
DIRTY_RATIO_EVENT_TYPE = 0.02     # event_type 拼写错误比例
DIRTY_RATIO_METADATA = 0.015      # metadata 特殊字符比例

# ==================== 初始化 Faker ====================
fake = Faker("zh_CN")
Faker.seed(42)  # 固定随机种子，保证可复现性

# ==================== 数据生成函数 ====================

def generate_event_id(index, is_dirty=False):
    """
    生成事件 ID（全局唯一请求 ID）
    :param index: 记录索引
    :param is_dirty: 是否注入脏数据（空字符串）
    :return: 事件 ID 字符串
    """
    if is_dirty:
        return ""  # 脏数据：空字符串
    # 生成唯一 ID：时间戳 + 随机字符串
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    random_suffix = f"{index:08d}{random.randint(1000, 9999)}"
    return f"EVT-{timestamp}-{random_suffix}"


def generate_user_id(is_dirty=False):
    """
    生成用户 ID
    :param is_dirty: 是否注入脏数据（默认值）
    :return: 用户 ID（统一为字符串格式，兼容 Polars）
    """
    if is_dirty:
        # 脏数据：返回默认值（统一转为字符串）
        return str(random.choice([-1, "guest", "unknown", "null", ""]))
    # 正常数据：生成随机用户 ID（统一为字符串格式）
    if random.random() < 0.3:
        return f"USER_{random.randint(10000, 99999)}"  # 字符串格式
    else:
        return str(random.randint(1000, 999999))  # 转为字符串格式


def generate_action_time(is_dirty=False):
    """
    生成事件时间戳
    :param is_dirty: 是否注入脏数据（混合格式）
    :return: 时间戳字符串（ISO 8601 或 Unix 毫秒）
    """
    # 生成 2025-2026 年之间的随机时间
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2026, 12, 31)
    delta = end_date - start_date
    random_seconds = random.randint(0, int(delta.total_seconds()))
    random_time = start_date + timedelta(seconds=random_seconds)
    
    if is_dirty:
        # 脏数据：混合格式，部分用 Unix 毫秒时间戳
        if random.random() < 0.5:
            return str(int(random_time.timestamp() * 1000))  # Unix 毫秒
        else:
            return random_time.strftime("%Y/%m/%d %H:%M:%S")  # 另一种格式
    else:
        # 正常数据：ISO 8601 格式
        return random_time.isoformat()


def generate_event_type(is_dirty=False):
    """
    生成事件类型
    :param is_dirty: 是否注入脏数据（拼写错误）
    :return: 事件类型字符串
    """
    # 正常事件类型
    normal_events = ["login", "click", "payment", "logout"]
    
    if is_dirty:
        # 脏数据：拼写错误
        typos = {
            "login": ["logn", "loign", "lgoin", "logi"],
            "click": ["clik", "clcik", "clikc", "clic"],
            "payment": ["paymet", "paymant", "payent", "pymt"],
            "logout": ["logut", "logot", "lgout", "loout"]
        }
        event = random.choice(normal_events)
        return random.choice(typos[event])
    else:
        return random.choice(normal_events)


def generate_device_info():
    """
    生成设备指纹（JSON 字符串）
    :return: 包含多个键值对的 JSON 字符串
    """
    device_info = {
        "device_id": fake.uuid4(),
        "device_type": random.choice(["mobile", "desktop", "tablet", "smart_tv"]),
        "os": random.choice(["Windows", "macOS", "Linux", "iOS", "Android"]),
        "os_version": f"{random.randint(10, 14)}.{random.randint(0, 9)}.{random.randint(0, 9)}",
        "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge", "Opera"]),
        "browser_version": f"{random.randint(80, 120)}.{random.randint(0, 9)}.{random.randint(0, 999)}",
        "screen_resolution": f"{random.choice([1920, 1366, 2560, 3840])}x{random.choice([1080, 768, 1440, 2160])}",
        "language": random.choice(["zh-CN", "en-US", "ja-JP", "ko-KR", "de-DE"]),
        "timezone": random.choice(["UTC+8", "UTC+0", "UTC-5", "UTC+9", "UTC+1"]),
        "ip_address": fake.ipv4(),
        "user_agent": fake.user_agent(),
        "is_bot": random.choice([True, False]),
        "app_version": f"{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 9)}"
    }
    return json.dumps(device_info, ensure_ascii=False)


def generate_metadata(is_dirty=False):
    """
    生成备注文本
    :param is_dirty: 是否注入脏数据（特殊字符）
    :return: 备注文本字符串
    """
    # 基础文本
    base_texts = [
        "用户操作正常",
        "系统处理完成",
        "请求成功响应",
        "数据同步中",
        "缓存已更新",
        "会话已创建",
        "权限验证通过",
        "日志记录成功"
    ]
    text = random.choice(base_texts)
    
    if is_dirty:
        # 脏数据：添加特殊字符
        special_chars = ["\n", "\t", "$", "#", "|", "@", "%", "&", "*", "[]", "{}"]
        # 随机插入特殊字符
        for _ in range(random.randint(1, 3)):
            char = random.choice(special_chars)
            pos = random.randint(0, len(text))
            text = text[:pos] + char + text[pos:]
    
    # 添加一些额外信息
    extra_info = f" [trace_id={fake.uuid4()[:8]}]"
    return text + extra_info


def check_dirty(ratio):
    """判断是否应该注入脏数据"""
    return random.random() < ratio


# ==================== 主生成函数 ====================

def generate_logs():
    """
    生成百万级日志数据
    :return: Polars DataFrame
    """
    print("=" * 60)
    print("开始生成百万级模拟日志数据...")
    print(f"目标记录数：{TOTAL_RECORDS:,}")
    print(f"输出文件：{OUTPUT_FILE}")
    print(f"编码格式：{OUTPUT_ENCODING}")
    print("=" * 60)
    
    start_time = time.time()
    
    # 预分配列表以提高性能
    event_ids = []
    user_ids = []
    action_times = []
    event_types = []
    device_infos = []
    metadatas = []
    
    # 使用 tqdm 显示进度
    for i in tqdm(range(TOTAL_RECORDS), desc="生成日志数据", unit="条"):
        # 判断是否注入脏数据
        is_dirty_event_id = check_dirty(DIRTY_RATIO_EVENT_ID)
        is_dirty_user_id = check_dirty(DIRTY_RATIO_USER_ID)
        is_dirty_timestamp = check_dirty(DIRTY_RATIO_TIMESTAMP)
        is_dirty_event_type = check_dirty(DIRTY_RATIO_EVENT_TYPE)
        is_dirty_metadata = check_dirty(DIRTY_RATIO_METADATA)
        
        # 生成各字段数据
        event_ids.append(generate_event_id(i, is_dirty_event_id))
        user_ids.append(generate_user_id(is_dirty_user_id))
        action_times.append(generate_action_time(is_dirty_timestamp))
        event_types.append(generate_event_type(is_dirty_event_type))
        device_infos.append(generate_device_info())
        metadatas.append(generate_metadata(is_dirty_metadata))
    
    # 使用 Polars 创建 DataFrame（高效处理大数据）
    df = pl.DataFrame({
        "event_id": event_ids,
        "user_id": user_ids,
        "action_time": action_times,
        "event_type": event_types,
        "device_info": device_infos,
        "metadata": metadatas
    })
    
    # 保存为 CSV 文件（Polars 默认使用 UTF-8 编码）
    print("\n正在保存 CSV 文件...")
    df.write_csv(OUTPUT_FILE)
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    # 获取文件大小
    file_size = os.path.getsize(OUTPUT_FILE)
    file_size_mb = file_size / (1024 * 1024)
    
    # 打印统计信息
    print("\n" + "=" * 60)
    print("生成完成！")
    print("=" * 60)
    print(f"✓ 数据条数：{len(df):,} 条")
    print(f"✓ 文件大小：{file_size_mb:.2f} MB ({file_size:,} 字节)")
    print(f"✓ 生成耗时：{elapsed_time:.2f} 秒 ({elapsed_time/60:.2f} 分钟)")
    print(f"✓ 生成速度：{TOTAL_RECORDS/elapsed_time:,.0f} 条/秒")
    print(f"✓ 输出路径：{os.path.abspath(OUTPUT_FILE)}")
    print("=" * 60)
    
    # 打印脏数据统计
    print("\n脏数据统计：")
    print(f"  - event_id 空字符串：{sum(1 for x in event_ids if x == ''):,} 条 ({DIRTY_RATIO_EVENT_ID*100:.1f}%)")
    print(f"  - user_id 默认值：{sum(1 for x in user_ids if x in [-1, 'guest', 'unknown', 'null', '']):,} 条 (约{DIRTY_RATIO_USER_ID*100:.1f}%)")
    print(f"  - action_time 混合格式：约 {DIRTY_RATIO_TIMESTAMP*100:.1f}%")
    print(f"  - event_type 拼写错误：约 {DIRTY_RATIO_EVENT_TYPE*100:.1f}%")
    print(f"  - metadata 特殊字符：约 {DIRTY_RATIO_METADATA*100:.1f}%")
    print("=" * 60)
    
    return df


# ==================== 程序入口 ====================

if __name__ == "__main__":
    try:
        generate_logs()
        print("\n✓ 脚本执行成功！")
    except Exception as e:
        print(f"\n✗ 脚本执行失败：{e}")
        raise
