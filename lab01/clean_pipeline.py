"""
数据清洗与脱敏流水线
使用 Polars Lazy API 流式处理 large_data.csv，完成数据清洗、脱敏和格式转换
"""

import polars as pl
import hashlib
import time
import os

# 配置
INPUT_FILE = r"D:\MyProjects\DataAnylyse\lab01\large_data.csv"
OUTPUT_FILE = r"D:\MyProjects\DataAnylyse\lab01\clean_data.parquet"

# 定义 event_type 拼写错误映射字典
EVENT_TYPE_FIX_MAP = {
    # click 的拼写错误
    "clik": "click",
    "clic": "click",
    "clcik": "click",
    "clikc": "click",
    # login 的拼写错误
    "loign": "login",
    "logn": "login",
    "logi": "login",
    "lgoin": "login",
    # logout 的拼写错误
    "logut": "logout",
    "lgout": "logout",
    "logot": "logout",
    "loout": "logout",
    # payment 的拼写错误
    "paymet": "payment",
    "payent": "payment",
    "pymt": "payment",
    "paymant": "payment",
}


def sha256_hash(value: str) -> str:
    """使用 SHA256 对字符串进行哈希"""
    if value is None or value == "":
        return ""
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


def main():
    print("=" * 60)
    print("数据清洗与脱敏流水线")
    print("=" * 60)
    
    # 记录开始时间
    start_time = time.time()
    
    # -------------------------------------------------------------------------
    # 步骤 1: 使用 Lazy API 流式读取 CSV
    # -------------------------------------------------------------------------
    print("\n[1/6] 流式读取 CSV 文件...")
    lf = pl.scan_csv(INPUT_FILE)
    
    # -------------------------------------------------------------------------
    # 步骤 2: 过滤脏数据 - 剔除 event_id 为空字符串的行
    # -------------------------------------------------------------------------
    print("[2/6] 过滤 event_id 为空的异常行...")
    lf = lf.filter(pl.col('event_id').str.strip_chars() != "")
    
    # -------------------------------------------------------------------------
    # 步骤 3: 字段修复 - 标准化 event_type 拼写错误
    # -------------------------------------------------------------------------
    print("[3/6] 修复 event_type 拼写错误...")
    
    # 使用 replace 方法批量替换拼写错误
    # Polars 的 replace 接受一个字典，将键替换为值
    lf = lf.with_columns(
        pl.col('event_type').replace(EVENT_TYPE_FIX_MAP).alias('event_type')
    )
    
    # -------------------------------------------------------------------------
    # 步骤 4: 隐私脱敏 - 对 user_id 进行 SHA256 哈希
    # -------------------------------------------------------------------------
    print("[4/6] 对 user_id 进行 SHA256 哈希脱敏...")
    
    # 由于 Polars 没有内置 SHA256 函数，需要使用 map_elements 调用 Python 函数
    # 注意：map_elements 会触发一定程度的计算，但仍在 lazy 框架内
    lf = lf.with_columns(
        pl.col('user_id')
        .map_elements(sha256_hash, return_dtype=pl.String)
        .alias('masked_user_id')
    )
    
    # 删除原始的 user_id 明文列
    lf = lf.drop('user_id')
    
    # 调整列顺序，将 masked_user_id 放在原来 user_id 的位置
    lf = lf.select([
        'event_id',
        'masked_user_id',
        'action_time',
        'event_type',
        'device_info',
        'metadata'
    ])
    
    # -------------------------------------------------------------------------
    # 步骤 5: 执行收集并保存为 Parquet（带压缩）
    # -------------------------------------------------------------------------
    print("[5/6] 执行流式计算并保存为 Parquet 格式...")
    
    # 收集数据并保存为 Parquet，使用 snappy 压缩（高效且快速）
    # 使用 engine='streaming' 启用流式执行引擎
    df = lf.collect(engine='streaming')
    df.write_parquet(
        OUTPUT_FILE,
        compression='snappy'  # 高效压缩算法
    )
    
    # 记录结束时间
    end_time = time.time()
    total_duration = end_time - start_time
    
    # -------------------------------------------------------------------------
    # 步骤 6: 计算文件体积和压缩比
    # -------------------------------------------------------------------------
    print("[6/6] 计算文件体积对比...")
    
    csv_size = os.path.getsize(INPUT_FILE)
    parquet_size = os.path.getsize(OUTPUT_FILE)
    
    csv_size_mb = csv_size / (1024 * 1024)
    parquet_size_mb = parquet_size / (1024 * 1024)
    compression_ratio = csv_size / parquet_size if parquet_size > 0 else 0
    
    # -------------------------------------------------------------------------
    # 输出结果摘要
    # -------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("处理结果摘要")
    print("=" * 60)
    
    print(f"""
【性能指标】
  - 全流程总耗时：{total_duration:.3f} 秒
  - 处理模式：Polars Lazy API 流式处理

【文件体积对比】
  - 原始文件 (CSV):     {csv_size_mb:>10.3f} MB
  - 输出文件 (Parquet):  {parquet_size_mb:>10.3f} MB
  - 压缩比：{compression_ratio:>10.3f}x
  - 空间节省：{(1 - parquet_size/csv_size) * 100:>10.2f}%

【数据质量】
  - 输入文件：{INPUT_FILE}
  - 输出文件：{OUTPUT_FILE}
  - 脱敏字段：user_id → masked_user_id (SHA256)
  - 修复的 event_type 拼写错误：{len(EVENT_TYPE_FIX_MAP)} 种

【拼写错误修复映射】
""")
    
    # 按类别分组显示修复映射
    categories = {
        "click": ["clik", "clic", "clcik", "clikc"],
        "login": ["loign", "logn", "logi", "lgoin"],
        "logout": ["logut", "lgout", "logot", "loout"],
        "payment": ["paymet", "payent", "pymt", "paymant"]
    }
    
    for correct, errors in categories.items():
        errors_found = [e for e in errors if e in EVENT_TYPE_FIX_MAP]
        if errors_found:
            print(f"  - {correct} ← {', '.join(errors_found)}")
    
    print("\n" + "=" * 60)
    print("数据清洗与脱敏完成！")
    print("=" * 60)


if __name__ == "__main__":
    main()
