"""
UserBehavior 数据清洗脚本
"""
import pandas as pd
import numpy as np
from datetime import datetime

# 定义列名
columns = ['user_id', 'item_id', 'category_id', 'behavior_type', 'timestamp']

print("开始读取数据...")
# 分块读取大文件
chunks = []
chunk_size = 500000

for i, chunk in enumerate(pd.read_csv(
    'UserBehavior.csv',
    names=columns,
    header=None,
    chunksize=chunk_size
)):
    print(f"已读取第 {i+1} 块，大小：{len(chunk)}")
    chunks.append(chunk)

df = pd.concat(chunks, ignore_index=True)
print(f"\n数据读取完成！总行数：{len(df):,}")

# ========== 1. 查看基本信息 ==========
print("\n" + "="*50)
print("1. 数据基本信息")
print("="*50)
print(f"数据形状：{df.shape}")
print(f"\n数据类型:\n{df.dtypes}")
print(f"\n前 5 行:\n{df.head()}")

# ========== 2. 检查缺失值 ==========
print("\n" + "="*50)
print("2. 缺失值检查")
print("="*50)
missing = df.isnull().sum()
print(f"\n缺失值统计:\n{missing}")
print(f"缺失值比例:\n{(df.isnull().sum() / len(df) * 100).round(2)}%")

# 删除缺失值（如果有）
if missing.sum() > 0:
    df = df.dropna()
    print(f"\n已删除缺失值，剩余行数：{len(df):,}")

# ========== 3. 检查重复值 ==========
print("\n" + "="*50)
print("3. 重复值检查")
print("="*50)
duplicates = df.duplicated().sum()
print(f"\n完全重复行数：{duplicates:,}")
if duplicates > 0:
    df = df.drop_duplicates()
    print(f"已删除重复值，剩余行数：{len(df):,}")

# ========== 4. 行为类型分析 ==========
print("\n" + "="*50)
print("4. 行为类型分布")
print("="*50)
behavior_dist = df['behavior_type'].value_counts()
print(f"\n{behavior_dist}")
print(f"\n行为类型比例:\n{(df['behavior_type'].value_counts() / len(df) * 100).round(2)}%")

# 检查是否有异常的行为类型
valid_behaviors = {'pv', 'buy', 'cart', 'fav'}
invalid_behaviors = set(df['behavior_type'].unique()) - valid_behaviors
if invalid_behaviors:
    print(f"\n发现异常行为类型：{invalid_behaviors}，将被删除")
    df = df[df['behavior_type'].isin(valid_behaviors)]

# ========== 5. 时间戳处理 ==========
print("\n" + "="*50)
print("5. 时间戳处理")
print("="*50)

# 检查时间戳范围
print(f"\n时间戳范围：{df['timestamp'].min()} - {df['timestamp'].max()}")

# 合理时间范围：2010-01-01 至 2020-01-01
min_valid_ts = datetime(2010, 1, 1).timestamp()
max_valid_ts = datetime(2020, 1, 1).timestamp()

print(f"有效时间戳范围：{min_valid_ts} - {max_valid_ts}")
print(f"时间戳最小值对应日期：{datetime.fromtimestamp(df['timestamp'].min()) if df['timestamp'].min() > 0 else '无效时间戳'}")

# 检查是否有异常时间戳
invalid_time = df[(df['timestamp'] < min_valid_ts) | (df['timestamp'] > max_valid_ts)]
if len(invalid_time) > 0:
    print(f"\n发现 {len(invalid_time):,} 条异常时间戳记录，已删除")
    df = df[(df['timestamp'] >= min_valid_ts) & (df['timestamp'] <= max_valid_ts)]

print(f"清洗后时间戳范围：{df['timestamp'].min()} - {df['timestamp'].max()}")
print(f"对应日期：{datetime.fromtimestamp(df['timestamp'].min())} 至 {datetime.fromtimestamp(df['timestamp'].max())}")

# 转换时间戳为 datetime 格式
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
df['date'] = df['timestamp'].dt.date
df['hour'] = df['timestamp'].dt.hour

# ========== 6. 数值型字段检查 ==========
print("\n" + "="*50)
print("6. 数值型字段检查")
print("="*50)
print(f"\nuser_id 统计:\n{df['user_id'].describe()}")
print(f"\nitem_id 统计:\n{df['item_id'].describe()}")
print(f"\ncategory_id 统计:\n{df['category_id'].describe()}")

# 检查是否有负值或零值
for col in ['user_id', 'item_id', 'category_id']:
    invalid_count = (df[col] <= 0).sum()
    if invalid_count > 0:
        print(f"\n{col} 有 {invalid_count:,} 条非正值记录，已删除")
        df = df[df[col] > 0]

# ========== 7. 保存清洗后的数据 ==========
print("\n" + "="*50)
print("7. 保存清洗后的数据")
print("="*50)

# 保存为 CSV
output_file = 'UserBehavior_cleaned.csv'
df.to_csv(output_file, index=False)
print(f"\n清洗后的数据已保存至：{output_file}")
print(f"最终数据行数：{len(df):,}")
print(f"最终数据列：{df.columns.tolist()}")

# 保存统计信息
with open('cleaning_report.txt', 'w', encoding='utf-8') as f:
    f.write("UserBehavior 数据清洗报告\n")
    f.write("="*50 + "\n\n")
    f.write(f"原始数据行数：{df.shape[0]:,}\n")
    f.write(f"数据列数：{df.shape[1]}\n\n")
    f.write("行为类型分布:\n")
    f.write(f"{df['behavior_type'].value_counts()}\n\n")
    f.write(f"时间范围：{df['timestamp'].min()} 至 {df['timestamp'].max()}\n\n")
    f.write(f"user_id 数量：{df['user_id'].nunique():,}\n")
    f.write(f"item_id 数量：{df['item_id'].nunique():,}\n")
    f.write(f"category_id 数量：{df['category_id'].nunique():,}\n")

print("\n清洗报告已保存至：cleaning_report.txt")
print("\n数据清洗完成！")
