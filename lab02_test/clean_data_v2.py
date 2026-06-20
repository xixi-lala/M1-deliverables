"""
UserBehavior 数据清洗脚本 v2
改进：基于数据实际分布来判断异常值
"""
import pandas as pd
import numpy as np
from datetime import datetime

columns = ['user_id', 'item_id', 'category_id', 'behavior_type', 'timestamp']

print("开始读取数据...")
chunks = []
chunk_size = 500000

for i, chunk in enumerate(pd.read_csv(
    'UserBehavior.csv',
    names=columns,
    header=None,
    chunksize=chunk_size
)):
    if i % 20 == 0:
        print(f"已读取第 {i+1} 块...")
    chunks.append(chunk)

df = pd.concat(chunks, ignore_index=True)
print(f"\n数据读取完成！总行数：{len(df):,}")

# ========== 1. 缺失值 ==========
print("\n" + "="*50)
print("1. 缺失值检查")
print("="*50)
missing = df.isnull().sum()
print(f"缺失值：{missing.sum()}")
if missing.sum() > 0:
    df = df.dropna()

# ========== 2. 重复值 ==========
print("\n" + "="*50)
print("2. 重复值检查")
print("="*50)
duplicates = df.duplicated().sum()
print(f"完全重复行数：{duplicates:,}")
if duplicates > 0:
    df = df.drop_duplicates()

# ========== 3. 行为类型验证 ==========
print("\n" + "="*50)
print("3. 行为类型验证")
print("="*50)
valid_behaviors = {'pv', 'buy', 'cart', 'fav'}
invalid = set(df['behavior_type'].unique()) - valid_behaviors
if invalid:
    print(f"异常行为类型：{invalid}，已删除")
    df = df[df['behavior_type'].isin(valid_behaviors)]
print(f"行为类型分布:\n{df['behavior_type'].value_counts()}")

# ========== 4. 时间戳处理（改进版） ==========
print("\n" + "="*50)
print("4. 时间戳处理")
print("="*50)

# 只删除明显异常的时间戳：负值或超过当前时间的值
current_ts = datetime.now().timestamp()
invalid_neg = (df['timestamp'] < 0).sum()
invalid_future = (df['timestamp'] > current_ts).sum()
print(f"负时间戳：{invalid_neg:,}")
print(f"未来时间戳：{invalid_future:,}")

df = df[(df['timestamp'] >= 0) & (df['timestamp'] <= current_ts)]

# 查看实际时间范围
print(f"\n实际时间戳范围：{df['timestamp'].min()} - {df['timestamp'].max()}")
print(f"对应日期：{datetime.fromtimestamp(df['timestamp'].min())} 至 {datetime.fromtimestamp(df['timestamp'].max())}")

df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
df['date'] = df['timestamp'].dt.date
df['hour'] = df['timestamp'].dt.hour

# ========== 5. 数值字段验证 ==========
print("\n" + "="*50)
print("5. 数值字段验证")
print("="*50)
for col in ['user_id', 'item_id', 'category_id']:
    invalid_count = (df[col] <= 0).sum()
    if invalid_count > 0:
        print(f"{col} 非正值：{invalid_count:,}，已删除")
        df = df[df[col] > 0]

# ========== 6. 保存 ==========
print("\n" + "="*50)
print("6. 保存数据")
print("="*50)

df.to_csv('UserBehavior_cleaned_v2.csv', index=False)
print(f"保存至：UserBehavior_cleaned_v2.csv")
print(f"最终行数：{len(df):,}")

with open('cleaning_report_v2.txt', 'w', encoding='utf-8') as f:
    f.write("UserBehavior 数据清洗报告 v2\n")
    f.write("="*50 + "\n\n")
    f.write(f"最终数据行数：{len(df):,}\n")
    f.write(f"数据列数：{df.shape[1]}\n\n")
    f.write("行为类型分布:\n")
    f.write(f"{df['behavior_type'].value_counts()}\n\n")
    f.write(f"时间范围：{df['timestamp'].min()} 至 {df['timestamp'].max()}\n\n")
    f.write(f"user_id 数量：{df['user_id'].nunique():,}\n")
    f.write(f"item_id 数量：{df['item_id'].nunique():,}\n")
    f.write(f"category_id 数量：{df['category_id'].nunique():,}\n")

print("清洗报告已保存至：cleaning_report_v2.txt")
print("\n完成！")
