"""
使用 DuckDB 和 Polars 分析 large_data.csv 中的 event_type 字段
- DuckDB: 直接对本地 CSV 发起查询，无需加载全量内存
- Polars: 高效数据处理和统计
"""

import duckdb
import polars as pl

CSV_FILE = "large_data.csv"

# =============================================================================
# 方法 1: 使用 DuckDB 直接查询 CSV（流式处理，不加载全量数据到内存）
# =============================================================================
print("=" * 60)
print("【DuckDB 分析】")
print("=" * 60)

# DuckDB 直接查询 CSV 文件
duckdb_query = """
SELECT 
    event_type,
    COUNT(*) as count
FROM read_csv_auto('{csv_file}')
GROUP BY event_type
ORDER BY count DESC
""".format(csv_file=CSV_FILE)

duckdb_result = duckdb.query(duckdb_query).df()
print("\n所有唯一 event_type 及其出现次数:")
print(duckdb_result.to_string(index=False))

# 计算总记录数
total_count = duckdb_result['count'].sum()
print(f"\n总记录数：{total_count:,}")

# 定义正常的事件类型（用于识别异常）
normal_event_types = {'click', 'login', 'logout', 'payment'}

# 找出异常数据（拼写错误）
duckdb_result['is_anomaly'] = ~duckdb_result['event_type'].isin(normal_event_types)
anomalies = duckdb_result[duckdb_result['is_anomaly']]
normal_data = duckdb_result[~duckdb_result['is_anomaly']]

anomaly_count = anomalies['count'].sum()
anomaly_ratio = anomaly_count / total_count * 100

print(f"\n正常 event_type ({len(normal_data)} 种):")
for _, row in normal_data.iterrows():
    print(f"  - {row['event_type']}: {row['count']:,} ({row['count']/total_count*100:.2f}%)")

print(f"\n异常 event_type ({len(anomalies)} 种，疑似拼写错误):")
for _, row in anomalies.iterrows():
    print(f"  - {row['event_type']}: {row['count']:,} ({row['count']/total_count*100:.4f}%)")

print(f"\n异常数据总量：{anomaly_count:,} 条")
print(f"异常数据占比：{anomaly_ratio:.4f}%")


# =============================================================================
# 方法 2: 使用 Polars 进行相同分析
# =============================================================================
print("\n" + "=" * 60)
print("【Polars 分析】")
print("=" * 60)

# Polars 流式读取 CSV
df = pl.scan_csv(CSV_FILE)

# 统计 event_type 分布
event_type_stats = (
    df
    .group_by('event_type')
    .agg(pl.len().alias('count'))
    .sort('count', descending=True)
    .collect()
)

print("\n所有唯一 event_type 及其出现次数:")
print(str(event_type_stats))

total_count_pl = event_type_stats['count'].sum()
print(f"\n总记录数：{total_count_pl:,}")

# 识别异常数据
event_type_stats = event_type_stats.with_columns(
    pl.col('event_type').is_in(normal_event_types).alias('is_normal')
)

anomalies_pl = event_type_stats.filter(pl.col('is_normal') == False)
normal_data_pl = event_type_stats.filter(pl.col('is_normal') == True)

anomaly_count_pl = anomalies_pl['count'].sum()
anomaly_ratio_pl = anomaly_count_pl / total_count_pl * 100

print(f"\n正常 event_type ({len(normal_data_pl)} 种):")
for row in normal_data_pl.iter_rows():
    print(f"  - {row[0]}: {row[1]:,} ({row[1]/total_count_pl*100:.2f}%)")

print(f"\n异常 event_type ({len(anomalies_pl)} 种，疑似拼写错误):")
for row in anomalies_pl.iter_rows():
    print(f"  - {row[0]}: {row[1]:,} ({row[1]/total_count_pl*100:.4f}%)")

print(f"\n异常数据总量：{anomaly_count_pl:,} 条")
print(f"异常数据占比：{anomaly_ratio_pl:.4f}%")

# =============================================================================
# 总结
# =============================================================================
print("\n" + "=" * 60)
print("【分析总结】")
print("=" * 60)
print(f"""
数据文件：{CSV_FILE}
总记录数：{total_count:,}

正常事件类型 (4 种): click, login, logout, payment
异常事件类型 ({len(anomalies)} 种): {', '.join(anomalies['event_type'].tolist())}

异常数据统计:
  - 异常记录数：{anomaly_count:,}
  - 异常占比：{anomaly_ratio:.4f}%
  - 正常占比：{100 - anomaly_ratio:.4f}%
""")
