"""
使用 DuckDB 和 Polars 分析 large_data.csv 中 device_info 字段的操作系统分布
- 从 JSON 字符串中提取 os 字段
- 按不同操作系统统计请求数及占比
- 处理 JSON 解析失败的情况并标注为 unknown
"""

import duckdb
import polars as pl

CSV_FILE = r"D:\MyProjects\DataAnylyse\lab01\large_data.csv"

# =============================================================================
# 方法 1: 使用 DuckDB 直接查询 CSV（流式处理，不加载全量数据到内存）
# =============================================================================
print("=" * 60)
print("【DuckDB 分析】")
print("=" * 60)

# DuckDB 直接查询 CSV 文件，使用 json_extract_string 解析 JSON
# TRY 开头的函数在解析失败时返回 NULL 而非报错
duckdb_query = """
WITH parsed AS (
    SELECT 
        COALESCE(
            TRY(json_extract_string(device_info::VARCHAR, '$.os')), 
            'unknown'
        ) as os
    FROM read_csv_auto('{csv_file}')
)
SELECT 
    os,
    COUNT(*) as count
FROM parsed
GROUP BY os
ORDER BY count DESC
""".format(csv_file=CSV_FILE)

duckdb_result = duckdb.query(duckdb_query).df()

total_count = duckdb_result['count'].sum()
duckdb_result['ratio'] = (duckdb_result['count'] / total_count * 100).round(4)

print(f"\n数据文件：{CSV_FILE}")
print(f"总记录数：{total_count:,}\n")

print("操作系统分布统计:")
print("-" * 50)
print(f"{'操作系统':<20} {'记录数':>12} {'占比':>12}")
print("-" * 50)
for _, row in duckdb_result.iterrows():
    os_name = row['os'] if row['os'] else 'unknown'
    print(f"{os_name:<20} {int(row['count']):>12,} {row['ratio']:>11.4f}%")
print("-" * 50)


# =============================================================================
# 方法 2: 使用 Polars 进行相同分析
# =============================================================================
print("\n" + "=" * 60)
print("【Polars 分析】")
print("=" * 60)

# Polars 流式读取 CSV
df = pl.scan_csv(CSV_FILE)

# 使用正则表达式从 JSON 字符串中提取 os 字段值
# 解析失败时返回 null，用 fill_null 填充为 'unknown'
df_parsed = df.with_columns(
    pl.col('device_info')
    .str.extract(r'"os"\s*:\s*"([^"]+)"', 1)
    .fill_null('unknown')
    .alias('os')
)

# 统计各操作系统的数量
os_stats = (
    df_parsed
    .group_by('os')
    .agg(pl.len().alias('count'))
    .sort('count', descending=True)
    .collect()
)

total_count_pl = os_stats['count'].sum()
os_stats = os_stats.with_columns(
    (pl.col('count') / total_count_pl * 100).alias('ratio')
)

print(f"\n数据文件：{CSV_FILE}")
print(f"总记录数：{total_count_pl:,}\n")

print("操作系统分布统计:")
print("-" * 50)
print(f"{'操作系统':<20} {'记录数':>12} {'占比':>12}")
print("-" * 50)
for row in os_stats.iter_rows():
    os_name = row[0] if row[0] else 'unknown'
    print(f"{os_name:<20} {int(row[1]):>12,} {row[2]:>11.4f}%")
print("-" * 50)


# =============================================================================
# 总结
# =============================================================================
print("\n" + "=" * 60)
print("【分析总结】")
print("=" * 60)

print(f"""
数据文件：{CSV_FILE}
总记录数：{total_count:,}

操作系统分布 (共 {len(duckdb_result)} 种):
""")

for _, row in duckdb_result.iterrows():
    os_name = row['os'] if row['os'] else 'unknown'
    marker = " ← 解析失败" if os_name == 'unknown' else ""
    print(f"  - {os_name}: {int(row['count']):,} 条 ({row['ratio']:.4f}%){marker}")

unknown_count = duckdb_result[duckdb_result['os'] == 'unknown']['count'].sum() if 'unknown' in duckdb_result['os'].values else 0
print(f"""
JSON 解析失败记录数：{unknown_count:,} 条 ({unknown_count/total_count*100:.4f}%)
""")
