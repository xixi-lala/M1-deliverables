"""
使用 DuckDB 和 Polars 分析 large_data.csv 中 user_id 异常数据
- 统计 user_id = -1 的记录数
- 统计 user_id = 'guest' 的记录数
- 计算各自占总记录数的比例
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

# DuckDB 直接查询 CSV 文件
duckdb_query = """
SELECT 
    COUNT(*) as total_count,
    SUM(CASE WHEN user_id = '-1' THEN 1 ELSE 0 END) as user_id_neg1_count,
    SUM(CASE WHEN user_id = 'guest' THEN 1 ELSE 0 END) as user_id_guest_count
FROM read_csv_auto('{csv_file}')
""".format(csv_file=CSV_FILE)

result = duckdb.query(duckdb_query).df().iloc[0]

total_count = int(result['total_count'])
neg1_count = int(result['user_id_neg1_count'])
guest_count = int(result['user_id_guest_count'])

neg1_ratio = neg1_count / total_count * 100 if total_count > 0 else 0
guest_ratio = guest_count / total_count * 100 if total_count > 0 else 0

print(f"""
数据文件：{CSV_FILE}

【异常类型统计】
┌─────────────────────┬──────────────┬──────────────┐
│ 异常类型            │ 记录数       │ 占比         │
├─────────────────────┼──────────────┼──────────────┤
│ user_id = -1        │ {neg1_count:>10,} │ {neg1_ratio:>10.4f}% │
│ user_id = 'guest'   │ {guest_count:>10,} │ {guest_ratio:>10.4f}% │
├─────────────────────┼──────────────┼──────────────┤
│ 异常合计            │ {neg1_count + guest_count:>10,} │ {(neg1_ratio + guest_ratio):>10.4f}% │
├─────────────────────┼──────────────┼──────────────┤
│ 总记录数            │ {total_count:>10,} │ 100.0000%    │
└─────────────────────┴──────────────┴──────────────┘
""")


# =============================================================================
# 方法 2: 使用 Polars 进行相同分析
# =============================================================================
print("=" * 60)
print("【Polars 分析】")
print("=" * 60)

# Polars 流式读取 CSV
df = pl.scan_csv(CSV_FILE)

# 统计总数
total_count_pl = df.select(pl.len()).collect().item()

# 统计 user_id = '-1' 的数量 (先 collect 再过滤，避免 lazy 执行计划问题)
df_collected = df.collect()
neg1_count_pl = df_collected.filter(pl.col('user_id') == '-1').shape[0]

# 统计 user_id = 'guest' 的数量
guest_count_pl = df_collected.filter(pl.col('user_id') == 'guest').shape[0]

neg1_ratio_pl = neg1_count_pl / total_count_pl * 100 if total_count_pl > 0 else 0
guest_ratio_pl = guest_count_pl / total_count_pl * 100 if total_count_pl > 0 else 0

print(f"""
数据文件：{CSV_FILE}

【异常类型统计】
┌─────────────────────┬──────────────┬──────────────┐
│ 异常类型            │ 记录数       │ 占比         │
├─────────────────────┼──────────────┼──────────────┤
│ user_id = -1        │ {neg1_count_pl:>10,} │ {neg1_ratio_pl:>10.4f}% │
│ user_id = 'guest'   │ {guest_count_pl:>10,} │ {guest_ratio_pl:>10.4f}% │
├─────────────────────┼──────────────┼──────────────┤
│ 异常合计            │ {neg1_count_pl + guest_count_pl:>10,} │ {(neg1_ratio_pl + guest_ratio_pl):>10.4f}% │
├─────────────────────┼──────────────┼──────────────┤
│ 总记录数            │ {total_count_pl:>10,} │ 100.0000%    │
└─────────────────────┴──────────────┴──────────────┘
""")


# =============================================================================
# 总结
# =============================================================================
print("=" * 60)
print("【分析总结】")
print("=" * 60)

anomaly_total = neg1_count + guest_count
anomaly_ratio = neg1_ratio + guest_ratio

print(f"""
异常数据汇总:
  - user_id = -1  的记录数：{neg1_count:,} 条 (占比 {neg1_ratio:.4f}%)
  - user_id = 'guest' 的记录数：{guest_count:,} 条 (占比 {guest_ratio:.4f}%)
  
  异常数据总计：{anomaly_total:,} 条 (占比 {anomaly_ratio:.4f}%)
  正常数据总计：{total_count - anomaly_total:,} 条 (占比 {100 - anomaly_ratio:.4f}%)
""")
