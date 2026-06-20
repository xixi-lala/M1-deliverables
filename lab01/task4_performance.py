import time
import pandas as pd
import polars as pl

# 1. 传统基准：Pandas
start = time.time()
df_pd = pd.read_csv("large_data.csv")
pandas_time = time.time() - start
print(f"Pandas 耗时: {pandas_time:.3f} 秒")

# 2. 现代多线程：Polars
start = time.time()
df_pl = pl.read_csv("large_data.csv")
polars_time = time.time() - start
print(f"Polars 耗时: {polars_time:.3f} 秒")

print(f"数据文件：large_data.csv（100万条，约558MB）")
print(f"性能提升倍数: {pandas_time / polars_time:.2f} 倍")