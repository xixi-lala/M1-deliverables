
import polars as pl

df = pl.read_parquet("m1_final_clean.parquet")
print("行数:", df.height)
print("字段及类型:")
for name, dtype in df.schema.items():
    print(f"  {name}: {dtype}")