import pandas as pd
df = pd.read_csv("../lab10/batch_1000_features.csv", encoding="utf-8-sig")
print("标签分布：")
print(df['label'].value_counts())