import pandas as pd

# ==================== 1. 加载任务四生成的中间数据 ====================
print("=" * 80)
print("🔄 任务五：闭环数据管道与特征落盘持久化")
print("=" * 80)

try:
    # 加载原始数据（第100-104行）
    df_raw = pd.read_csv("temp_raw_data.csv", encoding="utf-8")
    # 加载LLM提取的特征
    df_features = pd.read_csv("temp_features.csv", encoding="utf-8")
    
    print("✅ 成功加载中间数据")
    print(f"   原始数据行数：{len(df_raw)}，列数：{len(df_raw.columns)}")
    print(f"   特征数据行数：{len(df_features)}，列数：{len(df_features.columns)}")

except FileNotFoundError as e:
    print(f"❌ 错误：找不到中间文件 {e.filename}")
    print("请先运行任务四（task4_batch_processing.py）生成中间数据")
    exit(1)

# ==================== 2. 水平拼接原始数据与LLM特征（核心步骤） ====================
print("\n🔗 正在进行水平拼接...")

# 严格按索引对齐拼接（任务四已重置索引，保证一一对应）
# axis=1表示水平拼接，原始数据在前，新增特征在后
df_augmented = pd.concat([df_raw, df_features], axis=1)

# 验证拼接结果
print(f"✅ 拼接完成，最终宽表：")
print(f"   总行数：{len(df_augmented)}")
print(f"   总列数：{len(df_augmented.columns)}")
print(f"   包含字段：{list(df_augmented.columns)}")

# ==================== 3. 结构化持久化落盘 ====================
OUTPUT_FILE = "augmented_reviews_sample.csv"

# 使用utf-8-sig编码，防止Excel打开中文乱码（实验要求）
df_augmented.to_csv(
    OUTPUT_FILE,
    index=False,  # 不导出索引列
    encoding="utf-8-sig"  # 关键：解决中文乱码问题
)

print(f"\n💾 数据已成功落盘为：{OUTPUT_FILE}")

# ==================== 4. 自动数据质量校验（实验要求） ====================
print("\n" + "=" * 80)
print("🔍 自动数据质量校验")
print("=" * 80)

# 校验1：行数是否一致
if len(df_augmented) == 5:
    print("✅ 校验通过：行数正确（5条）")
else:
    print("❌ 校验失败：行数错误")

# 校验2：是否包含所有必需字段
required_fields = ["cat", "label", "review", "sentiment", "category", "summary"]
missing_fields = [field for field in required_fields if field not in df_augmented.columns]
if not missing_fields:
    print("✅ 校验通过：包含所有必需字段")
else:
    print(f"❌ 校验失败：缺少字段 {missing_fields}")

# 校验3：是否有缺失值
null_counts = df_augmented[required_fields].isnull().sum()
if null_counts.sum() == 0:
    print("✅ 校验通过：无缺失值")
else:
    print("⚠️  校验警告：存在缺失值")
    print(null_counts[null_counts > 0])

# 校验4：特征字段取值是否符合要求
valid_sentiments = {"正面", "负面", "中性", "错误"}
valid_categories = {"物流", "质量", "价格", "服务", "综合", "错误"}

sentiment_errors = df_augmented[~df_augmented["sentiment"].isin(valid_sentiments)]
category_errors = df_augmented[~df_augmented["category"].isin(valid_categories)]

if len(sentiment_errors) == 0 and len(category_errors) == 0:
    print("✅ 校验通过：特征取值符合要求")
else:
    print("⚠️  校验警告：存在非法特征取值")
    if len(sentiment_errors) > 0:
        print(f"   非法情感值：{sentiment_errors['sentiment'].unique()}")
    if len(category_errors) > 0:
        print(f"   非法分类值：{category_errors['category'].unique()}")

# ==================== 5. 打印最终结果（实验报告需要截图） ====================
print("\n" + "=" * 80)
print("📋 最终拼接后的完整宽表：")
print("=" * 80)
# 设置显示所有列，不截断
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 30)
print(df_augmented)
print("=" * 80)

print("\n🎉 任务五完成！")
print(f"📁 请打开 {OUTPUT_FILE} 手动验证中文显示和行列对齐情况")