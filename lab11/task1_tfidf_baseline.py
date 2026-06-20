# 1. 导入依赖库
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer

# 2. 【关键路径适配】读取lab10目录下的特征文件（同级目录相对路径）
df = pd.read_csv("../lab10/batch_1000_features.csv", encoding="utf-8-sig")

# 3. 数据预处理：填充评论空值，避免向量化报错
df['review'] = df['review'].fillna('')

# 4. 初始化字符级TF-IDF向量化器（严格匹配任务书要求）
tfidf = TfidfVectorizer(
    analyzer='char',    # 字符级拆分，符合中文短文本场景
    max_features=500,   # 仅保留最高频500个字，控制特征维度
    ngram_range=(1, 1)
)

# 5. 生成TF-IDF稀疏矩阵
X_text_sparse = tfidf.fit_transform(df['review'])

# 6. 结果验证与输出
print("="*60)
print("实验11 任务1：传统NLP基线（TF-IDF）构建完成")
print("="*60)
print(f"✅ 成功读取lab10的特征文件，数据总行数: {df.shape[0]}")
print(f"✅ TF-IDF稀疏矩阵形状: {X_text_sparse.shape}")
print(f"✅ 矩阵类型: {type(X_text_sparse)} (CSR稀疏格式，内存高效)")
print(f"✅ 非零元素占比: {X_text_sparse.nnz / (X_text_sparse.shape[0]*X_text_sparse.shape[1]):.4%}")
print("="*60)
print("🎉 任务1完成！生成的X_text_sparse矩阵将用于后续消融实验")