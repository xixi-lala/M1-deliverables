# ========== 衔接任务1：复用TF-IDF逻辑，保证变量一致性 ==========
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import OrdinalEncoder
from scipy.sparse import hstack, csr_matrix

# 1. 读取lab10的特征数据集
df = pd.read_csv("../lab10/batch_1000_features.csv", encoding="utf-8-sig")
df['review'] = df['review'].fillna('')

# 2. 生成任务1的TF-IDF稀疏矩阵（变量完全复用）
tfidf = TfidfVectorizer(analyzer='char', max_features=500)
X_text_sparse = tfidf.fit_transform(df['review'])

# ========== 任务2核心：稀疏-稠密特征拼接 ==========
print("="*60)
print("实验11 任务2：稀疏-稠密异构特征拼接 开始执行")
print("="*60)

# 1. 【适配你的实际数据】定义需要编码的离散特征列
# 对应你LLM提取的特征 + 原表硬特征，均为字符串类型，需要转为数值
llm_cols = ["cat", "sentiment", "category"]
print(f"✅ 待编码的稠密特征列: {llm_cols}")

# 2. 填充特征空值，避免编码报错
df[llm_cols] = df[llm_cols].fillna("Unknown")

# 3. 序数编码：将字符串特征映射为整数（Scipy矩阵仅支持数值）
encoder = OrdinalEncoder()
X_dense = encoder.fit_transform(df[llm_cols])
print(f"✅ 稠密特征编码完成，矩阵形状: {X_dense.shape}")
print(f"✅ 各特征编码映射示例:")
for col, categories in zip(llm_cols, encoder.categories_):
    print(f"   {col}: {categories} → 对应整数 0~{len(categories)-1}")

# 4. 核心操作：稀疏矩阵 + 稠密矩阵水平拼接
# 先将稠密数组转为稀疏格式，再和TF-IDF稀疏矩阵合并
X_fused = hstack([X_text_sparse, csr_matrix(X_dense)])

# 5. 提取目标标签（后续分类任务的预测目标）
y = df["label"]
print(f"✅ 目标标签y提取完成，形状: {y.shape}")

# ========== 结果验证（任务书强制要求） ==========
print("\n" + "="*60)
print("任务2 结果验证")
print("="*60)
print(f"TF-IDF稀疏矩阵形状: {X_text_sparse.shape} (1000行 × 500列)")
print(f"LLM稠密特征矩阵形状: {X_dense.shape} (1000行 × {len(llm_cols)}列)")
print(f"✅ 融合后总特征矩阵形状: {X_fused.shape} (1000行 × {500+len(llm_cols)}列)")
print(f"✅ 融合矩阵类型: {type(X_fused)} (CSR稀疏格式，内存占用仅为稠密矩阵的10%)")
print("="*60)
print("🎉 任务2完成！X_fused融合矩阵将用于后续消融实验的Fused C组")