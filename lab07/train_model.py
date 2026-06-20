# -*- coding: utf-8 -*-
"""
实验七 任务1：离线训练与模型资产序列化
功能：
1. 从清洗后的数据中提取训练样本
2. 构建二分类标签（购买=1，其他=0）
3. 构建包含预处理和分类器的完整Pipeline
4. 5折交叉验证评估模型性能
5. 将Pipeline序列化保存为 model.pkl
6. 验证模型文件可正常加载和推理
"""

import time
import random
import numpy as np
import pandas as pd
import polars as pl
from pathlib import Path
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_validate
from sklearn.metrics import make_scorer, accuracy_score, roc_auc_score
import joblib

# ===================== 配置区域（请根据实际情况修改） =====================
# 数据路径：优先使用实验四产出的 m1_final_clean.parquet
# 如果没有，也可以使用课程主数据集 user_behavior_100M.csv
DATA_PATH = Path("D:/MyProjects/DataAnalysis/lab02/UserBehavior.csv")  

# 训练采样数量：1-5万条
TRAIN_SAMPLE_SIZE = 20000

# 模型输出路径
MODEL_OUTPUT_PATH = Path("model.pkl")

# 随机种子，保证结果可复现
RANDOM_SEED = 42
random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

# ===================== 1. 数据读取与准备 =====================
print("=" * 80)
print("步骤 1/7：数据读取与准备")
print("=" * 80)

# 强制使用CSV读取逻辑
print(f"📂 正在读取 CSV 文件：{DATA_PATH}")
col_names = ['user_id', 'item_id', 'category_id', 'behavior_type', 'timestamp']
# 关键修改：指定header=None（无表头），并分块读取（避免加载100M数据到内存）
chunk_list = []
chunk_size = 1000000  # 每次读取100万行（可根据内存调整）
for chunk in pd.read_csv(
    DATA_PATH, 
    names=col_names, 
    header=None, 
    chunksize=chunk_size,
    encoding="utf-8"
):
    chunk_list.append(chunk)
    # 提前终止（100M数据太多，取前500万行即可满足采样）
    if len(chunk_list) * chunk_size >= 5000000:
        break
df = pd.concat(chunk_list, axis=0)

print(f"✅ 数据读取完成，总行数：{len(df):,}")
print(f"📊 行为类型分布：")
print(df['behavior_type'].value_counts())

# ===================== 2. 构造二分类标签 =====================
print("\n" + "=" * 80)
print("步骤 2/7：构造二分类标签")
print("=" * 80)

# 标签定义：behavior_type == 'buy' 为正样本(1)，其余为负样本(0)
df['label'] = (df['behavior_type'] == 'buy').astype(int)

print(f"✅ 标签构造完成")
print(f"📊 正负样本分布：")
print(df['label'].value_counts())
pos_rate = df['label'].mean() * 100
print(f"正样本占比：{pos_rate:.2f}%")

# ===================== 3. 特征工程 =====================
print("\n" + "=" * 80)
print("步骤 3/7：特征工程")
print("=" * 80)

# 从 timestamp 中提取特征
print("🔧 正在提取时间特征 (hour, dayofweek)...")
df['ts_datetime'] = pd.to_datetime(df['timestamp'], unit='s')
df['hour'] = df['ts_datetime'].dt.hour          # 一天中的第几小时 (0-23)
df['dayofweek'] = df['ts_datetime'].dt.dayofweek  # 一周中的第几天 (0=周一, 6=周日)

# 最终使用的特征列表
# 注意：这里的特征必须和后续实验七任务3在线推理时使用的特征完全一致！
FEATURE_COLS = ['category_id', 'hour', 'dayofweek']
print(f"✅ 特征工程完成")
print(f"📋 使用的特征：{FEATURE_COLS}")

# ===================== 4. 采样训练数据 =====================
print("\n" + "=" * 80)
print("步骤 4/7：采样训练数据")
print("=" * 80)

# 为了保证类别平衡，我们进行分层采样
# 先按label分组
df_pos = df[df['label'] == 1]
df_neg = df[df['label'] == 0]

# 计算采样数量：如果正样本不够，就用所有正样本
n_pos_sample = min(len(df_pos), int(TRAIN_SAMPLE_SIZE * pos_rate / 100))
n_neg_sample = TRAIN_SAMPLE_SIZE - n_pos_sample

print(f"📊 计划采样：正样本 {n_pos_sample} 条，负样本 {n_neg_sample} 条")

# 采样
df_pos_sampled = df_pos.sample(n=n_pos_sample, random_state=RANDOM_SEED)
df_neg_sampled = df_neg.sample(n=n_neg_sample, random_state=RANDOM_SEED)

# 合并并打乱
df_train = pd.concat([df_pos_sampled, df_neg_sampled], axis=0)
df_train = df_train.sample(frac=1.0, random_state=RANDOM_SEED).reset_index(drop=True)

print(f"✅ 训练数据采样完成，总样本数：{len(df_train)}")

# 提取 X 和 y
X = df_train[FEATURE_COLS].values
y = df_train['label'].values

# ===================== 5. 构建 Pipeline 与模型评估 =====================
print("\n" + "=" * 80)
print("步骤 5/7：构建 Pipeline 与 5折交叉验证评估")
print("=" * 80)

# 构建完整的 Pipeline：预处理 -> 分类器
# 注意：必须用 Pipeline 打包，绝对不能分开保存 scaler 和 model！
model_pipeline = Pipeline([
    # 步骤1：缺失值填充（使用中位数）
    ('imputer', SimpleImputer(strategy='median')),
    # 步骤2：标准化（Z-score）
    ('scaler', StandardScaler()),
    # 步骤3：分类器（RandomForest，效果好且稳定）
    ('classifier', RandomForestClassifier(
        n_estimators=100, 
        max_depth=10, 
        random_state=RANDOM_SEED,
        n_jobs=-1  # 使用所有CPU核心
    ))
])

print("✅ Pipeline 构建完成：")
print(model_pipeline)

# 5折交叉验证
print("\n🔍 正在进行 5折交叉验证...")
scoring = {
    'accuracy': make_scorer(accuracy_score),
    'roc_auc': make_scorer(roc_auc_score)
}

start_time = time.time()
cv_results = cross_validate(
    model_pipeline, 
    X, y, 
    cv=5, 
    scoring=scoring, 
    return_train_score=False,
    n_jobs=-1
)
cv_time = time.time() - start_time

# 打印评估结果
print("\n" + "📈 " + "=" * 50)
print("📊 5折交叉验证结果")
print("=" * 50)
print(f"⏱️  交叉验证耗时：{cv_time:.2f} 秒")
print(f"🎯 准确率 (Accuracy):  {cv_results['test_accuracy'].mean():.4f} (±{cv_results['test_accuracy'].std():.4f})")
print(f"🎯 ROC AUC 值:        {cv_results['test_roc_auc'].mean():.4f} (±{cv_results['test_roc_auc'].std():.4f})")
print("=" * 50)

# ===================== 6. 在全量训练数据上重新训练并保存 =====================
print("\n" + "=" * 80)
print("步骤 6/7：在全量训练数据上重新训练并序列化保存")
print("=" * 80)

print("🏋️  正在全量数据上训练最终模型...")
start_time = time.time()
model_pipeline.fit(X, y)
train_time = time.time() - start_time
print(f"✅ 模型训练完成，耗时：{train_time:.2f} 秒")

# 序列化保存
print(f"💾 正在保存模型到：{MODEL_OUTPUT_PATH}")
joblib.dump(model_pipeline, MODEL_OUTPUT_PATH)

# 检查文件大小
model_size_mb = MODEL_OUTPUT_PATH.stat().st_size / (1024 * 1024)
print(f"✅ 模型保存成功！文件大小：{model_size_mb:.2f} MB")

# ===================== 7. 资产验证：加载模型并测试推理 =====================
print("\n" + "=" * 80)
print("步骤 7/7：资产验证 - 加载模型并测试推理")
print("=" * 80)

print(f"📥 正在从 {MODEL_OUTPUT_PATH} 加载模型...")
loaded_pipeline = joblib.load(MODEL_OUTPUT_PATH)
print("✅ 模型加载成功！")

# 准备 3-5 条测试样本（从训练数据中取前5条）
print("\n🧪 测试推理（前5条样本）：")
print("-" * 80)
print(f"{'样本ID':<8} | {'真实标签':<10} | {'预测标签':<10} | {'购买概率':<10}")
print("-" * 80)

for i in range(min(5, len(df_train))):
    X_sample = X[i:i+1]  # 保持2D形状
    y_true = y[i]
    
    # 推理
    y_pred = loaded_pipeline.predict(X_sample)[0]
    y_proba = loaded_pipeline.predict_proba(X_sample)[0][1]  # 类别1的概率
    
    print(f"{i:<8} | {y_true:<10} | {y_pred:<10} | {y_proba:.4f}")

print("-" * 80)
print("\n🎉 任务1全部完成！")
print(f"📦 模型资产：{MODEL_OUTPUT_PATH.absolute()}")
