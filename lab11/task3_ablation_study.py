# ========== 复用任务1、任务2的特征生成逻辑，保证变量一致性 ==========
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import OrdinalEncoder
from scipy.sparse import hstack, csr_matrix
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score
import lightgbm as lgb

# 1. 读取数据与特征预处理
df = pd.read_csv("../lab10/batch_1000_features.csv", encoding="utf-8-sig")
df['review'] = df['review'].fillna('')
y = df["label"]  # 统一的预测目标（二分类：0/1 负面/正面）

# 2. 生成三组实验的特征矩阵
# 2.1 Baseline A：纯TF-IDF（传统NLP）
tfidf = TfidfVectorizer(analyzer='char', max_features=500)
X_text_sparse = tfidf.fit_transform(df['review'])

# 2.2 Baseline B：纯LLM稠密特征
llm_cols = ["cat", "sentiment", "category"]
df[llm_cols] = df[llm_cols].fillna("Unknown")
encoder = OrdinalEncoder()
X_dense = encoder.fit_transform(df[llm_cols])

# 2.3 Fused C：异构融合特征
X_fused = hstack([X_text_sparse, csr_matrix(X_dense)])

# ========== 消融实验核心：三组对照训练与评估 ==========
print("="*70)
print("实验11 任务3：LightGBM消融实验 开始执行")
print("="*70)
print(f"✅ 数据集统一切分：80%训练集 / 20%测试集，random_state=42（保证公平对比）")

# 1. 统一切分数据集（所有组用完全相同的训练/测试划分，保证对比公平）
# 仅特征不同，标签y和切分规则完全一致
X_train_A, X_test_A, y_train, y_test = train_test_split(
    X_text_sparse, y, test_size=0.2, random_state=42, stratify=y
)
X_train_B, X_test_B, _, _ = train_test_split(
    X_dense, y, test_size=0.2, random_state=42, stratify=y
)
X_train_C, X_test_C, _, _ = train_test_split(
    X_fused, y, test_size=0.2, random_state=42, stratify=y
)

# 2. 定义统一的模型参数（所有组模型参数完全一致，仅特征不同）
model_params = {
    "n_estimators": 100,
    "random_state": 42,
    "verbosity": -1,  # 关闭训练日志
    "n_jobs": -1
}

# 3. 训练+评估 Baseline A（纯TF-IDF）
print("\n🔹 训练 Baseline A：纯TF-IDF 传统NLP特征")
clf_A = lgb.LGBMClassifier(**model_params)
clf_A.fit(X_train_A, y_train)
y_pred_A = clf_A.predict(X_test_A)
y_proba_A = clf_A.predict_proba(X_test_A)[:, 1]
acc_A = accuracy_score(y_test, y_pred_A)
auc_A = roc_auc_score(y_test, y_proba_A)
print(f"   准确率: {acc_A:.4f} | AUC分数: {auc_A:.4f}")

# 4. 训练+评估 Baseline B（纯LLM特征）
print("\n🔹 训练 Baseline B：纯LLM提取结构化特征")
clf_B = lgb.LGBMClassifier(**model_params)
clf_B.fit(X_train_B, y_train)
y_pred_B = clf_B.predict(X_test_B)
y_proba_B = clf_B.predict_proba(X_test_B)[:, 1]
acc_B = accuracy_score(y_test, y_pred_B)
auc_B = roc_auc_score(y_test, y_proba_B)
print(f"   准确率: {acc_B:.4f} | AUC分数: {auc_B:.4f}")

# 5. 训练+评估 Fused C（异构融合特征）
print("\n🔹 训练 Fused C：TF-IDF + LLM 异构融合特征")
clf_C = lgb.LGBMClassifier(**model_params)
clf_C.fit(X_train_C, y_train)
y_pred_C = clf_C.predict(X_test_C)
y_proba_C = clf_C.predict_proba(X_test_C)[:, 1]
acc_C = accuracy_score(y_test, y_pred_C)
auc_C = roc_auc_score(y_test, y_proba_C)
print(f"   准确率: {acc_C:.4f} | AUC分数: {auc_C:.4f}")

# ========== 输出消融实验对比表（实验报告必须内容） ==========
print("\n" + "="*70)
print("📊 消融实验最终结果对照表")
print("="*70)
print(f"| 实验组别         | 准确率(Accuracy) | AUC分数 | 相对Baseline A提升 |")
print(f"|------------------|------------------|---------|--------------------|")
print(f"| Baseline A(纯TF-IDF) | {acc_A:.4f}           | {auc_A:.4f}  | -                  |")
print(f"| Baseline B(纯LLM)    | {acc_B:.4f}           | {auc_B:.4f}  | {(acc_B-acc_A)*100:.2f}%             |")
print(f"| Fused C(融合特征)    | {acc_C:.4f}           | {auc_C:.4f}  | {(acc_C-acc_A)*100:.2f}%             |")
print("="*70)

# 结果总结
print("\n💡 实验结论：")
if acc_C > acc_A:
    print(f"✅ 异构融合模型效果最优，相比传统TF-IDF准确率提升 {(acc_C-acc_A)*100:.2f}%")
if acc_B > acc_A:
    print(f"✅ 仅用3个LLM特征，效果就接近/超过500维TF-IDF特征，证明LLM语义特征的高价值")
print("\n🎉 任务3完成！上述对照表可直接写入实验报告")