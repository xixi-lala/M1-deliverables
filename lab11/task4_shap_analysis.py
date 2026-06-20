import matplotlib.pyplot as plt
import matplotlib
import numpy as np

# 负号+中文显示终极修复
matplotlib.rcParams['font.sans-serif'] = ['Microsoft YaHei']
matplotlib.rcParams['axes.unicode_minus'] = False
matplotlib.rcParams['text.usetex'] = False

# 修复负号编码问题
import matplotlib.text as mtext
original_text = mtext.Text.set_text
def patch_text(self, s):
    if s is not None:
        s = s.replace('\N{MINUS SIGN}', '-')
    return original_text(self, s)
mtext.Text.set_text = patch_text

# ========== 依赖导入 ==========
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import OrdinalEncoder
from scipy.sparse import hstack, csr_matrix, issparse
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score
import lightgbm as lgb
import shap

# ========== 1. 数据与特征预处理 ==========
df = pd.read_csv("../lab10/batch_1000_features.csv", encoding="utf-8-sig")
df['review'] = df['review'].fillna('')
y = df["label"]
llm_cols = ["cat", "sentiment", "category"]

# 生成三组特征矩阵
tfidf = TfidfVectorizer(analyzer='char', max_features=500)
X_text_sparse = tfidf.fit_transform(df['review'])

df[llm_cols] = df[llm_cols].fillna("Unknown")
encoder = OrdinalEncoder()
X_dense = encoder.fit_transform(df[llm_cols])
X_fused = hstack([X_text_sparse, csr_matrix(X_dense)])

# ========== 2. 统一切分数据集 ==========
X_train_A, X_test_A, y_train, y_test = train_test_split(
    X_text_sparse, y, test_size=0.2, random_state=42, stratify=y
)
X_train_B, X_test_B, _, _ = train_test_split(
    X_dense, y, test_size=0.2, random_state=42, stratify=y
)
X_train_C, X_test_C, _, _ = train_test_split(
    X_fused, y, test_size=0.2, random_state=42, stratify=y
)

# ========== 3. 训练三个模型 ==========
model_params = {"n_estimators":100, "random_state":42, "verbosity":-1}
# Baseline A 纯TF-IDF
clf_A = lgb.LGBMClassifier(**model_params)
clf_A.fit(X_train_A, y_train)
# Baseline B 纯LLM
clf_B = lgb.LGBMClassifier(**model_params)
clf_B.fit(X_train_B, y_train)
# Fused C 融合特征
clf_C = lgb.LGBMClassifier(**model_params)
clf_C.fit(X_train_C, y_train)

# ========== 任务4-1：计算SHAP值+绘制瀑布图 ==========
print("="*70)
print("任务4-1：SHAP特征贡献分析")
print("="*70)
explainer = shap.TreeExplainer(clf_C)
shap_values = explainer.shap_values(X_test_C)

# 处理二分类SHAP值
if isinstance(shap_values, list):
    shap_values = shap_values[1]
    expected_value = explainer.expected_value[1]
else:
    expected_value = explainer.expected_value

# 稀疏矩阵转稠密
shap_values = shap_values.toarray() if issparse(shap_values) else shap_values
X_test_dense = X_test_C.toarray() if issparse(X_test_C) else X_test_C

# 构建特征名
tfidf_names = [f"字_{c}" for c in tfidf.get_feature_names_out()]
feature_names = tfidf_names + llm_cols

# 绘制瀑布图
sample_idx = 6
row_data = np.asarray(X_test_C[sample_idx].todense()).flatten()
explanation = shap.Explanation(
    values=shap_values[sample_idx].flatten(),
    base_values=float(expected_value),
    data=row_data,
    feature_names=feature_names
)

plt.figure(figsize=(12, 8), dpi=150)
shap.plots.waterfall(explanation, max_display=15, show=False)
plt.title(f"SHAP瀑布图（测试集第{sample_idx}条样本）", fontsize=12)
plt.tight_layout()
plt.savefig("shap_waterfall2.png", dpi=150, bbox_inches='tight')
print("✅ SHAP瀑布图已生成并保存")
plt.close()

# ========== 任务4-2：打印消融实验对照表 ==========
print("\n" + "="*70)
print("📊 消融实验最终结果对照表")
print("="*70)
# 计算三组指标
acc_A = accuracy_score(y_test, clf_A.predict(X_test_A))
auc_A = roc_auc_score(y_test, clf_A.predict_proba(X_test_A)[:,1])

acc_B = accuracy_score(y_test, clf_B.predict(X_test_B))
auc_B = roc_auc_score(y_test, clf_B.predict_proba(X_test_B)[:,1])

acc_C = accuracy_score(y_test, clf_C.predict(X_test_C))
auc_C = roc_auc_score(y_test, clf_C.predict_proba(X_test_C)[:,1])

print(f"| 实验组别         | 准确率(Accuracy) | AUC分数 | 相对Baseline A提升 |")
print(f"|------------------|------------------|---------|--------------------|")
print(f"| Baseline A(纯TF-IDF) | {acc_A:.4f}           | {auc_A:.4f}  | -                  |")
print(f"| Baseline B(纯LLM)    | {acc_B:.4f}           | {auc_B:.4f}  | {(acc_B-acc_A)*100:.2f}%             |")
print(f"| Fused C(融合特征)    | {acc_C:.4f}           | {auc_C:.4f}  | {(acc_C-acc_A)*100:.2f}%             |")
print("="*70)
