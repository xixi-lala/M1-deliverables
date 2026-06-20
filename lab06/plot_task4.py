# -*- coding: utf-8 -*-
"""
任务4：流式预处理可视化（原始特征 vs 标准化特征）
修正点：
1. 列名替换为真实CSV中的英文列名
2. 标准化特征列名匹配 _scaled 后缀
3. 调整StandardScaler的x轴范围（Z-score不再是0-1）
"""
import pandas as pd
import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

# 读取数据
raw = pd.read_csv("task4_raw_features.csv")
scaled = pd.read_csv("task4_scaled_features.csv")

# 【关键修正】使用真实的CSV列名
raw_features = ["category_id", "timestamp", "user_id"]
scaled_features = ["category_id_scaled", "timestamp_scaled", "user_id_scaled"]
# 图表显示用的中文标题
feature_titles = ["商品类目ID", "时间戳", "用户ID"]
colors = ["#ff6b6b", "#4ecdc4", "#45b7d1"]

# 绘图：2行3列，对比原始/标准化特征
fig, axes = plt.subplots(2, 3, figsize=(18, 8))
axes = axes.flatten()

# 原始特征
for i in range(3):
    axes[i].hist(raw[raw_features[i]], bins=20, color=colors[i], alpha=0.7)
    axes[i].set_title(f"原始{feature_titles[i]}", fontsize=12)
    axes[i].grid(alpha=0.3)
    axes[i].ticklabel_format(style='sci', scilimits=(0,0), axis='x')  # 科学计数法显示大数字

# 标准化特征（Z-score标准化，范围不再是0-1）
for i in range(3):
    axes[i+3].hist(scaled[scaled_features[i]], bins=20, color=colors[i], alpha=0.7)
    axes[i+3].set_title(f"标准化{feature_titles[i]} (Z-score)", fontsize=12)
    axes[i+3].grid(alpha=0.3)

plt.suptitle("任务4：流式特征预处理 — 原始特征 VS 标准化特征", fontsize=16)
plt.tight_layout(rect=[0, 0, 1, 0.96])  # 调整布局给标题留空间
plt.savefig("task4_preprocess.png", dpi=150)
print("✅ 可视化图已保存为 task4_preprocess.png")
plt.show()