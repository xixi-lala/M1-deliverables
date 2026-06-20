# -*- coding: utf-8 -*-
import pandas as pd
import matplotlib.pyplot as plt

# 解决中文显示
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

# 读取数据
df = pd.read_csv("perturbation_metrics.csv")

# 4组实验对应关系
groups = {
    "none": "无扰动",
    "jitter": "均匀抖动",
    "burst_mild": "温和突发",
    "burst_heavy": "激烈突发"
}
colors = ['blue', 'orange', 'green', 'red']

# 创建画布：左曲线 + 右直方图
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

# ========== 左图：队列深度随时间变化曲线 ==========
for (mode, label), color in zip(groups.items(), colors):
    data = df[df['perturbation_mode'] == mode]
    ax1.plot(data['elapsed_sec'], data['queue_depth'], 
             label=label, color=color, linewidth=2)

ax1.set_title("队列深度随时间变化", fontsize=14)
ax1.set_xlabel("时间(秒)")
ax1.set_ylabel("队列深度")
ax1.legend()
ax1.grid(True, linestyle='--')

# ========== 右图：队列深度分布直方图 ==========
for (mode, label), color in zip(groups.items(), colors):
    data = df[df['perturbation_mode'] == mode]
    ax2.hist(data['queue_depth'], bins=20, alpha=0.5, 
             label=label, color=color)

ax2.set_title("队列深度分布直方图", fontsize=14)
ax2.set_xlabel("队列深度")
ax2.set_ylabel("频次")
ax2.legend()

plt.tight_layout()
plt.savefig("perturbation_result.png", dpi=150)
plt.show()