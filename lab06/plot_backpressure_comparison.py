# -*- coding: utf-8 -*-
"""
任务2：有/无背压曲线对比图（双生子图）
"""
import pandas as pd
import matplotlib.pyplot as plt

plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

# 读取背压实验数据
df = pd.read_csv("backpressure_metrics.csv")

# 分离两组数据
df_with_bp = df[df['max_capacity'] == 100].copy()
df_no_bp = df[df['max_capacity'] == -1].copy()

# 调整无背压数据的时间轴（因为是第二组实验，时间从45秒开始，我们把它平移回0-40秒）
df_no_bp['elapsed_sec'] = df_no_bp['elapsed_sec'] - df_no_bp['elapsed_sec'].min()

# 创建画布：上下两个子图
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), dpi=100, sharex=True)

# -------------------- 上图：禁用背压 --------------------
ax1.plot(
    df_no_bp['elapsed_sec'], 
    df_no_bp['queue_depth'], 
    label="禁用背压 (无限队列)",
    color='#e74c3c',
    linewidth=2.5,
    linestyle='-'
)
ax1.set_title("禁用背压：队列深度无限增长", fontsize=14, pad=15)
ax1.set_ylabel("队列深度 (条)", fontsize=12)
ax1.legend(fontsize=11, loc='upper left')
ax1.grid(True, linestyle='--', alpha=0.6)
ax1.set_ylim(bottom=0)

# -------------------- 下图：启用背压 --------------------
ax2.plot(
    df_with_bp['elapsed_sec'], 
    df_with_bp['queue_depth'], 
    label="启用背压 (有界队列+指数退避)",
    color='#27ae60',
    linewidth=2.5
)
# 绘制水位线
ax2.axhline(y=85, color='#f39c12', linestyle='--', linewidth=2, label='高水位线 (85%)')
ax2.axhline(y=30, color='#3498db', linestyle='--', linewidth=2, label='低水位线 (30%)')
ax2.set_title("启用背压：队列深度被限制在0-100之间振荡", fontsize=14, pad=15)
ax2.set_xlabel("实验运行时间 (秒)", fontsize=12)
ax2.set_ylabel("队列深度 (条)", fontsize=12)
ax2.legend(fontsize=11, loc='upper right')
ax2.grid(True, linestyle='--', alpha=0.6)
ax2.set_xlim(0, 40)
ax2.set_ylim(0, 105)

# 调整子图间距
plt.tight_layout(pad=3.0)

# 保存图片
plt.savefig("backpressure_comparison_final.png", bbox_inches='tight', dpi=150)
print("✅ 最终背压对比图已保存为: backpressure_comparison_final.png")

plt.show()