# -*- coding: utf-8 -*-
"""
任务2：6组基线实验队列深度对比图生成
"""
import pandas as pd
import matplotlib.pyplot as plt

# 设置中文显示
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

# 读取实验数据
df = pd.read_csv("experiment_metrics.csv")

# 定义实验组名称映射
exp_names = {
    (10.0, 0.2, 1): "A1 (λ=10, t=0.2, n=1)",
    (10.0, 0.05, 1): "A2 (λ=10, t=0.05, n=1)",
    (50.0, 0.2, 1): "B1 (λ=50, t=0.2, n=1)",
    (50.0, 0.2, 3): "B2 (λ=50, t=0.2, n=3)",
    (20.0, 0.05, 1): "C1 (λ=20, t=0.05, n=1)",
    (100.0, 0.05, 2): "C2 (λ=100, t=0.05, n=2)",
}

# 创建画布
plt.figure(figsize=(12, 8), dpi=100)

# 遍历每个实验组，绘制曲线
for (producer_rate, consumer_time, n_consumers), exp_name in exp_names.items():
    # 筛选对应实验组的数据
    exp_data = df[
        (df['producer_rate'] == producer_rate) &
        (df['consumer_time'] == consumer_time) &
        (df['n_consumers'] == n_consumers)
    ]
    
    # 绘制队列深度随时间变化的曲线
    plt.plot(
        exp_data['elapsed_sec'], 
        exp_data['queue_depth'], 
        label=exp_name,
        linewidth=2,
        marker='o',
        markersize=3,
        alpha=0.8
    )

# 设置图表样式
plt.title("6组基线实验队列深度对比 (无限队列，无背压)", fontsize=16, pad=20)
plt.xlabel("实验运行时间 (秒)", fontsize=12)
plt.ylabel("队列深度 (条)", fontsize=12)
plt.legend(fontsize=10, loc='upper left')
plt.grid(True, linestyle='--', alpha=0.6)
plt.xlim(0, 15)  # 所有实验都是15秒

# 保存图片
plt.savefig("baseline_queue_depth.png", bbox_inches='tight', dpi=150)
print("✅ 图表已保存为: baseline_queue_depth.png")

# 显示图片
plt.show()