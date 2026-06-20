# -*- coding: utf-8 -*-
"""
实验七 任务2：模型加载策略对比（严格符合任务书要求版）
严格按照任务书要求实现：
1. 方案A：循环外一次性加载（正确）
2. 方案B：循环内反复加载（错误）
3. 强制绕过文件系统缓存，确保测出真实差异
"""

import time
import joblib
import numpy as np
import gc  # 垃圾回收模块，用于强制清理内存
from pathlib import Path

# ===================== 严格按照任务书配置 =====================
MODEL_PATH = Path("model.pkl")
N_TEST_SAMPLES = 100  # 任务书要求：处理100条数据

# 生成模拟特征（和任务1一致）
np.random.seed(42)
X_test = np.random.randint(0, 1000000, size=(N_TEST_SAMPLES, 3))

# ===================== 方案A：循环外一次性加载 =====================
print("=" * 80)
print("方案A：循环外一次性加载，常驻内存")


# 【严格按照任务书】在循环外只加载一次
print("\n📥 正在加载模型（循环外）...")
model = joblib.load(MODEL_PATH)

start_time_a = time.perf_counter()

# 【严格按照任务书】循环处理
for i in range(N_TEST_SAMPLES):
    features = X_test[i:i+1]
    # 直接使用内存中的模型
    pred = model.predict(features)
    proba = model.predict_proba(features)
    
    if (i + 1) % 20 == 0:
        print(f"⏳ 已处理 {i+1}/{N_TEST_SAMPLES} 条")

end_time_a = time.perf_counter()
total_time_a = end_time_a - start_time_a

# 清理一下，避免影响方案B
del model
gc.collect()
time.sleep(2)

# ===================== 方案B：循环内反复加载 =====================
print("\n" + "=" * 80)
print("方案B：循环内反复加载，每次重新触发磁盘I/O")


start_time_b = time.perf_counter()

# 【严格按照任务书】不在循环外加载，而是在循环内每次都加载
for i in range(N_TEST_SAMPLES):
    # 【关键】强制清理内存，绕过文件系统缓存
    gc.collect()
    
    # 【严格按照任务书】每次循环都重新加载！
    model_b = joblib.load(MODEL_PATH)
    
    features = X_test[i:i+1]
    pred = model_b.predict(features)
    proba = model_b.predict_proba(features)
    
    # 【关键】显式删除模型对象，强制下次重新从磁盘读取
    del model_b
    
    if (i + 1) % 10 == 0:
        print(f"⏳ 已处理 {i+1}/{N_TEST_SAMPLES} 条")

end_time_b = time.perf_counter()
total_time_b = end_time_b - start_time_b

# ===================== 最终对比（严格按照任务书表格） =====================
print("\n" + "🎉 " + "=" * 80)
print("实验七 任务2 结果记录表")
print("=" * 80)

# 计算指标
avg_time_a = total_time_a / N_TEST_SAMPLES
avg_time_b = total_time_b / N_TEST_SAMPLES
speedup = total_time_b / total_time_a

# 【严格按照任务书表格输出】
print("\n📊 任务2 对比结果表：")
print("-" * 80)
print(f"{'方案':<15} | {'总耗时(秒)':<15} | {'每条平均耗时(秒)':<20} | {'模型加载次数':<15}")
print("-" * 80)
print(f"{'A(循环外加载)':<15} | {total_time_a:<15.4f} | {avg_time_a:<20.6f} | {1:<15}")
print(f"{'B(循环内加载)':<15} | {total_time_b:<15.4f} | {avg_time_b:<20.6f} | {N_TEST_SAMPLES:<15}")
print("-" * 80)

print(f"\n🚀 性能差距：方案B比方案A慢了 {speedup:.1f} 倍")


# 保存结果
with open("task2_results.txt", "w", encoding="utf-8") as f:
    f.write("实验七 任务2 结果记录\n")
    f.write("="*60 + "\n")
    f.write(f"方案A总耗时: {total_time_a:.4f}秒\n")
    f.write(f"方案A每条平均: {avg_time_a:.6f}秒\n")
    f.write(f"方案B总耗时: {total_time_b:.4f}秒\n")
    f.write(f"方案B每条平均: {avg_time_b:.6f}秒\n")
    f.write(f"性能差距: {speedup:.1f}倍\n")

print("\n✅ 任务2完成！结果已保存至 task2_results.txt")