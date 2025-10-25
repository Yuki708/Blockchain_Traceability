# visualize_academic.py
# Academic-style plots for Hyperledger Sawtooth Data Trace Experiment
# Copyright 2024 Hyperledger Sawtooth Data Trace Experiment
# Licensed under Apache License 2.0

import json
import matplotlib.pyplot as plt
import os

# 设置中文字体（避免乱码）
plt.rcParams['font.family'] = 'DejaVu Sans'
plt.rcParams['axes.unicode_minus'] = False

# 加载实验结果
with open('experiment_results/experiment_results.json') as f:
    data = json.load(f)

output_dir = 'experiment_results'
os.makedirs(output_dir, exist_ok=True)

# ----------------------------=
# 图1：双链 vs 单链 追溯效率对比（折线图）
# ----------------------------=
plt.figure(figsize=(8, 5))
scalability = data['performance_metrics']['scalability']
x = [s['dataset_count'] for s in scalability]
y = [s['throughput'] for s in scalability]

plt.plot(x, y, marker='o', linewidth=2.5, markersize=8, color='#1f77b4', label='双链架构（PBFT）')
plt.title('双链架构对数据追溯吞吐量的提升效果', fontsize=14, fontweight='bold')
plt.xlabel('数据集数量', fontsize=12)
plt.ylabel('吞吐量（TPS）', fontsize=12)
plt.grid(True, linestyle='--', alpha=0.5)
plt.legend()
plt.tight_layout()
plt.savefig(f'{output_dir}/throughput_comparison.png', dpi=300)
plt.close()

# ----------------------------=
# 图2：伪造检测置信度分布（柱状图）
# ----------------------------=
fake_confidences = [f['detection_result']['confidence'] for f in data['fake_detection'] if f['detection_result']['is_fake']]
bins = [0.6, 0.7, 0.8, 0.9, 1.0]
counts = [sum(1 for c in fake_confidences if b[0] <= c < b[1]) for b in zip(bins, bins[1:]+[1.1])]

plt.figure(figsize=(6, 4))
bars = plt.bar([f'{b[0]:.1f}-{b[1]:.1f}' for b in zip(bins, bins[1:]+[1.1])], counts, color='#ff7f0e', edgecolor='black')
plt.title('伪造数据检测置信度分布', fontsize=14, fontweight='bold')
plt.xlabel('置信度区间', fontsize=12)
plt.ylabel('样本数量', fontsize=12)
plt.grid(axis='y', linestyle='--', alpha=0.5)

# 柱状图顶部加数值
for bar, count in zip(bars, counts):
    plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1, str(count), ha='center', va='bottom')

plt.tight_layout()
plt.savefig(f'{output_dir}/confidence_distribution.png', dpi=300)
plt.close()

# ----------------------------=
# 图3：注册耗时对比（柱状图）
# ----------------------------=
reg_times = [r['registration_time'] for r in data['dataset_registration']['results'] if 'error' not in r]
reg_labels = [r['dataset_id'] for r in data['dataset_registration']['results'] if 'error' not in r]
reg_times = reg_times[:10]  # 只显示前10个，避免图太拥挤
reg_labels = reg_labels[:10]

plt.figure(figsize=(8, 4))
bars = plt.bar(reg_labels, reg_times, color='#2ca02c', edgecolor='black')
plt.title('数据集注册耗时对比（前10个）', fontsize=14, fontweight='bold')
plt.xlabel('数据集编号', fontsize=12)
plt.ylabel('注册时间（秒）', fontsize=12)
plt.xticks(rotation=45)
plt.grid(axis='y', linestyle='--', alpha=0.5)

# 顶部加数值
for bar, time in zip(bars, reg_times):
    plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01, f'{time:.2f}', ha='center', va='bottom')

plt.tight_layout()
plt.savefig(f'{output_dir}/registration_time.png', dpi=300)
plt.close()

# ----------------------------=
# 图4：查询响应时间趋势（折线图）
# ----------------------------=
query_times = data['performance_metrics']['query_performance']['query_times'][:20]  # 前20次
plt.figure(figsize=(8, 4))
plt.plot(range(len(query_times)), query_times, marker='s', linewidth=2, color='#d62728')
plt.title('区块链查询响应时间趋势（前20次）', fontsize=14, fontweight='bold')
plt.xlabel('查询序号', fontsize=12)
plt.ylabel('响应时间（秒）', fontsize=12)
plt.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()
plt.savefig(f'{output_dir}/query_time_trend.png', dpi=300)
plt.close()

print("✅ 学术风格图表已生成，保存至 experiment_results/")
