# data_preprocessor_complete.py
# Copyright 2024 Hyperledger Sawtooth Data Trace Experiment
# Licensed under Apache License 2.0

import os
import hashlib
import json
import logging
from collections import Counter
import random

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SchonlauDataPreprocessor:
    def __init__(self, data_dir='schonlau'):
        self.data_dir = os.path.abspath(data_dir)
        self.user_files = []

        logger.info(f"使用数据目录：{self.data_dir}")
        if os.path.isdir(self.data_dir):
            all_files = os.listdir(self.data_dir)
            self.user_files = [f for f in all_files if f.startswith('User') and os.path.isfile(os.path.join(self.data_dir, f))]
            logger.info(f"匹配到的用户文件：{self.user_files}")
        else:
            logger.error("数据目录不存在或不是文件夹")

    def process_datasets(self):
        datasets = {}
        logger.info(f"开始处理 {len(self.user_files)} 个用户文件")
        for user_file in self.user_files:
            user_id = user_file.replace('.txt', '').replace('User', 'User')
            file_path = os.path.join(self.data_dir, user_file)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                commands = self._parse_commands(content)
                file_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()
                datasets[user_id] = {
                    'original_file': user_file,
                    'hash': file_hash,
                    'command_count': len(commands),
                    'unique_commands': len(set(commands)),
                    'commands': commands,
                    'statistics': self._generate_statistics(commands)
                }
                logger.info(f"✅ 已处理：{user_id} - {len(commands)} 条命令")
            except Exception as e:
                logger.error(f"处理文件 {user_file} 失败：{e}")
        return datasets

    def _parse_commands(self, content):
        return [line.strip() for line in content.strip().splitlines() if line.strip()]

    def _generate_statistics(self, commands):
        command_counts = Counter(commands)
        return {
            'total_commands': len(commands),
            'unique_commands': len(set(commands)),
            'most_common': dict(command_counts.most_common(10))
        }

    # ✅ 新增：生成伪造数据
    def generate_fake_data(self, original_dataset, fake_type='modified'):
        commands = original_dataset['commands'].copy()
        if fake_type == 'modified':
            return self._generate_modified_commands(commands)
        elif fake_type == 'synthetic':
            return self._generate_synthetic_commands(original_dataset)
        elif fake_type == 'shuffled':
            return self._generate_shuffled_commands(commands)
        else:
            raise ValueError(f"Unknown fake type: {fake_type}")

    def _generate_modified_commands(self, commands):
        num_to_modify = max(1, int(len(commands) * 0.2))
        indices = random.sample(range(len(commands)), min(num_to_modify, len(commands)))
        modified = commands.copy()
        modifications = []
        for idx in indices:
            original = modified[idx]
            modified[idx] = original + "_modified"
            modifications.append({'index': idx, 'original': original, 'modified': modified[idx]})
        return {
            'type': 'modified',
            'commands': modified,
            'modifications': modifications,
            'modification_rate': len(indices) / len(commands)
        }

    def _generate_synthetic_commands(self, original_dataset):
        stats = original_dataset['statistics']
        command_freq = stats['most_common']
        commands = list(command_freq.keys())
        weights = list(command_freq.values())
        target_length = original_dataset['command_count']
        synthetic = []
        for _ in range(target_length):
            cmd = random.choices(commands, weights=weights)[0] if commands else 'echo'
            synthetic.append(cmd)
        return {
            'type': 'synthetic',
            'commands': synthetic,
            'based_on': original_dataset['original_file']
        }

    def _generate_shuffled_commands(self, commands):
        shuffled = commands.copy()
        random.shuffle(shuffled)
        return {
            'type': 'shuffled',
            'commands': shuffled
        }

    def save_processed_data(self, datasets, output_file='processed_datasets.json'):
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(datasets, f, indent=2, ensure_ascii=False)
        logger.info(f"✅ 已保存处理结果到 {output_file}")

# 主程序入口
def main():
    preprocessor = SchonlauDataPreprocessor()
    datasets = preprocessor.process_datasets()
    if datasets:
        preprocessor.save_processed_data(datasets)
        print("🎉 数据预处理完成！")
    else:
        print("⚠️ 未处理到任何数据集，请检查 schonlau 文件夹路径和文件内容。")

if __name__ == '__main__':
    main()
