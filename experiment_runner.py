#!/usr/bin/env python3
# Copyright 2024 Hyperledger Sawtooth Data Trace Experiment
# Licensed under Apache License 2.0

import os
import sys
import json
import time
import hashlib
import logging
import argparse
import statistics
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

# 添加项目路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from sawtooth_client import SawtoothClient
from data_preprocessor import SchonlauDataPreprocessor
from fake_data_detector import FakeDataDetector
from performance_analyzer import PerformanceAnalyzer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataTraceExperiment:
    def __init__(self, config_file='experiment_config.json'):
        self.config = self.load_config(config_file)
        self.client = SawtoothClient("http://localhost:8880")
        self.preprocessor = SchonlauDataPreprocessor(self.config['data_dir'])
        self.fake_detector = FakeDataDetector()
        self.performance_analyzer = PerformanceAnalyzer()
        self.experiment_results = {
            'dataset_registration': [],
            'trace_operations': [],
            'fake_detection': [],
            'performance_metrics': {},
            'timestamps': {}
        }

    def load_config(self, config_file):
        default_config = {
            'data_dir': 'schonlau',
            'sawtooth_url': 'http://rest-api-0:8008',
            'num_fake_samples': 10,
            'trace_confidence_threshold': 0.8,
            'performance_test_rounds': 5,
            'output_dir': 'experiment_results',
            'plot_results': False  # 禁用 matplotlib
        }
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                user_config = json.load(f)
                default_config.update(user_config)
        return default_config

    def run_complete_experiment(self):
        os.makedirs(self.config['output_dir'], exist_ok=True)
        self.experiment_results['timestamps']['start'] = datetime.now().isoformat()

        logger.info("Phase 1: Data preprocessing")
        datasets = self.preprocessor.process_datasets()

        logger.info("Phase 2: Dataset registration")
        registration_results = self.phase2_dataset_registration(datasets)

        logger.info("Phase 3: Fake data generation and detection")
        fake_detection_results = self.phase3_fake_data_detection(datasets)

        logger.info("Phase 4: Data trace operations")
        trace_results = self.phase4_data_trace(datasets, fake_detection_results)

        logger.info("Phase 5: Performance evaluation")
        performance_results = self.phase5_performance_evaluation()

        self.save_experiment_results()
        self.generate_experiment_report()
        logger.info("✅ Experiment completed successfully")

    def phase2_dataset_registration(self, datasets):
        registration_results = []
        total_start_time = time.time()

        for dataset_id, dataset_info in datasets.items():
            try:
                start_time = time.time()
                result = self.client.register_dataset(
                    dataset_id=dataset_id,
                    dataset_hash=dataset_info['hash'],
                    owner='experiment_user'
                )
                registration_time = time.time() - start_time
                blockchain_info = self.client.get_dataset_info(dataset_id)

                registration_results.append({
                    'dataset_id': dataset_id,
                    'registration_time': registration_time,
                    'blockchain_hash': blockchain_info['hash'] if blockchain_info else None,
                    'verification_status': blockchain_info['hash'] == dataset_info['hash'] if blockchain_info else False,
                    'transaction_id': result.get('link', '')
                })
                logger.info(f"Registered dataset {dataset_id} in {registration_time:.2f}s")
            except Exception as e:
                logger.error(f"Failed to register dataset {dataset_id}: {str(e)}")
                registration_results.append({'dataset_id': dataset_id, 'error': str(e)})

        total_time = time.time() - total_start_time
        self.experiment_results['dataset_registration'] = {
            'results': registration_results,
            'total_time': total_time,
            'successful_registrations': len([r for r in registration_results if 'error' not in r])
        }
        return registration_results

    def phase3_fake_data_detection(self, datasets):
        fake_detection_results = []
        selected_datasets = list(datasets.items())[:self.config['num_fake_samples']]

        for dataset_id, dataset_info in selected_datasets:
            try:
                fake_data = self.preprocessor.generate_fake_data(dataset_info, 'modified')
                detection_result = self.fake_detector.detect_fake_data(
                    original_dataset=dataset_info,
                    test_data=fake_data,
                    detection_method='statistical'
                )

                if detection_result['is_fake']:
                    fake_id = f"fake_{dataset_id}_{int(time.time())}"
                    self.client.mark_fake_data(
                        fake_id=fake_id,
                        original_dataset=dataset_id,
                        fake_content=fake_data['commands'][:10],
                        detection_method=detection_result['method'],
                        confidence=detection_result['confidence']
                    )

                fake_detection_results.append({
                    'original_dataset': dataset_id,
                    'fake_data_type': fake_data['type'],
                    'detection_result': detection_result,
                    'blockchain_recorded': detection_result['is_fake']
                })
                logger.info(f"Processed fake detection for {dataset_id}")
            except Exception as e:
                logger.error(f"Fake detection failed for {dataset_id}: {str(e)}")

        self.experiment_results['fake_detection'] = fake_detection_results
        return fake_detection_results

    def phase4_data_trace(self, datasets, fake_results):
        trace_results = []
        for dataset_id in list(datasets.keys())[:5]:
            try:
                trace_id = f"trace_{dataset_id}_{int(time.time())}"
                verification_result = self.client.verify_dataset(
                    dataset_id=dataset_id,
                    claimed_hash=datasets[dataset_id]['hash']
                )
                trace_result = self.client.trace_data(
                    trace_id=trace_id,
                    dataset_id=dataset_id,
                    tracer='experiment_system',
                    trace_type='authentic',
                    trace_data={'verification_status': verification_result},
                    confidence=self.config['trace_confidence_threshold']
                )
                trace_results.append({
                    'dataset_id': dataset_id,
                    'trace_id': trace_id,
                    'trace_type': 'authentic',
                    'verification_passed': verification_result,
                    'transaction_id': trace_result.get('link', '')
                })
            except Exception as e:
                logger.error(f"Trace failed for {dataset_id}: {str(e)}")

        self.experiment_results['trace_operations'] = trace_results
        return trace_results

    def phase5_performance_evaluation(self):
        logger.info("Evaluating system performance...")
        performance_metrics = {}
        performance_metrics['throughput'] = self._test_transaction_throughput()
        performance_metrics['latency'] = self._test_transaction_latency()
        performance_metrics['query_performance'] = self._test_query_performance()
        performance_metrics['scalability'] = self._test_scalability()
        self.experiment_results['performance_metrics'] = performance_metrics
        return performance_metrics

    def _test_transaction_throughput(self):
        num_transactions = 50
        start_time = time.time()
        completed = 0
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for i in range(num_transactions):
                dataset_id = f"perf_test_{i}_{int(time.time())}"
                future = executor.submit(self.client.register_dataset, dataset_id, f"hash_{i}", 'performance_test')
                futures.append(future)
            for future in futures:
                try:
                    future.result()
                    completed += 1
                except Exception as e:
                    logger.error(f"Transaction failed: {e}")
        total_time = time.time() - start_time
        return {
            'total_transactions': num_transactions,
            'completed_transactions': completed,
            'total_time': total_time,
            'throughput_tps': completed / total_time if total_time > 0 else 0
        }

    def _test_transaction_latency(self):
        latencies = []
        for i in range(20):
            start_time = time.time()
            dataset_id = f"latency_test_{i}_{int(time.time())}"
            try:
                self.client.register_dataset(dataset_id, f"hash_{i}", 'latency_test')
                latencies.append(time.time() - start_time)
            except Exception as e:
                logger.error(f"Latency test failed: {e}")
        return {
            'latencies': latencies,
            'avg_latency': statistics.mean(latencies) if latencies else 0,
            'min_latency': min(latencies) if latencies else 0,
            'max_latency': max(latencies) if latencies else 0,
            'std_deviation': statistics.stdev(latencies) if len(latencies) > 1 else 0
        }

    def _test_query_performance(self):
        datasets = self.experiment_results.get('data_preprocessing', {}).get('datasets', {})
        query_times = []
        successful_queries = 0
        for dataset_id in list(datasets.keys())[:10]:
            start_time = time.time()
            try:
                dataset_info = self.client.get_dataset_info(dataset_id)
                if dataset_info:
                    successful_queries += 1
                    query_times.append(time.time() - start_time)
            except Exception as e:
                logger.error(f"Query failed for {dataset_id}: {e}")
        return {
            'total_queries': 10,
            'successful_queries': successful_queries,
            'query_times': query_times,
            'avg_query_time': statistics.mean(query_times) if query_times else 0,
            'query_success_rate': successful_queries / 10
        }

    def _test_scalability(self):
        scalability_results = []
        data_sizes = [10, 50, 100]
        for size in data_sizes:
            logger.info(f"Testing scalability with {size} datasets")
            start_time = time.time()
            success_count = 0
            for i in range(size):
                dataset_id = f"scale_test_{size}_{i}_{int(time.time())}"
                try:
                    self.client.register_dataset(dataset_id, f"hash_{size}_{i}", 'scalability_test')
                    success_count += 1
                except Exception as e:
                    logger.error(f"Scalability test transaction failed: {e}")
            total_time = time.time() - start_time
            scalability_results.append({
                'dataset_count': size,
                'successful_registrations': success_count,
                'total_time': total_time,
                'throughput': success_count / total_time if total_time > 0 else 0
            })
        return scalability_results

    def save_experiment_results(self):
        self.experiment_results['timestamps']['end'] = datetime.now().isoformat()
        start_time = datetime.fromisoformat(self.experiment_results['timestamps']['start'])
        end_time = datetime.fromisoformat(self.experiment_results['timestamps']['end'])
        self.experiment_results['experiment_duration'] = (end_time - start_time).total_seconds()
        results_file = os.path.join(self.config['output_dir'], 'experiment_results.json')
        with open(results_file, 'w') as f:
            json.dump(self.experiment_results, f, indent=2, default=str)
        logger.info(f"✅ Experiment results saved to {results_file}")

    def generate_experiment_report(self):
        report = []
        report.append("HYPERLEDGER SAWTOOTH DATA TRACE EXPERIMENT REPORT")
        report.append("=" * 60)
        report.append(f"Experiment Duration: {self.experiment_results['experiment_duration']:.2f} seconds")
        report.append(f"Start Time: {self.experiment_results['timestamps']['start']}")
        report.append(f"End Time: {self.experiment_results['timestamps']['end']}")
        report.append("")

        if 'data_preprocessing' in self.experiment_results:
            preprocessing = self.experiment_results['data_preprocessing']
            report.append("1. DATA PREPROCESSING RESULTS")
            report.append("-" * 40)
            report.append(f"Total Datasets: {preprocessing['statistics']['total_datasets']}")
            report.append(f"Total Commands: {preprocessing['statistics']['total_commands']}")
            report.append(f"Avg Commands per Dataset: {preprocessing['statistics']['avg_commands']:.1f}")
            report.append("")

        if 'dataset_registration' in self.experiment_results:
            registration = self.experiment_results['dataset_registration']
            report.append("2. DATASET REGISTRATION RESULTS")
            report.append("-" * 40)
            report.append(f"Total Registration Time: {registration['total_time']:.2f}s")
            report.append(f"Successful Registrations: {registration['successful_registrations']}")
            report.append("")

        if 'fake_detection' in self.experiment_results:
            fake_detection = self.experiment_results['fake_detection']
            total_fake = len([f for f in fake_detection if f['detection_result']['is_fake']])
            report.append("3. FAKE DATA DETECTION RESULTS")
            report.append("-" * 40)
            report.append(f"Total Fake Samples Generated: {len(fake_detection)}")
            report.append(f"Fake Samples Detected: {total_fake}")
            if fake_detection:
                avg_confidence = statistics.mean([f['detection_result']['confidence'] for f in fake_detection])
                report.append(f"Average Detection Confidence: {avg_confidence:.3f}")
            report.append("")

        if 'performance_metrics' in self.experiment_results:
            metrics = self.experiment_results['performance_metrics']
            report.append("4. PERFORMANCE METRICS")
            report.append("-" * 40)
            if 'throughput' in metrics:
                throughput = metrics['throughput']
                report.append(f"Transaction Throughput: {throughput['throughput_tps']:.2f} TPS")
            if 'latency' in metrics:
                latency = metrics['latency']
                report.append(f"Average Transaction Latency: {latency['avg_latency']:.3f}s")
            if 'query_performance' in metrics:
                query = metrics['query_performance']
                report.append(f"Average Query Time: {query['avg_query_time']:.3f}s")
            report.append("")

        report_file = os.path.join(self.config['output_dir'], 'experiment_report.txt')
        with open(report_file, 'w') as f:
            f.write('\n'.join(report))
        logger.info(f"✅ Experiment report saved to {report_file}")
        print('\n'.join(report))

def main():
    parser = argparse.ArgumentParser(description='Hyperledger Sawtooth Data Trace Experiment')
    parser.add_argument('--config', default='experiment_config.json', help='Configuration file')
    parser.add_argument('--phase', choices=['all', 'preprocess', 'register', 'fake', 'trace', 'performance'], default='all', help='Experiment phase to run')
    args = parser.parse_args()

    experiment = DataTraceExperiment(args.config)
    # 在 experiment_runner.py 的 main() 里加这一行
    print("🔍 实际使用的 sawtooth_url:", experiment.config['sawtooth_url'])
    try:
        if args.phase == 'all':
            experiment.run_complete_experiment()
        elif args.phase == 'preprocess':
            datasets = experiment.preprocessor.process_datasets()
            print(f"✅ Preprocessed {len(datasets)} datasets")
        elif args.phase == 'register':
            if os.path.exists('processed_datasets.json'):
                with open('processed_datasets.json') as f:
                    datasets = json.load(f)
                experiment.phase2_dataset_registration(datasets)
            else:
                print("❌ 请先运行预处理阶段：--phase preprocess")
        elif args.phase == 'fake':
            if os.path.exists('processed_datasets.json'):
                with open('processed_datasets.json') as f:
                    datasets = json.load(f)
                experiment.phase3_fake_data_detection(datasets)
            else:
                print("❌ 请先运行预处理阶段：--phase preprocess")
        elif args.phase == 'trace':
            print("⚠️ 追溯阶段需要前置阶段完成，建议直接运行 --phase all")
        elif args.phase == 'performance':
            experiment.phase5_performance_evaluation()
    except Exception as e:
        logger.error(f"实验运行失败：{e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
