# performance_analyzer_no_pandas.py
# Copyright 2024 Hyperledger Sawtooth Data Trace Experiment
# Licensed under Apache License 2.0

import time
import statistics
import logging
import json

logger = logging.getLogger(__name__)

class PerformanceAnalyzer:
    def __init__(self):
        self.metrics = {}
        self.start_time = None
        self.end_time = None

    def start_monitoring(self):
        self.start_time = time.time()

    def stop_monitoring(self):
        self.end_time = time.time()

    def record_transaction_metrics(self, count, total_time, errors=0):
        self.metrics['transactions'] = {
            'count': count,
            'total_time': total_time,
            'errors': errors,
            'throughput': count / total_time if total_time > 0 else 0,
            'error_rate': errors / count if count > 0 else 0,
            'avg_time': total_time / count if count > 0 else 0
        }

    def record_query_metrics(self, query_count, total_time, results_size=0):
        self.metrics['queries'] = {
            'count': query_count,
            'total_time': total_time,
            'results_size': results_size,
            'avg_time': total_time / query_count if query_count > 0 else 0,
            'throughput': query_count / total_time if total_time > 0 else 0
        }

    def record_blockchain_metrics(self, block_count, transaction_count, state_size):
        self.metrics['blockchain'] = {
            'block_count': block_count,
            'transaction_count': transaction_count,
            'state_size': state_size,
            'avg_transactions_per_block': transaction_count / block_count if block_count > 0 else 0
        }

    def record_resource_usage(self, cpu_percent, memory_delta_mb, disk_io_mb, network_io_mb):
        self.metrics['resource_usage'] = {
            'cpu_percent': cpu_percent,
            'memory_delta_mb': memory_delta_mb,
            'disk_io_mb': disk_io_mb,
            'network_io_mb': network_io_mb
        }

    def generate_performance_report(self):
        report = []
        report.append("PERFORMANCE ANALYSIS REPORT")
        report.append("=" * 50)
        if self.start_time and self.end_time:
            duration = self.end_time - self.start_time
            report.append(f"Monitoring Duration: {duration:.2f} seconds")

        if 'transactions' in self.metrics:
            trans = self.metrics['transactions']
            report.append(f"Transaction Performance:")
            report.append(f"  Total Transactions: {trans['count']}")
            report.append(f"  Throughput: {trans['throughput']:.2f} TPS")
            report.append(f"  Error Rate: {trans['error_rate']*100:.2f}%")
            report.append(f"  Average Time: {trans['avg_time']:.3f}s")

        if 'queries' in self.metrics:
            queries = self.metrics['queries']
            report.append(f"Query Performance:")
            report.append(f"  Total Queries: {queries['count']}")
            report.append(f"  Average Time: {queries['avg_time']:.3f}s")
            report.append(f"  Throughput: {queries['throughput']:.2f} QPS")

        if 'blockchain' in self.metrics:
            bc = self.metrics['blockchain']
            report.append(f"Blockchain Metrics:")
            report.append(f"  Block Count: {bc['block_count']}")
            report.append(f"  Transaction Count: {bc['transaction_count']}")
            report.append(f"  State Size: {bc['state_size']} bytes")
            report.append(f"  Avg Txs/Block: {bc['avg_transactions_per_block']:.2f}")

        return "\n".join(report)

    def get_metrics(self):
        return self.metrics

    def export_metrics(self, filename):
        with open(filename, 'w') as f:
            json.dump(self.metrics, f, indent=2)
