# client/metrics.py
import time

class Metrics:
    def __init__(self):
        self.trace_times = []
        self.success_count = 0
        self.failure_count = 0

    def record_trace(self, start_time, success=True):
        elapsed = time.time() - start_time
        self.trace_times.append(elapsed)
        if success:
            self.success_count += 1
        else:
            self.failure_count += 1

    def summary(self):
        total = self.success_count + self.failure_count
        avg_time = sum(self.trace_times) / len(self.trace_times) if self.trace_times else 0
        success_rate = self.success_count / total if total > 0 else 0
        return {
            'average_trace_time': avg_time,
            'success_rate': success_rate,
            'total_traces': total
        }
