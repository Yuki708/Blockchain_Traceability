# client/experiment_runner.py
import time
import json
import os
import sys
sys.path.append('..')

from trace_client import TraceClient
from data_loader import load_email_data
from metrics import Metrics

LOG_DIR = '../logs'
os.makedirs(LOG_DIR, exist_ok=True)

def run_experiment(data_limit):
    client = TraceClient()
    data = load_email_data('/project/email_1w.csv', limit=data_limit)
    metrics = Metrics()

    store_log = []
    trace_log = []

    # 存储阶段
    for item in data:
        fakedata = item['id']
        resp = client.store_data(fakedata, metadata=item)
        store_log.append({
            'fakedata': fakedata,
            'status_code': resp.status_code,
            'response': resp.text
        })

    # 追溯阶段
    for item in data:
        fakedata = item['id']
        start = time.time()
        try:
            resp = client.trace_data(fakedata)
            metrics.record_trace(start, success=(resp.status_code == 200))
            trace_log.append({
                'fakedata': fakedata,
                'status_code': resp.status_code,
                'response': resp.text,
                'trace_time': time.time() - start
            })
        except Exception as e:
            metrics.record_trace(start, success=False)
            trace_log.append({
                'fakedata': fakedata,
                'error': str(e),
                'trace_time': time.time() - start
            })

    # 保存中间数据
    with open(f'{LOG_DIR}/store_log.json', 'w') as f:
        json.dump(store_log, f, indent=2)

    with open(f'{LOG_DIR}/trace_log.json', 'w') as f:
        json.dump(trace_log, f, indent=2)

    with open(f'{LOG_DIR}/metrics.json', 'w') as f:
        json.dump(metrics.summary(), f, indent=2)

    print("Success! pleace cp logs/ for .jsons")

if __name__ == '__main__':
    run_experiment(data_limit=3000)  # 可修改测试数据条数
