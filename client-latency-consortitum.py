import os
import time
import requests
import json
import random
import cbor2
import secrets
from hashlib import sha512
from sawtooth_sdk.protobuf.batch_pb2 import BatchHeader, Batch, BatchList
from sawtooth_sdk.protobuf.transaction_pb2 import TransactionHeader, Transaction
from sawtooth_signing import create_context, CryptoFactory, Signer

# 实验参数
BLOCK_COUNT = 100  # 测试的区块数量

# NODE_URLS = ["http://localhost:8880", "http://localhost:8881", "http://localhost:8882", "http://localhost:8883", "http://localhost:8884"]
# RESULTS_FILE = "results/latency_data_5.json"

# NODE_URLS = ["http://localhost:8880", "http://localhost:8881", "http://localhost:8882", "http://localhost:8883", "http://localhost:8884", "http://localhost:8885", "http://localhost:8886", "http://localhost:8887", "http://localhost:8888", "http://localhost:8889"]
# RESULTS_FILE = "results/latency_data_10.json"

NODE_URLS = ["http://localhost:8880", "http://localhost:8881", "http://localhost:8882", "http://localhost:8883", "http://localhost:8884", "http://localhost:8885", "http://localhost:8886", "http://localhost:8887", "http://localhost:8888", "http://localhost:8889", "http://localhost:8890", "http://localhost:8891", "http://localhost:8892", "http://localhost:8893", "http://localhost:8894"]
RESULTS_FILE = "results/latency_data_15.json"

FAMILY_NAME = "example_family"
FAMILY_VERSION = "1.0"
context = create_context('secp256k1')

# 用户密钥初始化
user_private_key = context.new_random_private_key()
user_signer = CryptoFactory(context).new_signer(user_private_key)

# 确保结果文件夹存在
os.makedirs("results", exist_ok=True)

def generate_address(user_id, data_id=None):
    prefix = sha512(FAMILY_NAME.encode('utf-8')).hexdigest()[0:6]
    if data_id:
        return prefix + sha512(f"{user_id}{data_id}".encode('utf-8')).hexdigest()[0:64]
    return prefix + sha512(user_id.encode('utf-8')).hexdigest()[0:64]

def create_transaction(user_id, operation="upload_data"):
    data_id = str(random.getrandbits(256))
    data = {
        "MID": random.getrandbits(256),
        "B": random.getrandbits(256),
        "Omega": random.getrandbits(256),
        "m": random.getrandbits(256),
        "Id": random.getrandbits(256),
        "Sig": random.getrandbits(256)
    }
    payload = [operation, user_id, data, data_id]
    payload_bytes = cbor2.dumps(payload)

    # 创建交易头部
    txn_header_bytes = TransactionHeader(
        family_name=FAMILY_NAME,
        family_version=FAMILY_VERSION,
        inputs=[generate_address(user_id)],
        outputs=[generate_address(user_id)],
        signer_public_key=user_signer.get_public_key().as_hex(),
        batcher_public_key=user_signer.get_public_key().as_hex(),
        dependencies=[],
        payload_sha512=sha512(payload_bytes).hexdigest(),
        nonce=secrets.token_hex(16)
    ).SerializeToString()

    # 签名
    signature = user_signer.sign(txn_header_bytes)
    txn = Transaction(
        header=txn_header_bytes,
        header_signature=signature,
        payload=payload_bytes
    )
    return txn

def send_block():
    # 模拟发送区块
    print("发送区块...")

    # 创建一个交易并发送
    user_id = "user_1"
    txn = create_transaction(user_id, operation="upload_data")
    
    # 创建批处理头部
    batch_header_bytes = BatchHeader(
        signer_public_key=user_signer.get_public_key().as_hex(),
        transaction_ids=[txn.header_signature]
    ).SerializeToString()

    # 批处理签名
    batch_signature = user_signer.sign(batch_header_bytes)
    batch = Batch(
        header=batch_header_bytes,
        header_signature=batch_signature,
        transactions=[txn],
        trace=True
    )

    # 构建批处理列表
    batch_list_bytes = BatchList(batches=[batch]).SerializeToString()

    # 提交批处理
    try:
        response = requests.post('http://localhost:8880/batches', data=batch_list_bytes)
        if response.status_code == 202:
            print("区块已成功提交")
        # else:
            # print(f"区块提交失败: {response.text}")
    except requests.RequestException as e:
        print(f"请求失败: {e}")
        print(f"响应内容: {response.text}")

def check_block_reception():
    delays = []
    for node_url in NODE_URLS:
        send_time = time.time()
        send_block()
        
        received = False
        while not received:
            try:
                resp = requests.get(f"{node_url}/blocks")
                if resp.status_code == 200:
                    receive_time = time.time()
                    delay = receive_time - send_time
                    delays.append({"node": node_url, "delay": delay})
                    print(f"节点 {node_url} 传播延迟: {delay:.2f}秒")
                    received = True
            except requests.RequestException:
                print(f"无法访问 {node_url}，重试中...")
            time.sleep(1)
    return delays

def main():
    all_delays = []
    for block in range(BLOCK_COUNT):
        print(f"测试第 {block+1} 个区块的传播延迟")
        delays = check_block_reception()
        all_delays.append({"block": block + 1, "delays": delays})

    # 将结果保存为 JSON 格式
    with open(RESULTS_FILE, "w") as f:
        json.dump(all_delays, f, indent=4)
    print(f"结果已保存至 {RESULTS_FILE}")

if __name__ == '__main__':
    main()

