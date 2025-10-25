import random
import time
import requests
from hashlib import sha512
import cbor2
from sawtooth_sdk.protobuf.batch_pb2 import BatchHeader, Batch, BatchList
from sawtooth_sdk.protobuf.transaction_pb2 import TransactionHeader, Transaction
from sawtooth_signing import create_context, CryptoFactory
import secrets
import os
import gc

# 实验参数
FAMILY_NAME = "example_family"
FAMILY_VERSION = "1.0"
context = create_context('secp256k1')
private_key = context.new_random_private_key()
signer = CryptoFactory(context).new_signer(private_key)
N = 500  # 每个区块的交易数量
V = 100   # 区块数量

# 确保结果文件夹存在
os.makedirs("results", exist_ok=True)

def generate_address(user_id, data_id=None):
    prefix = sha512(FAMILY_NAME.encode('utf-8')).hexdigest()[0:6]
    if data_id:
        return prefix + sha512(f"{user_id}{data_id}".encode('utf-8')).hexdigest()[0:64]
    return prefix + sha512(user_id.encode('utf-8')).hexdigest()[0:64]

def insert_transactions(txns, batch_size=50):
    # 分批提交
    for i in range(0, len(txns), batch_size):
        batch_txns = txns[i:i+batch_size]
        batch_header_bytes = BatchHeader(
            signer_public_key=signer.get_public_key().as_hex(),
            transaction_ids=[txn.header_signature for txn in batch_txns]
        ).SerializeToString()
        
        signature = signer.sign(batch_header_bytes)
        batch = Batch(
            header=batch_header_bytes,
            header_signature=signature,
            transactions=batch_txns,
            trace=True
        )

        batch_list_bytes = BatchList(batches=[batch]).SerializeToString()
        
        # 记录提交时间
        start_batch_time = time.time()
     	# 发送批次到Sawtooth REST API
        response = requests.post('http://localhost:8880/batches', data=batch_list_bytes)
        batch_time = time.time() - start_batch_time
        


    # 强制进行垃圾回收
    gc.collect()

def create_transactions(user_id, num_transactions=N):
    txns = []
    address = generate_address(user_id)
    for _ in range(num_transactions):
        data_id = str(random.getrandbits(256))
        data = {
            "MID": random.getrandbits(256),
            "B": random.getrandbits(256),
            "Omega": random.getrandbits(256),
            "m": random.getrandbits(256),
            "Id": random.getrandbits(256),
            "Sig": random.getrandbits(256)
        }
        operation = "upload_data"
        payload = [operation, user_id, data, data_id]
        payload_bytes = cbor2.dumps(payload)

        txn_header_bytes = TransactionHeader(
            family_name=FAMILY_NAME,
            family_version=FAMILY_VERSION,
            inputs=[address],
            outputs=[address],
            signer_public_key=signer.get_public_key().as_hex(),
            batcher_public_key=signer.get_public_key().as_hex(),
            dependencies=[],
            payload_sha512=sha512(payload_bytes).hexdigest(),
            nonce=secrets.token_hex(16)
        ).SerializeToString()

        signature = signer.sign(txn_header_bytes)
        txn = Transaction(
            header=txn_header_bytes,
            header_signature=signature,
            payload=payload_bytes
        )
        txns.append(txn)
    return txns

def experiment_computation_time():
    computation_times = []
    for blocks in range(1, V + 1):
        start_time = time.time()
        for user_id in range(1, blocks + 1):
            txns = create_transactions(str(user_id))
            insert_transactions(txns)
        computation_time = time.time() - start_time
        computation_times.append((blocks, computation_time))
        print(f"已处理区块数: {blocks}, 计算时间: {computation_time:.2f}秒")

    with open("results/computation_data.txt", "w") as f:
        for blocks, time_ in computation_times:
            f.write(f"{blocks},{time_}\n")

def experiment_transaction_time():
    transaction_times = []
    for tx_count in range(25, 501, 25):
        start_time = time.time()
        for _ in range(10):
            txns = create_transactions("test_user", num_transactions=tx_count)
            insert_transactions(txns)
        transaction_time = time.time() - start_time
        transaction_times.append((tx_count, transaction_time))
        print(f"已处理每个区块的交易数量: {tx_count}, 交易时间: {transaction_time:.2f}秒")

    with open("results/transaction_data.txt", "w") as f:
        for tx_count, time_ in transaction_times:
            f.write(f"{tx_count},{time_}\n")

def main():
    experiment_computation_time()
    experiment_transaction_time()

if __name__ == '__main__':
    main()

