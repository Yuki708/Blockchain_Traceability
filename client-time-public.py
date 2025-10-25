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
FAMILY_NAME = "public_blockchain_contract"
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
        # print(f"批次提交响应: {response.status_code}, 批次提交时间: {batch_time:.2f}秒")

    # 强制进行垃圾回收
    gc.collect()

def create_transactions(user_id, operation, data=None, data_id=None):
    txns = []
    address = generate_address(user_id, data_id)
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

# 认证合约
def authentication(user_id):
    operation = "validate_user"
    txns = create_transactions(user_id, operation)
    insert_transactions(txns)

# 上传区块链合约
def upload_asset(user_id, data, data_id):
    operation = "upload_data"
    txns = create_transactions(user_id, operation, data, data_id)
    insert_transactions(txns)
    # print(f"用户 {user_id} 的资产 {data_id} 上传成功。")

# 资产交易合约
def transfer_asset(sender_id, receiver_id, data_id):
    operation = "transfer_data"
    data = {"receiver": receiver_id}
    txns = create_transactions(sender_id, operation, data, data_id)
    insert_transactions(txns)
    print(f"资产 {data_id} 成功从用户 {sender_id} 转移给用户 {receiver_id}。")

# 资产查询合约
def query_asset(user_id, data_id):
    operation = "query_data"
    txns = create_transactions(user_id, operation, data_id=data_id)
    insert_transactions(txns)
    print(f"查询资产 {data_id} 的相关交易记录。")

# 溯源合约
def trace_origin_asset(data_id):
    operation = "trace_origin"
    user_id = "admin"  # 假定管理员身份
    txns = create_transactions(user_id, operation, data_id=data_id)
    insert_transactions(txns)
    print(f"溯源查询资产 {data_id} 的首次交易记录。")

# 实验计算时间
def experiment_computation_time():
    computation_times = []
    for blocks in range(1, V + 1):
        start_time = time.time()
        for user_id in range(1, blocks + 1):
            data_id = str(random.getrandbits(256))
            data = {
                "MID": random.getrandbits(256),
                "B": random.getrandbits(256),
                "Omega": random.getrandbits(256),
                "m": random.getrandbits(256),
                "Id": random.getrandbits(256),
                "Sig": random.getrandbits(256)
            }
            upload_asset(str(user_id), data, data_id)
        computation_time = time.time() - start_time
        computation_times.append((blocks, computation_time))
        print(f"已处理区块数: {blocks}, 计算时间: {computation_time:.2f}秒")

    with open("results/computation_data.txt", "w") as f:
        for blocks, time_ in computation_times:
            f.write(f"{blocks},{time_}\n")

# 实验交易时间
def experiment_transaction_time():
    transaction_times = []
    for tx_count in range(10, 501, 10):
        start_time = time.time()
        for _ in range(10):
            user_id = "test_user"
            data_id = str(random.getrandbits(256))
            data = {
                "MID": random.getrandbits(256),
                "B": random.getrandbits(256),
                "Omega": random.getrandbits(256),
                "m": random.getrandbits(256),
                "Id": random.getrandbits(256),
                "Sig": random.getrandbits(256)
            }
            upload_asset(user_id, data, data_id)
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

