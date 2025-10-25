# Copyright 2024 Hyperledger Sawtooth Data Trace Experiment
# Licensed under Apache License 2.0

import hashlib
import json
import time
import requests
import base64
from sawtooth_sdk.protobuf import transaction_pb2
from sawtooth_sdk.protobuf import batch_pb2
from sawtooth_sdk.protobuf.batch_pb2 import Batch, BatchHeader, BatchList
from sawtooth_sdk.protobuf.transaction_pb2 import Transaction, TransactionHeader
from sawtooth_signing import create_context
from sawtooth_signing import CryptoFactory
from sawtooth_signing.secp256k1 import Secp256k1PrivateKey

class SawtoothClient:
    def __init__(self, base_url='http://rest-api-0:8008'):
        self.base_url = base_url
        self._context = create_context('secp256k1')
        self._private_key = Secp256k1PrivateKey.new_random()
        self._signer = CryptoFactory(self._context).new_signer(self._private_key)
        self._public_key = self._signer.get_public_key().as_hex()
        
    def register_dataset(self, dataset_id, dataset_hash, owner):
        """注册数据集"""
        payload = {
            'action': 'register_dataset',
            'dataset_id': dataset_id,
            'dataset_hash': dataset_hash,
            'owner': owner,
            'timestamp': int(time.time())
        }
        
        return self._send_transaction(payload)
        
    def trace_data(self, trace_id, dataset_id, tracer, trace_type, trace_data, confidence):
        """追溯数据"""
        payload = {
            'action': 'trace_data',
            'trace_id': trace_id,
            'dataset_id': dataset_id,
            'tracer': tracer,
            'trace_type': trace_type,
            'trace_data': trace_data,
            'confidence_score': confidence
        }
        
        return self._send_transaction(payload)
        
    def mark_fake_data(self, fake_id, original_dataset, fake_content, detection_method, confidence):
        """标记伪造数据"""
        payload = {
            'action': 'mark_fake',
            'fake_id': fake_id,
            'original_dataset': original_dataset,
            'fake_content': fake_content,
            'detection_method': detection_method,
            'confidence': confidence
        }
        
        return self._send_transaction(payload)
        
    def verify_dataset(self, dataset_id, claimed_hash):
        """验证数据集"""
        payload = {
            'action': 'verify_dataset',
            'dataset_id': dataset_id,
            'claimed_hash': claimed_hash
        }
        
        return self._send_transaction(payload)
        
    def get_dataset_info(self, dataset_id):
        """获取数据集信息"""
        address = self._make_dataset_address(dataset_id)
        response = requests.get(f"{self.base_url}/state/{address}")
        
        if response.status_code == 200:
            state_data = response.json()
            if 'data' in state_data:
                decoded_data = base64.b64decode(state_data['data']).decode('utf-8')
                return json.loads(decoded_data)
        return None
        
    def _send_transaction(self, payload):
        """发送交易"""
        # 创建交易头
        transaction_header = TransactionHeader(
            signer_public_key=self._public_key,
            family_name='data_trace',
            family_version='1.0',
            inputs=[self._get_namespace_prefix()],
            outputs=[self._get_namespace_prefix()],
            dependencies=[],
            payload_sha512=hashlib.sha512(json.dumps(payload).encode()).hexdigest(),
            batcher_public_key=self._public_key,
            nonce=str(time.time())
        )
        
        # 签名交易头
        transaction_header_bytes = transaction_header.SerializeToString()
        signature = self._signer.sign(transaction_header_bytes)
        
        # 创建交易
        transaction = Transaction(
            header=transaction_header_bytes,
            header_signature=signature,
            payload=json.dumps(payload).encode('utf-8')
        )
        
        # 创建批次
        batch_header = BatchHeader(
            signer_public_key=self._public_key,
            transaction_ids=[transaction.header_signature]
        )
        
        batch_header_bytes = batch_header.SerializeToString()
        batch_signature = self._signer.sign(batch_header_bytes)
        
        batch = Batch(
            header=batch_header_bytes,
            header_signature=batch_signature,
            transactions=[transaction]
        )
        
        batch_list = BatchList(batches=[batch])
        
        # 提交批次
        response = requests.post(
            f"{self.base_url}/batches",
            data=batch_list.SerializeToString(),
            headers={'Content-Type': 'application/octet-stream'}
        )
        
        if response.status_code not in [200, 202]:
            raise Exception(f"Failed to submit transaction: {response.text}")
            
        return response.json()
        
    def _get_namespace_prefix(self):
        """获取命名空间前缀"""
        return hashlib.sha512('data_trace'.encode('utf-8')).hexdigest()[:6]
        
    def _make_dataset_address(self, dataset_id):
        """生成数据集地址"""
        prefix = self._get_namespace_prefix()
        return prefix + hashlib.sha512(dataset_id.encode('utf-8')).hexdigest()[:64]
