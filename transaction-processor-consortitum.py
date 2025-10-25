import sys
import logging
import hashlib
import json
from sawtooth_sdk.processor.core import TransactionProcessor
from sawtooth_sdk.processor.handler import TransactionHandler
from sawtooth_sdk.protobuf.transaction_pb2 import TransactionHeader
from sawtooth_sdk.processor.exceptions import InvalidTransaction
from transaction_family import State

logger = logging.getLogger(__name__)

FAMILY_NAME = "example_family"
NAMESPACE = hashlib.sha512(FAMILY_NAME.encode('utf-8')).hexdigest()[0:6]

class ConsortiumTransactionHandler(TransactionHandler):
    def __init__(self):
        super().__init__()

    @property
    def family_name(self):
        return FAMILY_NAME

    @property
    def family_versions(self):
        return ['1.0']

    @property
    def namespaces(self):
        return [NAMESPACE]

    def apply(self, transaction, context):
        header = transaction.header
        signer = header.signer_public_key
        payload = json.loads(transaction.payload.decode())
        
        operation = payload['operation']
        args = payload['args']
        
        state = State(context)
        
        if operation == "authenticate_user":
            self._authenticate_user(state, args, signer)
        elif operation == "upload_asset":
            self._upload_asset(state, args, signer)
        elif operation == "transfer_asset":
            self._transfer_asset(state, args, signer)
        elif operation == "query_asset":
            self._query_asset(state, args, signer)
        elif operation == "trace_asset":
            self._trace_asset(state, args, signer)
        else:
            raise InvalidTransaction(f"未知的操作: {operation}")

    def _authenticate_user(self, state, args, signer):
        user_id = args[0]
        user_address = state._make_user_address(user_id)

        # 记录用户的公钥，便于后续身份验证
        state.set(user_address, signer)
        logger.info(f"用户 {user_id} 认证成功，公钥已记录。")

    def _upload_asset(self, state, args, signer):
        user_id, asset_data, asset_id = args
        asset_address = state._make_asset_address(user_id, asset_id)
        
        # 使用签名哈希进行溯源
        asset_signature_hash = hashlib.sha256((asset_data + signer).encode()).hexdigest()

        if state.get(asset_address):
            raise InvalidTransaction("资产已存在")

        asset_entry = {
            'asset_id': asset_id,
            'data': asset_data,
            'signatures': [asset_signature_hash],
            'publisher': signer
        }

        state.set(asset_address, json.dumps(asset_entry))
        logger.info(f"资产 {asset_id} 上传成功，签名哈希: {asset_signature_hash}")

    def _transfer_asset(self, state, args, signer):
        seller_id, buyer_id, asset_id = args
        asset_address = state._make_asset_address(seller_id, asset_id)
        
        asset_entry = json.loads(state.get(asset_address))
        if asset_entry['publisher'] != signer:
            raise InvalidTransaction("只有发布者可以转移资产")

        # 更新签名，包含卖家和买家的签名
        buyer_signature_hash = hashlib.sha256((asset_entry['data'] + signer).encode()).hexdigest()
        asset_entry['signatures'].append(buyer_signature_hash)
        asset_entry['publisher'] = buyer_id
        
        state.set(asset_address, json.dumps(asset_entry))
        logger.info(f"资产 {asset_id} 从 {seller_id} 成功转移至 {buyer_id}，新签名哈希: {buyer_signature_hash}")

    def _query_asset(self, state, args, signer):
        user_id, asset_id = args
        asset_address = state._make_asset_address(user_id, asset_id)
        
        asset_entry = json.loads(state.get(asset_address))
        if not asset_entry:
            raise InvalidTransaction("资产不存在")

        # 只显示当前用户的签名哈希
        user_signature = hashlib.sha256((asset_entry['data'] + signer).encode()).hexdigest()
        logger.info(f"用户 {user_id} 查询资产 {asset_id} 的签名哈希: {user_signature}")

    def _trace_asset(self, state, args, signer):
        if not self._is_admin(signer):
            raise InvalidTransaction("只有管理员可以执行溯源功能")

        asset_id = args[0]
        trace_info = []

        for user_id in state.list_users():
            asset_address = state._make_asset_address(user_id, asset_id)
            asset_entry = json.loads(state.get(asset_address))
            if asset_entry:
                trace_info.append({
                    "user_id": user_id,
                    "signatures": asset_entry['signatures']
                })

        logger.info(f"资产 {asset_id} 的溯源信息: {trace_info}")

    def _is_admin(self, signer):
        # 简单判断是否管理员，实际中可根据特定地址或公钥判断
        return signer == "admin_public_key"


def main():
    if len(sys.argv) < 2:
        print("用法: python3 transaction_processor.py <连接地址>")
        sys.exit(1)
    
    url = sys.argv[1]
    processor = TransactionProcessor(url=url)
    handler = ConsortiumTransactionHandler()
    processor.add_handler(handler)

    try:
        processor.start()
    except KeyboardInterrupt:
        pass
    finally:
        processor.stop()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()

