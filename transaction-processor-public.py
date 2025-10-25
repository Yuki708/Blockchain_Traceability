import sys
import logging
import hashlib
from sawtooth_sdk.processor.core import TransactionProcessor
from sawtooth_sdk.processor.handler import TransactionHandler
from sawtooth_sdk.protobuf.transaction_pb2 import TransactionHeader
from transaction_family import TransactionPayload, State

logger = logging.getLogger(__name__)
FAMILY_NAME = "example_family"
NAMESPACE = hashlib.sha512(FAMILY_NAME.encode('utf-8')).hexdigest()[0:6]

class PublicBlockchainTransactionHandler(TransactionHandler):
    def __init__(self):
        super().__init__()
    
    @property
    def family_name(self):
        return FAMILY_NAME  # 定义为你在代码中的交易家族名称

    @property
    def family_versions(self):
        return ['1.0']  # 定义该交易家族的版本

    @property
    def namespaces(self):
        return [NAMESPACE]  # 定义该交易家族的命名空间
    
    def apply(self, transaction, context):
        header = transaction.header
        signer = header.signer_public_key

        # 解析交易负载
        payload = TransactionPayload.from_bytes(transaction.payload)
        state = State(context)

        # 根据操作类型调用相应的处理方法
        if payload.operation == "authenticate":
            self._authenticate_user(state, payload.args)
        elif payload.operation == "upload_asset":
            self._upload_asset(state, payload.args, signer)
        elif payload.operation == "trade_asset":
            self._trade_asset(state, payload.args, signer)
        elif payload.operation == "query_asset":
            self._query_asset(state, payload.args, signer)
        elif payload.operation == "trace_asset":
            self._trace_asset(state, payload.args, signer)
        else:
            raise InvalidTransaction(f"未知的操作: {payload.operation}")

    def _authenticate_user(self, state, args):
        user_id = args[0]
        address = State.make_address(user_id)

        # 检查用户是否存在
        if state.get(address):
            logger.info(f"用户 {user_id} 已验证")
        else:
            raise InvalidTransaction(f"用户 {user_id} 不存在")

    def _upload_asset(self, state, args, signer):
        user_id, asset_data, asset_id = args
        address = State.make_address(user_id, asset_id)

        # 确保该数字资产未被上传
        if state.get(address):
            raise InvalidTransaction("该资产已存在")

        asset_entry = {
            'asset_id': asset_id,
            'asset_data': asset_data,
            'publisher': signer,
            'upload_block': "block_height_example"  # 示例区块高度
        }

        # 存储到区块链并反馈上传成功信息
        state.set(address, json.dumps(asset_entry))
        logger.info(f"资产 {asset_id} 成功上传")

    def _trade_asset(self, state, args, signer):
        seller_id, buyer_id, asset_id = args
        seller_address = State.make_address(seller_id, asset_id)

        # 检查资产是否存在以及是否属于交易者
        result = state.get(seller_address)
        if not result:
            raise InvalidTransaction(f"资产 {asset_id} 不存在")
        asset_entry = json.loads(result)
        
        if asset_entry['publisher'] != signer:
            raise InvalidTransaction("只有发布者可以转移资产")

        # 转移资产至买方
        buyer_address = State.make_address(buyer_id, asset_id)
        asset_entry['publisher'] = signer  # 更新所有者信息
        state.set(buyer_address, json.dumps(asset_entry))
        state.delete(seller_address)
        logger.info(f"资产 {asset_id} 成功从 {seller_id} 转移给 {buyer_id}")

    def _query_asset(self, state, args, signer):
        user_id, asset_id = args
        address = State.make_address(user_id, asset_id)

        # 返回资产信息
        result = state.get(address)
        if not result:
            raise InvalidTransaction(f"未找到资产 {asset_id}")
        logger.info(f"查询到的资产信息: {result}")

    def _trace_asset(self, state, args, signer):
        asset_id = args[0]

        # 假设可以访问区块链中的所有区块
        # 此处只示例：从某位置查询特定资产的原始上传信息
        for block in ["block_1", "block_2"]:  # 示例区块
            address = State.make_address(block, asset_id)
            result = state.get(address)
            if result:
                logger.info(f"溯源到的发布者信息: {result}")
                return
        raise InvalidTransaction(f"未找到 {asset_id} 的发布者")

def main():
    if len(sys.argv) < 2:
        print("用法: python3 transaction_processor.py <连接地址>")
        sys.exit(1)

    url = sys.argv[1]
    processor = TransactionProcessor(url=url)
    handler = PublicBlockchainTransactionHandler()
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

