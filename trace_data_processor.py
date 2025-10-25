# processor/trace_data_processor.py
import hashlib
import json
import logging
from sawtooth_sdk.processor.handler import TransactionHandler
from sawtooth_sdk.processor.exceptions import InvalidTransaction, InternalError
from sawtooth_sdk.processor.core import TransactionProcessor

# 日志配置
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

# 命名空间前缀
FAMILY_NAME = "trace_data"
NAMESPACE = hashlib.sha512(FAMILY_NAME.encode('utf-8')).hexdigest()[:6]

# 地址生成函数
def make_address(key):
    return NAMESPACE + hashlib.sha512(key.encode('utf-8')).hexdigest()[:64]

# 处理器主类
class TraceDataHandler(TransactionHandler):
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
        try:
            payload = json.loads(transaction.payload.decode('utf-8'))
            action = payload.get('action')
            fakedata = payload.get('fakedata')
            metadata = payload.get('metadata', {})

            if not fakedata:
                raise InvalidTransaction("Missing fakedata")

            address = make_address(fakedata)
            state_data = self._get_state(context, address)

            if action == 'store':
                if state_data:
                    raise InvalidTransaction("fakedata already exists")
                data = {
                    'fakedata': fakedata,
                    'metadata': metadata,
                    'timestamp': payload.get('timestamp')
                }
                self._set_state(context, address, json.dumps(data))
                LOGGER.info(f"Stored fakedata: {fakedata}")

            elif action == 'trace':
                if not state_data:
                    raise InvalidTransaction("fakedata not found")
                LOGGER.info(f"Traced fakedata: {fakedata}")
                # 追溯操作不修改状态，仅读取
            else:
                raise InvalidTransaction("Invalid action")

        except Exception as e:
            LOGGER.error(f"Transaction error: {e}")
            raise InvalidTransaction(f"Error processing transaction: {e}")

    def _get_state(self, context, address):
        entries = context.get_state([address])
        if entries:
            return json.loads(entries[0].data.decode('utf-8'))
        return None

    def _set_state(self, context, address, data):
        context.set_state({address: data.encode('utf-8')})

# 启动函数
def main():
    processor = TransactionProcessor(url='tcp://validator-0:4004')
    handler = TraceDataHandler()
    processor.add_handler(handler)
    LOGGER.info("TraceData Transaction Processor started")
    processor.start()

if __name__ == '__main__':
    main()
