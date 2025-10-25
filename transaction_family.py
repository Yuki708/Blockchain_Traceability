import hashlib
import json
from sawtooth_sdk.processor.exceptions import InvalidTransaction
from sawtooth_sdk.processor.handler import TransactionHandler

# 定义交易家族名称和命名空间
FAMILY_NAME = "example_family"
NAMESPACE = hashlib.sha512(FAMILY_NAME.encode('utf-8')).hexdigest()[0:6]

# 定义交易负载类
class TransactionPayload:
    def __init__(self, operation, *args):
        self.operation = operation
        self.args = args

    @staticmethod
    def from_bytes(payload_bytes):
        try:
            payload_dict = json.loads(payload_bytes.decode('utf-8'))
            operation = payload_dict['operation']
            args = payload_dict['args']
            return TransactionPayload(operation, *args)
        except ValueError as e:
            raise InvalidTransaction(f"无效的交易负载: {e}")

# 定义状态管理类
class State:
    def __init__(self, context):
        self._context = context

    def get(self, address):
        state = self._context.get_state([address])
        if not state:
            return None
        return state.get(address)

    def set(self, address, data):
        state_data = {address: data.encode('utf-8')}
        self._context.set_state(state_data)

    def delete(self, address):
        self._context.delete_state([address])

    @staticmethod
    def _make_address(user_id, data_id):
    	# id_str = f"{user_id}{data_id}" if data_id else user_id
        return NAMESPACE + hashlib.sha512(f"{user_id}{data_id}".encode('utf-8')).hexdigest()[0:64]

    @staticmethod
    def _make_user_address(user_id):
        return NAMESPACE + hashlib.sha512(user_id.encode('utf-8')).hexdigest()[0:64]

class InvalidAction(Exception):
    pass

