# client/trace_client.py
import hashlib
import json
import time
import requests
from sawtooth_sdk.protobuf.transaction_pb2 import Transaction, TransactionHeader
from sawtooth_sdk.protobuf.batch_pb2 import Batch, BatchHeader, BatchList
from sawtooth_signing import create_context
from sawtooth_signing import CryptoFactory

FAMILY_NAME = "trace_data"
NAMESPACE = hashlib.sha512(FAMILY_NAME.encode('utf-8')).hexdigest()[:6]

def make_address(key):
    return NAMESPACE + hashlib.sha512(key.encode('utf-8')).hexdigest()[:64]

class TraceClient:
    def __init__(self, key_name='trace_client', validator_url='http://rest-api-0:8008'):
        self.context = create_context('secp256k1')
        self.private_key = self.context.new_random_private_key()
        self.signer = CryptoFactory(self.context).new_signer(self.private_key)
        self.validator_url = validator_url

    def store_data(self, fakedata, metadata):
        payload = {
            'action': 'store',
            'fakedata': fakedata,
            'metadata': metadata,
            'timestamp': time.time()
        }
        return self._send_transaction(payload)

    def trace_data(self, fakedata):
        payload = {
            'action': 'trace',
            'fakedata': fakedata,
            'timestamp': time.time()
        }
        return self._send_transaction(payload)

    def _send_transaction(self, payload):
        payload_bytes = json.dumps(payload).encode('utf-8')
        address = make_address(payload['fakedata'])

        header = TransactionHeader(
            signer_public_key=self.signer.get_public_key().as_hex(),
            family_name=FAMILY_NAME,
            family_version='1.0',
            inputs=[address],
            outputs=[address],
            dependencies=[],
            payload_sha512=hashlib.sha512(payload_bytes).hexdigest(),
            batcher_public_key=self.signer.get_public_key().as_hex(),
            nonce=str(time.time())
        ).SerializeToString()

        signature = self.signer.sign(header)

        transaction = Transaction(
            header=header,
            payload=payload_bytes,
            header_signature=signature
        )

        batch_header = BatchHeader(
            signer_public_key=self.signer.get_public_key().as_hex(),
            transaction_ids=[transaction.header_signature]
        ).SerializeToString()

        batch_signature = self.signer.sign(batch_header)

        batch = Batch(
            header=batch_header,
            transactions=[transaction],
            header_signature=batch_signature
        )

        batch_list = BatchList(batches=[batch])

        headers = {'Content-Type': 'application/octet-stream'}
        response = requests.post(
            f"{self.validator_url}/batches",
            data=batch_list.SerializeToString(),
            headers=headers
        )

        return response
