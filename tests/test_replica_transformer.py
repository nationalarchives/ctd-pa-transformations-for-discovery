import json
import pathlib
import types
import pytest
from pathlib import Path
import sys

repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root))
from src.transformers import ReplicaDataTransformer

class DummyBody:
    def __init__(self, payload: bytes):
        self._payload = payload
    def read(self):
        return self._payload

class DummyS3Client:
    def __init__(self, objects):
        # objects: dict key -> JSON-serialisable python object
        self.objects = objects
    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            from botocore.exceptions import ClientError
            raise ClientError({'Error': {'Code': 'NoSuchKey'}}, 'GetObject')
        payload = json.dumps(self.objects[Key]).encode('utf-8')
        return {'Body': DummyBody(payload)}

@pytest.fixture()
def sample_record():
    return {"record": {"iaid": "ABC123", "title": "Sample"}}

@pytest.fixture()
def s3_client():
    objects = {
        'metadata/ABC123.json': {"mock": True, "value": 42}
    }
    return DummyS3Client(objects)


def test_replica_transformer_enrich(sample_record, s3_client):
    rdt = ReplicaDataTransformer(bucket_name='ignored-bucket', prefix='metadata', s3_client=s3_client)
    enriched = rdt.transform(sample_record)
    assert enriched is not sample_record
    assert 'replica' in enriched['record']
    assert enriched['record']['replica']['value'] == 42


def test_replica_transformer_missing(sample_record, s3_client):
    rdt = ReplicaDataTransformer(bucket_name='ignored-bucket', prefix='metadata', s3_client=s3_client)
    # modify iaid to not exist
    missing = {"record": {"iaid": "ZZZ999"}}
    unchanged = rdt.transform(missing)
    assert 'replicaMetadata' not in unchanged['record']
