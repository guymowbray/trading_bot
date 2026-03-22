import boto3
from moto import mock_aws

from src.util.storage import LocalStorage, S3Storage


def test_local_storage_write_and_read_successfully(tmp_path):

    storage = LocalStorage(tmp_path)

    data = b"hello world"

    storage.write_bytes(data, f"{tmp_path}/signals/test.txt")

    result = storage.read_bytes(f"{tmp_path}/signals/test.txt")

    assert result == data


@mock_aws
def test_s3_write_bytes():
    region = "us-west-1"
    bucket_name = "test-bucket"
    
    s3 = boto3.client("s3", region_name=region)

    s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": region})

    storage = S3Storage(bucket_name)

    storage.write_bytes(b"hello", "signals/test.txt")

    obj = s3.get_object(Bucket=bucket_name, Key="signals/test.txt")

    assert obj["Body"].read() == b"hello"
