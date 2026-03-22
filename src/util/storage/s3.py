import boto3
import pandas as pd

from util.storage.base import Storage

# TODO: so many things wrong with this, but im only a one man band so alas.
S3_BUCKET_NAME_PROD = "trading-bot-data-prod"

# TODO: move somewhere else.
LOADERS = {
    "parquet": pd.read_parquet,
    "csv": pd.read_csv,
    "json": pd.read_json,
}


class S3Storage(Storage):
    def __init__(self, bucket):

        self.bucket = bucket
        self.s3 = boto3.client("s3")

    def write_bytes(self, data: bytes, path: str):
        self.s3.put_object(Bucket=self.bucket, Key=path, Body=data)

    def read_bytes(self, path: str) -> bytes:
        obj = self.s3.get_object(Bucket=self.bucket, Key=path)

        return obj["Body"].read()
