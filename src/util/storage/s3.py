import boto3
from pathlib import Path


class S3Storage:

    def __init__(self, bucket):

        self.bucket = bucket
        self.s3 = boto3.client("s3")

    def write_bytes(self, data: bytes, path: str):

        self.s3.put_object(
            Bucket=self.bucket,
            Key=path,
            Body=data
        )

    def read_bytes(self, path: str) -> bytes:

        obj = self.s3.get_object(
            Bucket=self.bucket,
            Key=path
        )

        return obj['Body'].read()

class LocalStorage:

    def write_bytes(self, data: bytes, path: str):

        file_path = Path(path) 

        # create directories if missing
        file_path.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        with open(file_path, "wb") as f:
            f.write(data)

    def read_bytes(self, path: str) -> bytes:

        with open(path, "rb") as f:
            data = f.read()

        return data