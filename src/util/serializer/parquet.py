from io import BytesIO

import pandas as pd

from util.serializer.base import Serializer


class ParquetSerializer(Serializer):
    extension = "parquet"

    def __init__(self, compression="snappy"):

        self.compression = compression

    def serialize(self, df) -> bytes:

        buffer = BytesIO()

        df.to_parquet(
            # Hard coded to KISS.
            buffer,
            engine="pyarrow",
            compression=self.compression,
            index=True,
        )

        buffer.seek(0)

        return buffer.getvalue()

    def deserialize(self, data: bytes):

        return pd.read_parquet(BytesIO(data))
