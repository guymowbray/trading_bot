import json
from io import BytesIO


class ParquetSerializer:
    def serialize(self, df):

        buffer = BytesIO()

        df.to_parquet(
            # Hard coded to KISS.
            buffer,
            engine="pyarrow",
            compression="snappy",
            index=True,
        )

        buffer.seek(0)

        return buffer.getvalue()


class JsonSerializer:
    extension = "json"

    def serialize(self, obj) -> bytes:

        json_str = json.dumps(
            obj,
            default=str,  # handles dates etc
        )

        return json_str.encode("utf-8")
