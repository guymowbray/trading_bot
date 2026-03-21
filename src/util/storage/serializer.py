from io import BytesIO
import json


class ParquetSerializer:

    def serialize(self, df):

        buffer = BytesIO()

        df.to_parquet(
            buffer,
            engine="pyarrow",
            compression="snappy",
            index=False
        )

        buffer.seek(0)

        return buffer.getvalue()
    

class JsonSerializer:

    extension = "json"

    def serialize(self, obj) -> bytes:

        json_str = json.dumps(
            obj,
            default=str   # handles dates etc
        )

        return json_str.encode("utf-8")
    