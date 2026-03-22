import json

from util.serializer.base import Serializer


class JsonSerializer(Serializer):
    extension = "json"

    def __init__(self, encoding="utf-8"):

        self.encoding = encoding

    def serialize(self, obj) -> bytes:

        json_str = json.dumps(
            obj,
            default=str,
        )

        return json_str.encode(self.encoding)

    def deserialize(self, obj) -> bytes:
        decoded_obj = obj.decode(self.encoding)

        return json.loads(decoded_obj)
