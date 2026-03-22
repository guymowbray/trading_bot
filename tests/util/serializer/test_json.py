from util.serializer.json import JsonSerializer


def test_json_serialize_successfully():

    serializer = JsonSerializer()

    data = {"ticker": "AAPL", "price": 150}

    result = serializer.serialize(data)

    assert isinstance(result, bytes)

    assert b"AAPL" in result


def test_json_deserialize_successfully():

    serializer = JsonSerializer()

    data = b'{"ticker":"AAPL"}'

    result = serializer.deserialize(data)

    assert result["ticker"] == "AAPL"


def test_json_roundtrip_successfully():

    serializer = JsonSerializer()

    original = {"ticker": "TSLA", "signal": "buy", "price": 200.5}

    data = serializer.serialize(original)

    loaded = serializer.deserialize(data)

    assert loaded == original
