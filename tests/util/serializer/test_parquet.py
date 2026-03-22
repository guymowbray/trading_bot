import pandas as pd

from util.serializer.parquet import ParquetSerializer


def test_parquet_custom_compression():

    serializer = ParquetSerializer(compression="gzip")

    df = pd.DataFrame({"a": [1, 2]})

    data = serializer.serialize(df)

    assert isinstance(data, bytes)


def test_parquet_preserves_index():

    serializer = ParquetSerializer()

    df = pd.DataFrame({"price": [100]}, index=pd.Index(["AAPL"]))

    data = serializer.serialize(df)

    loaded = serializer.deserialize(data)

    pd.testing.assert_frame_equal(df, loaded)


def test_parquet_datetime_index():

    serializer = ParquetSerializer()

    df = pd.DataFrame({"price": [100]}, index=pd.to_datetime(["2026-03-21"]))

    data = serializer.serialize(df)

    loaded = serializer.deserialize(data)

    pd.testing.assert_frame_equal(df, loaded)


def test_parquet_roundtrip():

    serializer = ParquetSerializer()

    original = pd.DataFrame({"ticker": ["AAPL", "TSLA"], "price": [150.5, 200.1]})

    data = serializer.serialize(original)

    loaded = serializer.deserialize(data)

    pd.testing.assert_frame_equal(original, loaded, check_dtype=True)
