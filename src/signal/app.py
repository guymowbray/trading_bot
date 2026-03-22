import pandas as pd

from src.signal.extractor import (
    load_market_data_batches_using_metadata,
)
from src.util.util import (
    DATA_DIR,
    DATA_DIR_LOCAL,
    EQUITY_DIR,
    INDEX_DIR,
    MA50,
    MA200,
    MACRO_DIR,
    VOLUME,
)
from util.file_io import load_file
from util.serializer.json import JsonSerializer
from util.serializer.parquet import ParquetSerializer
from util.storage.local import LocalStorage
from util.storage.s3 import S3_BUCKET_NAME_PROD, S3Storage


def calculate_moving_averages(data: pd.DataFrame):
    data[MA50] = data["Close"].rolling(window=50).mean()
    data[MA200] = data["Close"].rolling(window=200).mean()
    data["MEAN_VOL"] = data[VOLUME].rolling(window=50).mean()

    return data


def calculate_percent_away_from_ma(data: pd.DataFrame):
    """
    2% -> 5% healthty
    5% -> 10% Still good
    15% -> extended
    """
    data["perct_from_ma50"] = (data["Close"] - data[MA50]) / data[MA50] * 100
    data["perct_from_ma200"] = (data["Close"] - data[MA200]) / data[MA200] * 100
    data["perct_from_mean_vol"] = (data[VOLUME] - data["MEAN_VOL"]) / data["MEAN_VOL"] * 100

    return data


def calculate_signals(market_data_batch: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    payload = {}

    for ticker_name, data in market_data_batch.items():
        data = calculate_moving_averages(data)
        data = calculate_percent_away_from_ma(data)

        payload[ticker_name] = data

        return payload


def signal_app(execution_id: str, execution_date: str, save_location: str):
    parquet_serializer = ParquetSerializer()
    json_serializer = JsonSerializer()

    if save_location == "local":
        storage_client = LocalStorage()
        base_dir = DATA_DIR_LOCAL

    elif save_location == "s3":
        storage_client = S3Storage(bucket=S3_BUCKET_NAME_PROD)
        base_dir = DATA_DIR
    else:
        raise ValueError(f"Unsupported save location: {save_location}")

    # TODO: Can dry this up somehow but dont want to optimise now.
    loaded_macro_metadata = load_file(
        storage_client,
        json_serializer,
        f"{base_dir}/{MACRO_DIR}/{execution_date}/{execution_id}/metadata_{execution_id}.json",
    )
    loaded_index_metadata = load_file(
        storage_client,
        json_serializer,
        f"{base_dir}/{INDEX_DIR}/{execution_date}/{execution_id}/metadata_{execution_id}.json",
    )
    loaded_equities_metadata = load_file(
        storage_client,
        json_serializer,
        f"{base_dir}/{EQUITY_DIR}/{execution_date}/{execution_id}/metadata_{execution_id}.json",
    )

    macro_market_data_batch = load_market_data_batches_using_metadata(
        meta_data=loaded_macro_metadata, serializer=parquet_serializer, storage=storage_client
    )
    index_market_data_batch = load_market_data_batches_using_metadata(
        metadata=loaded_index_metadata, serializer=parquet_serializer, storage=storage_client
    )
    equities_market_data_batch = load_market_data_batches_using_metadata(
        metadata=loaded_equities_metadata, serializer=parquet_serializer, storage=storage_client
    )

    _processed_macro_data = calculate_signals(macro_market_data_batch)
    _processed_index_data = calculate_signals(index_market_data_batch)
    _processed_equities_data = calculate_signals(equities_market_data_batch)


if __name__ == "__main__":
    test_execution_id = "20260321_152729_4bc0ba07d7044b32ae2643e8a89a540f"
    execution_date = "2026/03/21"
    save_location = "s3"
    signal_app(test_execution_id, execution_date, save_location)
