import os
import uuid
from datetime import UTC, datetime
from typing import Dict

import pandas as pd

from market_data.extractor.yahoo import get_ticker_data
from src.util.util import (
    DATA_DIR,
    DATA_DIR_LOCAL,
    DATASETS,
    INDEX_TICKERS,
    MA50,
    MA200,
)
from util.serializer import JsonSerializer, ParquetSerializer
from util.storage import LocalStorage, S3Storage


class TickerDataQuery:
    ticker: str
    period: str


class TickerDataResult:
    ticker: str
    data: pd.DataFrame


def get_last_row_data(data):
    return data.loc[data.index[-1]]


def get_timestamp_from_last_row(data) -> pd.Timestamp:
    last_row = get_last_row_data(data)
    return last_row.name


def get_row_where_date_equals(data, date):
    """
    Find row data where date equals.
    Date format is '2025-02-14'

    :param data: pandas dataframe
    :param date: date(str)
    """
    return data.loc[data.index == date + " 00:00:00-05:00"]


def check_current_price_above_MA200(data: pd.DataFrame) -> bool:
    """
    Checks to see if current price is above the MA200 value.

    :param data: Description
    :type data: pd.DataFrame
    :return: Description
    :rtype: bool
    """
    if data.iloc[0]["Close"] >= data.iloc[0][MA200]:
        return True
    else:
        return False


def check_current_price_above_MA50(data: pd.DataFrame) -> bool:
    """
    Checks to see if current price is above the MA50 value.

    :param data: Description
    :type data: pd.DataFrame
    :return: Description
    :rtype: bool
    """
    if data.iloc[0]["Close"] >= data.iloc[0][MA50]:
        return True
    else:
        return False


def extract_ticker_data(ticker_list: dict = INDEX_TICKERS,):
    """
    Extract index data.
    """
    payload = {}

    for _, ticker_symbol in ticker_list.items():
        # TODO: add a data_source here.
        data = get_ticker_data(ticker_symbol)
        data = data.history(period="1y")
        payload[ticker_symbol] = data

    return payload


def generate_execution_uuid() -> str:
    """
    Generates a unique execution UUID using the current timestamp and a random UUID.

    :return: A unique execution UUID. eg 20260315_120505_5f2e4c8b9a1d4e5f8c9b0a7d6e3f2a1
    20260315_120505 is the timestamp and 5f2e4c8b9a1d4e5f8c9b0a7d6e3f2a1 is the random UUID.
    """
    return f"{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex}"


def save_pandas_dataframe(data: pd.DataFrame, filename: str, base_dir: str, file_extension: str):
    os.makedirs(base_dir, exist_ok=True)
    if file_extension == "parquet":
        data.to_parquet(f"{base_dir}/{filename}.{file_extension}")
    else:
        raise ValueError(f"Unsupported file extension: {file_extension}")


def save_file(df, storage, serializer, path):

    data = serializer.serialize(df)

    storage.write_bytes(
        data,
        path
    )


def save_raw_data(
    payload: Dict[str, pd.DataFrame], base_dir: str, execution_uuid: str, file_extension: str, storage_client: str, serializer
):
    # TODO: validate this properly, maybe make a MarketData model.
    # also should do validation elsewhere.
    if not payload:
        raise ValueError("Missing dict with ticker and data payload missing")

    # base_dir.mkdir(parents=True, exist_ok=True)

    for ticker, data in payload.items():
        fname = f"{ticker}_{execution_uuid}"

        save_file(
            df=data,
            storage=storage_client,
            serializer=serializer,
            path=f"{base_dir}/{fname}.{file_extension}"
        )

    return None


def create_dataset_metadata(data, dataset_name, execution_uuid, dataset_dir, file_extension):
    return {
        "dataset": dataset_name,
        "execution_uuid": execution_uuid,
        "tickers": list(data.keys()),
        "files": {
            ticker: f"{dataset_dir}/{ticker}_{execution_uuid}.{file_extension}"
            for ticker in data.keys()
        },
        "rows": {ticker: len(df) for ticker, df in data.items()},
    }


def save_metadata_file(metadata_payload: dict, base_dir: str, execution_uuid: str, storage_client, serializer):
    filename = f"metadata_{execution_uuid}.json"

    save_file(
        df=metadata_payload,
        storage=storage_client,
        serializer=serializer,
        path=f"{base_dir}/{filename}"
    )


def extract_main(today_date=None, run_uuid=None, save_location="local", file_extension="parquet"):
    # TODO: untested, need to add tests for this.
    today_date = today_date or datetime.today().strftime("%Y/%m/%d")
    run_uuid = run_uuid or generate_execution_uuid()

    # TODO: add S3 and test this.
    if save_location == "local":
        base_dir = DATA_DIR_LOCAL
        storage_client = LocalStorage()

    elif save_location == "s3":
        base_dir = DATA_DIR
        storage_client = S3Storage(bucket="trading-bot-data-prod")

    else:
        raise ValueError(f"Unsupported save location: {save_location}")


    for dataset_name, (dataset_dir_name, tickers) in DATASETS.items():
        if save_location == "local":
            dataset_dir = base_dir / dataset_dir_name / today_date / run_uuid

        elif save_location == "s3":
            dataset_dir = f"{base_dir}/{dataset_dir_name}/{today_date}/{run_uuid}"

        data = extract_ticker_data(ticker_list=tickers)

        parquet_serializer = ParquetSerializer()

        save_raw_data(
            payload=data,
            base_dir=dataset_dir,
            execution_uuid=run_uuid,
            file_extension=file_extension,
            storage_client=storage_client,
            serializer=parquet_serializer
        )

        metadata = create_dataset_metadata(
            data=data,
            dataset_name=dataset_name,
            execution_uuid=run_uuid,
            dataset_dir=dataset_dir,
            file_extension=file_extension,
        )

        save_metadata_file(
            metadata_payload=metadata,
            base_dir=dataset_dir,
            execution_uuid=run_uuid,
            storage_client=storage_client,
            serializer=JsonSerializer()
        )
