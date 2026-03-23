from datetime import UTC, datetime

import pandas as pd

from market_data.extractor.yahoo import get_ticker_data
from src.util.util import (
    MARKET_DATA_DOMAIN,
    DATA_DIR_LOCAL,
    DATASETS,
    INDEX_TICKERS,
    MA50,
    MA200,
    create_and_validate_s3_filepath,
    generate_execution_uuid,
)
from util.file_io import create_dataset_metadata, save_dataset_batch, save_metadata_file
from util.serializer.json import JsonSerializer
from util.serializer.parquet import ParquetSerializer
from util.storage.local import LocalStorage
from util.storage.s3 import S3_BUCKET_NAME_PROD, S3Storage


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


def extract_ticker_data(
    ticker_list: dict = INDEX_TICKERS,
):
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


def extract_main(today_date=None, run_uuid=None, save_location="local"):
    # TODO: untested, need to add tests for this.
    run_uuid = run_uuid or generate_execution_uuid()
    today_date = datetime.now(UTC).strftime("%Y/%m/%d")


    # TODO: add S3 and test this.
    if save_location == "local":
        storage_client = LocalStorage()

    elif save_location == "s3":
        storage_client = S3Storage(bucket=S3_BUCKET_NAME_PROD)

    else:
        raise ValueError(f"Unsupported save location: {save_location}")

    for dataset_name, (dataset_dir_name, tickers) in DATASETS.items():

        data = extract_ticker_data(ticker_list=tickers)

        parquet_serializer = ParquetSerializer()
        json_serializer = JsonSerializer()

        dir = create_and_validate_s3_filepath(
            data_domain=MARKET_DATA_DOMAIN,
            market_data_type=dataset_dir_name,
            today_date=today_date,
            execution_uuid=run_uuid,
        )

        save_dataset_batch(
            payload=data,
            base_dir=dir,
            execution_uuid=run_uuid,
            storage_client=storage_client,
            serializer=parquet_serializer,
        )

        metadata = create_dataset_metadata(
            data=data,
            dataset_name=dataset_name,
            execution_uuid=run_uuid,
            dataset_dir=dir,
            file_extension=parquet_serializer.extension,
        )

        save_metadata_file(
            metadata_payload=metadata,
            base_dir=dir,
            execution_uuid=run_uuid,
            storage_client=storage_client,
            serializer=json_serializer,
        )
