from datetime import UTC, datetime

import pandas as pd

from src.signal.extractor import (
    load_market_data_batches_using_metadata,
)
from src.signal.pipeline import SIGNAL_PIPELINE
from src.util.util import (
    DATA_DIR,
    DATA_DIR_LOCAL,
    DATASETS,
    SIGNALS_DIR,
    create_and_validate_s3_filepath,
    generate_execution_uuid,
    parse_execution_id,
)
from util.file_io import create_dataset_metadata, load_file, save_dataset_batch, save_metadata_file
from util.serializer.json import JsonSerializer
from util.serializer.parquet import ParquetSerializer
from util.storage.local import LocalStorage
from util.storage.s3 import S3_BUCKET_NAME_PROD, S3Storage


def calculate_signals_batch(
    market_data_batch: dict[str, pd.DataFrame], signal_pipeline
) -> dict[str, pd.DataFrame]:
    payload = {}

    for ticker_name, data in market_data_batch.items():
        for fn in signal_pipeline:
            data = fn(data)

        payload[ticker_name] = data

    return payload


# TODO: 1 E2E at this level
def signal_app(previous_execution_id: str, save_location: str):
    parquet_serializer = ParquetSerializer()
    json_serializer = JsonSerializer()

    # Need this from previous data during the market_data process
    processed_execution_uuid = parse_execution_id(previous_execution_id)
    previous_execution_date = processed_execution_uuid["date"]

    # Create for signal process which is independent.
    execution_uuid = generate_execution_uuid()
    today_date = datetime.now(UTC).strftime("%Y/%m/%d")

    if save_location == "local":
        storage_client = LocalStorage()
        base_dir = DATA_DIR_LOCAL

    elif save_location == "s3":
        storage_client = S3Storage(bucket=S3_BUCKET_NAME_PROD)
        base_dir = DATA_DIR
    else:
        raise ValueError(f"Unsupported save location: {save_location}")

    for dataset_name, (dataset_dir_name, tickers) in DATASETS.items():
        loaded_macro_metadata = load_file(
            storage_client,
            json_serializer,
            f"{base_dir}/{dataset_dir_name}/{previous_execution_date}/{previous_execution_id}/metadata_{previous_execution_id}.json",
        )

        macro_market_data_batch = load_market_data_batches_using_metadata(
            metadata=loaded_macro_metadata, serializer=parquet_serializer, storage=storage_client
        )

        # Later on we can make signal_pipeline configurable at config level.
        processed_macro_data = calculate_signals_batch(macro_market_data_batch, SIGNAL_PIPELINE)

        dir = create_and_validate_s3_filepath(
            base_dir=SIGNALS_DIR,
            market_data_type=dataset_dir_name,
            today_date=today_date,
            execution_uuid=execution_uuid,
        )

        save_dataset_batch(
            payload=processed_macro_data,
            base_dir=dir,
            execution_uuid=execution_uuid,
            storage_client=storage_client,
            serializer=parquet_serializer,
        )

        metadata = create_dataset_metadata(
            data=tickers,
            dataset_name=dataset_name,
            execution_uuid=execution_uuid,
            dataset_dir=dir,
            file_extension=json_serializer.extension,
        )

        save_metadata_file(
            metadata_payload=metadata,
            base_dir=dir,
            execution_uuid=execution_uuid,
            storage_client=storage_client,
            serializer=json_serializer,
        )


if __name__ == "__main__":
    test_execution_id = "20260321_152729_4bc0ba07d7044b32ae2643e8a89a540f"
    save_location = "s3"
    signal_app(test_execution_id, save_location)
