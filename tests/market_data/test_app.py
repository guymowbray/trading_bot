import json
import os
from io import BytesIO
from unittest.mock import patch

import boto3
import pandas as pd
import pytest
from freezegun import freeze_time
from moto import mock_aws
from pandas.testing import assert_frame_equal

from market_data.extractor.extract import (
    create_dataset_metadata,
    generate_execution_uuid,
    save_dataset_batch,
    save_metadata_file,
)
from util.serializer.json import JsonSerializer
from util.serializer.parquet import ParquetSerializer
from util.storage.local import LocalStorage
from util.storage.s3 import S3Storage


def mock_get_ticker_data(ticker_symbol, dummy_data):
    df = pd.DataFrame(dummy_data)
    return df


@pytest.fixture
def mock_uuid():

    with patch("src.market_data.extractor.extract.uuid.uuid4") as mock_uuid:
        mock_uuid.return_value.hex = "TEST_UUID"

        yield "TEST_UUID"


@pytest.fixture
def mock_test_s3_bucket(bucket_name="test-placeholder-bucket", region_name="us-west-1"):

    with mock_aws():
        s3 = boto3.client("s3", region_name=region_name)

        s3.create_bucket(
            Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": region_name}
        )

        yield s3, bucket_name, region_name


@pytest.mark.parametrize(
    "mock_uuid_hex, datetime_uuid, expected_uuid",
    [
        ("TEST_UUID_1", "2026-03-11 12:00:00", "20260311_120000_TEST_UUID_1"),
        ("TEST_UUID_2", "2026-03-10 12:001:01", "20260310_120101_TEST_UUID_2"),
    ],
)
def test_generate_execution_uuid_successfully(mock_uuid_hex, datetime_uuid, expected_uuid):
    with freeze_time(datetime_uuid):
        with patch("uuid.uuid4") as mock_uuid:
            mock_uuid.return_value.hex = mock_uuid_hex

            actual_generated_uuid = generate_execution_uuid()

            assert actual_generated_uuid == expected_uuid


@mock_aws
@freeze_time("2026-03-15 05:00:00")
def test_save_raw_data_to_s3_successfully(dummy_data, mock_test_s3_bucket):
    s3_client, bucket_name, _ = mock_test_s3_bucket

    ticker_name_1 = "TEST_TICKER_1"
    ticker_name_2 = "TEST_TICKER_2"

    execution_uuid = "TEST_UUID"

    file_extension_name = "parquet"

    payload = {
        ticker_name_1: dummy_data,
        ticker_name_2: dummy_data,
    }

    save_dataset_batch(
        payload=payload,
        base_dir="tmp_path",
        execution_uuid=execution_uuid,
        storage_client=S3Storage(bucket=bucket_name),
        serializer=ParquetSerializer(),
    )
    saved_file_1 = f"tmp_path/{ticker_name_1}_{execution_uuid}.{file_extension_name}"
    saved_file_2 = f"tmp_path/{ticker_name_2}_{execution_uuid}.{file_extension_name}"

    obj_1 = s3_client.get_object(Bucket=bucket_name, Key=saved_file_1)
    obj_2 = s3_client.get_object(Bucket=bucket_name, Key=saved_file_2)

    loaded_data_1 = pd.read_parquet(BytesIO(obj_1["Body"].read()))
    loaded_data_2 = pd.read_parquet(BytesIO(obj_2["Body"].read()))

    assert_frame_equal(loaded_data_1, dummy_data, check_dtype=True)
    assert_frame_equal(loaded_data_2, dummy_data, check_dtype=True)


@mock_aws
@freeze_time("2026-03-15 05:00:00")
def test_save_raw_data_to_local_dir_successfully(dummy_data, tmp_path):
    payload = {
        "TEST_TICKER_1": dummy_data,
        "TEST_TICKER_2": dummy_data,
    }

    storage_client = LocalStorage(base_path=tmp_path)
    parquet_serializer = ParquetSerializer()

    save_dataset_batch(
        payload=payload,
        base_dir=tmp_path,
        execution_uuid="TEST_UUID",
        storage_client=storage_client,
        serializer=parquet_serializer,
    )
    saved_file_1 = tmp_path / "TEST_TICKER_1_TEST_UUID.parquet"
    saved_file_2 = tmp_path / "TEST_TICKER_2_TEST_UUID.parquet"

    assert os.path.exists(saved_file_1)
    assert os.path.exists(saved_file_2)

    loaded_data = pd.read_parquet(saved_file_1)
    loaded_data = pd.read_parquet(saved_file_2)

    assert_frame_equal(loaded_data, dummy_data, check_dtype=True)


def test_save_raw_data_missing_payload_raises_error(tmp_path):
    with pytest.raises(ValueError) as error_info:
        save_dataset_batch(
            payload=None,
            base_dir=tmp_path,
            execution_uuid="TEST_UUID",
            storage_client=LocalStorage(base_path=tmp_path),
            serializer=ParquetSerializer(),
        )

    assert "Missing dict with ticker and data payload missing" in str(error_info.value)


def test_save_metadata_file_locally_successfully(tmp_path):
    metadata = {
        "dataset": "TEST_DATASET",
        "execution_id": "TEST_EXECUTION_ID",
        "tickers": ["TICKER1", "TICKER2"],
        "files": {
            "TICKER1": f"{tmp_path}/TICKER1_TEST_EXECUTION_ID.parquet",
            "TICKER2": f"{tmp_path}/TICKER2_TEST_EXECUTION_ID.parquet",
        },
        "rows": {
            "TICKER1": 100,
            "TICKER2": 200,
        },
    }

    save_metadata_file(
        metadata_payload=metadata,
        base_dir=tmp_path,
        execution_uuid="TEST_EXECUTION_ID",
        storage_client=LocalStorage(base_path=tmp_path),
        serializer=JsonSerializer(),
    )

    saved_file = tmp_path / "metadata_TEST_EXECUTION_ID.json"

    assert saved_file.exists()

    with open(saved_file, "r") as f:
        loaded_metadata = json.load(f)

    assert loaded_metadata == metadata


def test_save_metadata_file_to_s3_successfully(tmp_path, mock_test_s3_bucket):
    s3_client, bucket_name, _ = mock_test_s3_bucket

    metadata = {
        "dataset": "TEST_DATASET",
        "execution_id": "TEST_EXECUTION_ID",
        "tickers": ["TICKER1", "TICKER2"],
        "files": {
            "TICKER1": f"{tmp_path}/TICKER1_TEST_EXECUTION_ID.parquet",
            "TICKER2": f"{tmp_path}/TICKER2_TEST_EXECUTION_ID.parquet",
        },
        "rows": {
            "TICKER1": 100,
            "TICKER2": 200,
        },
    }

    save_metadata_file(
        metadata_payload=metadata,
        base_dir=tmp_path,
        execution_uuid="TEST_EXECUTION_ID",
        storage_client=S3Storage(bucket=bucket_name),
        serializer=JsonSerializer(),
    )

    saved_file = tmp_path / "metadata_TEST_EXECUTION_ID.json"

    obj = s3_client.get_object(Bucket=bucket_name, Key=f"{saved_file}")

    loaded_metadata = json.load(BytesIO(obj["Body"].read()))

    assert loaded_metadata == metadata


def test_create_dataset_metadata_successfully(dummy_data):
    data = {
        "TICKER1": dummy_data,
        "TICKER2": dummy_data,
    }
    dataset_name = "TEST_DATASET"
    execution_uuid = "TEST_EXECUTION_UUID"
    dataset_dir = "/path/to/dataset"
    file_extension = "parquet"

    expected_metadata = {
        "dataset": dataset_name,
        "execution_uuid": execution_uuid,
        "tickers": ["TICKER1", "TICKER2"],
        "files": {
            "TICKER1": f"{dataset_dir}/TICKER1_{execution_uuid}.{file_extension}",
            "TICKER2": f"{dataset_dir}/TICKER2_{execution_uuid}.{file_extension}",
        },
        "rows": {
            "TICKER1": len(dummy_data),
            "TICKER2": len(dummy_data),
        },
    }

    actual_metadata = create_dataset_metadata(
        data=data,
        dataset_name=dataset_name,
        execution_uuid=execution_uuid,
        dataset_dir=dataset_dir,
        file_extension=file_extension,
    )

    assert actual_metadata == expected_metadata
