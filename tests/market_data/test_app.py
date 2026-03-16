from datetime import datetime
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from freezegun import freeze_time

from src.market_data.extractor.yahoo import generate_execution_uuid, save_pandas_dataframe


def mock_get_ticker_data(ticker_symbol):
    dummy_data = {"Close": [100], "MA200": [90], "MA50": [95]}
    df = pd.DataFrame(dummy_data)
    df.index = [datetime(2026, 3, 15, 00, 00, 00, 5, 00)]
    return df


@pytest.fixture
def mock_uuid():

    with patch("src.market_data.extractor.yahoo.uuid.uuid4") as mock_uuid:
        mock_uuid.return_value.hex = "TEST_UUID"

        yield "TEST_UUID"


@freeze_time("2026-03-15")
@patch("src.market_data.extractor.yahoo.get_ticker_data")
def test_app_yahoo_main_works_correctly(mock_get_ticker_data, mock_uuid, tmp_path):
    dir = tmp_path / "2026/03/15/TEST_UUID"
    dir.mkdir(parents=True, exist_ok=True)
    p = dir / "hello.txt"
    p.write_text("HI", encoding="utf-8")
    assert p.read_text(encoding="utf-8") == "HI"

    # mock_get_ticker_data.return_value = mock_get_ticker_data("TEST_TICKER")

    # yahoo_main()


def test_save_pandas_dataframe_successfully(tmp_path):
    rng = np.random.default_rng(42)

    data = pd.DataFrame(
        {
            "A": [rng.integers(0, 100, 10), rng.integers(0, 100, 10)],
            "B": [rng.integers(0, 100, 10), rng.integers(0, 100, 10)],
        }
    )
    target_dir = tmp_path

    save_pandas_dataframe(
        data=data, filename="test_dataframe", base_dir=tmp_path, file_extension="parquet"
    )

    saved_file = tmp_path / "test_dataframe.parquet"

    assert saved_file.exists()
    assert (target_dir / "test_dataframe.parquet").exists()

    loaded_data = pd.read_parquet(saved_file)
    pd.testing.assert_frame_equal(data, loaded_data, check_dtype=True)


def test_save_pandas_dataframe_unsupported_extension(tmp_path):
    rng = np.random.default_rng(42)

    data = pd.DataFrame(
        {
            "A": [rng.integers(0, 100, 10), rng.integers(0, 100, 10)],
            "B": [rng.integers(0, 100, 10), rng.integers(0, 100, 10)],
        }
    )

    with pytest.raises(ValueError) as error_info:
        save_pandas_dataframe(
            data=data, filename="test_dataframe", base_dir=tmp_path, file_extension="FAKE_EXTENSION"
        )

    assert "Unsupported file extension: FAKE_EXTENSION" in str(error_info.value)


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
