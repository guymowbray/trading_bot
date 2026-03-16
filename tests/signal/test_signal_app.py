

import json
import pandas as pd


from src.signal.app import load_json_file, load_dataframe_file
from pandas.testing import assert_frame_equal
from tests.conftest import dummy_data


def test_load_json_file_successfully(tmp_path):
    metadata = {
        "FAKE_KEY": "A_LOAD_OF_JIBBERISH",
    }

    metadata_file = tmp_path / "metadata_TEST_EXECUTION_ID.json"
    with open(metadata_file, "w") as f:
        json.dump(metadata, f)

    loaded_metadata = load_json_file(metadata_file)

    assert loaded_metadata == metadata

def test_load_dataframe_filesuccessfully(tmp_path, dummy_data):
    dummy_data.to_parquet(tmp_path / "dummy_data.parquet")

    loaded_data = load_dataframe_file(tmp_path / "dummy_data.parquet", "parquet")

    assert_frame_equal(loaded_data, dummy_data, check_dtype=True)
