import json

import pandas as pd

from src.util.util import DATA_DIR_LOCAL

LOADERS = {
    "parquet": pd.read_parquet,
    "csv": pd.read_csv,
    "json": pd.read_json,
}


def load_json_file(file_path: str) -> dict:
    with open(file_path, "r") as f:
        data = json.load(f)
    return data


def load_dataframe_file(file_path: str, file_extension: str) -> pd.DataFrame:
    """
    Read pandas dataframe from file based on the file extension.
    """
    loader = LOADERS.get(file_extension)

    if not loader:
        raise ValueError(f"Unsupported file extension: {file_extension}")

    return loader(file_path)


def load_metadata_files_locally(
    execution_id: str, execution_date: str, dataset_dir
) -> tuple[dict, dict, dict]:
    """
    TODO: this shouldnt live here tbh, will need to clean up.
    """
    return load_json_file(
        f"{DATA_DIR_LOCAL}/{dataset_dir}/{execution_date}/{execution_id}/metadata_{execution_id}.json"
    )


def load_market_data_batches_using_metadata(metadata: dict, file_extension: str) -> dict:
    """
    Bit annoying we pass in the file extension as the info is in the file location,
    maybe ill clean this up later.
    """
    payload = {}
    for ticker_name, file_location in metadata["files"].items():
        data = load_dataframe_file(file_location, file_extension)

        payload[ticker_name] = data

    return payload
