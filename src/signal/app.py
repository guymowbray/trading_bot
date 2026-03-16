import json

import pandas as pd

from src.util.util import DATA_DIR, MACRO_DIR, INDEX_DIR, EQUITY_DIR

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

def load_metadata_files_locally(execution_id: str, execution_date: str, dataset_dir) -> tuple[dict, dict, dict]:
    """
    TODO: this shouldnt live here tbh, will need to clean up. 
    """
    return load_json_file(f"{DATA_DIR}/{dataset_dir}/{execution_date}/{execution_id}/metadata_{execution_id}.json")


def signal_app(execution_id: str, execution_date: str):
    loaded_macro_metadata = load_metadata_files_locally(execution_id, execution_date, MACRO_DIR)
    loaded_index_metadata = load_metadata_files_locally(execution_id, execution_date, INDEX_DIR)
    loaded_equities_metadata = load_metadata_files_locally(execution_id, execution_date, EQUITY_DIR)

    import pdb; pdb.set_trace()

if __name__ == "__main__":
    test_execution_id = "20260316_164159_7923d2f2c4bf4eefb9200be731aeadc9"
    execution_date = "2026/03/16"
    signal_app(test_execution_id, execution_date)