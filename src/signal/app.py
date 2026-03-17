import json

import pandas as pd

from src.util.util import DATASETS, DATA_DIR, MA200, MA50, MACRO_DIR, INDEX_DIR, EQUITY_DIR, VOLUME
from src.signal.extractor import LOADERS, load_json_file, load_dataframe_file, load_metadata_files_locally, load_market_data_batches_using_metadata


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


def signal_app(execution_id: str, execution_date: str):
    # TODO: Can dry this up somehow but dont want to optimise now.
    loaded_macro_metadata = load_metadata_files_locally(execution_id, execution_date, MACRO_DIR)
    loaded_index_metadata = load_metadata_files_locally(execution_id, execution_date, INDEX_DIR)
    loaded_equities_metadata = load_metadata_files_locally(execution_id, execution_date, EQUITY_DIR)

    macro_market_data_batch = load_market_data_batches_using_metadata(loaded_macro_metadata, "parquet")
    index_market_data_batch = load_market_data_batches_using_metadata(loaded_index_metadata, "parquet")
    equities_market_data_batch = load_market_data_batches_using_metadata(loaded_equities_metadata, "parquet")

    processed_macro_data = calculate_signals(macro_market_data_batch)
    processed_index_data = calculate_signals(index_market_data_batch)
    processed_equities_data = calculate_signals(equities_market_data_batch)


if __name__ == "__main__":
    test_execution_id = "20260316_170604_1e0ed1aee3f648c1a1f713f1f7f7d5e2"
    execution_date = "2026/03/16"
    signal_app(test_execution_id, execution_date)