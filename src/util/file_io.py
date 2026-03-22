import pandas as pd

from typing import Dict


def save_file(df, storage, serializer, path: str):
    data = serializer.serialize(df)

    storage.write_bytes(data, path)


def load_file(storage, serializer, path: str):
    payload = storage.read_bytes(path)

    return serializer.deserialize(payload)


def save_dataset_batch(
    payload: Dict[str, pd.DataFrame],
    base_dir: str,
    execution_uuid: str,
    storage_client: str,
    serializer,
):
    # TODO: validate this properly, maybe make a MarketData model.
    # also should do validation elsewhere.
    if not payload:
        raise ValueError("Missing dict with ticker and data payload missing")

    for ticker, data in payload.items():
        fname = f"{ticker}_{execution_uuid}"

        save_file(
            df=data,
            storage=storage_client,
            serializer=serializer,
            path=f"{base_dir}/{execution_uuid}{fname}.{serializer.extension}",
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

def save_metadata_file(
    metadata_payload: dict, base_dir: str, execution_uuid: str, storage_client, serializer
):
    filename = f"metadata_{execution_uuid}.json"

    save_file(
        df=metadata_payload,
        storage=storage_client,
        serializer=serializer,
        path=f"{base_dir}/{filename}",
    )
