from util.file_io import load_file


def load_market_data_batches_using_metadata(metadata: dict, storage, serializer) -> dict:
    """
    Bit annoying we pass in the file extension as the info is in the file location,
    maybe ill clean this up later.
    """
    payload = {}
    for ticker_name, file_location in metadata["files"].items():
        data = load_file(
            storage=storage,
            serializer=serializer,
            path=file_location,
        )

        payload[ticker_name] = data

    return payload
