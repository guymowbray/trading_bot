def save_file(df, storage, serializer, path: str):
    data = serializer.serialize(df)

    storage.write_bytes(data, path)


def load_file(storage, serializer, path: str):
    payload = storage.read_bytes(path)

    return serializer.deserialize(payload)
