from pathlib import Path

from util.storage.base import Storage


class LocalStorage(Storage):
    def __init__(self, base_path="data"):
        self.base_path = Path(base_path)

    def write_bytes(self, data: bytes, path: str):

        file_path = self.base_path / path

        # create directories if missing
        file_path.parent.mkdir(parents=True, exist_ok=True)

        with open(file_path, "wb") as f:
            f.write(data)

    def read_bytes(self, path: str) -> bytes:

        with open(path, "rb") as f:
            data = f.read()

        return data
