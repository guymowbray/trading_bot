from abc import ABC, abstractmethod


class Storage(ABC):
    @abstractmethod
    def write_bytes(self, data: bytes, path: str):
        pass

    @abstractmethod
    def read_bytes(self, path: str) -> bytes:
        pass
