from abc import ABC, abstractmethod


class Serializer(ABC):
    extension = ""

    @abstractmethod
    def serialize(self, obj) -> bytes:
        pass

    @abstractmethod
    def deserialize(self, data: bytes):
        pass
