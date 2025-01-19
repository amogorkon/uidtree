from __future__ import annotations

import abc
from .const import ENDIAN
from beartype import beartype


class Serializer(metaclass=abc.ABCMeta):
    __slots__ = []

    @beartype
    @abc.abstractmethod
    def serialize(self, obj: int, key_size: int) -> bytes:
        """Serialize a key to bytes."""

    @beartype
    @abc.abstractmethod
    def deserialize(self, data: bytes) -> int:
        """Create a key object from bytes."""


class IntSerializer(Serializer):
    __slots__ = []

    @beartype
    def serialize(self, obj: int, key_size: int) -> bytes:
        return obj.to_bytes(key_size, ENDIAN)

    @beartype
    def deserialize(self, data: bytes | bytearray) -> int:
        return int.from_bytes(data, ENDIAN)

    @beartype
    def __repr__(self) -> str:
        return "IntSerializer()"
