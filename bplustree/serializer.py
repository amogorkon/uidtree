from __future__ import annotations

import abc
from datetime import datetime, timezone
from uuid import UUID

try:
    import temporenc
except ImportError:
    temporenc = None

from .const import ENDIAN
from beartype import beartype


class Serializer(metaclass=abc.ABCMeta):
    __slots__ = []

    @beartype
    @abc.abstractmethod
    def serialize(self, obj: object, key_size: int) -> bytes:
        """Serialize a key to bytes."""

    @beartype
    @abc.abstractmethod
    def deserialize(self, data: bytes) -> object:
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


class StrSerializer(Serializer):
    __slots__ = []

    @beartype
    def serialize(self, obj: str, key_size: int) -> bytes:
        rv = obj.encode(encoding="utf-8")
        assert len(rv) <= key_size
        return rv

    @beartype
    def deserialize(self, data: bytes) -> str:
        return data.decode(encoding="utf-8")

    @beartype
    def __repr__(self) -> str:
        return "StrSerializer()"


class UUIDSerializer(Serializer):
    __slots__ = []

    @beartype
    def serialize(self, obj: UUID, key_size: int) -> bytes:
        return obj.bytes

    @beartype
    def deserialize(self, data: bytes) -> UUID:
        return UUID(bytes=data)

    @beartype
    def __repr__(self) -> str:
        return "UUIDSerializer()"


class DatetimeUTCSerializer(Serializer):
    __slots__ = []

    @beartype
    def __init__(self) -> None:
        if temporenc is None:
            raise RuntimeError(
                "Serialization to/from datetime needs the "
                'third-party library "temporenc"'
            )

    @beartype
    def serialize(self, obj: datetime, key_size: int) -> bytes:
        if obj.tzinfo is None:
            raise ValueError("DatetimeUTCSerializer needs a timezone aware datetime")
        return temporenc.packb(obj, type="DTS")

    @beartype
    def deserialize(self, data: bytes) -> datetime:
        rv = temporenc.unpackb(data).datetime()
        rv = rv.replace(tzinfo=timezone.utc)
        return rv

    @beartype
    def __repr__(self) -> str:
        return "DatetimeUTCSerializer()"
