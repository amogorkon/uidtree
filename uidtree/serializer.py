from __future__ import annotations

from beartype import beartype

from .const import ENDIAN


@beartype
def serialize(obj: int, key_size: int) -> bytes:
    """Serialize an integer to bytes."""
    return obj.to_bytes(key_size, ENDIAN)


@beartype
def deserialize(data: bytes | bytearray) -> int:
    """Deserialize bytes to an integer."""
    return int.from_bytes(data, ENDIAN)
