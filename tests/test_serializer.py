from bplustree.serializer import (
    deserialize,
    serialize,
)


def test_int_serializer() -> None:
    assert serialize(42, 16) == b"*\x00" + b"\x00" * 14
    assert deserialize(b"*\x00" + b"\x00" * 14) == 42
