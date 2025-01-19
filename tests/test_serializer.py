from bplustree.serializer import (
    deserialize,
    serialize,
)


def test_int_serializer():
    assert serialize(42, 2) == b"*\x00"
    assert deserialize(b"*\x00") == 42
