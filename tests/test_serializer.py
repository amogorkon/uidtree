import pytest

from bplustree.serializer import (
    IntSerializer,
)


def test_int_serializer():
    s = IntSerializer()
    assert s.serialize(42, 2) == b"*\x00"
    assert s.deserialize(b"*\x00") == 42
    assert repr(s) == "IntSerializer()"


def test_serializer_slots():
    s = IntSerializer()
    with pytest.raises(AttributeError):
        s.foo = True
