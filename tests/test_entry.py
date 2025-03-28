import pytest

from bplustree.const import TreeConf
from bplustree.entry import OpaqueData, Record, Reference

tree_conf = TreeConf(4096, 4, 16, 16)


def test_record_int_serialization() -> None:
    r1 = Record(tree_conf, 42, b"foo")
    data = r1.dump()

    r2 = Record(tree_conf, data=data)
    assert r1 == r2
    assert r1.value == r2.value
    assert r1.overflow_page == r2.overflow_page


def test_record_int_serialization_overflow_value() -> None:
    r1 = Record(tree_conf, 42, overflow_page=5)
    data = r1.dump()

    r2 = Record(tree_conf, data=data)
    assert r1 == r2
    assert r1.value == r2.value
    assert r1.overflow_page == r2.overflow_page


def test_record_repr() -> None:
    r1 = Record(tree_conf, 42, b"foo")
    assert repr(r1) == "<Record: 42 value=b'foo'>"

    r1.value = None
    assert repr(r1) == "<Record: 42 unknown value>"

    r1.overflow_page = 5
    assert repr(r1) == "<Record: 42 overflowing value>"


def test_record_slots() -> None:
    r1 = Record(tree_conf, 42, b"foo")
    with pytest.raises(AttributeError):
        r1.foo = True


def test_record_lazy_load() -> None:
    data = Record(tree_conf, 42, b"foo").dump()
    r = Record(tree_conf, data=data)

    assert r._data == data
    assert r._key == ...
    assert r._value == ...
    assert r._overflow_page == ...

    _ = r.key
    assert r._key == 42
    assert r._value == b"foo"
    assert r._overflow_page is None
    assert r._data == data

    r.key = 27
    assert r._key == 27
    assert r._data is None


def test_reference_int_serialization() -> None:
    r1 = Reference(tree_conf, 42, 1, 2)
    data = r1.dump()

    r2 = Reference(tree_conf, data=data)
    assert r1 == r2
    assert r1.before == r2.before
    assert r1.after == r2.after


def test_reference_repr() -> None:
    r1 = Reference(tree_conf, 42, 1, 2)
    assert repr(r1) == "<Reference: key=42 before=1 after=2>"


def test_reference_lazy_load() -> None:
    data = Reference(tree_conf, 42, 1, 2).dump()
    r = Reference(tree_conf, data=data)

    assert r._data == data
    assert r._key == ...
    assert r._before == ...
    assert r._after == ...

    _ = r.key
    assert r._key == 42
    assert r._before == 1
    assert r._after == 2
    assert r._data == data

    r.key = 27
    assert r._key == 27
    assert r._data is None


def test_opaque_data() -> None:
    data = b"foo"
    o = OpaqueData(data=data)
    assert o.data == data
    assert o.dump() == data

    o = OpaqueData()
    o.load(data)
    assert o.data == data
    assert o.dump() == data

    assert repr(o) == "<OpaqueData: b'foo'>"
