from __future__ import annotations

from unittest import mock

import pytest
from bplustree.memory import FileMemory
from bplustree.node import LeafNode, LonelyRootNode
from bplustree.tree import BPlusTree


def test_create_and_load_file(clean_file):
    btree = BPlusTree(clean_file)
    assert isinstance(btree._mem, FileMemory)
    btree.insert(5, b"foo")
    btree.close()

    btree = BPlusTree(clean_file)
    assert isinstance(btree._mem, FileMemory)
    assert btree.get(5) == b"foo"
    btree.close()


@mock.patch("bplustree.tree.BPlusTree.close")
def test_closing_context_manager(mock_close, clean_file):
    with BPlusTree(clean_file, page_size=512, value_size=128):
        pass
    mock_close.assert_called_once_with()


def test_initial_values(clean_file):
    btree = BPlusTree(clean_file, page_size=512, value_size=128)
    assert btree._tree_conf.page_size == 512
    assert btree._tree_conf.order == 100
    assert btree._tree_conf.key_size == 8
    assert btree._tree_conf.value_size == 128
    btree.close()


def test_partial_constructors(clean_file):
    btree = BPlusTree(clean_file, key_size=16, value_size=16, order=4)
    node = btree.RootNode()
    record = btree.Record()
    assert node._tree_conf == btree._tree_conf
    assert record._tree_conf == btree._tree_conf
    btree.close()


def test_insert_setitem_tree(clean_file):
    btree = BPlusTree(clean_file, key_size=16, value_size=16, order=4)
    btree.insert(1, b"foo")

    with pytest.raises(ValueError):
        btree.insert(1, b"bar")
    assert btree.get(1) == b"foo"

    btree.insert(1, b"baz", replace=True)
    assert btree.get(1) == b"baz"

    btree[1] = b"foo"
    assert btree.get(1) == b"foo"
    btree.close()


def test_get_tree(clean_file):
    btree = BPlusTree(clean_file, key_size=16, value_size=16, order=4)
    btree.insert(1, b"foo")
    assert btree.get(1) == b"foo"
    assert btree.get(2) is None
    assert btree.get(2, b"bar") == b"bar"
    btree.close()


def test_getitem_tree(clean_file):
    btree = BPlusTree(clean_file, key_size=16, value_size=16, order=4)
    btree.insert(1, b"foo")
    btree.insert(2, b"bar")
    btree.insert(5, b"baz")

    assert btree[1] == b"foo"
    with pytest.raises(KeyError):
        btree[4]

    assert btree[1:3] == {1: b"foo", 2: b"bar"}
    assert btree[:10] == {1: b"foo", 2: b"bar", 5: b"baz"}
    btree.close()


def test_contains_tree(clean_file):
    btree = BPlusTree(clean_file, key_size=16, value_size=16, order=4)
    btree.insert(1, b"foo")
    assert 1 in btree
    assert 2 not in btree
    btree.close()


def test_len_tree(clean_file):
    btree = BPlusTree(clean_file, key_size=16, value_size=16, order=4)
    assert len(btree) == 0
    btree.insert(1, b"foo")
    assert len(btree) == 1
    for i in range(2, 101):
        btree.insert(i, str(i).encode())
    assert len(btree) == 100
    btree.close()


def test_length_hint_tree(clean_file):
    btree = BPlusTree(clean_file, key_size=16, value_size=16, order=100)
    assert btree.__length_hint__() == 49
    btree.insert(1, b"foo")
    assert btree.__length_hint__() == 49
    for i in range(2, 10001):
        btree.insert(i, str(i).encode())
    assert btree.__length_hint__() == 7242
    btree.close()


def test_bool_tree(clean_file):
    btree = BPlusTree(clean_file, key_size=16, value_size=16, order=4)
    assert not btree
    btree.insert(1, b"foo")
    assert btree
    btree.close()


# sourcery skip: no-loop-in-tests
def test_iter_keys_values_items_tree(clean_file):
    btree = BPlusTree(clean_file, key_size=16, value_size=16, order=4)
    # Empty tree
    with pytest.raises(StopIteration):
        next(iter(btree))

    # Insert in reverse...
    for i in range(1000, 0, -1):
        btree.insert(i, str(i).encode())
    for previous, i in enumerate(btree):
        assert i == previous + 1
    for previous, i in enumerate(btree.keys()):
        assert i == previous + 1
    # Test slice .keys()
    assert list(btree.keys(slice(10, 13))) == [10, 11, 12]

    for previous, i in enumerate(btree.values()):
        assert int(i.decode()) == previous + 1
    # Test slice .values()
    assert list(btree.values(slice(10, 13))) == [b"10", b"11", b"12"]

    for previous, (k, v) in enumerate(btree.items()):
        expected = previous + 1
        assert (k, int(v.decode())) == (expected, expected)
    # Contains from 0 to 9 included
    for i in range(10):
        btree.insert(i, str(i).encode(), replace=True)

    for key, expected in [
        (slice(None, 2), [0, 1]),
        (slice(5, 7), [5, 6]),
        (slice(8, 9), [8]),
        (slice(9, 12), [9]),
    ]:
        it = btree._iter_slice(key)
        for exp in expected:
            assert next(it).key == exp
        # with pytest.raises(StopIteration):
        #    next(it)
    btree.close()


def test_iter_items_order5(clean_file):
    # Contains from 10, 20, 30 .. 200
    btree = BPlusTree(clean_file, order=5)
    for i in range(10, 201, 10):
        btree.insert(i, str(i).encode())

    it = btree._iter_slice(slice(65, 85))
    assert next(it).key == 70
    assert next(it).key == 80
    with pytest.raises(StopIteration):
        next(it)
    btree.close()


def test_checkpoint(clean_file):
    btree = BPlusTree(clean_file, key_size=16, value_size=16, order=4)
    btree.checkpoint()
    btree.insert(1, b"foo")
    assert not btree._mem._wal._not_committed_pages
    assert btree._mem._wal._committed_pages

    btree.checkpoint()
    assert not btree._mem._wal._not_committed_pages
    assert not btree._mem._wal._committed_pages
    btree.close()


def test_left_record_node_in_tree(clean_file):
    btree = BPlusTree(clean_file, order=3)
    assert btree._left_record_node == btree._root_node
    assert isinstance(btree._left_record_node, LonelyRootNode)
    btree.insert(1, b"1")
    btree.insert(2, b"2")
    btree.insert(3, b"3")
    assert isinstance(btree._left_record_node, LeafNode)
    btree.close()


def test_overflow(clean_file):
    btree = BPlusTree(clean_file, key_size=16, value_size=16, order=4)
    data = b"f" * 323343
    with btree._mem.write_transaction:
        first_overflow_page = btree._create_overflow(data)
        assert btree._read_from_overflow(first_overflow_page) == data

    with btree._mem.read_transaction:
        assert btree._read_from_overflow(first_overflow_page) == data

    assert btree._mem.last_page == 81

    with btree._mem.write_transaction:
        btree._delete_overflow(first_overflow_page)

    with btree._mem.write_transaction:
        for i in range(81, 2, -1):
            assert btree._mem.next_available_page == i
    btree.close()


def test_batch_insert(clean_file):
    btree = BPlusTree(clean_file, key_size=16, value_size=16, order=4)

    def generate(from_, to):
        for i in range(from_, to):
            yield i, str(i).encode()

    btree.batch_insert(generate(0, 1000))
    btree.batch_insert(generate(1000, 2000))

    i = 0
    for k, v in btree.items():
        assert k == i
        assert v == str(i).encode()
        i += 1
    assert i == 2000
    btree.close()


def test_batch_insert_no_in_order(clean_file):
    btree = BPlusTree(clean_file, key_size=16, value_size=16, order=4)
    with pytest.raises(ValueError):
        btree.batch_insert([(2, b"2"), (1, b"1")])
    assert btree.get(1) is None
    assert btree.get(2) is None

    btree.insert(2, b"2")
    with pytest.raises(ValueError):
        btree.batch_insert([(1, b"1")])

    with pytest.raises(ValueError):
        btree.batch_insert([(2, b"2")])

    assert btree.get(1) is None
    assert btree.get(2) == b"2"
    btree.close()
    btree.close()
