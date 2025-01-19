from __future__ import annotations

from pathlib import Path

import pytest

from bplustree.const import TreeConf
from bplustree.memory import FileMemory, ReachedEndOfFile
from bplustree.node import LeafNode
from bplustree.serializer import IntSerializer
from bplustree.tree import BPlusTree


def test_create_and_load_file(clean_file: Path):
    btree = BPlusTree(clean_file)
    assert isinstance(btree._mem, FileMemory)
    btree.insert(5, b"foo")
    btree.close()

    btree = BPlusTree(clean_file)
    assert isinstance(btree._mem, FileMemory)
    assert btree.get(5) == b"foo"
    btree.close()


def test_insert_and_get(clean_file: Path):
    btree = BPlusTree(clean_file, key_size=16, value_size=16, order=4)
    btree.insert(1, b"foo")
    assert btree.get(1) == b"foo"
    btree.close()


def test_batch_insert(clean_file: Path):
    btree = BPlusTree(clean_file, key_size=16, value_size=16, order=4)

    btree.batch_insert([(i, str(i).encode()) for i in range(1000)])
    btree.batch_insert([(i, str(i).encode()) for i in range(1000, 2000)])

    for k, v in btree.items():
        assert str(k).encode() == v

    btree.close()


def test_file_memory_node(clean_file: Path):
    tree_conf = TreeConf(4096, 4, 16, 16, IntSerializer())
    node = LeafNode(tree_conf, page=3)
    mem = FileMemory(clean_file, tree_conf)
    with pytest.raises(ReachedEndOfFile):
        mem.get_node(3)

    mem.set_node(node)
