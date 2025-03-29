from __future__ import annotations

from pathlib import Path

import pytest
from beartype import beartype

from uidtree.const import TreeConf
from uidtree.memory import FileMemory, ReachedEndOfFile
from uidtree.node import LeafNode
from uidtree.tree import BPlusTree


@beartype
def test_create_and_load_file(clean_file: Path) -> None:
    btree = BPlusTree(clean_file)
    assert isinstance(btree._mem, FileMemory)
    btree.insert(5, b"foo")
    btree.close()

    btree = BPlusTree(clean_file)
    assert isinstance(btree._mem, FileMemory)
    assert btree.get(5) == b"foo"
    btree.close()


@beartype
def test_insert_and_get(clean_file: Path) -> None:
    btree = BPlusTree(clean_file, key_size=16, value_size=16, order=4)
    btree.insert(1, b"foo")
    assert btree.get(1) == b"foo"
    btree.close()


@beartype
def test_batch_insert(clean_file: Path) -> None:
    btree = BPlusTree(clean_file, key_size=16, value_size=16, order=4)

    btree.batch_insert([(i, str(i).encode()) for i in range(1000)])
    btree.batch_insert([(i, str(i).encode()) for i in range(1000, 2000)])

    for k, v in btree.items():
        assert str(k).encode() == v

    btree.close()


@beartype
def test_file_memory_node(clean_file: Path) -> None:
    tree_conf = TreeConf(4096, 4, 16, 16)
    node = LeafNode(tree_conf, page=3)
    mem = FileMemory(clean_file, tree_conf)
    with pytest.raises(ReachedEndOfFile):
        mem.get_node(3)

    mem.set_node(node)
