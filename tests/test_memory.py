from __future__ import annotations

import io
import os
import platform
from pathlib import Path
from unittest import mock

import pytest

from bplustree.const import TreeConf
from bplustree.memory import (
    WAL,
    FileMemory,
    ReachedEndOfFile,
    open_file_in_dir,
)
from bplustree.node import FreelistNode, LeafNode
from bplustree.serializer import IntSerializer

tree_conf = TreeConf(4096, 4, 16, 16, IntSerializer())
node = LeafNode(tree_conf, page=3)


def test_file_memory_node(clean_file):
    mem = FileMemory(clean_file, tree_conf)

    with pytest.raises(ReachedEndOfFile):
        mem.get_node(3)

    mem.set_node(node)
    assert node == mem.get_node(3)

    mem.close()


def test_file_memory_metadata(clean_file):
    mem = FileMemory(clean_file, tree_conf)
    with pytest.raises(ValueError):
        mem.get_metadata()
    mem.set_metadata(6, tree_conf)
    assert mem.get_metadata() == (6, tree_conf)
    mem.close()


def test_file_memory_next_available_page(clean_file):
    mem = FileMemory(clean_file, tree_conf)
    for i in range(1, 100):
        assert mem.next_available_page == i


def test_file_memory_freelist(clean_file):
    mem = FileMemory(clean_file, tree_conf)
    assert mem.next_available_page == 1
    assert mem._traverse_free_list() == (None, None)

    mem.del_page(1)
    assert mem._traverse_free_list() == (
        None,
        FreelistNode(tree_conf, page=1, next_page=None),
    )
    assert mem.next_available_page == 1
    assert mem._traverse_free_list() == (None, None)

    mem.del_page(1)
    mem.del_page(2)
    assert mem._traverse_free_list() == (
        FreelistNode(tree_conf, page=1, next_page=2),
        FreelistNode(tree_conf, page=2, next_page=None),
    )
    mem.del_page(3)
    assert mem._traverse_free_list() == (
        FreelistNode(tree_conf, page=2, next_page=3),
        FreelistNode(tree_conf, page=3, next_page=None),
    )

    assert mem._pop_from_freelist() == 3
    assert mem._pop_from_freelist() == 2
    assert mem._pop_from_freelist() == 1
    assert mem._pop_from_freelist() is None
    mem.close()


def test_open_file_in_dir_invalid_path():
    with pytest.raises(ValueError):
        open_file_in_dir(Path("/foo/bar/does/not/exist"))


@pytest.mark.skipif(platform.system() != "Windows", reason="Only runs on Windows")
def test_open_file_in_dir_create_and_reopen_windows(clean_file):
    file_fd, dir_fd = open_file_in_dir(clean_file)
    assert isinstance(file_fd, io.FileIO)
    file_fd.close()
    assert dir_fd is None

    file_fd, dir_fd = open_file_in_dir(clean_file)
    assert isinstance(file_fd, io.FileIO)
    file_fd.close()
    assert dir_fd is None


@pytest.mark.skipif(platform.system() != "Linux", reason="Only runs on Linux")
def test_open_file_in_dir_create_and_reopen_linux(clean_file):
    file_fd, dir_fd = open_file_in_dir(clean_file)
    assert isinstance(file_fd, io.FileIO)
    file_fd.close()
    assert isinstance(dir_fd, int)
    os.close(dir_fd)

    file_fd, dir_fd = open_file_in_dir(clean_file)
    assert isinstance(file_fd, io.FileIO)
    file_fd.close()
    assert isinstance(dir_fd, int)
    os.close(dir_fd)


def test_file_memory_write_transaction(clean_file):
    mem = FileMemory(clean_file, tree_conf)
    mem._lock = mock.Mock()

    assert mem._wal._not_committed_pages == {}
    assert mem._wal._committed_pages == {}

    with mem.write_transaction:
        mem.set_node(node)
        assert mem._wal._not_committed_pages == {3: 9}
        assert mem._wal._committed_pages == {}
        assert mem._lock.writer_lock.acquire.call_count == 1

    assert mem._wal._not_committed_pages == {}
    assert mem._wal._committed_pages == {3: 9}
    assert mem._lock.writer_lock.release.call_count == 1
    assert mem._lock.reader_lock.acquire.call_count == 0

    with mem.read_transaction:
        assert mem._lock.reader_lock.acquire.call_count == 1
        assert node == mem.get_node(3)

    assert mem._lock.reader_lock.release.call_count == 1
    mem.close()


def test_file_memory_write_transaction_error(clean_file):
    mem = FileMemory(clean_file, tree_conf)
    mem._lock = mock.Mock()
    mem._cache[424242] = node

    with pytest.raises(ValueError):
        with mem.write_transaction:
            mem.set_node(node)
            assert mem._wal._not_committed_pages == {3: 9}
            assert mem._wal._committed_pages == {}
            assert mem._lock.writer_lock.acquire.call_count == 1
            raise ValueError("Foo")

    assert mem._wal._not_committed_pages == {}
    assert mem._wal._committed_pages == {}
    assert mem._lock.writer_lock.release.call_count == 1
    assert mem._cache.get(424242) is None
    mem.close()


def test_file_memory_repr(clean_file):
    mem = FileMemory(clean_file, tree_conf)
    assert repr(mem) == f"<FileMemory: {clean_file}>"
    mem.close()


def test_wal_create_reopen_empty(clean_file):
    WAL(clean_file, 64)

    wal = WAL(clean_file, 64)
    assert wal._page_size == 64


def test_wal_create_reopen_uncommitted(clean_file):
    wal = WAL(clean_file, 64)
    wal.set_page(1, b"1" * 64)
    wal.commit()
    wal.set_page(2, b"2" * 64)
    assert wal.get_page(1) == b"1" * 64
    assert wal.get_page(2) == b"2" * 64

    wal = WAL(clean_file, 64)
    assert wal.get_page(1) == b"1" * 64
    assert wal.get_page(2) is None


def test_wal_rollback(clean_file):
    wal = WAL(clean_file, 64)
    wal.set_page(1, b"1" * 64)
    wal.commit()
    wal.set_page(2, b"2" * 64)
    assert wal.get_page(1) == b"1" * 64
    assert wal.get_page(2) == b"2" * 64

    wal.rollback()
    assert wal.get_page(1) == b"1" * 64
    assert wal.get_page(2) is None


def test_wal_checkpoint(clean_file):
    wal = WAL(clean_file, 64)
    wal.set_page(1, b"1" * 64)
    wal.commit()
    wal.set_page(2, b"2" * 64)

    rv = wal.checkpoint()
    assert list(rv) == [(1, b"1" * 64)]

    with pytest.raises(ValueError):
        wal.set_page(3, b"3" * 64)

    assert not Path(f"{clean_file}-wal").is_file()


def test_wal_repr(clean_file):
    wal = WAL(clean_file, 64)
    assert repr(wal) == f"<WAL: {clean_file}-wal>"
