from __future__ import annotations
import enum
import io
import os
import platform
from logging import getLogger
from pathlib import Path

import cachetools
import rwlock

from .const import (
    ENDIAN,
    FRAME_TYPE_BYTES,
    OTHERS_BYTES,
    PAGE_REFERENCE_BYTES,
    TreeConf,
)
from .node import FreelistNode, Node

logger = getLogger(__name__)


class ReachedEndOfFile(Exception):
    """Read a file until its end."""


def open_file_in_dir(path: Path) -> tuple[io.FileIO, int | None]:
    """Open a file and its directory.

    The file is opened in binary mode and created if it does not exist.
    Both file descriptors must be closed after use to prevent them from
    leaking.

    On Windows, the directory is not opened, as it is useless.
    """
    directory = path.parent
    if not directory.is_dir():
        raise ValueError(f"No directory {directory}")

    if not path.exists():
        file_fd = open(path, mode="x+b", buffering=0)
    else:
        file_fd = open(path, mode="r+b", buffering=0)

    if platform.system() == "Windows":
        # Opening a directory is not possible on Windows, but that is not
        # a problem since Windows does not need to fsync the directory in
        # order to persist metadata
        dir_fd = None
    else:
        dir_fd = os.open(directory, os.O_RDONLY)

    return file_fd, dir_fd


def write_to_file(
    file_fd: io.FileIO,
    dir_fileno: int | None,
    data: bytes | bytearray,
    fsync: bool = True,
):
    length_to_write = len(data)
    written = 0
    while written < length_to_write:
        written += file_fd.write(data[written:])
    if fsync:
        fsync_file_and_dir(file_fd.fileno(), dir_fileno)


def fsync_file_and_dir(file_fileno: int, dir_fileno: int | None):
    os.fsync(file_fileno)
    if dir_fileno is not None:
        os.fsync(dir_fileno)


def read_from_file(file_fd: io.FileIO, start: int, stop: int) -> bytes:
    length = stop - start
    assert length >= 0
    file_fd.seek(start)
    data = bytes()
    while file_fd.tell() < stop:
        read_data = file_fd.read(stop - file_fd.tell())
        if read_data == b"":
            raise ReachedEndOfFile("Read until the end of file")
        data += read_data
    assert len(data) == length
    return data


class FakeCache:
    """A cache that doesn't cache anything.

    Because cachetools does not work with maxsize=0.
    """

    def get(self, k):
        pass

    def __setitem__(self, key, value):
        pass

    def clear(self):
        pass


class FileMemory:
    __slots__ = [
        "_filepath",
        "_tree_conf",
        "_lock",
        "_cache",
        "_fd",
        "_dir_fd",
        "_wal",
        "last_page",
        "_freelist_start_page",
        "_root_node_page",
    ]

    def __init__(self, filepath: Path, tree_conf: TreeConf, cache_size: int = 512):
        self._filepath = filepath
        self._tree_conf = tree_conf
        self._lock = rwlock.RWLock()

        if cache_size == 0:
            self._cache = FakeCache()
        else:
            self._cache = cachetools.LRUCache(maxsize=cache_size)

        self._fd, self._dir_fd = open_file_in_dir(filepath)

        self._wal = WAL(filepath, tree_conf.page_size)
        if self._wal.needs_recovery:
            self.perform_checkpoint(reopen_wal=True)

        # Get the next available page
        self._fd.seek(0, io.SEEK_END)
        last_byte = self._fd.tell()
        self.last_page = int(last_byte / self._tree_conf.page_size)
        self._freelist_start_page = 0

        # Todo: Remove this, it should only be in Tree
        self._root_node_page = 0

    def get_node(self, page: int):
        """Get a node from storage.

        The cache is not there to prevent hitting the disk, the OS is already
        very good at it. It is there to avoid paying the price of deserializing
        the data to create the Node object and its entry. This is a very
        expensive operation in Python.

        Since we have at most a single writer we can write to cache on
        `set_node` if we invalidate the cache when a transaction is rolled
        back.
        """
        node = self._cache.get(page)
        if node is not None:
            return node

        data = self._wal.get_page(page) or self._read_page(page)

        node = Node.from_page_data(self._tree_conf, data=data, page=page)
        self._cache[node.page] = node
        return node

    def set_node(self, node: Node):
        self._wal.set_page(node.page, node.dump())
        self._cache[node.page] = node

    def del_node(self, node: Node):
        self._insert_in_freelist(node.page)

    def del_page(self, page: int):
        self._insert_in_freelist(page)

    @property
    def read_transaction(self):
        class ReadTransaction:
            def __enter__(self2):
                self._lock.reader_lock.acquire()

            def __exit__(self2, exc_type, exc_val, exc_tb):
                self._lock.reader_lock.release()

        return ReadTransaction()

    @property
    def write_transaction(self):
        class WriteTransaction:
            def __enter__(self2):
                self._lock.writer_lock.acquire()

            def __exit__(self2, exc_type, exc_val, exc_tb):
                if exc_type:
                    # When an error happens in the middle of a write
                    # transaction we must roll it back and clear the cache
                    # because the writer may have partially modified the Nodes
                    self._wal.rollback()
                    self._cache.clear()
                else:
                    self._wal.commit()
                self._lock.writer_lock.release()

        return WriteTransaction()

    @property
    def next_available_page(self) -> int:
        last_freelist_page = self._pop_from_freelist()
        if last_freelist_page is not None:
            return last_freelist_page

        self.last_page += 1
        return self.last_page

    def _traverse_free_list(
        self,
    ) -> tuple[FreelistNode | None, FreelistNode | None]:
        if self._freelist_start_page == 0:
            return None, None

        second_to_last_node = None
        last_node = self.get_node(self._freelist_start_page)

        while last_node.next_page is not None:
            second_to_last_node = last_node
            last_node = self.get_node(second_to_last_node.next_page)

        return second_to_last_node, last_node

    def _insert_in_freelist(self, page: int):
        """Insert a page at the end of the freelist."""
        _, last_node = self._traverse_free_list()

        self.set_node(FreelistNode(self._tree_conf, page=page, next_page=None))

        if last_node is None:
            # Write in metadata that the freelist got a new starting point
            self._freelist_start_page = page
            self.set_metadata(None, None)
        else:
            last_node.next_page = page
            self.set_node(last_node)

    def _pop_from_freelist(self) -> int | None:
        """Remove the last page from the freelist and return its page."""
        second_to_last_node, last_node = self._traverse_free_list()

        if last_node is None:
            # Freelist is completely empty, nothing to pop
            return None

        if second_to_last_node is None:
            # Write in metadata that the freelist is empty
            self._freelist_start_page = 0
            self.set_metadata(None, None)
        else:
            second_to_last_node.next_page = None
            self.set_node(second_to_last_node)

        return last_node.page

    # Todo: make metadata as a normal Node
    def get_metadata(self) -> tuple:
        try:
            data = self._read_page(0)
        except ReachedEndOfFile as e:
            raise ValueError("Metadata not set yet") from e
        end_root_node_page = PAGE_REFERENCE_BYTES
        root_node_page = int.from_bytes(data[:end_root_node_page], ENDIAN)
        end_page_size = end_root_node_page + OTHERS_BYTES
        page_size = int.from_bytes(data[end_root_node_page:end_page_size], ENDIAN)
        end_order = end_page_size + OTHERS_BYTES
        order = int.from_bytes(data[end_page_size:end_order], ENDIAN)
        end_key_size = end_order + OTHERS_BYTES
        key_size = int.from_bytes(data[end_order:end_key_size], ENDIAN)
        end_value_size = end_key_size + OTHERS_BYTES
        value_size = int.from_bytes(data[end_key_size:end_value_size], ENDIAN)
        end_freelist_start_page = end_value_size + PAGE_REFERENCE_BYTES
        self._freelist_start_page = int.from_bytes(
            data[end_value_size:end_freelist_start_page], ENDIAN
        )
        self._tree_conf = TreeConf(
            page_size, order, key_size, value_size, self._tree_conf.serializer
        )
        self._root_node_page = root_node_page
        return root_node_page, self._tree_conf

    def set_metadata(self, root_node_page: int | None, tree_conf: TreeConf | None):
        if root_node_page is None:
            root_node_page = self._root_node_page

        if tree_conf is None:
            tree_conf = self._tree_conf

        length = 2 * PAGE_REFERENCE_BYTES + 4 * OTHERS_BYTES
        data = (
            root_node_page.to_bytes(PAGE_REFERENCE_BYTES, ENDIAN)
            + tree_conf.page_size.to_bytes(OTHERS_BYTES, ENDIAN)
            + tree_conf.order.to_bytes(OTHERS_BYTES, ENDIAN)
            + tree_conf.key_size.to_bytes(OTHERS_BYTES, ENDIAN)
            + tree_conf.value_size.to_bytes(OTHERS_BYTES, ENDIAN)
            + self._freelist_start_page.to_bytes(PAGE_REFERENCE_BYTES, ENDIAN)
            + bytes(tree_conf.page_size - length)
        )
        self._write_page_in_tree(0, data, fsync=True)

        self._tree_conf = tree_conf
        self._root_node_page = root_node_page

    def close(self):
        self.perform_checkpoint()
        self._fd.close()
        if self._dir_fd is not None:
            os.close(self._dir_fd)

    def perform_checkpoint(self, reopen_wal=False):
        logger.info("Performing checkpoint of %s", self._filepath)
        for page, page_data in self._wal.checkpoint():
            self._write_page_in_tree(page, page_data, fsync=False)
        fsync_file_and_dir(self._fd.fileno(), self._dir_fd)
        if reopen_wal:
            self._wal = WAL(self._filepath, self._tree_conf.page_size)

    def _read_page(self, page: int) -> bytes:
        start = page * self._tree_conf.page_size
        stop = start + self._tree_conf.page_size
        assert stop - start == self._tree_conf.page_size
        return read_from_file(self._fd, start, stop)

    def _write_page_in_tree(
        self, page: int, data: bytes | bytearray, fsync: bool = True
    ):
        """Write a page of data in the tree file itself.

        To be used during checkpoints and other non-standard uses.
        """
        assert len(data) == self._tree_conf.page_size
        self._fd.seek(page * self._tree_conf.page_size)
        write_to_file(self._fd, self._dir_fd, data, fsync=fsync)

    def __repr__(self):
        return f"<FileMemory: {self._filepath}>"


class FrameType(enum.Enum):
    PAGE = 1
    COMMIT = 2
    ROLLBACK = 3


class WAL:
    __slots__ = [
        "filepath",
        "_fd",
        "_dir_fd",
        "_page_size",
        "_committed_pages",
        "_not_committed_pages",
        "needs_recovery",
    ]

    FRAME_HEADER_LENGTH = FRAME_TYPE_BYTES + PAGE_REFERENCE_BYTES

    def __init__(self, filepath: Path, page_size: int):
        self.filepath = filepath.with_suffix(f"{filepath.suffix}-wal")
        self._fd, self._dir_fd = open_file_in_dir(self.filepath)
        self._page_size = page_size
        self._committed_pages = {}
        self._not_committed_pages = {}

        self._fd.seek(0, io.SEEK_END)
        if self._fd.tell() == 0:
            self._create_header()
            self.needs_recovery = False
        else:
            logger.warning(
                "Found an existing WAL file, the B+Tree was not closed properly"
            )
            self.needs_recovery = True
            self._load_wal()

    def checkpoint(self):
        """Transfer the modified data back to the tree and close the WAL."""
        if self._not_committed_pages:
            logger.warning("Closing WAL with uncommitted data, discarding it")

        fsync_file_and_dir(self._fd.fileno(), self._dir_fd)

        for page, page_start in self._committed_pages.items():
            page_data = read_from_file(
                self._fd, page_start, page_start + self._page_size
            )
            yield page, page_data

        self._fd.close()
        self.filepath.unlink()
        if self._dir_fd is not None:
            os.fsync(self._dir_fd)
            os.close(self._dir_fd)

    def _create_header(self):
        data = self._page_size.to_bytes(OTHERS_BYTES, ENDIAN)
        self._fd.seek(0)
        write_to_file(self._fd, self._dir_fd, data, True)

    def _load_wal(self):
        self._fd.seek(0)
        header_data = read_from_file(self._fd, 0, OTHERS_BYTES)
        assert int.from_bytes(header_data, ENDIAN) == self._page_size

        while True:
            try:
                self._load_next_frame()
            except ReachedEndOfFile:
                break
        if self._not_committed_pages:
            logger.warning("WAL has uncommitted data, discarding it")
            self._not_committed_pages = {}

    def _load_next_frame(self):
        start = self._fd.tell()
        stop = start + self.FRAME_HEADER_LENGTH
        data = read_from_file(self._fd, start, stop)

        frame_type = int.from_bytes(data[:FRAME_TYPE_BYTES], ENDIAN)
        page = int.from_bytes(
            data[FRAME_TYPE_BYTES : FRAME_TYPE_BYTES + PAGE_REFERENCE_BYTES], ENDIAN
        )

        frame_type = FrameType(frame_type)
        if frame_type is FrameType.PAGE:
            self._fd.seek(stop + self._page_size)

        self._index_frame(frame_type, page, stop)

    def _index_frame(self, frame_type: FrameType, page: int, page_start: int):
        if frame_type is FrameType.PAGE:
            self._not_committed_pages[page] = page_start
        elif frame_type is FrameType.COMMIT:
            self._committed_pages.update(self._not_committed_pages)
            self._not_committed_pages = {}
        elif frame_type is FrameType.ROLLBACK:
            self._not_committed_pages = {}
        else:
            assert False

    def _add_frame(
        self,
        frame_type: FrameType,
        page: int | None = None,
        page_data: bytes | bytearray | None = None,
    ):
        if frame_type is FrameType.PAGE and (not page or not page_data):
            raise ValueError("PAGE frame without page data")
        if page_data and len(page_data) != self._page_size:
            raise ValueError("Page data is different from page size")
        if not page:
            page = 0
        if frame_type is not FrameType.PAGE:
            page_data = b""
        data = (
            frame_type.value.to_bytes(FRAME_TYPE_BYTES, ENDIAN)
            + page.to_bytes(PAGE_REFERENCE_BYTES, ENDIAN)
            + page_data
        )
        self._fd.seek(0, io.SEEK_END)
        write_to_file(self._fd, self._dir_fd, data, fsync=frame_type != FrameType.PAGE)
        self._index_frame(frame_type, page, self._fd.tell() - self._page_size)

    def get_page(self, page: int) -> bytes | None:
        page_start = None
        for store in (self._not_committed_pages, self._committed_pages):
            page_start = store.get(page)
            if page_start:
                break

        if not page_start:
            return None

        return read_from_file(self._fd, page_start, page_start + self._page_size)

    def set_page(self, page: int, page_data: bytes | bytearray):
        self._add_frame(FrameType.PAGE, page, page_data)

    def commit(self):
        # Commit is a no-op when there is no uncommitted pages
        if self._not_committed_pages:
            self._add_frame(FrameType.COMMIT)

    def rollback(self):
        # Rollback is a no-op when there is no uncommitted pages
        if self._not_committed_pages:
            self._add_frame(FrameType.ROLLBACK)

    def __repr__(self):
        return f"<WAL: {self.filepath}>"
