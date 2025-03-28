# tree.py
from __future__ import annotations

from collections.abc import Iterable, Iterator
from functools import partial
from logging import getLogger
from pathlib import Path
from typing import Tuple

from beartype import beartype

from . import utils
from .const import TreeConf
from .entry import OpaqueData, Record, Reference
from .memory import FileMemory
from .node import InternalNode, LeafNode, LonelyRootNode, Node, OverflowNode, RootNode

logger = getLogger(__name__)


class BPlusTree:
    __slots__ = [
        "_filepath",
        "_tree_conf",
        "_mem",
        "_root_node_page",
        "_is_open",
        "LonelyRootNode",
        "RootNode",
        "InternalNode",
        "LeafNode",
        "OverflowNode",
        "Record",
        "Reference",
    ]

    # ######################### Public API ################################

    @beartype
    def __init__(
        self,
        filepath: Path,
        page_size: int = 4096,
        order: int = 100,
        key_size: int = 16,  # 128-bit keys
        value_size: int = 32,
        cache_size: int = 64,
    ) -> None:
        self._filepath = filepath
        self._tree_conf = TreeConf(page_size, order, key_size, value_size)
        self._create_partials()
        self._mem = FileMemory(filepath, self._tree_conf, cache_size=cache_size)
        try:
            metadata = self._mem.get_metadata()
        except ValueError:
            self._initialize_empty_tree()
        else:
            self._root_node_page, self._tree_conf = metadata
        self._is_open = True

    @beartype
    def close(self) -> None:
        with self._mem.write_transaction:
            if not self._is_open:
                logger.info("Tree is already closed")
                return

            self._mem.close()
            self._is_open = False

    @beartype
    def __enter__(self) -> BPlusTree:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):  # type: ignore
        self.close()

    @beartype
    def checkpoint(self) -> None:
        with self._mem.write_transaction:
            self._mem.perform_checkpoint(reopen_wal=True)

    @beartype
    def insert(self, key: int, value: bytes, replace: bool = False) -> None:
        """Insert a value in the tree.

        :param key: The key at which the value will be recorded, must be of the
                    same type used by the Serializer
        :param value: The value to record in bytes
        :param replace: If True, already existing value will be overridden,
                        otherwise a ValueError is raised.
        """
        if not isinstance(value, bytes):
            ValueError("Values must be bytes objects")

        with self._mem.write_transaction:
            node = self._search_in_tree(key, self._root_node)

            # Check if a record with the key already exists
            try:
                existing_record = node.get_entry(key)
            except ValueError:
                pass
            else:
                if not replace:
                    raise ValueError(f"Key {key} already exists")

                if existing_record.overflow_page:
                    self._delete_overflow(existing_record.overflow_page)

                if len(value) <= self._tree_conf.value_size:
                    existing_record.value = value
                    existing_record.overflow_page = None
                else:
                    existing_record.value = None
                    existing_record.overflow_page = self._create_overflow(value)
                self._mem.set_node(node)
                return

            if len(value) <= self._tree_conf.value_size:
                record = self.Record(key, value=value)
            else:
                # Record values exceeding the max value_size must be placed
                # into overflow pages
                first_overflow_page = self._create_overflow(value)
                record = self.Record(key, value=None, overflow_page=first_overflow_page)

            if node.can_add_entry:
                node.insert_entry(record)
                self._mem.set_node(node)
            else:
                node.insert_entry(record)
                self._split_leaf(node)

    @beartype
    def batch_insert(self, iterable: Iterable[tuple[int, bytes]]) -> None:
        """Insert many elements in the tree at once.

        The iterable object must yield tuples (key, value) in ascending order.
        All keys to insert must be bigger than all keys currently in the tree.
        All inserts happen in a single transaction. This is way faster than
        manually inserting in a loop.
        """
        node = None
        with self._mem.write_transaction:
            for key, value in iterable:
                if node is None:
                    node = self._search_in_tree(key, self._root_node)

                try:
                    biggest_entry = node.biggest_entry
                except IndexError:
                    biggest_entry = None
                if biggest_entry and key <= biggest_entry.key:
                    raise ValueError(
                        "Keys to batch insert must be sorted and "
                        "bigger than keys currently in the tree"
                    )

                if len(value) <= self._tree_conf.value_size:
                    record = self.Record(key, value=value)
                else:
                    # Record values exceeding the max value_size must be placed
                    # into overflow pages
                    first_overflow_page = self._create_overflow(value)
                    record = self.Record(
                        key, value=None, overflow_page=first_overflow_page
                    )

                if node.can_add_entry:
                    node.insert_entry_at_the_end(record)
                else:
                    node.insert_entry_at_the_end(record)
                    self._split_leaf(node)
                    node = None

            if node is not None:
                self._mem.set_node(node)

    @beartype
    def get(
        self,
        key: int,
        default: bytes | None = None,
    ) -> bytes | None:
        with self._mem.read_transaction:
            node = self._search_in_tree(key, self._root_node)
            try:
                record = node.get_entry(key)
            except ValueError:
                return default
            else:
                rv = self._get_value_from_record(record)
                assert isinstance(rv, bytes)
                return rv

    @beartype
    def __contains__(self, item: int) -> bool:
        with self._mem.read_transaction:
            return self.get(item) is not None

    @beartype
    def __setitem__(self, key: int, value: bytes) -> None:
        self.insert(key, value, replace=True)

    @beartype
    def __getitem__(self, item: int | slice) -> dict[int, bytes] | bytes:
        with self._mem.read_transaction:
            if isinstance(item, slice):
                rv = {
                    record.key: self._get_value_from_record(record)
                    for record in self._iter_slice(item)
                }
            else:
                rv = self.get(item)
                if rv is None:
                    raise KeyError(item)

            return rv

    @beartype
    def __len__(self) -> int:
        with self._mem.read_transaction:
            node = self._left_record_node
            rv = 0
            while True:
                rv += len(node.entries)
                if not node.next_page:
                    return rv
                node = self._mem.get_node(node.next_page)

    @beartype
    def __length_hint__(self) -> int:
        with self._mem.read_transaction:
            node = self._root_node
            if isinstance(node, LonelyRootNode):
                # Assume that the lonely root node is half full
                return node.max_children // 2
            # Assume that there are no holes in pages
            last_page = self._mem.last_page
            # Assume that 70% of nodes in a tree carry values
            num_leaf_nodes = int(last_page * 0.70)
            # Assume that every leaf node is half full
            num_records_per_leaf_node = int((node.max_children + node.min_children) / 2)
            return num_leaf_nodes * num_records_per_leaf_node

    @beartype
    def __iter__(self, slice_: slice | None = None) -> Iterator[int]:
        if not slice_:
            slice_ = slice(None)
        with self._mem.read_transaction:
            for record in self._iter_slice(slice_):
                yield record.key

    keys = __iter__

    @beartype
    def items(self, slice_: slice | None = None) -> Iterator[Tuple[int, bytes]]:
        if not slice_:
            slice_ = slice(None)
        with self._mem.read_transaction:
            for record in self._iter_slice(slice_):
                yield record.key, self._get_value_from_record(record)

    @beartype
    def values(self, slice_: slice | None = None) -> Iterator[bytes]:
        if not slice_:
            slice_ = slice(None)
        with self._mem.read_transaction:
            for record in self._iter_slice(slice_):
                yield self._get_value_from_record(record)

    @beartype
    def __bool__(self) -> bool:
        with self._mem.read_transaction:
            for _ in self:
                return True
            return False

    @beartype
    def __repr__(self) -> str:
        return f"<BPlusTree: {self._filepath} {self._tree_conf}>"

    # ####################### Implementation ##############################

    @beartype
    def _initialize_empty_tree(self) -> None:
        self._root_node_page = self._mem.next_available_page
        with self._mem.write_transaction:
            self._mem.set_node(self.LonelyRootNode(page=self._root_node_page))
        self._mem.set_metadata(self._root_node_page, self._tree_conf)

    @beartype
    def _create_partials(self) -> None:
        self.LonelyRootNode = partial(LonelyRootNode, self._tree_conf)
        self.RootNode = partial(RootNode, self._tree_conf)
        self.InternalNode = partial(InternalNode, self._tree_conf)
        self.LeafNode = partial(LeafNode, self._tree_conf)
        self.OverflowNode = partial(OverflowNode, self._tree_conf)
        self.Record = partial(Record, self._tree_conf)
        self.Reference = partial(Reference, self._tree_conf)

    @property
    @beartype
    def _root_node(self) -> LonelyRootNode | RootNode:
        root_node = self._mem.get_node(self._root_node_page)
        assert isinstance(root_node, (LonelyRootNode, RootNode))
        return root_node

    @property
    @beartype
    def _left_record_node(self) -> LonelyRootNode | LeafNode:
        node = self._root_node
        while not isinstance(node, (LonelyRootNode, LeafNode)):
            node = self._mem.get_node(node.smallest_entry.before)
        return node

    @beartype
    def _iter_slice(self, slice_: slice) -> Iterator[Record]:
        if slice_.step is not None:
            raise ValueError("Cannot iterate with a custom step")

        if (
            slice_.start is not None
            and slice_.stop is not None
            and slice_.start >= slice_.stop
        ):
            raise ValueError("Cannot iterate backwards")

        if slice_.start is None:
            node = self._left_record_node
        else:
            node = self._search_in_tree(slice_.start, self._root_node)

        while True:
            for entry in node.entries:
                if slice_.start is not None and entry.key < slice_.start:
                    continue

                if slice_.stop is not None and entry.key >= slice_.stop:
                    return

                yield entry

            if node.next_page:
                node = self._mem.get_node(node.next_page)
            else:
                return

    @beartype
    def _search_in_tree(self, key: int, node: Node) -> Node:
        if isinstance(node, (LonelyRootNode, LeafNode)):
            return node

        page = None

        if key < node.smallest_key:
            page = node.smallest_entry.before

        elif node.biggest_key <= key:
            page = node.biggest_entry.after

        else:
            for ref_a, ref_b in utils.pairwise(node.entries):
                if ref_a.key <= key < ref_b.key:
                    page = ref_a.after
                    break

        assert page is not None

        child_node = self._mem.get_node(page)
        child_node.parent = node
        return self._search_in_tree(key, child_node)

    @beartype
    def _split_leaf(self, old_node: Node) -> None:
        """Split a leaf Node to allow the tree to grow."""
        parent = old_node.parent
        new_node = self.LeafNode(
            page=self._mem.next_available_page, next_page=old_node.next_page
        )
        new_entries = old_node.split_entries()
        new_node.entries = new_entries
        ref = self.Reference(new_node.smallest_key, old_node.page, new_node.page)

        if isinstance(old_node, LonelyRootNode):
            # Convert the LonelyRoot into a Leaf
            old_node = old_node.convert_to_leaf()
            self._create_new_root(ref)
        elif parent.can_add_entry:
            parent.insert_entry(ref)
            self._mem.set_node(parent)
        else:
            parent.insert_entry(ref)
            self._split_parent(parent)

        old_node.next_page = new_node.page

        self._mem.set_node(old_node)
        self._mem.set_node(new_node)

    @beartype
    def _split_parent(self, old_node: Node) -> None:
        parent = old_node.parent
        new_node = self.InternalNode(page=self._mem.next_available_page)
        new_entries = old_node.split_entries()
        new_node.entries = new_entries

        ref = new_node.pop_smallest()
        ref.before = old_node.page
        ref.after = new_node.page

        if isinstance(old_node, RootNode):
            # Convert the Root into an Internal
            old_node = old_node.convert_to_internal()
            self._create_new_root(ref)
        elif parent.can_add_entry:
            parent.insert_entry(ref)
            self._mem.set_node(parent)
        else:
            parent.insert_entry(ref)
            self._split_parent(parent)

        self._mem.set_node(old_node)
        self._mem.set_node(new_node)

    @beartype
    def _create_new_root(self, reference: Reference) -> None:
        new_root = self.RootNode(page=self._mem.next_available_page)
        new_root.insert_entry(reference)
        self._root_node_page = new_root.page
        self._mem.set_metadata(self._root_node_page, self._tree_conf)
        self._mem.set_node(new_root)

    @beartype
    def _create_overflow(self, value: bytes) -> int:
        first_overflow_page = self._mem.next_available_page
        next_overflow_page = first_overflow_page

        iterator = utils.iter_slice(value, self.OverflowNode().max_payload)
        for slice_value, is_last in iterator:
            current_overflow_page = next_overflow_page

            next_overflow_page = None if is_last else self._mem.next_available_page
            overflow_node = self.OverflowNode(
                page=current_overflow_page, next_page=next_overflow_page
            )
            overflow_node.insert_entry_at_the_end(OpaqueData(data=slice_value))
            self._mem.set_node(overflow_node)

        return first_overflow_page

    @beartype
    def _traverse_overflow(self, first_overflow_page: int) -> Iterator[OverflowNode]:
        """Yield all Nodes of an overflow chain."""
        next_overflow_page = first_overflow_page
        while True:
            overflow_node = self._mem.get_node(next_overflow_page)
            yield overflow_node

            next_overflow_page = overflow_node.next_page
            if next_overflow_page is None:
                break

    @beartype
    def _read_from_overflow(self, first_overflow_page: int) -> bytes:
        """Collect all values of an overflow chain."""
        rv = bytearray()
        for overflow_node in self._traverse_overflow(first_overflow_page):
            rv.extend(overflow_node.smallest_entry.data)

        return bytes(rv)

    @beartype
    def _delete_overflow(self, first_overflow_page: int) -> None:
        """Delete all Nodes in an overflow chain."""
        for overflow_node in self._traverse_overflow(first_overflow_page):
            self._mem.del_node(overflow_node)

    @beartype
    def _get_value_from_record(self, record: Record) -> bytes:
        if record.value is not None:
            return record.value

        return self._read_from_overflow(record.overflow_page)
