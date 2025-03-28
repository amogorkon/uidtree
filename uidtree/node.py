# node.py
from __future__ import annotations

import abc
import bisect
import math

from beartype import beartype

from .const import (
    NODE_TYPE_BYTES,
    PAGE_REFERENCE_BYTES,
    USED_PAGE_LENGTH_BYTES,
    TreeConf,
)
from .entry import Entry, OpaqueData, Record, Reference
from .serializer import deserialize, serialize


class Node(metaclass=abc.ABCMeta):
    __slots__ = ["_tree_conf", "entries", "page", "parent", "next_page"]

    # Attributes to redefine in inherited classes
    _node_type_int = 0
    max_children = 0
    min_children = 0
    _entry_class = None

    @beartype
    def __init__(
        self,
        tree_conf: TreeConf,
        data: bytes | bytearray | None = None,
        page: int | None = None,
        parent: Node | None = None,
        next_page: int | None = None,
    ) -> None:
        self._tree_conf = tree_conf
        self.entries: list[Entry] = []
        self.page = page
        self.parent = parent
        self.next_page = next_page
        if data:
            self.load(data)

    @beartype
    def load(self, data: bytes | bytearray) -> None:
        assert len(data) == self._tree_conf.page_size
        end_used_page_length = NODE_TYPE_BYTES + USED_PAGE_LENGTH_BYTES
        used_page_length = deserialize(data[NODE_TYPE_BYTES:end_used_page_length])
        end_header = end_used_page_length + PAGE_REFERENCE_BYTES
        self.next_page = deserialize(data[end_used_page_length:end_header])
        if self.next_page == 0:
            self.next_page = None

        if self._entry_class is None:
            # For Nodes that cannot hold Entries
            return

        try:
            # For Nodes that can hold multiple sized Entries
            entry_length = self._entry_class(self._tree_conf).length
        except AttributeError:
            # For Nodes that can hold a single variable sized Entry
            entry_length = used_page_length - end_header

        for start_offset in range(end_header, used_page_length, entry_length):
            entry_data = data[start_offset : start_offset + entry_length]
            entry = self._entry_class(self._tree_conf, data=entry_data)
            self.entries.append(entry)

    @beartype
    def dump(self) -> bytearray:
        data = bytearray()
        for record in self.entries:
            data.extend(record.dump())

        # used_page_length = len(header) + len(data), but the header is
        # generated later
        used_page_length = len(data) + 4 + PAGE_REFERENCE_BYTES
        assert 0 < used_page_length <= self._tree_conf.page_size
        assert len(data) <= self.max_payload

        next_page = 0 if self.next_page is None else self.next_page
        header = (
            serialize(self._node_type_int, 1)
            + serialize(used_page_length, 3)
            + serialize(next_page, PAGE_REFERENCE_BYTES)
        )

        data = bytearray(header) + data

        padding = self._tree_conf.page_size - used_page_length
        assert padding >= 0
        data.extend(bytearray(padding))
        assert len(data) == self._tree_conf.page_size

        return data

    @property
    @beartype
    def max_payload(self) -> int:
        """Size in bytes of serialized payload a Node can carry."""
        return self._tree_conf.page_size - 4 - PAGE_REFERENCE_BYTES

    @property
    @beartype
    def can_add_entry(self) -> bool:
        return self.num_children < self.max_children

    @property
    @beartype
    def can_delete_entry(self) -> bool:
        return self.num_children > self.min_children

    @property
    @beartype
    def smallest_key(self) -> int:
        return self.smallest_entry.key

    @property
    @beartype
    def smallest_entry(self) -> Entry:
        return self.entries[0]

    @property
    @beartype
    def biggest_key(self) -> int:
        return self.biggest_entry.key

    @property
    @beartype
    def biggest_entry(self) -> Entry:
        return self.entries[-1]

    @property
    @beartype
    def num_children(self) -> int:
        """Number of entries or other nodes connected to the node."""
        return len(self.entries)

    @beartype
    def pop_smallest(self) -> Entry:
        """Remove and return the smallest entry."""
        return self.entries.pop(0)

    @beartype
    def insert_entry(self, entry: Entry) -> None:
        bisect.insort(self.entries, entry)

    @beartype
    def insert_entry_at_the_end(self, entry: Entry) -> None:
        """Insert an entry at the end of the entry list.

        This is an optimized version of `insert_entry` when it is known that
        the key to insert is bigger than any other entries.
        """
        self.entries.append(entry)

    @beartype
    def remove_entry(self, key: int) -> None:
        self.entries.pop(self._find_entry_index(key))

    @beartype
    def get_entry(self, key: int) -> Entry:
        return self.entries[self._find_entry_index(key)]

    @beartype
    def _find_entry_index(self, key: int) -> int:
        entry = self._entry_class(
            self._tree_conf,
            key=key,  # Hack to compare and order
        )
        i = bisect.bisect_left(self.entries, entry)
        if i != len(self.entries) and self.entries[i] == entry:
            return i
        raise ValueError(f"No entry for key {key}")

    @beartype
    def split_entries(self) -> list[Entry]:
        """Split the entries in half.

        Keep the lower part in the node and return the upper one."""
        len_entries = len(self.entries)
        rv = self.entries[len_entries // 2 :]
        self.entries = self.entries[: len_entries // 2]
        assert len(self.entries) + len(rv) == len_entries
        return rv

    @classmethod
    @beartype
    def from_page_data(
        cls, tree_conf: TreeConf, data: bytes | bytearray, page: int | None = None
    ) -> Node:
        node_type_byte = data[:NODE_TYPE_BYTES]
        node_type_int = deserialize(node_type_byte)
        if node_type_int == 1:
            return LonelyRootNode(tree_conf, data, page)
        elif node_type_int == 2:
            return RootNode(tree_conf, data, page)
        elif node_type_int == 3:
            return InternalNode(tree_conf, data, page)
        elif node_type_int == 4:
            return LeafNode(tree_conf, data, page)
        elif node_type_int == 5:
            return OverflowNode(tree_conf, data, page)
        elif node_type_int == 6:
            return FreelistNode(tree_conf, data, page)
        else:
            assert False, f"No Node with type {node_type_int} exists"

    @beartype
    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__}: page={self.page} entries={len(self.entries)}>"
        )

    @beartype
    def __eq__(self, other: Node) -> bool:
        return (
            self.__class__ is other.__class__
            and self.page == other.page
            and self.entries == other.entries
        )


class RecordNode(Node):
    __slots__ = ["_entry_class"]

    @beartype
    def __init__(
        self,
        tree_conf: TreeConf,
        data: bytes | bytearray | None = None,
        page: int | None = None,
        parent: Node | None = None,
        next_page: int | None = None,
    ) -> None:
        self._entry_class = Record
        super().__init__(tree_conf, data, page, parent, next_page)


class LonelyRootNode(RecordNode):
    """A Root node that holds records.

    It is an exception for when there is only a single node in the tree.
    """

    __slots__ = ["_node_type_int", "min_children", "max_children"]

    @beartype
    def __init__(
        self,
        tree_conf: TreeConf,
        data: bytes | bytearray | None = None,
        page: int | None = None,
        parent: Node | None = None,
    ) -> None:
        self._node_type_int = 1
        self.min_children = 0
        self.max_children = tree_conf.order - 1
        super().__init__(tree_conf, data, page, parent)

    @beartype
    def convert_to_leaf(self) -> LeafNode:
        leaf = LeafNode(self._tree_conf, page=self.page)
        leaf.entries = self.entries
        return leaf


class LeafNode(RecordNode):
    """Node that holds the actual records within the tree."""

    __slots__ = ["_node_type_int", "min_children", "max_children"]

    @beartype
    def __init__(
        self,
        tree_conf: TreeConf,
        data: bytes | bytearray | None = None,
        page: int | None = None,
        parent: Node | None = None,
        next_page: int | None = None,
    ) -> None:
        self._node_type_int = 4
        self.min_children = math.ceil(tree_conf.order / 2) - 1
        self.max_children = tree_conf.order - 1
        super().__init__(tree_conf, data, page, parent, next_page)


class ReferenceNode(Node):
    __slots__ = ["_entry_class"]

    @beartype
    def __init__(
        self,
        tree_conf: TreeConf,
        data: bytes | bytearray | None = None,
        page: int | None = None,
        parent: Node | None = None,
    ) -> None:
        self._entry_class = Reference
        super().__init__(tree_conf, data, page, parent)

    @property
    @beartype
    def num_children(self) -> int:
        return len(self.entries) + 1 if self.entries else 0

    @beartype
    def insert_entry(self, entry: Reference) -> None:
        """Make sure that after of a reference matches before of the next one.

        Probably very inefficient approach.
        """
        super().insert_entry(entry)
        i = self.entries.index(entry)
        if i > 0:
            previous_entry = self.entries[i - 1]
            previous_entry.after = entry.before
        try:
            next_entry = self.entries[i + 1]
        except IndexError:
            pass
        else:
            next_entry.before = entry.after


class RootNode(ReferenceNode):
    """The first node at the top of the tree."""

    __slots__ = ["_node_type_int", "min_children", "max_children"]

    @beartype
    def __init__(
        self,
        tree_conf: TreeConf,
        data: bytes | bytearray | None = None,
        page: int | None = None,
        parent: Node | None = None,
    ) -> None:
        self._node_type_int = 2
        self.min_children = 2
        self.max_children = tree_conf.order
        super().__init__(tree_conf, data, page, parent)

    @beartype
    def convert_to_internal(self) -> InternalNode:
        internal = InternalNode(self._tree_conf, page=self.page)
        internal.entries = self.entries
        return internal


class InternalNode(ReferenceNode):
    """Node that only holds references to other Internal nodes or Leaves."""

    __slots__ = ["_node_type_int", "min_children", "max_children"]

    @beartype
    def __init__(
        self,
        tree_conf: TreeConf,
        data: bytes | bytearray | None = None,
        page: int | None = None,
        parent: Node | None = None,
    ) -> None:
        self._node_type_int = 3
        self.min_children = math.ceil(tree_conf.order / 2)
        self.max_children = tree_conf.order
        super().__init__(tree_conf, data, page, parent)


class OverflowNode(Node):
    """Node that holds a single Record value too large for its Node."""

    @beartype
    def __init__(
        self,
        tree_conf: TreeConf,
        data: bytes | bytearray | None = None,
        page: int | None = None,
        next_page: int | None = None,
    ) -> None:
        self._node_type_int = 5
        self.max_children = 1
        self.min_children = 1
        self._entry_class = OpaqueData
        super().__init__(tree_conf, data, page, next_page=next_page)

    @beartype
    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__}: page={self.page} next_page={self.next_page}>"
        )


class FreelistNode(Node):
    """Node that is a marker for a deallocated page."""

    @beartype
    def __init__(
        self,
        tree_conf: TreeConf,
        data: bytes | bytearray | None = None,
        page: int | None = None,
        next_page: int | None = None,
    ) -> None:
        self._node_type_int = 6
        self.max_children = 0
        self.min_children = 0
        super().__init__(tree_conf, data, page, next_page=next_page)

    @beartype
    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__}: page={self.page} next_page={self.next_page}>"
        )
