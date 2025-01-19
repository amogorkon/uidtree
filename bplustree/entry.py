import abc

from .const import (
    ENDIAN,
    PAGE_REFERENCE_BYTES,
    USED_KEY_LENGTH_BYTES,
    USED_VALUE_LENGTH_BYTES,
    TreeConf,
)

# Sentinel value indicating that a lazy loaded attribute is not yet loaded
NOT_LOADED = object()


class Entry(metaclass=abc.ABCMeta):
    __slots__ = []

    @abc.abstractmethod
    def load(self, data: bytes):
        """Deserialize data into an object."""

    @abc.abstractmethod
    def dump(self) -> bytes:
        """Serialize object to data."""


class ComparableEntry(Entry, metaclass=abc.ABCMeta):
    """Entry that can be sorted against other entries based on their key."""

    __slots__ = []

    def __eq__(self, other):
        return self.key == other.key

    def __lt__(self, other):
        return self.key < other.key

    def __le__(self, other):
        return self.key <= other.key

    def __gt__(self, other):
        return self.key > other.key

    def __ge__(self, other):
        return self.key >= other.key


class Record(ComparableEntry):
    """A container for the actual data the tree stores."""

    __slots__ = ["_tree_conf", "length", "_key", "_value", "_overflow_page", "_data"]

    def __init__(
        self,
        tree_conf: TreeConf,
        key=None,
        value: bytes | None = None,
        data: bytes | None = None,
        overflow_page: int | None = None,
    ):
        self._tree_conf = tree_conf
        self.length = (
            USED_KEY_LENGTH_BYTES
            + self._tree_conf.key_size
            + USED_VALUE_LENGTH_BYTES
            + self._tree_conf.value_size
            + PAGE_REFERENCE_BYTES
        )
        self._data = data

        if self._data:
            self._key = NOT_LOADED
            self._value = NOT_LOADED
            self._overflow_page = NOT_LOADED
        else:
            self._key = key
            self._value = value
            self._overflow_page = overflow_page

    @property
    def key(self):
        if self._key == NOT_LOADED:
            self.load(self._data)
        return self._key

    @key.setter
    def key(self, v):
        self._data = None
        self._key = v

    @property
    def value(self):
        if self._value == NOT_LOADED:
            self.load(self._data)
        return self._value

    @value.setter
    def value(self, v):
        self._data = None
        self._value = v

    @property
    def overflow_page(self):
        if self._overflow_page == NOT_LOADED:
            self.load(self._data)
        return self._overflow_page

    @overflow_page.setter
    def overflow_page(self, v):
        self._data = None
        self._overflow_page = v

    def load(self, data: bytes):
        assert len(data) == self.length

        end_used_key_length = USED_KEY_LENGTH_BYTES
        used_key_length = int.from_bytes(data[:end_used_key_length], ENDIAN)
        assert 0 <= used_key_length <= self._tree_conf.key_size

        end_key = end_used_key_length + used_key_length
        self._key = self._tree_conf.serializer.deserialize(
            data[end_used_key_length:end_key]
        )

        start_used_value_length = end_used_key_length + self._tree_conf.key_size
        end_used_value_length = start_used_value_length + USED_VALUE_LENGTH_BYTES
        used_value_length = int.from_bytes(
            data[start_used_value_length:end_used_value_length], ENDIAN
        )
        assert 0 <= used_value_length <= self._tree_conf.value_size

        end_value = end_used_value_length + used_value_length

        start_overflow = end_used_value_length + self._tree_conf.value_size
        end_overflow = start_overflow + PAGE_REFERENCE_BYTES
        if overflow_page := int.from_bytes(data[start_overflow:end_overflow], ENDIAN):
            self._overflow_page = overflow_page
            self._value = None
        else:
            self._overflow_page = None
            self._value = data[end_used_value_length:end_value]

    def dump(self) -> bytes:
        if self._data:
            return self._data

        assert self._value is None or self._overflow_page is None
        key_as_bytes = self._tree_conf.serializer.serialize(
            self._key, self._tree_conf.key_size
        )
        used_key_length = len(key_as_bytes)
        overflow_page = self._overflow_page or 0
        value = b"" if overflow_page else self._value
        used_value_length = len(value)

        return (
            used_key_length.to_bytes(USED_VALUE_LENGTH_BYTES, ENDIAN)
            + key_as_bytes
            + bytes(self._tree_conf.key_size - used_key_length)
            + used_value_length.to_bytes(USED_VALUE_LENGTH_BYTES, ENDIAN)
            + value
            + bytes(self._tree_conf.value_size - used_value_length)
            + overflow_page.to_bytes(PAGE_REFERENCE_BYTES, ENDIAN)
        )

    def __repr__(self):
        if self.overflow_page:
            return f"<Record: {self.key} overflowing value>"
        if self.value:
            return f"<Record: {self.key} value={self.value[:16]}>"
        return f"<Record: {self.key} unknown value>"


class Reference(ComparableEntry):
    """A container for a reference to other nodes."""

    __slots__ = ["_tree_conf", "length", "_key", "_before", "_after", "_data"]

    def __init__(
        self, tree_conf: TreeConf, key=None, before=None, after=None, data: bytes = None
    ):
        self._tree_conf = tree_conf
        self.length = (
            2 * PAGE_REFERENCE_BYTES + USED_KEY_LENGTH_BYTES + self._tree_conf.key_size
        )
        self._data = data

        if self._data:
            self._key = NOT_LOADED
            self._before = NOT_LOADED
            self._after = NOT_LOADED
        else:
            self._key = key
            self._before = before
            self._after = after

    @property
    def key(self):
        if self._key == NOT_LOADED:
            self.load(self._data)
        return self._key

    @key.setter
    def key(self, v):
        self._data = None
        self._key = v

    @property
    def before(self):
        if self._before == NOT_LOADED:
            self.load(self._data)
        return self._before

    @before.setter
    def before(self, v):
        self._data = None
        self._before = v

    @property
    def after(self):
        if self._after == NOT_LOADED:
            self.load(self._data)
        return self._after

    @after.setter
    def after(self, v):
        self._data = None
        self._after = v

    def load(self, data: bytes):
        assert len(data) == self.length
        end_before = PAGE_REFERENCE_BYTES
        self._before = int.from_bytes(data[:end_before], ENDIAN)

        end_used_key_length = end_before + USED_KEY_LENGTH_BYTES
        used_key_length = int.from_bytes(data[end_before:end_used_key_length], ENDIAN)
        assert 0 <= used_key_length <= self._tree_conf.key_size

        end_key = end_used_key_length + used_key_length
        self._key = self._tree_conf.serializer.deserialize(
            data[end_used_key_length:end_key]
        )

        start_after = end_used_key_length + self._tree_conf.key_size
        end_after = start_after + PAGE_REFERENCE_BYTES
        self._after = int.from_bytes(data[start_after:end_after], ENDIAN)

    def dump(self) -> bytes:
        if self._data:
            return self._data

        assert isinstance(self._before, int)
        assert isinstance(self._after, int)

        key_as_bytes = self._tree_conf.serializer.serialize(
            self._key, self._tree_conf.key_size
        )
        used_key_length = len(key_as_bytes)

        return (
            self._before.to_bytes(PAGE_REFERENCE_BYTES, ENDIAN)
            + used_key_length.to_bytes(USED_VALUE_LENGTH_BYTES, ENDIAN)
            + key_as_bytes
            + bytes(self._tree_conf.key_size - used_key_length)
            + self._after.to_bytes(PAGE_REFERENCE_BYTES, ENDIAN)
        )

    def __repr__(self):
        return f"<Reference: key={self.key} before={self.before} after={self.after}>"


class OpaqueData(Entry):
    """Entry holding opaque data."""

    __slots__ = ["data"]

    def __init__(self, tree_conf: TreeConf = None, data: bytes = None):
        self.data = data

    def load(self, data: bytes):
        self.data = data

    def dump(self) -> bytes:
        return self.data

    def __repr__(self):
        return f"<OpaqueData: {self.data}>"
