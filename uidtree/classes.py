from __future__ import annotations

from typing import Final
from uuid import NAMESPACE_DNS, UUID, uuid4, uuid5

import numpy as np
from numpy.typing import NDArray

_kv_store: dict[int, str] = {}
"Key-Value store for UUIDs and their string representations."


class E(int):
    dtype: Final = np.dtype([("high", "<u8"), ("low", "<u8")])
    "Static dtype definition for HDF5 serialization"

    __slots__ = ()

    def __new__(cls, id_: int | None = None) -> E:
        if id_ is None:
            id_ = uuid4().int
        return super().__new__(cls, id_)

    @classmethod
    def from_str(cls, value: str) -> E:
        id_ = uuid5(NAMESPACE_DNS, value).int
        _kv_store.setdefault(id_, value)
        return cls(id_)

    @property
    def value(self) -> str | None:
        return _kv_store.get(self)

    @property
    def high(self) -> int:
        return self >> 64

    @property
    def low(self) -> int:
        return self & ((1 << 64) - 1)

    @property
    def uuid(self) -> UUID:
        return UUID(int=self)

    def __repr__(self) -> str:
        return f"E({hex(self)})"

    def __str__(self) -> str:
        hex_str = hex(self)
        return f"E({hex_str[2:9]}..)" if len(hex_str) > 8 else f"E({hex_str[2:]})"

    def to_hdf5(self) -> NDArray[np.void]:
        """Convert to HDF5-compatible array"""
        return np.array((self.high, self.low), dtype=self.dtype)

    @classmethod
    def from_hdf5(cls, arr: NDArray[np.void]) -> E:
        """Instantiate from HDF5 data (18ns)"""
        return cls((arr["high"].item() << 64) | arr["low"].item())
