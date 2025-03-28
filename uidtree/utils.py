import itertools
from typing import Iterable, Iterator, Tuple


def pairwise(iterable: Iterable[int]) -> Iterator[Tuple[int, int]]:
    """Iterate over elements two by two.

    s -> (s0,s1), (s1,s2), (s2, s3), ...
    """
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


def iter_slice(iterable: bytes, n: int) -> Iterator[Tuple[bytes, bool]]:
    """Yield slices of size n and says if each slice is the last one.

    s -> (b'123', False), (b'45', True)
    """
    start = 0
    stop = start + n
    final_offset = len(iterable)

    while start < final_offset:
        rv = iterable[start:stop]
        start = stop
        stop = start + n
        yield rv, start >= final_offset
