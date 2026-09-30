"""Type stubs for the read-only published keyed-state readers.

Each read takes the user key as its first argument. The query options match
the handler handles: see :meth:`prosody.state.MapState.keys` and
:meth:`prosody.state.DequeState.values`.
"""

from typing import Generic, List, Optional, Tuple, Union

from typing_extensions import TypeVar

from prosody.message import JSONValue
from prosody.query import Direction, _StateScan

T = TypeVar("T", default=JSONValue)
V = TypeVar("V", default=JSONValue)


class PublishedValue(Generic[T]):
    """Read-only access to a value collection that another client publishes."""

    async def get(self, key: str) -> Optional[T]:
        """Read the value for ``key``, or ``None`` when there is none."""
        ...


class PublishedMap(Generic[V]):
    """Read-only access to a map collection that another client publishes."""

    async def get(self, key: str, map_key: str) -> Optional[V]:
        """Read the entry for ``map_key``, or ``None`` when there is none."""
        ...
    async def get_many(self, key: str, map_keys: List[str]) -> List[Optional[V]]:
        """Read several entries in one batch, one result for each map key."""
        ...
    async def contains(self, key: str, map_key: str) -> bool:
        """Report whether ``map_key`` has an entry. This does not decode it."""
        ...
    async def contains_many(self, key: str, map_keys: List[str]) -> List[bool]:
        """Report presence for several map keys, one result for each."""
        ...
    async def is_empty(self, key: str) -> bool:
        """Report whether the map for ``key`` has no entries."""
        ...
    def items(
        self,
        key: str,
        direction: Direction = ...,
        *,
        prefix: Optional[str] = ...,
        from_: Optional[str] = ...,
        after: Optional[str] = ...,
        to: Optional[str] = ...,
        before: Optional[str] = ...,
        range: Optional[slice] = ...,
        limit: Optional[int] = ...,
    ) -> _StateScan[Tuple[str, V]]:
        """Async iterator over the ``(map_key, value)`` entries in key order."""
        ...
    def keys(
        self,
        key: str,
        direction: Direction = ...,
        *,
        prefix: Optional[str] = ...,
        from_: Optional[str] = ...,
        after: Optional[str] = ...,
        to: Optional[str] = ...,
        before: Optional[str] = ...,
        range: Optional[slice] = ...,
        limit: Optional[int] = ...,
    ) -> _StateScan[str]:
        """Async iterator over the map keys in key order. It does not decode values."""
        ...
    def values(
        self,
        key: str,
        direction: Direction = ...,
        *,
        prefix: Optional[str] = ...,
        from_: Optional[str] = ...,
        after: Optional[str] = ...,
        to: Optional[str] = ...,
        before: Optional[str] = ...,
        range: Optional[slice] = ...,
        limit: Optional[int] = ...,
    ) -> _StateScan[V]:
        """Async iterator over the values in key order."""
        ...


class PublishedSet:
    """Read-only access to a set collection that another client publishes."""

    async def contains(self, key: str, member: str) -> bool:
        """Report whether the set for ``key`` has ``member``."""
        ...
    async def contains_many(self, key: str, members: List[str]) -> List[bool]:
        """Report presence for several members, one result for each."""
        ...
    async def is_empty(self, key: str) -> bool:
        """Report whether the set for ``key`` has no members."""
        ...
    def members(
        self,
        key: str,
        direction: Direction = ...,
        *,
        prefix: Optional[str] = ...,
        from_: Optional[str] = ...,
        after: Optional[str] = ...,
        to: Optional[str] = ...,
        before: Optional[str] = ...,
        range: Optional[slice] = ...,
        limit: Optional[int] = ...,
    ) -> _StateScan[str]:
        """Async iterator over the members in order."""
        ...


class PublishedDeque(Generic[T]):
    """Read-only access to a deque collection that another client publishes."""

    async def get(self, key: str, index: int) -> Optional[T]:
        """Read the value at ``index``, or ``None`` past either end.

        A negative index counts from the back, as in a Python sequence.
        """
        ...
    async def size(self, key: str) -> int:
        """Return the number of live values."""
        ...
    async def is_empty(self, key: str) -> bool:
        """Report whether the deque for ``key`` has no live values."""
        ...
    async def peek(self, key: str) -> Optional[T]:
        """Read the back value, or ``None`` when there is none."""
        ...
    async def peekleft(self, key: str) -> Optional[T]:
        """Read the front value, or ``None`` when there is none."""
        ...
    def values(
        self,
        key: str,
        direction: Direction = ...,
        *,
        from_: Optional[int] = ...,
        after: Optional[int] = ...,
        to: Optional[int] = ...,
        before: Optional[int] = ...,
        range: Union[range, slice, None] = ...,
        limit: Optional[int] = ...,
    ) -> _StateScan[T]:
        """Async iterator over the values in position order."""
        ...
