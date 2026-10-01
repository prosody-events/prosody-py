"""Read-only views over published keyed state.

:meth:`ProsodyClient.state` opens these readers. Each read takes the user key
as its first argument because no handler supplies one. The query options
match the handler handles: see :meth:`prosody.state.MapState.keys` and
:meth:`prosody.state.DequeState.values`.
"""

from typing import Any, Generic, List, Optional

from typing_extensions import TypeVar

from prosody.message import JSONValue
from prosody.query import (
    Direction,
    _StateScan,
    _deque_index,
    _identity,
    _key_query,
    _position_query,
)

T = TypeVar("T", default=JSONValue)
V = TypeVar("V", default=JSONValue)


class PublishedValue(Generic[T]):
    """Read-only access to a value collection that another client publishes."""

    def __init__(self, native: Any) -> None:
        self._native = native

    async def get(self, key: str) -> Optional[T]:
        """Read the value for ``key``, or ``None`` when there is none."""
        return await self._native.get(key)


class PublishedMap(Generic[V]):
    """Read-only access to a map collection that another client publishes."""

    def __init__(self, native: Any) -> None:
        self._native = native

    async def get(self, key: str, map_key: str) -> Optional[V]:
        """Read the entry for ``map_key``, or ``None`` when there is none."""
        return await self._native.get(key, map_key)

    async def get_many(self, key: str, map_keys: List[str]) -> List[Optional[V]]:
        """Read several entries in one batch, one result for each map key."""
        return await self._native.get_many(key, map_keys)

    async def contains(self, key: str, map_key: str) -> bool:
        """Report whether ``map_key`` has an entry. This does not decode it."""
        return await self._native.contains_key(key, map_key)

    async def contains_many(self, key: str, map_keys: List[str]) -> List[bool]:
        """Report presence for several map keys, one result for each."""
        return await self._native.contains_many(key, map_keys)

    async def is_empty(self, key: str) -> bool:
        """Report whether the map for ``key`` has no entries."""
        return await self._native.is_empty(key)

    def items(
        self, key: str, direction: Direction = Direction.FORWARD, **options: Any
    ) -> "_StateScan[tuple[str, V]]":
        """Async iterator over the ``(map_key, value)`` entries in key order."""
        query = _key_query(direction, **options)
        return _StateScan(self._native.scan(key, query), _identity)

    def keys(
        self, key: str, direction: Direction = Direction.FORWARD, **options: Any
    ) -> "_StateScan[str]":
        """Async iterator over the map keys in key order. It does not decode values."""
        query = _key_query(direction, **options)
        return _StateScan(self._native.keys(key, query), _identity)

    def values(
        self, key: str, direction: Direction = Direction.FORWARD, **options: Any
    ) -> "_StateScan[V]":
        """Async iterator over the values in key order."""
        query = _key_query(direction, **options)
        return _StateScan(self._native.scan(key, query), lambda entry: entry[1])


class PublishedSet:
    """Read-only access to a set collection that another client publishes."""

    def __init__(self, native: Any) -> None:
        self._native = native

    async def contains(self, key: str, member: str) -> bool:
        """Report whether the set for ``key`` has ``member``."""
        return await self._native.contains(key, member)

    async def contains_many(self, key: str, members: List[str]) -> List[bool]:
        """Report presence for several members, one result for each."""
        return await self._native.contains_many(key, members)

    async def is_empty(self, key: str) -> bool:
        """Report whether the set for ``key`` has no members."""
        return await self._native.is_empty(key)

    def members(
        self, key: str, direction: Direction = Direction.FORWARD, **options: Any
    ) -> "_StateScan[str]":
        """Async iterator over the members in order."""
        query = _key_query(direction, **options)
        return _StateScan(self._native.keys(key, query), _identity)


class PublishedDeque(Generic[T]):
    """Read-only access to a deque collection that another client publishes."""

    def __init__(self, native: Any) -> None:
        self._native = native

    async def get(self, key: str, index: int) -> Optional[T]:
        """Read the value at ``index``, or ``None`` past either end.

        A negative index counts from the back, as in a Python sequence.
        """
        position = await _deque_index(index, lambda: self.size(key))
        return None if position is None else await self._native.get(key, position)

    async def size(self, key: str) -> int:
        """Return the number of live values."""
        return await self._native.len(key)

    async def is_empty(self, key: str) -> bool:
        """Report whether the deque for ``key`` has no live values."""
        return await self._native.is_empty(key)

    async def peek(self, key: str) -> Optional[T]:
        """Read the back value, or ``None`` when there is none."""
        return await self._native.peek_back(key)

    async def peekleft(self, key: str) -> Optional[T]:
        """Read the front value, or ``None`` when there is none."""
        return await self._native.peek_front(key)

    def values(
        self, key: str, direction: Direction = Direction.FORWARD, **options: Any
    ) -> "_StateScan[T]":
        """Async iterator over the values in position order."""
        query = _position_query(direction, **options)
        return _StateScan(self._native.scan(key, query), _identity)

