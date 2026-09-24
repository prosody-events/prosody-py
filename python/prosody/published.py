"""Read-only views over published keyed state.

:meth:`ProsodyClient.state` opens these readers. Each read takes the user key
as its first argument because no handler supplies one.
"""

from typing import Generic, List, Optional, Protocol, Union

from typing_extensions import TypeVar

from prosody.message import JSONValue
from prosody.query import (
    Direction,
    _KeyQuery,
    _NativeScan,
    _PositionQuery,
    _StateScan,
    _deque_index,
    _identity,
    _key_query,
    _position_query,
)

T = TypeVar("T", default=JSONValue)
V = TypeVar("V", default=JSONValue)


class PublishedValue(Generic[T]):
    """Read-only access to a published value collection."""

    def __init__(self, native: "_PublishedValueNative[T]") -> None:
        self._native = native

    async def get(self, key: str) -> Optional[T]:
        return await self._native.get(key)


class PublishedMap(Generic[V]):
    """Read-only access to a published ordered-map collection."""

    def __init__(self, native: "_PublishedMapNative[V]") -> None:
        self._native = native

    async def get(self, key: str, map_key: str) -> Optional[V]:
        return await self._native.get(key, map_key)

    async def get_many(self, key: str, map_keys: List[str]) -> List[Optional[V]]:
        return await self._native.get_many(key, map_keys)

    async def contains(self, key: str, map_key: str) -> bool:
        return await self._native.contains_key(key, map_key)

    async def contains_many(self, key: str, map_keys: List[str]) -> List[bool]:
        return await self._native.contains_many(key, map_keys)

    async def is_empty(self, key: str) -> bool:
        return await self._native.is_empty(key)

    def items(
        self,
        key: str,
        direction: Direction = Direction.FORWARD,
        *,
        prefix: Optional[str] = None,
        from_: Optional[str] = None,
        after: Optional[str] = None,
        to: Optional[str] = None,
        before: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> "_StateScan[tuple[str, V]]":
        query = _key_query(direction, prefix, from_, after, to, before, limit)
        return _StateScan(self._native.scan(key, query), _identity)

    def keys(
        self,
        key: str,
        direction: Direction = Direction.FORWARD,
        *,
        prefix: Optional[str] = None,
        from_: Optional[str] = None,
        after: Optional[str] = None,
        to: Optional[str] = None,
        before: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> "_StateScan[str]":
        query = _key_query(direction, prefix, from_, after, to, before, limit)
        return _StateScan(self._native.keys(key, query), _identity)

    def values(
        self,
        key: str,
        direction: Direction = Direction.FORWARD,
        *,
        prefix: Optional[str] = None,
        from_: Optional[str] = None,
        after: Optional[str] = None,
        to: Optional[str] = None,
        before: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> "_StateScan[V]":
        query = _key_query(direction, prefix, from_, after, to, before, limit)
        return _StateScan(self._native.scan(key, query), lambda entry: entry[1])


class PublishedSet:
    """Read-only access to a published set collection."""

    def __init__(self, native: "_PublishedSetNative") -> None:
        self._native = native

    async def contains(self, key: str, member: str) -> bool:
        return await self._native.contains(key, member)

    async def contains_many(self, key: str, members: List[str]) -> List[bool]:
        return await self._native.contains_many(key, members)

    async def is_empty(self, key: str) -> bool:
        return await self._native.is_empty(key)

    def members(
        self,
        key: str,
        direction: Direction = Direction.FORWARD,
        *,
        prefix: Optional[str] = None,
        from_: Optional[str] = None,
        after: Optional[str] = None,
        to: Optional[str] = None,
        before: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> "_StateScan[str]":
        query = _key_query(direction, prefix, from_, after, to, before, limit)
        return _StateScan(self._native.keys(key, query), _identity)


class PublishedDeque(Generic[T]):
    """Read-only access to a published deque collection."""

    def __init__(self, native: "_PublishedDequeNative[T]") -> None:
        self._native = native

    async def get(self, key: str, index: int) -> Optional[T]:
        position = await _deque_index(index, lambda: self.size(key))
        return None if position is None else await self._native.get(key, position)

    async def size(self, key: str) -> int:
        return await self._native.len(key)

    async def is_empty(self, key: str) -> bool:
        return await self._native.is_empty(key)

    async def peek(self, key: str) -> Optional[T]:
        return await self._native.peek_back(key)

    async def peekleft(self, key: str) -> Optional[T]:
        return await self._native.peek_front(key)

    def values(
        self,
        key: str,
        direction: Direction = Direction.FORWARD,
        *,
        from_: Optional[int] = None,
        after: Optional[int] = None,
        to: Optional[int] = None,
        before: Optional[int] = None,
        range: Union[range, slice, None] = None,
        limit: Optional[int] = None,
    ) -> "_StateScan[T]":
        query = _position_query(direction, from_, after, to, before, range, limit)
        return _StateScan(self._native.scan(key, query), _identity)


class _PublishedValueNative(Protocol[T]):
    async def get(self, key: str) -> Optional[T]:
        raise NotImplementedError


class _PublishedMapNative(Protocol[V]):
    async def get(self, key: str, map_key: str) -> Optional[V]:
        raise NotImplementedError

    async def get_many(self, key: str, map_keys: List[str]) -> List[Optional[V]]:
        raise NotImplementedError

    async def contains_key(self, key: str, map_key: str) -> bool:
        raise NotImplementedError

    async def contains_many(self, key: str, map_keys: List[str]) -> List[bool]:
        raise NotImplementedError

    async def is_empty(self, key: str) -> bool:
        raise NotImplementedError

    def scan(self, key: str, query: _KeyQuery) -> "_NativeScan[tuple[str, V]]":
        raise NotImplementedError

    def keys(self, key: str, query: _KeyQuery) -> "_NativeScan[str]":
        raise NotImplementedError


class _PublishedSetNative(Protocol):
    async def contains(self, key: str, member: str) -> bool:
        raise NotImplementedError

    async def contains_many(self, key: str, members: List[str]) -> List[bool]:
        raise NotImplementedError

    async def is_empty(self, key: str) -> bool:
        raise NotImplementedError

    def keys(self, key: str, query: _KeyQuery) -> "_NativeScan[str]":
        raise NotImplementedError


class _PublishedDequeNative(Protocol[T]):
    async def get(self, key: str, index: int) -> Optional[T]:
        raise NotImplementedError

    async def len(self, key: str) -> int:
        raise NotImplementedError

    async def is_empty(self, key: str) -> bool:
        raise NotImplementedError

    async def peek_front(self, key: str) -> Optional[T]:
        raise NotImplementedError

    async def peek_back(self, key: str) -> Optional[T]:
        raise NotImplementedError

    def scan(self, key: str, query: _PositionQuery) -> "_NativeScan[T]":
        raise NotImplementedError
