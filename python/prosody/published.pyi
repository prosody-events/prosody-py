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
    async def get(self, key: str) -> Optional[T]: ...


class PublishedMap(Generic[V]):
    async def get(self, key: str, map_key: str) -> Optional[V]: ...
    async def get_many(self, key: str, map_keys: List[str]) -> List[Optional[V]]: ...
    async def contains(self, key: str, map_key: str) -> bool: ...
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
        limit: Optional[int] = ...,
    ) -> _StateScan[Tuple[str, V]]: ...
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
        limit: Optional[int] = ...,
    ) -> _StateScan[str]: ...
    def values(
        self,
        key: str,
        *,
        direction: Direction = ...,
        prefix: Optional[str] = ...,
        from_: Optional[str] = ...,
        after: Optional[str] = ...,
        to: Optional[str] = ...,
        before: Optional[str] = ...,
        limit: Optional[int] = ...,
    ) -> _StateScan[V]: ...


class PublishedSet:
    async def contains(self, key: str, member: str) -> bool: ...
    async def contains_many(self, key: str, members: List[str]) -> List[bool]: ...
    async def is_empty(self, key: str) -> bool: ...
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
        limit: Optional[int] = ...,
    ) -> _StateScan[str]: ...


class PublishedDeque(Generic[T]):
    async def get(self, key: str, index: int) -> Optional[T]: ...
    async def size(self, key: str) -> int: ...
    async def is_empty(self, key: str) -> bool: ...
    async def peek(self, key: str) -> Optional[T]: ...
    async def peekleft(self, key: str) -> Optional[T]: ...
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
    ) -> _StateScan[T]: ...
