"""Type stubs for the keyed-state surface.

These stubs override :mod:`prosody.state` for type-checkers, restoring the
generic types that the runtime erases to ``Any``. The runtime module is a thin
transport over the native handles vended by :meth:`Context.state`; the native
(Rust) layer owns every semantic (carrier injection, chunk draining,
error-category classification, null/shape/kind guards, and scan flattening).

The type parameter of every handle (``T`` / ``V``) is a structural JSON
annotation; see :mod:`prosody.definition`. Map keys and set members are always
``str``.
"""

import enum
from typing import Generic, List, Optional, Tuple, Union, overload

from typing_extensions import TypeVar

from prosody.definition import (
    DequeDefinition as DequeDefinition,
    MapDefinition as MapDefinition,
    MessageDequeDefinition as MessageDequeDefinition,
    MessageMapDefinition as MessageMapDefinition,
    MessageValueDefinition as MessageValueDefinition,
    D_co as D_co,
    P as P,
    ReadCache as ReadCache,
    SetDefinition as SetDefinition,
    ValueDefinition as ValueDefinition,
    _StateConfig as _StateConfig,
    deque as deque,
    map as map,
    message_deque as message_deque,
    message_map as message_map,
    message_value as message_value,
    set as set,
    value as value,
)
from prosody.message import JSONValue
from prosody.published import (
    PublishedDeque as PublishedDeque,
    PublishedMap as PublishedMap,
    PublishedSet as PublishedSet,
    PublishedValue as PublishedValue,
)
from prosody.query import Direction as Direction, _StateScan as _StateScan

# PEP 696 defaults: an unparameterized handle uses ``JSONValue``.
T = TypeVar("T", default=JSONValue)  # value / deque item type
V = TypeVar("V", default=JSONValue)  # map value type
D = TypeVar("D")  # get() default's own type, preserved in the return


class StoreOutcome(enum.Enum):
    """The effect of ``commit()`` or ``rollback()`` on a collection.

    ``APPLIED`` means the call wrote or discarded buffered operations.
    ``NO_OP`` means nothing was buffered.
    """

    APPLIED = "applied"
    NO_OP = "no_op"


class ValueState(Generic[T]):
    """Typed handle over a single-value collection.

    Valid only within the handler invocation that vended it. All methods are
    async; the native layer owns validation.
    """

    async def get(self) -> Optional[T]:
        """Read the current value, or ``None`` when absent/cleared."""
        ...
    async def set(self, value: T) -> None:
        """Buffer a write of ``value``.

        Writing ``None`` (JSON ``null``) is rejected with :class:`NullValueError`
        (transient) — call :meth:`clear` to delete instead.
        """
        ...
    async def clear(self) -> None:
        """Buffer a delete of the value."""
        ...
    async def commit(self) -> StoreOutcome:
        """Durably flush the buffered operations mid-handler.

        Returns :attr:`StoreOutcome.NO_OP` when nothing was buffered.
        """
        ...
    async def rollback(self) -> StoreOutcome:
        """Discard buffered uncommitted operations back to the committed floor.

        Returns :attr:`StoreOutcome.NO_OP` when nothing was buffered.
        """
        ...


class MapState(Generic[V]):
    """Typed handle over an ordered-map collection with string keys.

    Valid only within the handler invocation that vended it. ``remove`` exists
    because ``del`` cannot be async; map keys are always ``str``.
    """

    @overload
    async def get(self, key: str) -> Optional[V]:
        """Read the value for ``key``, or ``None`` when absent."""
        ...
    @overload
    async def get(self, key: str, default: D) -> Union[V, D]:
        """Read the value for ``key``; return ``default`` only when absent.

        A present-but-falsy value (``0``, ``False``, ``""``, ``[]``) returns
        that value, never ``default`` — the branch tests core absence, not
        truthiness. Fully decodes and resolves the value (unlike
        :meth:`contains` / :meth:`keys`).
        """
        ...
    async def contains(self, key: str) -> bool:
        """Report whether a stored cell exists for ``key`` (read-your-writes).

        The cheap presence check — no value decode, no resolver — so a
        message-backed map answers ``True`` even for a key whose Kafka message
        can no longer be fetched. Not zero-I/O: a cache miss still reads
        Cassandra. Not ``__contains__`` — Python's ``in`` cannot ``await``.
        """
        ...
    async def get_many(self, keys: List[str]) -> List[Optional[V]]:
        """Read several keys in one isolated batch, one result per key in order.

        ``result[i]`` is the value for ``keys[i]`` (``None`` for a missing key).
        The batched, cache-populating way to read a known set of keys — prefer
        it over iterating :meth:`keys` and calling :meth:`get` per key.
        """
        ...
    async def contains_many(self, keys: List[str]) -> List[bool]:
        """Report presence for several keys in one batch, one result per key.

        The batched form of :meth:`contains`: it never decodes a value.
        """
        ...
    async def is_empty(self) -> bool:
        """Whether the map holds no entries."""
        ...
    async def set(self, key: str, value: V) -> None:
        """Insert or overwrite ``key``.

        Writing ``None`` (JSON ``null``) is rejected with :class:`NullValueError`
        (transient) — call :meth:`remove` to delete instead.
        """
        ...
    async def remove(self, key: str) -> None:
        """Remove ``key`` (named ``remove`` because ``del`` cannot be async).

        Returns ``None`` deliberately — no hidden "was present" read.
        """
        ...
    async def clear(self) -> None:
        """Remove every entry."""
        ...
    def items(
        self,
        direction: Direction = ...,
        *,
        prefix: Optional[str] = ...,
        from_: Optional[str] = ...,
        after: Optional[str] = ...,
        to: Optional[str] = ...,
        before: Optional[str] = ...,
        limit: Optional[int] = ...,
    ) -> _StateScan[Tuple[str, V]]:
        """Async iterator over ``(key, value)`` entries in key order.

        The query options match :meth:`keys`.
        """
        ...
    def keys(
        self,
        direction: Direction = ...,
        *,
        prefix: Optional[str] = ...,
        from_: Optional[str] = ...,
        after: Optional[str] = ...,
        to: Optional[str] = ...,
        before: Optional[str] = ...,
        limit: Optional[int] = ...,
    ) -> _StateScan[str]:
        """Async iterator over the keys in key order — the cheap key-only scan.

        Never decodes a value or runs the resolver, so a message-backed map
        enumerates keys with **zero Kafka fetches**. Not zero-I/O: pulling a
        chunk still does a presence-only read. When you also need the values,
        iterate :meth:`items`; for a known set of keys, call :meth:`get_many`.

        ``prefix`` keeps keys that start with it. ``from_`` and ``after`` start
        at or after a key. ``to`` and ``before`` stop at or before a key. These
        edges are in iteration order, so a ``BACKWARD`` scan starts at the high
        end. ``limit`` caps the number of keys. Options narrow the scan and
        never widen it. To page, pass the last key of a page as ``after``.
        Setting both ``from_`` and ``after``, or both ``to`` and ``before``,
        raises ``ValueError``. A ``limit`` below 1 raises ``ValueError``.
        """
        ...
    def values(
        self,
        *,
        direction: Direction = ...,
        prefix: Optional[str] = ...,
        from_: Optional[str] = ...,
        after: Optional[str] = ...,
        to: Optional[str] = ...,
        before: Optional[str] = ...,
        limit: Optional[int] = ...,
    ) -> _StateScan[V]:
        """Async iterator over the values in key order.

        A projection of the full ``(key, value)`` scan that drops the keys.
        Value iteration inherently decodes and resolves, so it is not the cheap
        path :meth:`keys` is; it costs the same as :meth:`items`. The query
        options match :meth:`keys`.
        """
        ...
    def __aiter__(self) -> _StateScan[str]:
        """Forward iteration over the **keys**, like ``dict``.

        Use :meth:`items` when you need the values — one batched, fully-resolving
        scan — rather than per-key :meth:`get` after key iteration.
        """
        ...
    async def commit(self) -> StoreOutcome:
        """Durably flush the buffered operations mid-handler.

        Returns :attr:`StoreOutcome.NO_OP` when nothing was buffered.
        """
        ...
    async def rollback(self) -> StoreOutcome:
        """Discard buffered uncommitted operations back to the committed floor.

        Returns :attr:`StoreOutcome.NO_OP` when nothing was buffered.
        """
        ...


class SetState:
    """Typed handle over a presence-only ordered set of string members.

    Valid only within the handler invocation that vended it. ``contains``
    exists because Python's ``in`` cannot ``await``.
    """

    async def add(self, member: str) -> None:
        """Add ``member``."""
        ...
    async def discard(self, member: str) -> None:
        """Remove ``member`` if present."""
        ...
    async def contains(self, member: str) -> bool:
        """Whether ``member`` belongs to the set (read-your-writes)."""
        ...
    async def contains_many(self, members: List[str]) -> List[bool]:
        """Test several members in one batch, one result per member in order."""
        ...
    async def is_empty(self) -> bool:
        """Whether the set has no members."""
        ...
    async def clear(self) -> None:
        """Remove every member."""
        ...
    def members(
        self,
        direction: Direction = ...,
        *,
        prefix: Optional[str] = ...,
        from_: Optional[str] = ...,
        after: Optional[str] = ...,
        to: Optional[str] = ...,
        before: Optional[str] = ...,
        limit: Optional[int] = ...,
    ) -> _StateScan[str]:
        """Async iterator over the members in order.

        The query options match :meth:`MapState.keys`.
        """
        ...
    def __aiter__(self) -> _StateScan[str]:
        """Forward iteration over the members."""
        ...
    async def commit(self) -> StoreOutcome:
        """Durably flush the buffered operations mid-handler.

        Returns :attr:`StoreOutcome.NO_OP` when nothing was buffered.
        """
        ...
    async def rollback(self) -> StoreOutcome:
        """Discard buffered uncommitted operations back to the committed floor.

        Returns :attr:`StoreOutcome.NO_OP` when nothing was buffered.
        """
        ...


class DequeState(Generic[T]):
    """Typed handle over a double-ended queue.

    Valid only within the handler invocation that vended it. ``size()`` and
    ``is_empty()`` are methods because ``len`` cannot be async.
    """

    async def append(self, item: T) -> None:
        """Append ``item`` at the back.

        Writing ``None`` (JSON ``null``) is rejected with :class:`NullValueError`
        (transient).
        """
        ...
    async def appendleft(self, item: T) -> None:
        """Prepend ``item`` at the front.

        Writing ``None`` (JSON ``null``) is rejected with :class:`NullValueError`
        (transient).
        """
        ...
    async def pop(self) -> Optional[T]:
        """Remove and return the back element, or ``None`` when empty."""
        ...
    async def popleft(self) -> Optional[T]:
        """Remove and return the front element, or ``None`` when empty."""
        ...
    async def peek(self) -> Optional[T]:
        """Read the back element without removing it, or ``None`` when empty.

        Pairs with :meth:`pop`. An endpoint-*slot* read — ``get(size - 1)`` minus
        the length round trip. Under a TTL an expired back slot yields ``None``
        even when live interior elements exist; a peek never searches inward.
        """
        ...
    async def peekleft(self) -> Optional[T]:
        """Read the front element without removing it, or ``None`` when empty.

        Pairs with :meth:`popleft`; the front-endpoint counterpart of
        :meth:`peek` (``get(0)`` minus the length round trip).
        """
        ...
    async def get(self, index: int) -> Optional[T]:
        """Read the element at front-relative ``index``, or ``None`` past the end.

        Negative indices resolve from the back, following Python sequence
        semantics. An index before the front returns ``None``.
        """
        ...
    async def size(self) -> int:
        """Number of live elements (named ``size`` because ``len`` cannot be async)."""
        ...
    async def is_empty(self) -> bool:
        """Whether the deque holds no live elements."""
        ...
    async def clear(self) -> None:
        """Remove every element."""
        ...
    def values(
        self,
        direction: Direction = ...,
        *,
        from_: Optional[int] = ...,
        after: Optional[int] = ...,
        to: Optional[int] = ...,
        before: Optional[int] = ...,
        range: Union[range, slice, None] = ...,
        limit: Optional[int] = ...,
    ) -> _StateScan[T]:
        """Async iterator over the elements in index order.

        Positions count from the front and cannot be negative. ``from_`` and
        ``after`` start at or after a position. ``to`` and ``before`` stop at
        or before a position. These edges are in iteration order. ``range``
        takes a ``range`` or a ``slice`` of positions with step 1 and applies
        in either direction. ``limit`` caps the number of elements. Negative
        positions raise ``ValueError``; read the last N elements with
        ``values(Direction.BACKWARD, limit=N)``.
        """
        ...
    def __aiter__(self) -> _StateScan[T]:
        """Forward iteration over the elements."""
        ...
    async def commit(self) -> StoreOutcome:
        """Durably flush the buffered operations mid-handler.

        Returns :attr:`StoreOutcome.NO_OP` when nothing was buffered.
        """
        ...
    async def rollback(self) -> StoreOutcome:
        """Discard buffered uncommitted operations back to the committed floor.

        Returns :attr:`StoreOutcome.NO_OP` when nothing was buffered.
        """
        ...
