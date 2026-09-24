"""Typed, idiomatic Python surface for keyed state.

A thin transport over the native handles vended by :meth:`Context.state`. The
native layer (Rust) already owns every semantic: carrier injection, chunk
draining, error-category classification (raising ``PermanentStateError`` /
``TransientStateError`` / ``NullValueError`` directly), write validation, and
scan flattening. These wrappers therefore only:

* restore the caller's **types** through generics,
* resolve scan query options into one native value, and
* delegate every operation to the native coroutine.

Definitions live in :mod:`prosody.definition`, query options in
:mod:`prosody.query`, and published readers in :mod:`prosody.published`. This
module re-exports them.
"""

import enum
from typing import Any, List, Optional, Generic, Union

from typing_extensions import TypeVar

from prosody.definition import (
    DequeDefinition,
    MapDefinition,
    MessageDequeDefinition,
    MessageMapDefinition,
    MessageValueDefinition,
    ReadCache,
    SetDefinition,
    ValueDefinition,
    deque,
    map,
    message_deque,
    message_map,
    message_value,
    set,
    value,
)
from prosody.message import JSONValue
from prosody.published import (
    PublishedDeque,
    PublishedMap,
    PublishedSet,
    PublishedValue,
)
from prosody.query import (
    Direction,
    _StateScan,
    _deque_index,
    _identity,
    _key_query,
    _position_query,
)

# PEP 696 defaults: an unparameterized handle uses ``JSONValue``.
T = TypeVar("T", default=JSONValue)  # value / deque item type
V = TypeVar("V", default=JSONValue)  # map value type


class StoreOutcome(enum.Enum):
    """The effect of :meth:`commit` or :meth:`rollback` on a collection.

    ``APPLIED`` means the call wrote or discarded buffered operations.
    ``NO_OP`` means nothing was buffered. The string values are the tokens
    the native handles return.
    """

    APPLIED = "applied"
    NO_OP = "no_op"


class ValueState(Generic[T]):
    """Typed handle over a single-value collection.

    Valid only within the handler invocation that vended it. All methods are
    async; the native layer owns validation, so writing ``None`` (or an
    unrepresentable value) raises ``NullValueError`` from the native layer, not
    from here.
    """

    def __init__(self, native: Any) -> None:
        self._native = native

    async def get(self) -> Optional[T]:
        """Read the current value, or ``None`` when absent/cleared."""
        return await self._native.get()

    async def set(self, value: T) -> None:
        """Buffer a write of ``value`` (``None`` raises ``NullValueError``)."""
        await self._native.set(value)

    async def clear(self) -> None:
        """Buffer a delete of the value."""
        await self._native.clear()

    async def commit(self) -> StoreOutcome:
        """Durably commit the buffered operations mid-handler."""
        return StoreOutcome(await self._native.commit())

    async def rollback(self) -> StoreOutcome:
        """Discard buffered uncommitted operations back to the committed floor."""
        return StoreOutcome(await self._native.rollback())


class MapState(Generic[V]):
    """Typed handle over an ordered-map collection with string keys.

    Valid only within the handler invocation that vended it. ``remove`` exists
    because ``del`` cannot be async; map keys are always ``str``.
    """

    def __init__(self, native: Any) -> None:
        self._native = native

    async def get(self, key: str, default: Any = None) -> Any:
        """Read the value for ``key``; return ``default`` only when the key is
        absent.

        A present-but-falsy value (``0``, ``False``, ``""``, ``[]``) returns that
        value, never ``default`` — the branch tests core absence, not
        truthiness. Unlike the cheap-path methods, ``get`` fully decodes and
        resolves the value. The ``.pyi`` overload pair restores the precise
        return type the runtime erases to ``Any``.
        """
        value = await self._native.get(key)
        return default if value is None else value

    async def contains(self, key: str) -> bool:
        """Report whether a stored cell exists for ``key`` (read-your-writes).

        The cheap presence check: it never decodes the value or runs the
        resolver, so a message-backed map answers ``True`` even for a key whose
        Kafka message can no longer be fetched — presence is about the cell, not
        fetchability. The guarantee is "no value decode, no resolver," **not**
        "no I/O": a cache miss still reads Cassandra and surfaces errors like
        :meth:`get`. Not ``__contains__`` — Python's ``in`` cannot ``await``.
        """
        return await self._native.contains_key(key)

    async def get_many(self, keys: List[str]) -> List[Optional[V]]:
        """Read several keys in one isolated batch, one result per key in order.

        The batched, cache-populating way to read a known set of keys — prefer
        it over iterating :meth:`keys` and calling :meth:`get` per key.
        """
        return await self._native.get_many(keys)

    async def contains_many(self, keys: List[str]) -> List[bool]:
        """Report presence for several keys in one batch, one result per key.

        The batched form of :meth:`contains`: it never decodes a value.
        """
        return await self._native.contains_many(keys)

    async def is_empty(self) -> bool:
        """Whether the map holds no entries."""
        return await self._native.is_empty()

    async def set(self, key: str, value: V) -> None:
        """Insert or overwrite ``key`` (``None`` raises ``NullValueError``)."""
        await self._native.set(key, value)

    async def remove(self, key: str) -> None:
        """Remove ``key`` (named ``remove`` because ``del`` cannot be async)."""
        await self._native.remove(key)

    async def clear(self) -> None:
        """Remove every entry."""
        await self._native.clear()

    def items(
        self,
        direction: Direction = Direction.FORWARD,
        *,
        prefix: Optional[str] = None,
        from_: Optional[str] = None,
        after: Optional[str] = None,
        to: Optional[str] = None,
        before: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> _StateScan:
        """Async iterator over ``(key, value)`` entries in key order.

        The query options select a part of the map; see :meth:`keys`.
        """
        query = _key_query(direction, prefix, from_, after, to, before, limit)
        return _StateScan(self._native.scan(query), _identity)

    def keys(
        self,
        direction: Direction = Direction.FORWARD,
        *,
        prefix: Optional[str] = None,
        from_: Optional[str] = None,
        after: Optional[str] = None,
        to: Optional[str] = None,
        before: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> _StateScan:
        """Async iterator over the keys in key order — the cheap key-only scan.

        Never decodes a value or runs the resolver, so a message-backed map
        enumerates keys with **zero Kafka fetches**. It is not zero-I/O: pulling
        a chunk still does a presence-only read. When you also need the values,
        iterate :meth:`items`; for a known set of keys, call :meth:`get_many`.

        ``prefix`` keeps keys that start with it. ``from_`` and ``after`` start
        at or after a key. ``to`` and ``before`` stop at or before a key. These
        edges are in iteration order, so a ``BACKWARD`` scan starts at the high
        end. ``limit`` caps the number of keys. Options narrow the scan and
        never widen it. To page, pass the last key of a page as ``after``.
        """
        query = _key_query(direction, prefix, from_, after, to, before, limit)
        return _StateScan(self._native.keys(query), _identity)

    def values(
        self,
        *,
        direction: Direction = Direction.FORWARD,
        prefix: Optional[str] = None,
        from_: Optional[str] = None,
        after: Optional[str] = None,
        to: Optional[str] = None,
        before: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> _StateScan:
        """Async iterator over the values in key order.

        A projection of the full ``(key, value)`` scan that drops the keys.
        Value iteration inherently decodes and resolves, so this is not the
        cheap path :meth:`keys` is; it costs the same as :meth:`items`. The
        query options match :meth:`keys`.
        """
        query = _key_query(direction, prefix, from_, after, to, before, limit)
        return _StateScan(self._native.scan(query), lambda e: e[1])

    def __aiter__(self) -> _StateScan:
        """Forward iteration over the **keys**, like ``dict``.

        Use :meth:`items` when you need the values — one batched, fully-resolving
        scan — rather than per-key :meth:`get` after key iteration (a round trip
        per key).
        """
        return self.keys()

    async def commit(self) -> StoreOutcome:
        """Durably commit the buffered operations mid-handler."""
        return StoreOutcome(await self._native.commit())

    async def rollback(self) -> StoreOutcome:
        """Discard buffered uncommitted operations back to the committed floor."""
        return StoreOutcome(await self._native.rollback())


class SetState:
    """Typed handle over a presence-only ordered set of string members.

    Valid only within the handler invocation that vended it. ``contains``
    exists because Python's ``in`` cannot ``await``.
    """

    def __init__(self, native: Any) -> None:
        self._native = native

    async def add(self, member: str) -> None:
        """Add ``member``."""
        await self._native.insert(member)

    async def discard(self, member: str) -> None:
        """Remove ``member`` if present."""
        await self._native.remove(member)

    async def contains(self, member: str) -> bool:
        """Whether ``member`` belongs to the set (read-your-writes)."""
        return await self._native.contains(member)

    async def contains_many(self, members: List[str]) -> List[bool]:
        """Test several members in one batch, one result per member in order."""
        return await self._native.contains_many(members)

    async def is_empty(self) -> bool:
        """Whether the set has no members."""
        return await self._native.is_empty()

    async def clear(self) -> None:
        """Remove every member."""
        await self._native.clear()

    def members(
        self,
        direction: Direction = Direction.FORWARD,
        *,
        prefix: Optional[str] = None,
        from_: Optional[str] = None,
        after: Optional[str] = None,
        to: Optional[str] = None,
        before: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> _StateScan:
        """Async iterator over the members in order.

        The query options match :meth:`MapState.keys`.
        """
        query = _key_query(direction, prefix, from_, after, to, before, limit)
        return _StateScan(self._native.keys(query), _identity)

    def __aiter__(self) -> _StateScan:
        """Forward iteration over the members."""
        return self.members()

    async def commit(self) -> StoreOutcome:
        """Durably commit the buffered operations mid-handler."""
        return StoreOutcome(await self._native.commit())

    async def rollback(self) -> StoreOutcome:
        """Discard buffered uncommitted operations back to the committed floor."""
        return StoreOutcome(await self._native.rollback())


class DequeState(Generic[T]):
    """Typed handle over a double-ended queue.

    Valid only within the handler invocation that vended it. ``size()`` and
    ``is_empty()`` are methods because ``len`` cannot be async.
    """

    def __init__(self, native: Any) -> None:
        self._native = native

    async def append(self, item: T) -> None:
        """Append ``item`` at the back (``None`` raises ``NullValueError``).

        On a capacity-bounded deque (``capacity=`` on the definition), a push is
        the only operation that enforces the bound: it evicts from the opposite
        (front) end toward capacity — decode-free, no Kafka fetch — before
        appending. Enforcement is lazy and capped per push, so a deque just
        reconfigured smaller reports its old length until pushes trim it, and a
        shrunk bound converges over the next few pushes rather than at once.
        """
        await self._native.push_back(item)

    async def appendleft(self, item: T) -> None:
        """Prepend ``item`` at the front (``None`` raises ``NullValueError``).

        The front-push counterpart of :meth:`append`; on a bounded deque it
        evicts from the back toward capacity before prepending.
        """
        await self._native.push_front(item)

    async def pop(self) -> Optional[T]:
        """Remove and return the back element, or ``None`` when empty."""
        return await self._native.pop_back()

    async def popleft(self) -> Optional[T]:
        """Remove and return the front element, or ``None`` when empty."""
        return await self._native.pop_front()

    async def peek(self) -> Optional[T]:
        """Read the back element without removing it, or ``None`` when empty.

        Pairs with :meth:`pop`. An endpoint-*slot* read — exactly
        ``get(size - 1)`` minus the length round trip. Under a TTL the window can
        hold holes, so an expired back slot yields ``None`` even when live
        interior elements exist; a peek never searches inward.
        """
        return await self._native.peek_back()

    async def peekleft(self) -> Optional[T]:
        """Read the front element without removing it, or ``None`` when empty.

        Pairs with :meth:`popleft`; the front-endpoint counterpart of
        :meth:`peek` (``get(0)`` minus the length round trip, same TTL-hole
        semantics).
        """
        return await self._native.peek_front()

    async def get(self, index: int) -> Optional[T]:
        """Read the element at front-relative ``index``, or ``None`` past the end.

        Negative indices resolve from the back, following Python sequence
        semantics. An index before the front returns ``None``.
        """
        position = await _deque_index(index, self.size)
        return None if position is None else await self._native.get(position)

    async def size(self) -> int:
        """Number of live elements (named ``size`` because ``len`` cannot be async)."""
        return await self._native.len()

    async def is_empty(self) -> bool:
        """Whether the deque holds no live elements."""
        return await self._native.is_empty()

    async def clear(self) -> None:
        """Remove every element."""
        await self._native.clear()

    def values(
        self,
        direction: Direction = Direction.FORWARD,
        *,
        from_: Optional[int] = None,
        after: Optional[int] = None,
        to: Optional[int] = None,
        before: Optional[int] = None,
        range: Union[range, slice, None] = None,
        limit: Optional[int] = None,
    ) -> _StateScan:
        """Async iterator over the elements in index order.

        Positions count from the front and cannot be negative. ``from_`` and
        ``after`` start at or after a position. ``to`` and ``before`` stop at
        or before a position. These edges are in iteration order. ``range``
        takes a ``range`` or a ``slice`` of positions with step 1 and applies
        in either direction. ``limit`` caps the number of elements. To read
        the back of the deque, iterate ``BACKWARD`` with a ``limit``.
        """
        query = _position_query(direction, from_, after, to, before, range, limit)
        return _StateScan(self._native.scan(query), _identity)

    def __aiter__(self) -> _StateScan:
        """Forward iteration over the elements."""
        return self.values(Direction.FORWARD)

    async def commit(self) -> StoreOutcome:
        """Durably commit the buffered operations mid-handler."""
        return StoreOutcome(await self._native.commit())

    async def rollback(self) -> StoreOutcome:
        """Discard buffered uncommitted operations back to the committed floor."""
        return StoreOutcome(await self._native.rollback())
