"""Typed, idiomatic Python surface for keyed state.

A thin transport over the native handles vended by :meth:`Context.state`. The
native layer (Rust) already owns every semantic: carrier injection, chunk
draining, error-category classification (raising ``PermanentStateError`` /
``TransientStateError`` directly), write validation, and
scan flattening. These wrappers therefore only:

* restore the caller's **types** through generics,
* resolve scan query options into one native value, and
* delegate every operation to the native coroutine.

The type parameter of every handle (``T`` / ``V``) is a structural JSON
annotation; see :mod:`prosody.definition`. Map keys and set members are always
``str``. A handle with the default ``JSONValue`` type accepts any
:data:`~prosody.message.JSONInput` write, such as a ``TypedDict``.

Definitions live in :mod:`prosody.definition`, query options in
:mod:`prosody.query`, and published readers in :mod:`prosody.published`. This
module re-exports them.
"""

import enum
from typing import Any, List, Optional, Generic

from typing_extensions import TypeVar

from prosody.definition import (
    DequeDefinition,
    MapDefinition,
    MessageDequeDefinition,
    MessageMapDefinition,
    MessageValueDefinition,
    P,
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
    X,
    Y,
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
    """The effect of ``commit()`` or ``rollback()`` on a collection.

    ``APPLIED`` means the call wrote or discarded buffered operations.
    ``NO_OP`` means nothing was buffered. The string values are the tokens
    the native handles return.
    """

    APPLIED = "applied"
    NO_OP = "no_op"


class _Handle:
    """The transaction methods that every keyed-state handle shares."""

    def __init__(self, native: Any) -> None:
        self._native = native

    async def commit(self) -> StoreOutcome:
        """Durably commit the buffered operations mid-handler."""
        return StoreOutcome(await self._native.commit())

    async def rollback(self) -> StoreOutcome:
        """Discard buffered uncommitted operations back to the committed floor."""
        return StoreOutcome(await self._native.rollback())


class ValueState(_Handle, Generic[T]):
    """Typed handle over a single-value collection.

    Valid only within the handler invocation that vended it. All methods are
    async; the native layer owns validation.
    """

    async def get(self) -> Optional[T]:
        """Read the current value, or ``None`` when absent or cleared."""
        return await self._native.get()

    async def set(self, value: T) -> None:
        """Buffer a write of ``value``.

        Writing ``None`` (JSON ``null``) raises :class:`PermanentStateError`.
        Call :meth:`clear` to delete instead.
        """
        await self._native.set(value)

    async def clear(self) -> None:
        """Buffer a delete of the value."""
        await self._native.clear()


class MapState(_Handle, Generic[V]):
    """Typed handle over an ordered-map collection with string keys.

    Valid only within the handler invocation that vended it. ``remove`` exists
    because ``del`` cannot be async; map keys are always ``str``.
    """

    async def get(self, key: str, default: Any = None) -> Any:
        """Read the value for ``key``; return ``default`` only when the key is
        absent.

        A stored falsy value, such as ``0`` or ``""``, returns that value and
        not ``default``. ``get`` decodes the value, unlike :meth:`contains`
        and :meth:`keys`.
        """
        value = await self._native.get(key)
        return default if value is None else value

    async def contains(self, key: str) -> bool:
        """Report whether ``key`` has an entry, including buffered writes.

        This check does not decode the value. A message map reports ``True``
        even when Prosody can no longer fetch the Kafka message. A cache miss
        still reads Cassandra. Python's ``in`` cannot ``await``, so this is
        not ``__contains__``.
        """
        return await self._native.contains_key(key)

    async def get_many(self, keys: List[str]) -> List[Optional[V]]:
        """Read several keys in one isolated batch, one result per key in order.

        ``result[i]`` is the value for ``keys[i]``, or ``None`` for a missing
        key. Prefer this batched read to a :meth:`get` call for each key of
        :meth:`keys`: it fills the cache in one batch.
        """
        return await self._native.get_many(keys)

    async def contains_many(self, keys: List[str]) -> List[bool]:
        """Report presence for several keys in one batch, one result per key.

        The batched form of :meth:`contains`: it never decodes a value.
        """
        return await self._native.contains_many(keys)

    async def is_empty(self) -> bool:
        """Report whether the map holds no entries."""
        return await self._native.is_empty()

    async def set(self, key: str, value: V) -> None:
        """Insert or overwrite ``key``.

        Writing ``None`` (JSON ``null``) raises :class:`PermanentStateError`.
        Call :meth:`remove` to delete instead.
        """
        await self._native.set(key, value)

    async def remove(self, key: str) -> None:
        """Remove ``key``. The name is ``remove`` because ``del`` cannot be async.

        It returns ``None``, so it makes no read to learn whether the key was
        present.
        """
        await self._native.remove(key)

    async def clear(self) -> None:
        """Remove every entry."""
        await self._native.clear()

    def items(
        self, direction: Direction = Direction.FORWARD, **options: Any
    ) -> _StateScan:
        """Async iterator over ``(key, value)`` entries in key order.

        The query options match :meth:`keys`.
        """
        query = _key_query(direction, **options)
        return _StateScan(self._native.scan(query), _identity)

    def keys(
        self, direction: Direction = Direction.FORWARD, **options: Any
    ) -> _StateScan:
        """Async iterator over the keys in key order.

        The scan does not decode values, so a message map lists its keys
        without Kafka fetches. Each pull still reads which keys exist. To also
        read the values, iterate :meth:`items`. For a known list of keys, call
        :meth:`get_many`.

        ``prefix`` keeps keys that start with it. ``from_`` and ``after`` start
        at or after a key. ``to`` and ``before`` stop at or before a key. These
        edges are in iteration order, so a ``BACKWARD`` scan starts at the high
        end. ``range`` takes a ``slice`` of keys, such as ``slice("a", "m")``.
        It is an ascending half-open span that applies in either direction. A
        ``None`` bound leaves that end open. ``limit`` caps the number of keys.

        Options narrow the scan and never widen it. To page, pass the last key
        of a page as ``after``. A wrong type raises ``TypeError``. Both
        ``from_`` and ``after``, both ``to`` and ``before``, a ``range`` with a
        step, or a ``limit`` below 1 raise ``ValueError``.
        """
        query = _key_query(direction, **options)
        return _StateScan(self._native.keys(query), _identity)

    def values(
        self, direction: Direction = Direction.FORWARD, **options: Any
    ) -> _StateScan:
        """Async iterator over the values in key order.

        The scan decodes each value, so it costs the same as :meth:`items`.
        The query options match :meth:`keys`.
        """
        query = _key_query(direction, **options)
        return _StateScan(self._native.scan(query), lambda e: e[1])

    def __aiter__(self) -> _StateScan:
        """Forward iteration over the **keys**, like ``dict``.

        To read the values too, iterate :meth:`items`. Do not call :meth:`get`
        for each key: that makes one read for each key.
        """
        return self.keys()


class SetState(_Handle):
    """Typed handle over a presence-only ordered set of string members.

    Valid only within the handler invocation that vended it. ``contains``
    exists because Python's ``in`` cannot ``await``.
    """

    async def add(self, member: str) -> None:
        """Add ``member``."""
        await self._native.insert(member)

    async def discard(self, member: str) -> None:
        """Remove ``member`` if present."""
        await self._native.remove(member)

    async def contains(self, member: str) -> bool:
        """Report whether ``member`` belongs to the set, including buffered writes."""
        return await self._native.contains(member)

    async def contains_many(self, members: List[str]) -> List[bool]:
        """Test several members in one batch, one result per member in order."""
        return await self._native.contains_many(members)

    async def is_empty(self) -> bool:
        """Report whether the set has no members."""
        return await self._native.is_empty()

    async def clear(self) -> None:
        """Remove every member."""
        await self._native.clear()

    def members(
        self, direction: Direction = Direction.FORWARD, **options: Any
    ) -> _StateScan:
        """Async iterator over the members in order.

        The query options match :meth:`MapState.keys`.
        """
        query = _key_query(direction, **options)
        return _StateScan(self._native.keys(query), _identity)

    def __aiter__(self) -> _StateScan:
        """Forward iteration over the members."""
        return self.members()


class DequeState(_Handle, Generic[T]):
    """Typed handle over a double-ended queue.

    Valid only within the handler invocation that vended it. ``size()`` and
    ``is_empty()`` are methods because ``len`` cannot be async.
    """

    async def append(self, item: T) -> None:
        """Append ``item`` at the back.

        Writing ``None`` (JSON ``null``) raises :class:`PermanentStateError`.
        Call :meth:`clear` to delete the deque.

        On a deque with a ``capacity``, a push is the only operation that
        enforces the bound. It evicts from the front toward the capacity
        before it appends, with no decode and no Kafka fetch. Each push evicts
        a capped number of elements. A deque that a deploy made smaller keeps
        its old length until later pushes trim it.
        """
        await self._native.push_back(item)

    async def appendleft(self, item: T) -> None:
        """Prepend ``item`` at the front.

        Writing ``None`` (JSON ``null``) raises :class:`PermanentStateError`.
        On a deque with a ``capacity``, it evicts from the back toward the
        capacity before it prepends, as :meth:`append` does from the front.
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

        This reads the back position only and does not read the length. With a
        TTL, the back element can expire while elements before it stay live.
        The peek then returns ``None``. It does not search for a live element.
        """
        return await self._native.peek_back()

    async def peekleft(self) -> Optional[T]:
        """Read the front element without removing it, or ``None`` when empty.

        This reads the front position only, with the same TTL behavior as
        :meth:`peek`.
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
        """Return the number of live elements. ``len`` cannot be async."""
        return await self._native.len()

    async def is_empty(self) -> bool:
        """Report whether the deque holds no live elements."""
        return await self._native.is_empty()

    async def clear(self) -> None:
        """Remove every element."""
        await self._native.clear()

    def values(
        self, direction: Direction = Direction.FORWARD, **options: Any
    ) -> _StateScan:
        """Async iterator over the elements in index order.

        Positions count from the front and cannot be negative. ``from_`` and
        ``after`` start at or after a position. ``to`` and ``before`` stop at
        or before a position. These edges are in iteration order. ``range``
        takes a ``range`` or a ``slice`` of positions with step 1. It is an
        ascending span that applies in either direction, and an empty span
        yields nothing. ``limit`` caps the number of elements. Negative
        positions raise ``ValueError``; read the last N elements with
        ``values(Direction.BACKWARD, limit=N)``.
        """
        query = _position_query(direction, **options)
        return _StateScan(self._native.scan(query), _identity)

    def __aiter__(self) -> _StateScan:
        """Forward iteration over the elements."""
        return self.values(Direction.FORWARD)
