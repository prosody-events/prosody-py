"""Typed, idiomatic Python surface for keyed state.

A thin transport over the native handles vended by :meth:`Context.state`. The
native layer (Rust) already owns every semantic: carrier injection, chunk
draining, error-category classification (raising ``PermanentStateError`` /
``TransientStateError`` / ``NullValueError`` directly), null/shape/kind guards,
and scan flattening. These wrappers therefore only:

* shape and freeze typed **definitions** and turn them into the config dict the
  client consumes,
* restore the caller's **types** through generics, and
* delegate every operation to the native coroutine.

No error translation, carrier handling, or re-validation lives here — the
native layer owns all of it.
"""

import enum
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, ClassVar, Dict, Generic, Optional, Union

from typing_extensions import TypeVar

from prosody.message import JSONValue

# PEP 696 defaults: an unparameterized handle/definition uses ``JSONValue``.
T = TypeVar("T", default=JSONValue)  # value / deque item type
V = TypeVar("V", default=JSONValue)  # map value type
P = TypeVar("P", default=JSONValue)  # message payload type


class Direction(enum.Enum):
    """Scan direction over an ordered collection.

    The string values are the tokens the native ``scan`` accepts; wrappers pass
    ``direction.value`` straight through.
    """

    FORWARD = "forward"
    BACKWARD = "backward"


def _ttl_seconds(ttl: Optional[Union[timedelta, int]]) -> Optional[int]:
    """Normalize a TTL to whole seconds, accepting a ``timedelta`` or an int."""
    if ttl is None:
        return None
    if isinstance(ttl, timedelta):
        return int(ttl.total_seconds())
    return int(ttl)


def _config(defn: Any) -> Dict[str, Any]:
    """Build the registration/vend config dict the client layer consumes."""
    return {
        "name": defn.name,
        "kind": defn.kind,
        "payload": defn.payload,
        "ttl_seconds": _ttl_seconds(defn.ttl),
        "read_uncommitted": defn.read_uncommitted,
        "keyset_limit": getattr(defn, "keyset_limit", None),
    }


@dataclass(frozen=True)
class ValueDefinition(Generic[T]):
    """A single-value JSON collection definition."""

    name: str
    ttl: Optional[Union[timedelta, int]] = None
    read_uncommitted: Optional[bool] = None
    kind: ClassVar[str] = "value"
    payload: ClassVar[str] = "json"

    def to_config(self) -> Dict[str, Any]:
        """Return the config dict passed to the client and to ``state()``."""
        return _config(self)


@dataclass(frozen=True)
class MapDefinition(Generic[V]):
    """An ordered-map JSON collection definition (string keys)."""

    name: str
    ttl: Optional[Union[timedelta, int]] = None
    read_uncommitted: Optional[bool] = None
    keyset_limit: Optional[int] = None
    kind: ClassVar[str] = "map"
    payload: ClassVar[str] = "json"

    def to_config(self) -> Dict[str, Any]:
        """Return the config dict passed to the client and to ``state()``."""
        return _config(self)


@dataclass(frozen=True)
class DequeDefinition(Generic[T]):
    """A double-ended-queue JSON collection definition."""

    name: str
    ttl: Optional[Union[timedelta, int]] = None
    read_uncommitted: Optional[bool] = None
    kind: ClassVar[str] = "deque"
    payload: ClassVar[str] = "json"

    def to_config(self) -> Dict[str, Any]:
        """Return the config dict passed to the client and to ``state()``."""
        return _config(self)


@dataclass(frozen=True)
class MessageValueDefinition(Generic[P]):
    """A single-value collection storing whole Kafka messages."""

    name: str
    ttl: Optional[Union[timedelta, int]] = None
    read_uncommitted: Optional[bool] = None
    kind: ClassVar[str] = "value"
    payload: ClassVar[str] = "message"

    def to_config(self) -> Dict[str, Any]:
        """Return the config dict passed to the client and to ``state()``."""
        return _config(self)


@dataclass(frozen=True)
class MessageMapDefinition(Generic[P]):
    """An ordered-map collection storing whole Kafka messages."""

    name: str
    ttl: Optional[Union[timedelta, int]] = None
    read_uncommitted: Optional[bool] = None
    keyset_limit: Optional[int] = None
    kind: ClassVar[str] = "map"
    payload: ClassVar[str] = "message"

    def to_config(self) -> Dict[str, Any]:
        """Return the config dict passed to the client and to ``state()``."""
        return _config(self)


@dataclass(frozen=True)
class MessageDequeDefinition(Generic[P]):
    """A double-ended-queue collection storing whole Kafka messages."""

    name: str
    ttl: Optional[Union[timedelta, int]] = None
    read_uncommitted: Optional[bool] = None
    kind: ClassVar[str] = "deque"
    payload: ClassVar[str] = "message"

    def to_config(self) -> Dict[str, Any]:
        """Return the config dict passed to the client and to ``state()``."""
        return _config(self)


def value(
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = None,
    read_uncommitted: Optional[bool] = None,
) -> ValueDefinition[T]:
    """Define a single-value JSON collection."""
    return ValueDefinition(name, ttl=ttl, read_uncommitted=read_uncommitted)


def map(  # this module-local name mirrors the collection kind; no builtin use here
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = None,
    read_uncommitted: Optional[bool] = None,
    keyset_limit: Optional[int] = None,
) -> MapDefinition[V]:
    """Define an ordered-map JSON collection (string keys)."""
    return MapDefinition(
        name,
        ttl=ttl,
        read_uncommitted=read_uncommitted,
        keyset_limit=keyset_limit,
    )


def deque(
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = None,
    read_uncommitted: Optional[bool] = None,
) -> DequeDefinition[T]:
    """Define a double-ended-queue JSON collection."""
    return DequeDefinition(name, ttl=ttl, read_uncommitted=read_uncommitted)


def message_value(
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = None,
    read_uncommitted: Optional[bool] = None,
) -> MessageValueDefinition[P]:
    """Define a single-value collection of whole Kafka messages."""
    return MessageValueDefinition(name, ttl=ttl, read_uncommitted=read_uncommitted)


def message_map(
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = None,
    read_uncommitted: Optional[bool] = None,
    keyset_limit: Optional[int] = None,
) -> MessageMapDefinition[P]:
    """Define an ordered-map collection of whole Kafka messages."""
    return MessageMapDefinition(
        name,
        ttl=ttl,
        read_uncommitted=read_uncommitted,
        keyset_limit=keyset_limit,
    )


def message_deque(
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = None,
    read_uncommitted: Optional[bool] = None,
) -> MessageDequeDefinition[P]:
    """Define a double-ended-queue collection of whole Kafka messages."""
    return MessageDequeDefinition(name, ttl=ttl, read_uncommitted=read_uncommitted)


def _identity(item: Any) -> Any:
    return item


class _StateScan:
    """Async iterator over a native scan cursor, applying a per-flavour transform.

    The native cursor already handles retained-chunk flattening, serialization,
    and ``StopAsyncIteration`` at exhaustion, so this is a thin adapter: each
    ``__anext__`` awaits the native pull and reshapes the item (map entries to
    keys/values/pairs; deque items pass through).

    Iterating with ``async for`` and then ``break`` does NOT call ``aclose()``.
    That is harmless by construction — no store permit is held between pulls, the
    cursor is attempt-epoch fenced, and the native ``Drop`` closes it on GC. For
    a deterministic early close use ``contextlib.aclosing(...)``.
    """

    def __init__(self, native: Any, transform: Any) -> None:
        self._native = native
        self._transform = transform

    def __aiter__(self) -> "_StateScan":
        return self

    async def __anext__(self) -> Any:
        # Re-raises the native StopAsyncIteration at exhaustion (never coerced
        # by PEP 479 since it crosses no generator boundary here).
        return self._transform(await self._native.__anext__())

    async def aclose(self) -> None:
        await self._native.aclose()


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
        return await self._native.set(value)

    async def clear(self) -> None:
        """Buffer a delete of the value."""
        return await self._native.clear()

    async def commit(self) -> None:
        """Durably commit the buffered operations mid-handler."""
        await self._native.commit()

    async def rollback(self) -> None:
        """Discard buffered uncommitted operations back to the committed floor."""
        await self._native.rollback()


class MapState(Generic[V]):
    """Typed handle over an ordered-map collection with string keys.

    Valid only within the handler invocation that vended it. ``remove`` exists
    because ``del`` cannot be async; map keys are always ``str``.
    """

    def __init__(self, native: Any) -> None:
        self._native = native

    async def get(self, key: str) -> Optional[V]:
        """Read the value for ``key``, or ``None`` when absent."""
        return await self._native.get(key)

    async def get_many(self, keys):
        """Read several keys in one isolated batch, one result per key in order."""
        return await self._native.get_many(keys)

    async def set(self, key: str, value: V) -> None:
        """Insert or overwrite ``key`` (``None`` raises ``NullValueError``)."""
        return await self._native.set(key, value)

    async def remove(self, key: str) -> None:
        """Remove ``key`` (named ``remove`` because ``del`` cannot be async)."""
        return await self._native.remove(key)

    async def clear(self) -> None:
        """Remove every entry."""
        return await self._native.clear()

    def items(self, direction: Direction = Direction.FORWARD) -> _StateScan:
        """Async iterator over ``(key, value)`` entries in key order."""
        return _StateScan(self._native.scan(direction.value), _identity)

    def keys(self) -> _StateScan:
        """Async iterator over the keys in forward key order."""
        return _StateScan(self._native.scan(Direction.FORWARD.value), lambda e: e[0])

    def values(self) -> _StateScan:
        """Async iterator over the values in forward key order."""
        return _StateScan(self._native.scan(Direction.FORWARD.value), lambda e: e[1])

    def __aiter__(self) -> _StateScan:
        """Forward iteration over ``(key, value)`` entries."""
        return self.items(Direction.FORWARD)

    async def commit(self) -> None:
        """Durably commit the buffered operations mid-handler."""
        await self._native.commit()

    async def rollback(self) -> None:
        """Discard buffered uncommitted operations back to the committed floor."""
        await self._native.rollback()


class DequeState(Generic[T]):
    """Typed handle over a double-ended queue.

    Valid only within the handler invocation that vended it. ``size()`` and
    ``is_empty()`` are methods because ``len`` cannot be async.
    """

    def __init__(self, native: Any) -> None:
        self._native = native

    async def append(self, item: T) -> None:
        """Append ``item`` at the back (``None`` raises ``NullValueError``)."""
        return await self._native.push_back(item)

    async def appendleft(self, item: T) -> None:
        """Prepend ``item`` at the front (``None`` raises ``NullValueError``)."""
        return await self._native.push_front(item)

    async def pop(self) -> Optional[T]:
        """Remove and return the back element, or ``None`` when empty."""
        return await self._native.pop_back()

    async def popleft(self) -> Optional[T]:
        """Remove and return the front element, or ``None`` when empty."""
        return await self._native.pop_front()

    async def get(self, index: int) -> Optional[T]:
        """Read the element at front-relative ``index``, or ``None`` past the end.

        No Python-side index guard: the native ``u32`` conversion rejects a
        float (``TypeError``) or a negative/oversized int (``OverflowError``),
        both of which classify transient at the handler bridge — matching the
        "invalid index is transient" rule without duplicating a guard.
        """
        return await self._native.get(index)

    async def size(self) -> int:
        """Number of live elements (named ``size`` because ``len`` cannot be async)."""
        return await self._native.len()

    async def is_empty(self) -> bool:
        """Whether the deque holds no live elements."""
        return await self._native.is_empty()

    def values(self, direction: Direction = Direction.FORWARD) -> _StateScan:
        """Async iterator over the elements in index order."""
        return _StateScan(self._native.scan(direction.value), _identity)

    def __aiter__(self) -> _StateScan:
        """Forward iteration over the elements."""
        return self.values(Direction.FORWARD)

    async def commit(self) -> None:
        """Durably commit the buffered operations mid-handler."""
        await self._native.commit()

    async def rollback(self) -> None:
        """Discard buffered uncommitted operations back to the committed floor."""
        await self._native.rollback()
