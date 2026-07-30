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
from typing import Any, Callable, ClassVar, Generic, List, Optional, Protocol, Union

from typing_extensions import Literal, TypedDict, TypeVar

from prosody.message import JSONValue

# PEP 696 defaults: an unparameterized handle/definition uses ``JSONValue``.
T = TypeVar("T", default=JSONValue)  # value / deque item type
V = TypeVar("V", default=JSONValue)  # map value type
P = TypeVar("P", default=JSONValue)  # message payload type
X = TypeVar("X")
Y = TypeVar("Y")


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


ReadCache = Optional[Union[timedelta, float, Literal[False]]]


class _StateConfig(TypedDict):
    name: str
    kind: str
    payload: str
    ttl_seconds: Optional[int]
    read_uncommitted: Optional[bool]
    published: Optional[bool]
    read_cache: ReadCache
    keyset_limit: Optional[int]
    capacity: Optional[int]


class _Definition(Protocol):
    name: str
    kind: str
    payload: str
    ttl: Optional[Union[timedelta, int]]
    read_uncommitted: Optional[bool]
    published: Optional[bool]
    read_cache: ReadCache
    keyset_limit: Optional[int]
    capacity: Optional[int]


def _config(definition: _Definition) -> _StateConfig:
    """Build the registration/vend config dict the client layer consumes."""
    return {
        "name": definition.name,
        "kind": definition.kind,
        "payload": definition.payload,
        "ttl_seconds": _ttl_seconds(definition.ttl),
        "read_uncommitted": definition.read_uncommitted,
        "published": definition.published,
        "read_cache": definition.read_cache,
        "keyset_limit": definition.keyset_limit,
        "capacity": definition.capacity,
    }


@dataclass(frozen=True)
class ValueDefinition(Generic[T]):
    """A single-value JSON collection definition."""

    name: str
    ttl: Optional[Union[timedelta, int]] = None
    read_uncommitted: Optional[bool] = None
    published: Optional[bool] = None
    read_cache: ReadCache = None
    keyset_limit: ClassVar[Optional[int]] = None
    capacity: ClassVar[Optional[int]] = None
    kind: ClassVar[str] = "value"
    payload: ClassVar[str] = "json"

    def to_config(self) -> _StateConfig:
        """Return the config dict passed to the client and to ``state()``."""
        return _config(self)


@dataclass(frozen=True)
class MapDefinition(Generic[V]):
    """An ordered-map JSON collection definition (string keys)."""

    name: str
    ttl: Optional[Union[timedelta, int]] = None
    read_uncommitted: Optional[bool] = None
    published: Optional[bool] = None
    read_cache: ReadCache = None
    keyset_limit: Optional[int] = None
    capacity: ClassVar[Optional[int]] = None
    kind: ClassVar[str] = "map"
    payload: ClassVar[str] = "json"

    def to_config(self) -> _StateConfig:
        """Return the config dict passed to the client and to ``state()``."""
        return _config(self)


@dataclass(frozen=True)
class DequeDefinition(Generic[T]):
    """A double-ended-queue JSON collection definition."""

    name: str
    ttl: Optional[Union[timedelta, int]] = None
    read_uncommitted: Optional[bool] = None
    published: Optional[bool] = None
    read_cache: ReadCache = None
    capacity: Optional[int] = None
    keyset_limit: ClassVar[Optional[int]] = None
    kind: ClassVar[str] = "deque"
    payload: ClassVar[str] = "json"

    def to_config(self) -> _StateConfig:
        """Return the config dict passed to the client and to ``state()``."""
        return _config(self)


@dataclass(frozen=True)
class MessageValueDefinition(Generic[P]):
    """A single-value collection storing whole Kafka messages."""

    name: str
    ttl: Optional[Union[timedelta, int]] = None
    read_uncommitted: Optional[bool] = None
    published: ClassVar[Optional[bool]] = None
    read_cache: ClassVar[ReadCache] = None
    keyset_limit: ClassVar[Optional[int]] = None
    capacity: ClassVar[Optional[int]] = None
    kind: ClassVar[str] = "value"
    payload: ClassVar[str] = "message"

    def to_config(self) -> _StateConfig:
        """Return the config dict passed to the client and to ``state()``."""
        return _config(self)


@dataclass(frozen=True)
class MessageMapDefinition(Generic[P]):
    """An ordered-map collection storing whole Kafka messages."""

    name: str
    ttl: Optional[Union[timedelta, int]] = None
    read_uncommitted: Optional[bool] = None
    keyset_limit: Optional[int] = None
    published: ClassVar[Optional[bool]] = None
    read_cache: ClassVar[ReadCache] = None
    capacity: ClassVar[Optional[int]] = None
    kind: ClassVar[str] = "map"
    payload: ClassVar[str] = "message"

    def to_config(self) -> _StateConfig:
        """Return the config dict passed to the client and to ``state()``."""
        return _config(self)


@dataclass(frozen=True)
class MessageDequeDefinition(Generic[P]):
    """A double-ended-queue collection storing whole Kafka messages."""

    name: str
    ttl: Optional[Union[timedelta, int]] = None
    read_uncommitted: Optional[bool] = None
    capacity: Optional[int] = None
    published: ClassVar[Optional[bool]] = None
    read_cache: ClassVar[ReadCache] = None
    keyset_limit: ClassVar[Optional[int]] = None
    kind: ClassVar[str] = "deque"
    payload: ClassVar[str] = "message"

    def to_config(self) -> _StateConfig:
        """Return the config dict passed to the client and to ``state()``."""
        return _config(self)


def value(
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = None,
    read_uncommitted: Optional[bool] = None,
    published: Optional[bool] = None,
    read_cache: Optional[Union[timedelta, float, Literal[False]]] = None,
) -> ValueDefinition[T]:
    """Define a single-value JSON collection."""
    return ValueDefinition(
        name,
        ttl=ttl,
        read_uncommitted=read_uncommitted,
        published=published,
        read_cache=read_cache,
    )


def map(  # this module-local name mirrors the collection kind; no builtin use here
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = None,
    read_uncommitted: Optional[bool] = None,
    published: Optional[bool] = None,
    read_cache: Optional[Union[timedelta, float, Literal[False]]] = None,
    keyset_limit: Optional[int] = None,
) -> MapDefinition[V]:
    """Define an ordered-map JSON collection (string keys)."""
    return MapDefinition(
        name,
        ttl=ttl,
        read_uncommitted=read_uncommitted,
        published=published,
        read_cache=read_cache,
        keyset_limit=keyset_limit,
    )


def deque(
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = None,
    read_uncommitted: Optional[bool] = None,
    published: Optional[bool] = None,
    read_cache: Optional[Union[timedelta, float, Literal[False]]] = None,
    capacity: Optional[int] = None,
) -> DequeDefinition[T]:
    """Define a double-ended-queue JSON collection.

    ``capacity`` caps the deque at N slots, enforced lazily on push (see
    :meth:`DequeState.append`). Runtime-only — never persisted and freely
    changed across deploys.
    """
    return DequeDefinition(
        name,
        ttl=ttl,
        read_uncommitted=read_uncommitted,
        published=published,
        read_cache=read_cache,
        capacity=capacity,
    )


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
    capacity: Optional[int] = None,
) -> MessageDequeDefinition[P]:
    """Define a double-ended-queue collection of whole Kafka messages.

    ``capacity`` caps the deque at N slots, enforced lazily on push (see
    :meth:`DequeState.append`). Runtime-only — never persisted and freely
    changed across deploys.
    """
    return MessageDequeDefinition(
        name, ttl=ttl, read_uncommitted=read_uncommitted, capacity=capacity
    )


def _identity(item: X) -> X:
    return item


class _NativeScan(Protocol[X]):
    async def __anext__(self) -> X: ...
    async def aclose(self) -> None: ...


class _StateScan(Generic[Y]):
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

    def __init__(
        self, native: _NativeScan[X], transform: Callable[[X], Y]
    ) -> None:
        self._native = native
        self._transform = transform

    def __aiter__(self) -> "_StateScan[Y]":
        return self

    async def __anext__(self) -> Y:
        # Re-raises the native StopAsyncIteration at exhaustion (never coerced
        # by PEP 479 since it crosses no generator boundary here).
        return self._transform(await self._native.__anext__())

    async def aclose(self) -> None:
        await self._native.aclose()


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

    async def items(
        self, key: str, direction: Direction = Direction.FORWARD
    ) -> "_StateScan[tuple[str, V]]":
        return _StateScan(await self._native.scan(key, direction.value), _identity)

    async def keys(
        self, key: str, direction: Direction = Direction.FORWARD
    ) -> "_StateScan[str]":
        return _StateScan(await self._native.keys(key, direction.value), _identity)

    async def values(self, key: str) -> "_StateScan[V]":
        return _StateScan(
            await self._native.scan(key, Direction.FORWARD.value),
            lambda entry: entry[1],
        )


class PublishedDeque(Generic[T]):
    """Read-only access to a published deque collection."""

    def __init__(self, native: "_PublishedDequeNative[T]") -> None:
        self._native = native

    async def get(self, key: str, index: int) -> Optional[T]:
        return await self._native.get(key, index)

    async def size(self, key: str) -> int:
        return await self._native.len(key)

    async def is_empty(self, key: str) -> bool:
        return await self._native.is_empty(key)

    async def peek(self, key: str) -> Optional[T]:
        return await self._native.peek_back(key)

    async def peekleft(self, key: str) -> Optional[T]:
        return await self._native.peek_front(key)

    async def values(
        self, key: str, direction: Direction = Direction.FORWARD
    ) -> "_StateScan[T]":
        return _StateScan(await self._native.scan(key, direction.value), _identity)


class _PublishedValueNative(Protocol[T]):
    async def get(self, key: str) -> Optional[T]: ...


class _PublishedMapNative(Protocol[V]):
    async def get(self, key: str, map_key: str) -> Optional[V]: ...

    async def get_many(
        self, key: str, map_keys: List[str]
    ) -> List[Optional[V]]: ...

    async def contains_key(self, key: str, map_key: str) -> bool: ...

    async def scan(
        self, key: str, direction: str
    ) -> "_NativeScan[tuple[str, V]]": ...

    async def keys(self, key: str, direction: str) -> "_NativeScan[str]": ...


class _PublishedDequeNative(Protocol[T]):
    async def get(self, key: str, index: int) -> Optional[T]: ...

    async def len(self, key: str) -> int: ...

    async def is_empty(self, key: str) -> bool: ...

    async def peek_front(self, key: str) -> Optional[T]: ...

    async def peek_back(self, key: str) -> Optional[T]: ...

    async def scan(self, key: str, direction: str) -> "_NativeScan[T]": ...


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

    async def set(self, key: str, value: V) -> None:
        """Insert or overwrite ``key`` (``None`` raises ``NullValueError``)."""
        await self._native.set(key, value)

    async def remove(self, key: str) -> None:
        """Remove ``key`` (named ``remove`` because ``del`` cannot be async)."""
        await self._native.remove(key)

    async def clear(self) -> None:
        """Remove every entry."""
        await self._native.clear()

    def items(self, direction: Direction = Direction.FORWARD) -> _StateScan:
        """Async iterator over ``(key, value)`` entries in key order."""
        return _StateScan(self._native.scan(direction.value), _identity)

    def keys(self, direction: Direction = Direction.FORWARD) -> _StateScan:
        """Async iterator over the keys in key order — the cheap key-only scan.

        Never decodes a value or runs the resolver, so a message-backed map
        enumerates keys with **zero Kafka fetches**. It is not zero-I/O: pulling
        a chunk still does a presence-only read. Accepts a :class:`Direction`
        (``FORWARD`` default / ``BACKWARD``). When you also need the values,
        iterate :meth:`items` (one batched, fully-resolving scan); for a known
        set of keys, call :meth:`get_many`.
        """
        return _StateScan(self._native.keys(direction.value), _identity)

    def values(self) -> _StateScan:
        """Async iterator over the values in forward key order.

        A projection of the full ``(key, value)`` scan that drops the keys.
        Value iteration inherently decodes and resolves, so this is not the
        cheap path :meth:`keys` is; it costs the same as :meth:`items`.
        """
        return _StateScan(self._native.scan(Direction.FORWARD.value), lambda e: e[1])

    def __aiter__(self) -> _StateScan:
        """Forward iteration over the **keys**, like ``dict``.

        Use :meth:`items` when you need the values — one batched, fully-resolving
        scan — rather than per-key :meth:`get` after key iteration (a round trip
        per key).
        """
        return self.keys()

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

    async def clear(self) -> None:
        """Remove every element."""
        await self._native.clear()

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
