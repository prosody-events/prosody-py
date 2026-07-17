"""Type stubs for the keyed-state surface.

These stubs override :mod:`prosody.state` for type-checkers, restoring the
generic types that the runtime erases to ``Any``. The runtime module is a thin
transport over the native handles vended by :meth:`Context.state`; the native
(Rust) layer owns every semantic (carrier injection, chunk draining,
error-category classification, null/shape/kind guards, and scan flattening).

The type parameter of every definition and handle (``T`` / ``V`` / ``P``) is a
**structural JSON annotation** — TypedDict-oriented. Payloads cross the boundary
as plain JSON with no model construction or validation in v1, so
``dataclass`` / Pydantic types are **not** valid type arguments; an adapter hook
is future work. Map keys are always ``str``.
"""

import enum
from datetime import timedelta
from typing import Any, Dict, Generic, List, Optional, Tuple, Union

from typing_extensions import TypeVar

from prosody.message import JSONValue, Message

# PEP 696 defaults: an unparameterized handle/definition uses ``JSONValue``.
T = TypeVar("T", default=JSONValue)  # value / deque item type
V = TypeVar("V", default=JSONValue)  # map value type
P = TypeVar("P", default=JSONValue)  # message payload type
_Y = TypeVar("_Y")  # yielded item type of a scan


class Direction(enum.Enum):
    """Scan direction over an ordered collection.

    The string values are the tokens the native ``scan`` accepts; wrappers pass
    ``direction.value`` straight through.
    """

    FORWARD = "forward"
    BACKWARD = "backward"


class _StateScan(Generic[_Y]):
    """Async iterator over a native scan cursor.

    Returned by every scan method (:meth:`MapState.items`, :meth:`MapState.keys`,
    :meth:`MapState.values`, :meth:`DequeState.values`) and by ``__aiter__``. The
    native cursor owns retained-chunk flattening, serialization, and
    ``StopAsyncIteration`` at exhaustion; this adapter only reshapes each item.

    Drive it with ``async for``. Exiting the loop early with a bare ``break``
    does **not** call :meth:`aclose` — that is harmless by construction (no store
    permit is held between pulls, the cursor is attempt-epoch fenced, and native
    ``Drop`` closes it on GC). For a deterministic early close wrap it in
    ``contextlib.aclosing(...)``.

    The generic parameter restores the yielded type even though the runtime
    class is one non-generic adapter (``_StateScan[Tuple[str, V]]`` for map
    items, ``_StateScan[str]`` for keys, ``_StateScan[V]`` / ``_StateScan[T]``
    for values).
    """

    def __aiter__(self) -> "_StateScan[_Y]": ...
    async def __anext__(self) -> _Y: ...
    async def aclose(self) -> None:
        """Close the underlying native cursor (idempotent)."""
        ...


class ValueDefinition(Generic[T]):
    """A single-value JSON collection definition.

    ``kind = "value"``, ``payload = "json"``. Vends :class:`ValueState` ``[T]``.
    """

    name: str
    ttl: Optional[Union[timedelta, int]]
    read_uncommitted: Optional[bool]
    kind: str
    payload: str

    def __init__(
        self,
        name: str,
        ttl: Optional[Union[timedelta, int]] = ...,
        read_uncommitted: Optional[bool] = ...,
    ) -> None: ...
    def to_config(self) -> Dict[str, Any]:
        """Return the config dict passed to the client and to ``state()``."""
        ...


class MapDefinition(Generic[V]):
    """An ordered-map JSON collection definition (string keys).

    ``kind = "map"``, ``payload = "json"``. Vends :class:`MapState` ``[V]``.
    ``keyset_limit`` is map-only.
    """

    name: str
    ttl: Optional[Union[timedelta, int]]
    read_uncommitted: Optional[bool]
    keyset_limit: Optional[int]
    kind: str
    payload: str

    def __init__(
        self,
        name: str,
        ttl: Optional[Union[timedelta, int]] = ...,
        read_uncommitted: Optional[bool] = ...,
        keyset_limit: Optional[int] = ...,
    ) -> None: ...
    def to_config(self) -> Dict[str, Any]:
        """Return the config dict passed to the client and to ``state()``."""
        ...


class DequeDefinition(Generic[T]):
    """A double-ended-queue JSON collection definition.

    ``kind = "deque"``, ``payload = "json"``. Vends :class:`DequeState` ``[T]``.
    """

    name: str
    ttl: Optional[Union[timedelta, int]]
    read_uncommitted: Optional[bool]
    kind: str
    payload: str

    def __init__(
        self,
        name: str,
        ttl: Optional[Union[timedelta, int]] = ...,
        read_uncommitted: Optional[bool] = ...,
    ) -> None: ...
    def to_config(self) -> Dict[str, Any]:
        """Return the config dict passed to the client and to ``state()``."""
        ...


class MessageValueDefinition(Generic[P]):
    """A single-value collection storing whole Kafka messages.

    ``kind = "value"``, ``payload = "message"``. Vends
    :class:`ValueState` ``[Message[P]]``.
    """

    name: str
    ttl: Optional[Union[timedelta, int]]
    read_uncommitted: Optional[bool]
    kind: str
    payload: str

    def __init__(
        self,
        name: str,
        ttl: Optional[Union[timedelta, int]] = ...,
        read_uncommitted: Optional[bool] = ...,
    ) -> None: ...
    def to_config(self) -> Dict[str, Any]:
        """Return the config dict passed to the client and to ``state()``."""
        ...


class MessageMapDefinition(Generic[P]):
    """An ordered-map collection storing whole Kafka messages.

    ``kind = "map"``, ``payload = "message"``. Vends
    :class:`MapState` ``[Message[P]]``. ``keyset_limit`` is map-only.
    """

    name: str
    ttl: Optional[Union[timedelta, int]]
    read_uncommitted: Optional[bool]
    keyset_limit: Optional[int]
    kind: str
    payload: str

    def __init__(
        self,
        name: str,
        ttl: Optional[Union[timedelta, int]] = ...,
        read_uncommitted: Optional[bool] = ...,
        keyset_limit: Optional[int] = ...,
    ) -> None: ...
    def to_config(self) -> Dict[str, Any]:
        """Return the config dict passed to the client and to ``state()``."""
        ...


class MessageDequeDefinition(Generic[P]):
    """A double-ended-queue collection storing whole Kafka messages.

    ``kind = "deque"``, ``payload = "message"``. Vends
    :class:`DequeState` ``[Message[P]]``.
    """

    name: str
    ttl: Optional[Union[timedelta, int]]
    read_uncommitted: Optional[bool]
    kind: str
    payload: str

    def __init__(
        self,
        name: str,
        ttl: Optional[Union[timedelta, int]] = ...,
        read_uncommitted: Optional[bool] = ...,
    ) -> None: ...
    def to_config(self) -> Dict[str, Any]:
        """Return the config dict passed to the client and to ``state()``."""
        ...


def value(
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = ...,
    read_uncommitted: Optional[bool] = ...,
) -> ValueDefinition[T]:
    """Define a single-value JSON collection (vends :class:`ValueState` ``[T]``).

    ``T`` is a structural JSON annotation only — no runtime validation happens,
    so ``dataclass`` / Pydantic types are not valid arguments (adapter hook is
    future work).
    """
    ...


def map(
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = ...,
    read_uncommitted: Optional[bool] = ...,
    keyset_limit: Optional[int] = ...,
) -> MapDefinition[V]:
    """Define an ordered-map JSON collection (vends :class:`MapState` ``[V]``).

    Map keys are always ``str``; ``keyset_limit`` bounds ordered-scan tracking.
    ``V`` is a structural JSON annotation only (no runtime validation).
    """
    ...


def deque(
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = ...,
    read_uncommitted: Optional[bool] = ...,
) -> DequeDefinition[T]:
    """Define a double-ended-queue JSON collection (vends :class:`DequeState` ``[T]``).

    ``T`` is a structural JSON annotation only (no runtime validation).
    """
    ...


def message_value(
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = ...,
    read_uncommitted: Optional[bool] = ...,
) -> MessageValueDefinition[P]:
    """Define a single-value collection of whole Kafka messages.

    Vends :class:`ValueState` ``[Message[P]]``. ``P`` annotates the message
    payload structurally only (no runtime validation).
    """
    ...


def message_map(
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = ...,
    read_uncommitted: Optional[bool] = ...,
    keyset_limit: Optional[int] = ...,
) -> MessageMapDefinition[P]:
    """Define an ordered-map collection of whole Kafka messages (string keys).

    Vends :class:`MapState` ``[Message[P]]``. ``P`` annotates the message
    payload structurally only (no runtime validation).
    """
    ...


def message_deque(
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = ...,
    read_uncommitted: Optional[bool] = ...,
) -> MessageDequeDefinition[P]:
    """Define a double-ended-queue collection of whole Kafka messages.

    Vends :class:`DequeState` ``[Message[P]]``. ``P`` annotates the message
    payload structurally only (no runtime validation).
    """
    ...


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
    async def commit(self) -> None:
        """Durably flush the buffered operations mid-handler.

        Returns ``None`` — the erased core seam drops any store outcome, so there
        is no applied/noop value.
        """
        ...
    async def rollback(self) -> None:
        """Discard buffered uncommitted operations back to the committed floor."""
        ...


class MapState(Generic[V]):
    """Typed handle over an ordered-map collection with string keys.

    Valid only within the handler invocation that vended it. ``remove`` exists
    because ``del`` cannot be async; map keys are always ``str``.
    """

    async def get(self, key: str) -> Optional[V]:
        """Read the value for ``key``, or ``None`` when absent."""
        ...
    async def get_many(self, keys: List[str]) -> List[Optional[V]]:
        """Read several keys in one isolated batch, one result per key in order.

        ``result[i]`` is the value for ``keys[i]`` (``None`` for a missing key).
        The batched, cache-populating way to read a known set of keys — prefer
        it over iterating :meth:`keys` and calling :meth:`get` per key.
        """
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
    def items(self, direction: Direction = ...) -> _StateScan[Tuple[str, V]]:
        """Async iterator over ``(key, value)`` entries in key order.

        Accepts a :class:`Direction` (``FORWARD`` default / ``BACKWARD``).
        """
        ...
    def keys(self) -> _StateScan[str]:
        """Async iterator over the keys in forward key order (forward-only).

        Runs the same full ``(key, value)`` scan as :meth:`items` and resolves
        every value before discarding it — not a cheaper key-only enumeration
        (core has no keys-only scan). If you will also read the values, iterate
        :meth:`items`; to read a known set of keys, call :meth:`get_many`.
        """
        ...
    def values(self) -> _StateScan[V]:
        """Async iterator over the values in forward key order (forward-only).

        Runs the same full ``(key, value)`` scan as :meth:`items` and discards
        the keys; it is not cheaper than :meth:`items`.
        """
        ...
    def __aiter__(self) -> _StateScan[Tuple[str, V]]:
        """Forward iteration over ``(key, value)`` entries."""
        ...
    async def commit(self) -> None:
        """Durably flush the buffered operations mid-handler.

        Returns ``None`` — the erased core seam drops any store outcome.
        """
        ...
    async def rollback(self) -> None:
        """Discard buffered uncommitted operations back to the committed floor."""
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
    async def get(self, index: int) -> Optional[T]:
        """Read the element at front-relative ``index``, or ``None`` past the end.

        ``index`` must be a non-negative integer that fits a native ``u32``. A
        fractional value raises :class:`TypeError` and a negative or oversized
        value raises :class:`OverflowError` at the native boundary; both
        classify transient at the handler bridge, so the caller mistake retries
        rather than discarding the message.
        """
        ...
    async def size(self) -> int:
        """Number of live elements (named ``size`` because ``len`` cannot be async)."""
        ...
    async def is_empty(self) -> bool:
        """Whether the deque holds no live elements."""
        ...
    def values(self, direction: Direction = ...) -> _StateScan[T]:
        """Async iterator over the elements in index order.

        Accepts a :class:`Direction` (``FORWARD`` default / ``BACKWARD``).
        """
        ...
    def __aiter__(self) -> _StateScan[T]:
        """Forward iteration over the elements."""
        ...
    async def commit(self) -> None:
        """Durably flush the buffered operations mid-handler.

        Returns ``None`` — the erased core seam drops any store outcome.
        """
        ...
    async def rollback(self) -> None:
        """Discard buffered uncommitted operations back to the committed floor."""
        ...
