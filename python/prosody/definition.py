"""Collection definitions for keyed state.

A definition sets a collection's durable name, kind, and options. It serializes
into the config dict the client registers, and :meth:`Context.state` binds the
same object in a handler.
"""

from dataclasses import dataclass
from datetime import timedelta
from typing import ClassVar, Generic, Optional, Protocol, Union

from typing_extensions import Literal, TypedDict, TypeVar

from prosody.message import JSONValue

# PEP 696 defaults: an unparameterized definition uses ``JSONValue``.
T = TypeVar("T", default=JSONValue)  # value / deque item type
V = TypeVar("V", default=JSONValue)  # map value type
P = TypeVar("P", default=JSONValue)  # message payload type


def _ttl_seconds(ttl: Optional[Union[timedelta, int]]) -> Optional[Union[float, int]]:
    """Expose a TTL in seconds without truncating invalid host values."""
    if ttl is None:
        return None
    if isinstance(ttl, timedelta):
        return ttl.total_seconds()
    return ttl


ReadCache = Optional[Union[timedelta, float, Literal[False]]]


class _StateConfig(TypedDict):
    name: str
    kind: str
    payload: str
    ttl_seconds: Optional[Union[float, int]]
    read_uncommitted: Optional[bool]
    published: Optional[bool]
    keyset_limit: Optional[int]
    capacity: Optional[int]


class _Definition(Protocol):
    name: str
    kind: str
    payload: str
    ttl: Optional[Union[timedelta, int]]
    read_uncommitted: Optional[bool]
    published: Optional[bool]
    keyset_limit: Optional[int]
    capacity: Optional[int]


def _config(definition: _Definition) -> _StateConfig:
    """Build the registration config dict the owning client consumes.

    Excludes ``read_cache``: the owner-side registration path never reads it.
    A published reader receives it as an explicit argument instead (see
    :meth:`ProsodyClient.state`).
    """
    return {
        "name": definition.name,
        "kind": definition.kind,
        "payload": definition.payload,
        "ttl_seconds": _ttl_seconds(definition.ttl),
        "read_uncommitted": definition.read_uncommitted,
        "published": definition.published,
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
class SetDefinition:
    """A presence-only ordered set of string members."""

    name: str
    ttl: Optional[Union[timedelta, int]] = None
    read_uncommitted: Optional[bool] = None
    published: Optional[bool] = None
    read_cache: ReadCache = None
    keyset_limit: Optional[int] = None
    capacity: ClassVar[Optional[int]] = None
    kind: ClassVar[str] = "set"
    payload: ClassVar[str] = "presence"

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


def set(  # this module-local name mirrors the collection kind; no builtin use here
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = None,
    read_uncommitted: Optional[bool] = None,
    published: Optional[bool] = None,
    read_cache: Optional[Union[timedelta, float, Literal[False]]] = None,
    keyset_limit: Optional[int] = None,
) -> SetDefinition:
    """Define a presence-only ordered set of string members."""
    return SetDefinition(
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
