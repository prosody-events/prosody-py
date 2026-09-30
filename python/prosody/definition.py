"""Collection definitions for keyed state.

A definition sets a collection's durable name, kind, and options. It serializes
into the config dict the client registers, and :meth:`Context.state` binds the
same object in a handler.

The type parameter of every definition (``T``, ``V``, or ``P``) describes a
JSON shape, such as a ``TypedDict``. Values cross the boundary as plain JSON.
Prosody does not build or validate a model, so a ``dataclass`` or a Pydantic
model is not a valid type argument. Map keys and set members are always
``str``.
"""

from dataclasses import dataclass
from datetime import timedelta
from typing import ClassVar, Generic, Optional, Union

from typing_extensions import Literal, TypedDict, TypeVar

from prosody.message import JSONValue

# PEP 696 defaults: an unparameterized definition uses ``JSONValue``.
T = TypeVar("T", default=JSONValue)  # value / deque item type
V = TypeVar("V", default=JSONValue)  # map value type
P = TypeVar("P", default=JSONValue)  # message payload type


def _ttl_seconds(ttl: Optional[Union[timedelta, int]]) -> Optional[Union[float, int]]:
    """Expose a TTL in seconds without truncating invalid host values.

    A whole timedelta becomes an ``int``. A fractional one stays a ``float``,
    which the client rejects.
    """
    if not isinstance(ttl, timedelta):
        return ttl
    seconds = ttl.total_seconds()
    return int(seconds) if seconds.is_integer() else seconds


ReadCache = Optional[Union[timedelta, float, Literal[False]]]


class _StateConfig(TypedDict):
    name: str
    kind: str
    payload: Optional[str]
    ttl_seconds: Optional[Union[float, int]]
    read_uncommitted: Optional[bool]
    published: Optional[bool]
    keyset_limit: Optional[int]
    capacity: Optional[int]


class _Definition:
    """The registration options every definition shares.

    Each definition declares its own fields and overrides only the class
    defaults that differ.
    """

    published: Optional[bool] = None
    read_cache: ReadCache = None
    keyset_limit: Optional[int] = None
    capacity: Optional[int] = None
    payload: Optional[str] = "json"

    def to_config(self) -> _StateConfig:
        """Return the config dict passed to the client and to ``state()``.

        Excludes ``read_cache``: the owner-side registration path never reads
        it. A published reader receives it as an explicit argument instead
        (see :meth:`ProsodyClient.state`).
        """
        return {
            "name": self.name,
            "kind": self.kind,
            "payload": self.payload,
            "ttl_seconds": _ttl_seconds(self.ttl),
            "read_uncommitted": self.read_uncommitted,
            "published": self.published,
            "keyset_limit": self.keyset_limit,
            "capacity": self.capacity,
        }


@dataclass(frozen=True)
class ValueDefinition(_Definition, Generic[T]):
    """A single-value JSON collection definition. Vends :class:`ValueState`."""

    name: str
    ttl: Optional[Union[timedelta, int]] = None
    read_uncommitted: Optional[bool] = None
    published: Optional[bool] = None
    read_cache: ReadCache = None
    kind: ClassVar[str] = "value"


@dataclass(frozen=True)
class MapDefinition(_Definition, Generic[V]):
    """An ordered-map JSON collection definition. Vends :class:`MapState`.

    ``keyset_limit`` bounds ordered-scan tracking.
    """

    name: str
    ttl: Optional[Union[timedelta, int]] = None
    read_uncommitted: Optional[bool] = None
    published: Optional[bool] = None
    read_cache: ReadCache = None
    keyset_limit: Optional[int] = None
    kind: ClassVar[str] = "map"


@dataclass(frozen=True)
class SetDefinition(_Definition):
    """A presence-only ordered set of string members. Vends :class:`SetState`.

    ``keyset_limit`` bounds ordered-scan tracking, as on a map.
    """

    name: str
    ttl: Optional[Union[timedelta, int]] = None
    read_uncommitted: Optional[bool] = None
    published: Optional[bool] = None
    read_cache: ReadCache = None
    keyset_limit: Optional[int] = None
    kind: ClassVar[str] = "set"
    payload: ClassVar[Optional[str]] = None


@dataclass(frozen=True)
class DequeDefinition(_Definition, Generic[T]):
    """A double-ended-queue JSON collection definition. Vends :class:`DequeState`."""

    name: str
    ttl: Optional[Union[timedelta, int]] = None
    read_uncommitted: Optional[bool] = None
    published: Optional[bool] = None
    read_cache: ReadCache = None
    capacity: Optional[int] = None
    kind: ClassVar[str] = "deque"


@dataclass(frozen=True)
class MessageValueDefinition(_Definition, Generic[P]):
    """A single-value collection of whole Kafka messages.

    Vends :class:`ValueState` ``[Message[P]]``.
    """

    name: str
    ttl: Optional[Union[timedelta, int]] = None
    read_uncommitted: Optional[bool] = None
    kind: ClassVar[str] = "value"
    payload: ClassVar[str] = "message"


@dataclass(frozen=True)
class MessageMapDefinition(_Definition, Generic[P]):
    """An ordered-map collection of whole Kafka messages.

    Vends :class:`MapState` ``[Message[P]]``.
    """

    name: str
    ttl: Optional[Union[timedelta, int]] = None
    read_uncommitted: Optional[bool] = None
    keyset_limit: Optional[int] = None
    kind: ClassVar[str] = "map"
    payload: ClassVar[str] = "message"


@dataclass(frozen=True)
class MessageDequeDefinition(_Definition, Generic[P]):
    """A double-ended-queue collection of whole Kafka messages.

    Vends :class:`DequeState` ``[Message[P]]``.
    """

    name: str
    ttl: Optional[Union[timedelta, int]] = None
    read_uncommitted: Optional[bool] = None
    capacity: Optional[int] = None
    kind: ClassVar[str] = "deque"
    payload: ClassVar[str] = "message"


def value(
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = None,
    read_uncommitted: Optional[bool] = None,
    published: Optional[bool] = None,
    read_cache: Optional[Union[timedelta, float, Literal[False]]] = None,
) -> ValueDefinition[T]:
    """Define a single-value JSON collection (vends :class:`ValueState` ``[T]``).

    ``T`` describes a JSON shape only. Prosody does not validate the value.
    """
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
    """Define an ordered-map JSON collection (vends :class:`MapState` ``[V]``).

    Map keys are always ``str``. ``keyset_limit`` bounds ordered-scan tracking.
    """
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
    """Define a presence-only ordered set of string members.

    Vends :class:`SetState`. ``keyset_limit`` bounds ordered-scan tracking.
    """
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
    """Define a double-ended-queue JSON collection (vends :class:`DequeState` ``[T]``).

    ``capacity`` caps the deque at N slots. A push enforces it; see
    :meth:`DequeState.append`. Prosody does not persist the capacity, so a
    deploy can change it.
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
    """Define a single-value collection of whole Kafka messages.

    Vends :class:`ValueState` ``[Message[P]]``. Only a message prosody
    delivered can be stored; see :class:`Message`.
    """
    return MessageValueDefinition(name, ttl=ttl, read_uncommitted=read_uncommitted)


def message_map(
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = None,
    read_uncommitted: Optional[bool] = None,
    keyset_limit: Optional[int] = None,
) -> MessageMapDefinition[P]:
    """Define an ordered-map collection of whole Kafka messages (string keys).

    Vends :class:`MapState` ``[Message[P]]``. Only a message prosody delivered
    can be stored; see :class:`Message`.
    """
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

    Vends :class:`DequeState` ``[Message[P]]``. ``capacity`` works as on
    :func:`deque`. Only a message prosody delivered can be stored; see
    :class:`Message`.
    """
    return MessageDequeDefinition(
        name, ttl=ttl, read_uncommitted=read_uncommitted, capacity=capacity
    )
