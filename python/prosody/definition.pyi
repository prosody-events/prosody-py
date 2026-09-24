"""Type stubs for keyed-state collection definitions.

The type parameter of every definition (``T`` / ``V`` / ``P``) is a
**structural JSON annotation** — TypedDict-oriented. Payloads cross the
boundary as plain JSON with no model construction or validation, so
``dataclass`` / Pydantic types are **not** valid type arguments. Map keys and
set members are always ``str``.
"""

from datetime import timedelta
from typing import Generic, Optional, Union

from typing_extensions import Literal, TypedDict, TypeVar

from prosody.message import JSONValue

# PEP 696 defaults: an unparameterized definition uses ``JSONValue``.
T = TypeVar("T", default=JSONValue)  # value / deque item type
V = TypeVar("V", default=JSONValue)  # map value type
P = TypeVar("P", default=JSONValue)  # message payload type
D_co = TypeVar("D_co", covariant=True, default=JSONValue)
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



class ValueDefinition(Generic[D_co]):
    """A single-value JSON collection definition.

    ``kind = "value"``, ``payload = "json"``. Vends :class:`ValueState` ``[T]``.
    """

    name: str
    ttl: Optional[Union[timedelta, int]]
    read_uncommitted: Optional[bool]
    published: Optional[bool]
    read_cache: ReadCache
    kind: str
    payload: str

    def __init__(
        self,
        name: str,
        ttl: Optional[Union[timedelta, int]] = ...,
        read_uncommitted: Optional[bool] = ...,
        published: Optional[bool] = ...,
        read_cache: ReadCache = ...,
    ) -> None: ...
    def to_config(self) -> _StateConfig:
        """Return the config dict passed to the client and to ``state()``."""
        ...


class MapDefinition(Generic[D_co]):
    """An ordered-map JSON collection definition (string keys).

    ``kind = "map"``, ``payload = "json"``. Vends :class:`MapState` ``[V]``.
    ``keyset_limit`` bounds ordered-scan tracking.
    """

    name: str
    ttl: Optional[Union[timedelta, int]]
    read_uncommitted: Optional[bool]
    published: Optional[bool]
    read_cache: ReadCache
    keyset_limit: Optional[int]
    kind: str
    payload: str

    def __init__(
        self,
        name: str,
        ttl: Optional[Union[timedelta, int]] = ...,
        read_uncommitted: Optional[bool] = ...,
        published: Optional[bool] = ...,
        read_cache: ReadCache = ...,
        keyset_limit: Optional[int] = ...,
    ) -> None: ...
    def to_config(self) -> _StateConfig:
        """Return the config dict passed to the client and to ``state()``."""
        ...


class SetDefinition:
    """A presence-only ordered set of string members.

    ``kind = "set"``, ``payload = "presence"``. Vends :class:`SetState`.
    ``keyset_limit`` bounds ordered-scan tracking, as on a map.
    """

    name: str
    ttl: Optional[Union[timedelta, int]]
    read_uncommitted: Optional[bool]
    published: Optional[bool]
    read_cache: ReadCache
    keyset_limit: Optional[int]
    kind: str
    payload: str

    def __init__(
        self,
        name: str,
        ttl: Optional[Union[timedelta, int]] = ...,
        read_uncommitted: Optional[bool] = ...,
        published: Optional[bool] = ...,
        read_cache: ReadCache = ...,
        keyset_limit: Optional[int] = ...,
    ) -> None: ...
    def to_config(self) -> _StateConfig:
        """Return the config dict passed to the client and to ``state()``."""
        ...


class DequeDefinition(Generic[D_co]):
    """A double-ended-queue JSON collection definition.

    ``kind = "deque"``, ``payload = "json"``. Vends :class:`DequeState` ``[T]``.
    ``capacity`` is deque-only.
    """

    name: str
    ttl: Optional[Union[timedelta, int]]
    read_uncommitted: Optional[bool]
    published: Optional[bool]
    read_cache: ReadCache
    capacity: Optional[int]
    kind: str
    payload: str

    def __init__(
        self,
        name: str,
        ttl: Optional[Union[timedelta, int]] = ...,
        read_uncommitted: Optional[bool] = ...,
        published: Optional[bool] = ...,
        read_cache: ReadCache = ...,
        capacity: Optional[int] = ...,
    ) -> None: ...
    def to_config(self) -> _StateConfig:
        """Return the config dict passed to the client and to ``state()``."""
        ...


class MessageValueDefinition(Generic[D_co]):
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
    def to_config(self) -> _StateConfig:
        """Return the config dict passed to the client and to ``state()``."""
        ...


class MessageMapDefinition(Generic[D_co]):
    """An ordered-map collection storing whole Kafka messages.

    ``kind = "map"``, ``payload = "message"``. Vends
    :class:`MapState` ``[Message[P]]``. ``keyset_limit`` bounds ordered-scan tracking.
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
    def to_config(self) -> _StateConfig:
        """Return the config dict passed to the client and to ``state()``."""
        ...


class MessageDequeDefinition(Generic[D_co]):
    """A double-ended-queue collection storing whole Kafka messages.

    ``kind = "deque"``, ``payload = "message"``. Vends
    :class:`DequeState` ``[Message[P]]``. ``capacity`` is deque-only.
    """

    name: str
    ttl: Optional[Union[timedelta, int]]
    read_uncommitted: Optional[bool]
    capacity: Optional[int]
    kind: str
    payload: str

    def __init__(
        self,
        name: str,
        ttl: Optional[Union[timedelta, int]] = ...,
        read_uncommitted: Optional[bool] = ...,
        capacity: Optional[int] = ...,
    ) -> None: ...
    def to_config(self) -> _StateConfig:
        """Return the config dict passed to the client and to ``state()``."""
        ...


def value(
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = ...,
    read_uncommitted: Optional[bool] = ...,
    published: Optional[bool] = ...,
    read_cache: Optional[Union[timedelta, float, Literal[False]]] = ...,
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
    published: Optional[bool] = ...,
    read_cache: Optional[Union[timedelta, float, Literal[False]]] = ...,
    keyset_limit: Optional[int] = ...,
) -> MapDefinition[V]:
    """Define an ordered-map JSON collection (vends :class:`MapState` ``[V]``).

    Map keys are always ``str``; ``keyset_limit`` bounds ordered-scan tracking.
    ``V`` is a structural JSON annotation only (no runtime validation).
    """
    ...


def set(
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = ...,
    read_uncommitted: Optional[bool] = ...,
    published: Optional[bool] = ...,
    read_cache: Optional[Union[timedelta, float, Literal[False]]] = ...,
    keyset_limit: Optional[int] = ...,
) -> SetDefinition:
    """Define a presence-only ordered set of string members.

    Vends :class:`SetState`. ``keyset_limit`` bounds ordered-scan tracking.
    """
    ...


def deque(
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = ...,
    read_uncommitted: Optional[bool] = ...,
    published: Optional[bool] = ...,
    read_cache: Optional[Union[timedelta, float, Literal[False]]] = ...,
    capacity: Optional[int] = ...,
) -> DequeDefinition[T]:
    """Define a double-ended-queue JSON collection (vends :class:`DequeState` ``[T]``).

    ``capacity`` caps the deque at N slots, enforced lazily on push; runtime-only
    and freely changed across deploys. ``T`` is a structural JSON annotation only
    (no runtime validation).
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

    Only a message prosody delivered can be stored; see :class:`Message`.
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

    Only a message prosody delivered can be stored; see :class:`Message`.
    """
    ...


def message_deque(
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = ...,
    read_uncommitted: Optional[bool] = ...,
    capacity: Optional[int] = ...,
) -> MessageDequeDefinition[P]:
    """Define a double-ended-queue collection of whole Kafka messages.

    Vends :class:`DequeState` ``[Message[P]]``. ``capacity`` caps the deque at N
    slots, enforced lazily on push; runtime-only and freely changed across
    deploys. ``P`` annotates the message payload structurally only (no runtime
    validation).

    Only a message prosody delivered can be stored; see :class:`Message`.
    """
    ...
