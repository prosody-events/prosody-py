from datetime import timedelta
from typing import Final, Generic, Optional, Union

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
    payload: Optional[str]
    ttl_seconds: Optional[Union[int, float]]
    read_uncommitted: Optional[bool]
    published: Optional[bool]
    keyset_limit: Optional[int]
    capacity: Optional[int]


class ValueDefinition(Generic[D_co]):
    name: Final[str]
    ttl: Final[Optional[Union[timedelta, int]]]
    read_uncommitted: Final[Optional[bool]]
    published: Final[Optional[bool]]
    read_cache: Final[ReadCache]
    kind: Final[str]
    payload: Final[str]

    def __init__(
        self,
        name: str,
        ttl: Optional[Union[timedelta, int]] = ...,
        read_uncommitted: Optional[bool] = ...,
        published: Optional[bool] = ...,
        read_cache: ReadCache = ...,
    ) -> None: ...
    def to_config(self) -> _StateConfig: ...


class MapDefinition(Generic[D_co]):
    name: Final[str]
    ttl: Final[Optional[Union[timedelta, int]]]
    read_uncommitted: Final[Optional[bool]]
    published: Final[Optional[bool]]
    read_cache: Final[ReadCache]
    keyset_limit: Final[Optional[int]]
    kind: Final[str]
    payload: Final[str]

    def __init__(
        self,
        name: str,
        ttl: Optional[Union[timedelta, int]] = ...,
        read_uncommitted: Optional[bool] = ...,
        published: Optional[bool] = ...,
        read_cache: ReadCache = ...,
        keyset_limit: Optional[int] = ...,
    ) -> None: ...
    def to_config(self) -> _StateConfig: ...


class SetDefinition:
    name: Final[str]
    ttl: Final[Optional[Union[timedelta, int]]]
    read_uncommitted: Final[Optional[bool]]
    published: Final[Optional[bool]]
    read_cache: Final[ReadCache]
    keyset_limit: Final[Optional[int]]
    kind: Final[str]
    payload: Final[None]

    def __init__(
        self,
        name: str,
        ttl: Optional[Union[timedelta, int]] = ...,
        read_uncommitted: Optional[bool] = ...,
        published: Optional[bool] = ...,
        read_cache: ReadCache = ...,
        keyset_limit: Optional[int] = ...,
    ) -> None: ...
    def to_config(self) -> _StateConfig: ...


class DequeDefinition(Generic[D_co]):
    name: Final[str]
    ttl: Final[Optional[Union[timedelta, int]]]
    read_uncommitted: Final[Optional[bool]]
    published: Final[Optional[bool]]
    read_cache: Final[ReadCache]
    capacity: Final[Optional[int]]
    kind: Final[str]
    payload: Final[str]

    def __init__(
        self,
        name: str,
        ttl: Optional[Union[timedelta, int]] = ...,
        read_uncommitted: Optional[bool] = ...,
        published: Optional[bool] = ...,
        read_cache: ReadCache = ...,
        capacity: Optional[int] = ...,
    ) -> None: ...
    def to_config(self) -> _StateConfig: ...


class MessageValueDefinition(Generic[D_co]):
    name: Final[str]
    ttl: Final[Optional[Union[timedelta, int]]]
    read_uncommitted: Final[Optional[bool]]
    kind: Final[str]
    payload: Final[str]

    def __init__(
        self,
        name: str,
        ttl: Optional[Union[timedelta, int]] = ...,
        read_uncommitted: Optional[bool] = ...,
    ) -> None: ...
    def to_config(self) -> _StateConfig: ...


class MessageMapDefinition(Generic[D_co]):
    name: Final[str]
    ttl: Final[Optional[Union[timedelta, int]]]
    read_uncommitted: Final[Optional[bool]]
    keyset_limit: Final[Optional[int]]
    kind: Final[str]
    payload: Final[str]

    def __init__(
        self,
        name: str,
        ttl: Optional[Union[timedelta, int]] = ...,
        read_uncommitted: Optional[bool] = ...,
        keyset_limit: Optional[int] = ...,
    ) -> None: ...
    def to_config(self) -> _StateConfig: ...


class MessageDequeDefinition(Generic[D_co]):
    name: Final[str]
    ttl: Final[Optional[Union[timedelta, int]]]
    read_uncommitted: Final[Optional[bool]]
    capacity: Final[Optional[int]]
    kind: Final[str]
    payload: Final[str]

    def __init__(
        self,
        name: str,
        ttl: Optional[Union[timedelta, int]] = ...,
        read_uncommitted: Optional[bool] = ...,
        capacity: Optional[int] = ...,
    ) -> None: ...
    def to_config(self) -> _StateConfig: ...


def value(
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = ...,
    read_uncommitted: Optional[bool] = ...,
    published: Optional[bool] = ...,
    read_cache: Optional[Union[timedelta, float, Literal[False]]] = ...,
) -> ValueDefinition[T]: ...


def map(
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = ...,
    read_uncommitted: Optional[bool] = ...,
    published: Optional[bool] = ...,
    read_cache: Optional[Union[timedelta, float, Literal[False]]] = ...,
    keyset_limit: Optional[int] = ...,
) -> MapDefinition[V]: ...


def set(
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = ...,
    read_uncommitted: Optional[bool] = ...,
    published: Optional[bool] = ...,
    read_cache: Optional[Union[timedelta, float, Literal[False]]] = ...,
    keyset_limit: Optional[int] = ...,
) -> SetDefinition: ...


def deque(
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = ...,
    read_uncommitted: Optional[bool] = ...,
    published: Optional[bool] = ...,
    read_cache: Optional[Union[timedelta, float, Literal[False]]] = ...,
    capacity: Optional[int] = ...,
) -> DequeDefinition[T]: ...


def message_value(
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = ...,
    read_uncommitted: Optional[bool] = ...,
) -> MessageValueDefinition[P]: ...


def message_map(
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = ...,
    read_uncommitted: Optional[bool] = ...,
    keyset_limit: Optional[int] = ...,
) -> MessageMapDefinition[P]: ...


def message_deque(
    name: str,
    *,
    ttl: Optional[Union[timedelta, int]] = ...,
    read_uncommitted: Optional[bool] = ...,
    capacity: Optional[int] = ...,
) -> MessageDequeDefinition[P]: ...
