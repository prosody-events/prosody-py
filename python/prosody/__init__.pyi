from prosody.context import Context as Context
from prosody.errors import (
    EventHandlerError as EventHandlerError,
    NullValueError as NullValueError,
    PermanentError as PermanentError,
    PermanentStateError as PermanentStateError,
    StateError as StateError,
    TransientError as TransientError,
    TransientStateError as TransientStateError,
    permanent as permanent,
    transient as transient,
)
from prosody.handler import EventHandler as EventHandler, ProsodyHandler as ProsodyHandler
from prosody.message import JSONValue as _JSONValue, Message as Message
from datetime import timedelta
from typing import Mapping, Sequence, TypeVar, overload

from prosody.prosody import (
    AdminClient as AdminClient,
    _NativeProsodyClient,
    flush_telemetry as flush_telemetry,
    shutdown_telemetry as shutdown_telemetry,
)
from prosody.request import (
    Err as Err,
    HandlerResponseError as HandlerResponseError,
    MalformedResponseError as MalformedResponseError,
    Ok as Ok,
    RequestResult as RequestResult,
    ResponseError as ResponseError,
    ResponseFormatMismatchError as ResponseFormatMismatchError,
    ResponseTimeoutError as ResponseTimeoutError,
)
from prosody.state import (
    DequeDefinition as DequeDefinition,
    DequeState as DequeState,
    Direction as Direction,
    MapDefinition as MapDefinition,
    MapState as MapState,
    MessageDequeDefinition as MessageDequeDefinition,
    MessageMapDefinition as MessageMapDefinition,
    MessageValueDefinition as MessageValueDefinition,
    ValueDefinition as ValueDefinition,
    ValueState as ValueState,
    PublishedValue as PublishedValue,
    PublishedMap as PublishedMap,
    PublishedDeque as PublishedDeque,
    deque as deque,
    map as map,
    message_deque as message_deque,
    message_map as message_map,
    message_value as message_value,
    value as value,
)
from prosody.timer import Timer as Timer

T = TypeVar("T")
V = TypeVar("V")

class ProsodyClient(_NativeProsodyClient):
    async def request(
        self,
        topic: str,
        key: str,
        payload: _JSONValue,
        subsystems: Sequence[str],
        timeout: float | timedelta,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> Sequence[RequestResult[_JSONValue]]: ...

    @overload
    async def state(
        self,
        subsystem: str,
        definition: ValueDefinition[T],
    ) -> PublishedValue[T]: ...
    @overload
    async def state(
        self,
        subsystem: str,
        definition: MapDefinition[V],
    ) -> PublishedMap[V]: ...
    @overload
    async def state(
        self,
        subsystem: str,
        definition: DequeDefinition[T],
    ) -> PublishedDeque[T]: ...
