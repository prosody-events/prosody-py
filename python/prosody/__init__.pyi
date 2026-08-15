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
from prosody.message import Message as Message
from typing import TypeVar, overload

from prosody.prosody import (
    AdminClient as AdminClient,
    _ProsodyClientApi,
    flush_telemetry as flush_telemetry,
    shutdown_telemetry as shutdown_telemetry,
)
from prosody.request import (
    Failure as Failure,
    FormatMismatch as FormatMismatch,
    HandlerError as HandlerError,
    MalformedResponse as MalformedResponse,
    Outcome as Outcome,
    ResponseError as ResponseError,
    Success as Success,
    Timeout as Timeout,
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

class ProsodyClient(_ProsodyClientApi):
    def __init__(self) -> None: ...

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
