from prosody.context import Context as Context
from prosody.demand import Demand as Demand, DemandKind as DemandKind
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
from prosody.handler import EventHandler as EventHandler
from prosody.message import ExciseMessage as ExciseMessage, Message as Message
from types import TracebackType
from typing import Optional, TypeVar, overload

from typing_extensions import Self

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
    SetDefinition as SetDefinition,
    SetState as SetState,
    StoreOutcome as StoreOutcome,
    ValueDefinition as ValueDefinition,
    ValueState as ValueState,
    PublishedValue as PublishedValue,
    PublishedMap as PublishedMap,
    PublishedSet as PublishedSet,
    PublishedDeque as PublishedDeque,
    deque as deque,
    map as map,
    set as set,
    message_deque as message_deque,
    message_map as message_map,
    message_value as message_value,
    value as value,
)
from prosody.timer import Timer as Timer

T = TypeVar("T")
V = TypeVar("V")

class ProsodyClient(_ProsodyClientApi):
    """A Kafka client. Create one with ``await ProsodyClient.create(...)``.

    Use it as an async context manager to shut it down when the block exits.
    """

    def __init__(self) -> None: ...
    async def __aenter__(self) -> Self: ...
    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        """Call :meth:`shutdown`."""
        ...

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
        definition: SetDefinition,
    ) -> PublishedSet: ...
    @overload
    async def state(
        self,
        subsystem: str,
        definition: DequeDefinition[T],
    ) -> PublishedDeque[T]: ...
