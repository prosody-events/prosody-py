from collections.abc import Awaitable, Mapping
from typing import Generic, Protocol

from typing_extensions import TypeVar

from prosody.context import Context
from prosody.message import ExciseMessage, JSONValue, Message
from prosody.timer import Timer

P = TypeVar("P", default=JSONValue)

class _ShutdownEvent(Protocol):
    def wait(self) -> Awaitable[None]: ...

class EventHandler(Generic[P]):
    async def on_message(self, context: Context, message: Message[P]) -> JSONValue: ...
    async def on_excise(self, context: Context, message: ExciseMessage) -> JSONValue: ...
    async def on_timer(self, context: Context, timer: Timer) -> None: ...

class ProsodyHandler(Generic[P]):
    handler: EventHandler[P]

    def __init__(self, handler: EventHandler[P]) -> None: ...
    async def on_message(
        self,
        context: Context,
        message: Message[P],
        opentelemetry_context: Mapping[str, str],
        shutdown_event: _ShutdownEvent,
    ) -> JSONValue: ...
    async def on_excise(
        self,
        context: Context,
        message: ExciseMessage,
        opentelemetry_context: Mapping[str, str],
        shutdown_event: _ShutdownEvent,
    ) -> JSONValue: ...
    async def on_timer(
        self,
        context: Context,
        timer: Timer,
        opentelemetry_context: Mapping[str, str],
        shutdown_event: _ShutdownEvent,
    ) -> None: ...
