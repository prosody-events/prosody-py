from typing import Any, Generic

from typing_extensions import TypeVar

from prosody.context import Context
from prosody.message import JSONValue, Message
from prosody.timer import Timer

P = TypeVar("P", default=JSONValue)

class EventHandler(Generic[P]):
    async def on_message(self, context: Context, message: Message[P]) -> None: ...
    async def on_timer(self, context: Context, timer: Timer) -> None: ...

class ProsodyHandler:
    handler: EventHandler[Any]

    def __init__(self, handler: EventHandler[Any]) -> None: ...
    async def on_message(
        self,
        context: Context,
        message: Message[Any],
        opentelemetry_context: Any,
        shutdown_event: Any,
    ) -> None: ...
    async def on_timer(
        self,
        context: Context,
        timer: Timer,
        opentelemetry_context: Any,
        shutdown_event: Any,
    ) -> None: ...
