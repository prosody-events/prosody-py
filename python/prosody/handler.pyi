from typing import Any

from prosody.context import Context
from prosody.message import Message
from prosody.timer import Timer

class EventHandler:
    async def on_message(self, context: Context, message: Message) -> None: ...
    async def on_timer(self, context: Context, timer: Timer) -> None: ...

class ProsodyHandler:
    handler: EventHandler

    def __init__(self, handler: EventHandler) -> None: ...
    async def on_message(
        self,
        context: Context,
        message: Message,
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
