from abc import ABC, abstractmethod
from typing import Generic

from typing_extensions import TypeVar

from prosody.context import Context
from prosody.message import ExciseMessage, JSONValue, Message
from prosody.timer import Timer

P = TypeVar("P", default=JSONValue)
Response = TypeVar("Response", default=JSONValue)

class EventHandler(ABC, Generic[P, Response]):
    @abstractmethod
    async def on_message(self, context: Context, message: Message[P]) -> Response: ...
    @abstractmethod
    async def on_excise(self, context: Context, message: ExciseMessage) -> Response: ...
    @abstractmethod
    async def on_timer(self, context: Context, timer: Timer) -> None: ...
