from abc import ABC, abstractmethod
from typing import Generic

from typing_extensions import TypeVar

from prosody.context import Context
from prosody.message import ExciseMessage, JSONValue, Message
from prosody.timer import Timer

P = TypeVar("P", default=JSONValue)
Response = TypeVar("Response", default=JSONValue)

class EventHandler(ABC, Generic[P, Response]):
    """Base class for a handler. Implement all three methods."""

    @abstractmethod
    async def on_message(self, context: Context, message: Message[P]) -> Response:
        """Handle a Kafka message and return the response for a request."""
        ...
    @abstractmethod
    async def on_excise(self, context: Context, message: ExciseMessage) -> Response:
        """Handle an excise record and return the response for a request."""
        ...
    @abstractmethod
    async def on_timer(self, context: Context, timer: Timer) -> None:
        """Handle a timer that fired for the current key."""
        ...
