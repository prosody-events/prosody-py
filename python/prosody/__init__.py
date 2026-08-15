import logging

from prosody.prosody import (
    _NativeProsodyClient,
    AdminClient,
    flush_telemetry,
    shutdown_telemetry,
)
from prosody.request import (
    Failure,
    FormatMismatch,
    HandlerError,
    MalformedResponse,
    Outcome,
    ResponseError,
    Success,
    Timeout,
)

from prosody.context import Context
from prosody.errors import (
    EventHandlerError,
    PermanentError,
    TransientError,
    permanent,
    transient,
    StateError,
    PermanentStateError,
    TransientStateError,
    NullValueError,
)
from prosody.handler import EventHandler, ProsodyHandler
from prosody.message import Message
from prosody.state import (
    Direction,
    value,
    map,
    deque,
    message_value,
    message_map,
    message_deque,
    ValueDefinition,
    MapDefinition,
    DequeDefinition,
    MessageValueDefinition,
    MessageMapDefinition,
    MessageDequeDefinition,
    ValueState,
    MapState,
    DequeState,
    PublishedValue,
    PublishedMap,
    PublishedDeque,
)
from prosody.timer import Timer


class ProsodyClient:
    """Prosody client with typed published-state composition."""

    def __init__(self):
        raise TypeError("Use await ProsodyClient.create(**configuration)")

    @classmethod
    def create(cls, **configuration):
        """Create a client without blocking the Python event loop."""
        async def finish():
            client = object.__new__(cls)
            client._native = await _NativeProsodyClient.create(**configuration)
            return client

        return finish()

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_native"), name)

    async def state(self, subsystem, definition):
        """Open a read-only view of a published JSON collection."""
        if isinstance(definition, ValueDefinition):
            return PublishedValue(
                await self._published_value(
                    subsystem, definition.name, read_cache=definition.read_cache
                )
            )
        if isinstance(definition, MapDefinition):
            return PublishedMap(
                await self._published_map(
                    subsystem, definition.name, read_cache=definition.read_cache
                )
            )
        if isinstance(definition, DequeDefinition):
            return PublishedDeque(
                await self._published_deque(
                    subsystem, definition.name, read_cache=definition.read_cache
                )
            )
        raise TypeError(
            "definition must be a JSON ValueDefinition, MapDefinition, or "
            "DequeDefinition"
        )

logging.getLogger('prosody.consumer.poll').setLevel(logging.ERROR)
