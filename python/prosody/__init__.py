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
from prosody.demand import Demand, DemandKind
from prosody.errors import (
    EventHandlerError,
    PermanentError,
    TransientError,
    permanent,
    transient,
    StateError,
    PermanentStateError,
    TransientStateError,
)
from prosody.handler import EventHandler
from prosody.message import ExciseMessage, Message
from prosody.state import (
    Direction,
    value,
    map,
    set,
    deque,
    message_value,
    message_map,
    message_deque,
    ValueDefinition,
    MapDefinition,
    SetDefinition,
    DequeDefinition,
    MessageValueDefinition,
    MessageMapDefinition,
    MessageDequeDefinition,
    ValueState,
    MapState,
    SetState,
    DequeState,
    StoreOutcome,
    PublishedValue,
    PublishedMap,
    PublishedSet,
    PublishedDeque,
)
from prosody.timer import Timer


class ProsodyClient:
    """A Kafka client. Create one with ``await ProsodyClient.create(...)``.

    Use it as an async context manager to shut it down when the block exits.
    """

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

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        """Call :meth:`shutdown`."""
        await self.shutdown()

    async def state(self, subsystem, definition):
        """Open a read-only view of a collection that ``subsystem`` publishes.

        Pass the JSON value, map, set, or deque definition that the owner
        registered with ``published=True``. The reader uses the definition's
        ``read_cache``.

        Raises:
            PermanentStateError: If the reader rejects the definition, such as
                for a zero ``read_cache``.
            TransientStateError: If the reader fails to open for a reason that
                a retry can fix.
            RuntimeError: If the client cannot build the reader for a different
                reason, such as an empty subsystem name.
            TypeError: If ``definition`` is a message collection definition.
        """
        reader = next((r for cls, r in _READERS.items() if isinstance(definition, cls)), None)
        if reader is None:
            raise TypeError(
                "definition must be a JSON ValueDefinition, MapDefinition, "
                "SetDefinition, or DequeDefinition"
            )
        native = await self._published(
            subsystem, definition.kind, definition.name, read_cache=definition.read_cache
        )
        return reader(native)


_READERS = {
    ValueDefinition: PublishedValue,
    MapDefinition: PublishedMap,
    SetDefinition: PublishedSet,
    DequeDefinition: PublishedDeque,
}

logging.getLogger('prosody.consumer.poll').setLevel(logging.ERROR)
