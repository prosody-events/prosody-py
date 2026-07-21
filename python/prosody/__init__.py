import logging

from prosody.prosody import ProsodyClient, AdminClient

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
)
from prosody.timer import Timer

logging.getLogger('prosody.consumer.poll').setLevel(logging.ERROR)
