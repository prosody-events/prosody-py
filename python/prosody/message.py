from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Union, TypeAlias, Dict, Generic

from typing_extensions import TypeVar

JSONValue: TypeAlias = Union[
    None,
    bool,
    int,
    float,
    str,
    List['JSONValue'],
    Dict[str, 'JSONValue']
]

# PEP 696 default: `Message` (unparameterized) is `Message[JSONValue]`.
P = TypeVar("P", default=JSONValue)


@dataclass(frozen=True)
class Message(Generic[P]):
    """
    Represents a Kafka message with associated metadata.

    This class encapsulates the core components of a Kafka message, including
    its topic, partition, offset, timestamp, key, and payload.

    The payload type is generic: ``Message[Cart]`` narrows ``payload`` to
    ``Cart`` while a bare ``Message`` keeps the JSON-serializable default.

    A message prosody delivered can be stored in a message collection, whether
    it arrived from the topic or was read back out of a collection. A message
    collection stores where a message sits in Kafka, which only a delivered
    message knows, so storing one built in Python raises
    :class:`TransientStateError`.
    """

    topic: str
    """The name of the topic."""

    partition: int
    """The partition number."""

    offset: int
    """The message offset within the partition."""

    timestamp: datetime
    """The timestamp when the message was created or sent."""

    key: str
    """The message key."""

    payload: P
    """The message payload."""

    _core: Optional[object] = field(default=None, compare=False, repr=False)
    """Internal handle to the message prosody delivered.

    Set on every message prosody hands to a handler, and only readable by the
    native layer, which needs it to store this message in a message collection.
    Not part of the public API: it is excluded from equality and ``repr``, so a
    message built in Python still compares equal to the delivered one it mirrors.
    """


@dataclass(frozen=True)
class ExciseMessage:
    """A Kafka excise record with no payload."""

    topic: str
    partition: int
    offset: int
    timestamp: datetime
    key: str
