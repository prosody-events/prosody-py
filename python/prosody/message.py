from dataclasses import dataclass, field
from datetime import datetime
from collections.abc import Mapping, Sequence
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

# A value that the client can write as JSON. Mapping and Sequence accept a
# TypedDict, a ``dict[str, str]``, or a tuple. The client returns JSONValue.
JSONInput: TypeAlias = Union[
    None,
    bool,
    int,
    float,
    str,
    Sequence['JSONInput'],
    Mapping[str, object],
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

    source_system: Optional[str] = field(default=None, compare=False)
    """The system that produced the message, or ``None`` when the record has no
    source system header."""

    response_requested: bool = field(default=False, compare=False)
    """``True`` when a request expects a response from this handler.

    For an ordinary event, Prosody discards the handler result. Check this flag
    to skip the work of building a response that nobody reads.
    """

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

    source_system: Optional[str] = field(default=None, compare=False)
    """The system that produced the record, or ``None`` when the record has no
    source system header."""

    response_requested: bool = field(default=False, compare=False)
    """``True`` when an excise request expects a response from this handler."""
