from dataclasses import dataclass
from datetime import datetime
from typing import List, Union, TypeAlias, Dict, Generic

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
    """The message payload (JSON-serializable by default)."""
