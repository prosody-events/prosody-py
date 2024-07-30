"""
Type stubs for the Prosody Kafka client library.

This module provides type information and documentation for the Prosody library,
which offers high-performance Python bindings for Kafka message handling.
"""

from abc import ABC, abstractmethod
from datetime import timedelta, datetime
from typing import Any, List, Optional, Union, TypeAlias, Dict

# Define a JSONValue type that represents all possible JSON-serializable values
JSONValue: TypeAlias = Union[
    None,
    bool,
    int,
    float,
    str,
    List['JSONValue'],
    Dict[str, 'JSONValue']
]

# Define a Duration type alias for time-related parameters
Duration: TypeAlias = Union[float, timedelta]

# Define a StringOrList type alias for parameters that accept either a string or a list of strings
StringOrList: TypeAlias = Union[str, List[str]]


class Context:
    """
    Represents the context of a Kafka message.

    This class encapsulates contextual information about a Kafka message,
    which may be useful for message handling and processing.
    """
    ...


class Message:
    """
    Represents a Kafka message with associated metadata.

    This class encapsulates the core components of a Kafka message, including
    its topic, partition, offset, timestamp, key, and payload.
    """

    def topic(self) -> str:
        """
        Get the topic of the message.

        Returns:
            str: The name of the topic.
        """
        ...

    def partition(self) -> int:
        """
        Get the partition of the message.

        Returns:
            int: The partition number.
        """
        ...

    def offset(self) -> int:
        """
        Get the offset of the message.

        Returns:
            int: The message offset within the partition.
        """
        ...

    def timestamp(self) -> datetime:
        """
        Get the timestamp of the message.

        Returns:
            datetime: The timestamp when the message was created or sent.
        """
        ...

    def key(self) -> str:
        """
        Get the key of the message.

        Returns:
            str: The message key.
        """
        ...

    def payload(self) -> JSONValue:
        """
        Get the payload of the message.

        Returns:
            JSONValue: The message payload as a JSON-serializable value.
        """
        ...

    def __str__(self) -> str:
        """
        Get a string representation of the Message.

        Returns:
            str: A human-readable representation of the Message.
        """
        ...

    def __repr__(self) -> str:
        """
        Get a detailed string representation of the Message.

        Returns:
            str: A detailed representation of the Message, suitable for debugging.
        """
        ...


class AbstractMessageHandler(ABC):
    """
    Abstract base class for message handlers.

    Subclasses must implement the `handle` method to define custom message
    processing logic.
    """

    @abstractmethod
    async def handle(self, context: Context, message: Message) -> None:
        """
        Handle a Kafka message.

        Args:
            context (Context): The context of the message.
            message (Message): The Kafka message to be processed.

        Returns:
            None
        """
        ...


class ProsodyClient:
    """
    A client for interacting with Kafka using the Prosody library.

    This class provides methods for sending messages to Kafka topics and
    subscribing to topics for message consumption.
    """

    def __init__(
            self,
            *,
            bootstrap_servers: Optional[StringOrList] = None,
            mock: Optional[bool] = None,
            send_timeout: Optional[Duration] = None,
            group_id: Optional[str] = None,
            subscribed_topics: Optional[StringOrList] = None,
            max_uncommitted: Optional[int] = None,
            max_enqueued_per_key: Optional[int] = None,
            partition_shutdown_timeout: Optional[Duration] = None,
            poll_interval: Optional[Duration] = None,
            commit_interval: Optional[Duration] = None,
            mode: Optional[str] = None,
            retry_base: Optional[int] = None,
            max_retries: Optional[int] = None,
            max_retry_delay: Optional[Duration] = None,
            failure_topic: Optional[str] = None
    ) -> None:
        """
        Initialize a new ProsodyClient.

        Args:
            bootstrap_servers: Kafka servers for initial connection.
            mock: Use mock client for testing if True.
            send_timeout: Timeout for message send operations.
            group_id: Consumer group name.
            subscribed_topics: Topics to subscribe to.
            max_uncommitted: Max number of uncommitted messages.
            max_enqueued_per_key: Max enqueued messages per key.
            partition_shutdown_timeout: Timeout for partition shutdown.
            poll_interval: Time between message polls.
            commit_interval: Time between offset commits.
            mode: Operating mode ('pipeline' or 'low-latency').
            retry_base: Base for exponential backoff in retries.
            max_retries: Maximum number of retries.
            max_retry_delay: Maximum delay between retries.
            failure_topic: Topic for failed messages in low-latency mode.

        Raises:
            ValueError: If the configuration is invalid.
            RuntimeError: If the client fails to initialize.
        """
        ...

    async def send(self, topic: str, key: str, payload: Any) -> None:
        """
        Send a message to a specified topic.

        Args:
            topic (str): The topic to which the message should be sent.
            key (str): The key associated with the message.
            payload (Any): The content of the message (must be JSON-serializable).

        Raises:
            RuntimeError: If there's an error sending the message.
        """
        ...

    def consumer_state(self) -> str:
        """
        Get the current state of the consumer.

        Returns:
            str: The current state ('unconfigured', 'configured', or 'running').
        """
        ...

    def subscribe(self, handler: AbstractMessageHandler) -> None:
        """
        Subscribe to messages using the provided handler.

        Args:
            handler (AbstractMessageHandler): An instance implementing the
                AbstractMessageHandler interface.

        Raises:
            RuntimeError: If the consumer is not configured or is already
                subscribed.
        """
        ...

    async def unsubscribe(self) -> None:
        """
        Unsubscribe from messages and shut down the consumer.

        Raises:
            RuntimeError: If the consumer is not configured or not subscribed.
        """
        ...

    def __repr__(self) -> str:
        """
        Get a string representation of the ProsodyClient.

        Returns:
            str: A string representation of the ProsodyClient.
        """
        ...

    def __str__(self) -> str:
        """
        Get a human-readable string description of the ProsodyClient.

        Returns:
            str: A human-readable description of the ProsodyClient.
        """
        ...
