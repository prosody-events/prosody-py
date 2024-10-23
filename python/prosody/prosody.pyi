"""
Type stubs for the Prosody Kafka client library.

This module provides type information and documentation for the Prosody library,
which offers high-performance Python bindings for Kafka message handling.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import timedelta, datetime
from typing import List, Optional, Union, TypeAlias, Dict, Literal
from typing import Type, Callable, Any, TypeVar

import tsasync

T = TypeVar('T')

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


@dataclass(frozen=True)
class Message:
    """
    Represents a Kafka message with associated metadata.

    This class encapsulates the core components of a Kafka message, including
    its topic, partition, offset, timestamp, key, and payload.
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

    payload: JSONValue
    """The message payload as a JSON-serializable value."""


class EventHandler(ABC):
    """
    Abstract base class for event handlers.

    Subclasses must implement the `on_message` method to define custom message
    processing logic.
    """

    @abstractmethod
    async def on_message(self, context: 'Context', message: Message) -> None:
        """
        Handle a Kafka message.

        Args:
            context (Context): The context of the message.
            message (Message): The Kafka message to be processed.

        Notes:
            - This method may be cancelled at any time. Implement it to respond quickly to cancellation.
            - Use `try/finally` blocks or context managers for proper resource cleanup.
            - This method may be called from different threads. Ensure that any handler state is thread-safe.

        Raises:
            asyncio.CancelledError: If the task is cancelled.
        """
        ...


class ProsodyHandler:
    """
    A wrapper class for message handlers that adds OpenTelemetry tracing and cancellation support.

    This class wraps an instance of EventHandler and adds tracing
    functionality to the message handling process. It also manages task
    cancellation during partition revocation or shutdown.

    Note:
        This class is intended for internal use within the Prosody library only.
        Users should not need to interact with this class directly in normal usage.
    """

    def __init__(self, handler: EventHandler) -> None:
        """
        Initialize a new ProsodyHandler.

        Args:
            handler (EventHandler): The message handler to be wrapped.

        Note:
            This constructor is not intended to be called directly by users of the Prosody library.
        """
        self.handler = handler
        self.tracer: Any  # OpenTelemetry tracer

    async def on_message(self, context: 'Context', message: Message, opentelemetry_context: Dict[str, str],
                         shutdown_event: tsasync.Event) -> None:
        """
        Handle a Kafka message with added tracing and cancellation support.

        This method creates a new span for the message handling process,
        calls the wrapped handler's handle method within the span,
        and ensures proper propagation of the OpenTelemetry context.
        It also manages task cancellation when shutdown is signaled.

        Args:
            context (Context): The context of the message.
            message (Message): The Kafka message to be processed.
            opentelemetry_context (Dict[str, str]): Serialized OpenTelemetry context.
            shutdown_event (tsasync.Event): Event used to signal shutdown.

        Raises:
            Exception: Any exception raised by the wrapped handler's handle method.
            asyncio.CancelledError: If the task is cancelled due to shut down or partition revocation.

        Note:
            This method is intended to be called internally by the Prosody library,
            not directly by users. It ensures that the handler task is properly
            cancelled and cleaned up during shutdown or partition revocation.
        """
        ...


class Context:
    """
    Represents the context of a Kafka message.

    This class encapsulates contextual information about a Kafka message,
    which may be useful for message handling and processing.
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
            stall_threshold: Optional[Duration] = None,
            poll_interval: Optional[Duration] = None,
            commit_interval: Optional[Duration] = None,
            mode: Optional[Literal['pipeline', 'low-latency']] = None,
            retry_base: Optional[int] = None,
            max_retries: Optional[int] = None,
            max_retry_delay: Optional[Duration] = None,
            failure_topic: Optional[str] = None,
            probe_port: Optional[int] = None,
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
            stall_threshold: Threshold determining when message processing has stalled. During partition revocation,
                tasks are given 80% of this time to finish before being cancelled. The remaining 20% is used to wait for
                the cancellation hooks to complete.
            poll_interval: Time between message polls.
            commit_interval: Time between offset commits.
            mode: Operating mode ('pipeline' or 'low-latency').
            retry_base: Initial delay for exponential backoff in retries.
            max_retries: Maximum number of retries.
            max_retry_delay: Maximum delay between retries.
            failure_topic: Topic for failed messages in low-latency mode.
            probe_port: Port for the probe server. Set to None to disable.
        Raises:
            ValueError: If the configuration is invalid.
            RuntimeError: If the client fails to initialize.
        """
        ...

    async def send(self, topic: str, key: str, payload: JSONValue) -> None:
        """
        Send a message to a specified topic.

        Args:
            topic (str): The topic to which the message should be sent.
            key (str): The key associated with the message.
            payload (JSONValue): The content of the message (must be JSON-serializable).

        Raises:
            RuntimeError: If there's an error sending the message.
        """
        ...

    def consumer_state(self) -> Literal['unconfigured', 'configured', 'running']:
        """
        Get the current state of the consumer.

        Returns:
            Literal['unconfigured', 'configured', 'running']: The current state.
        """
        ...

    def subscribe(self, handler: EventHandler) -> None:
        """
        Subscribe to messages using the provided handler.

        Args:
            handler (EventHandler): An instance implementing the EventHandler interface.

        Raises:
            RuntimeError: If the consumer is not configured or is already
                subscribed.

        Note:
            The subscribed handler should be prepared for cancellation at any time.
        """
        ...

    async def unsubscribe(self) -> None:
        """
        Unsubscribe from messages and shut down the consumer.

        This method initiates a graceful shutdown of the consumer, cancelling
        any in-flight message handling tasks. It ensures that all resources
        are properly cleaned up before returning.

        Raises:
            RuntimeError: If the consumer is not configured or not subscribed.

        Note:
            This method will wait for all tasks to complete or be cancelled
            before returning. Ensure that your message handlers respond
            promptly to cancellation to avoid delays during shutdown.
        """
        ...


class EventHandlerError(Exception, ABC):
    """
    Abstract base class for event handler errors.

    This class defines the structure for errors that can occur during event handling.
    Subclasses must implement the `is_permanent` property to indicate whether
    the error is permanent and should not be retried.
    """

    @property
    @abstractmethod
    def is_permanent(self) -> bool:
        """
        Indicates whether the error is permanent and should not be retried.

        Returns:
            bool: True if the error is permanent, False if it's transient.
        """
        ...


class TransientError(EventHandlerError):
    """
    Represents a transient error in event handling.

    Transient errors are temporary and can be retried. Messages that raise
    this error will be attempted again.
    """
    is_permanent: bool


class PermanentError(EventHandlerError):
    """
    Represents a permanent error in event handling.

    Permanent errors are not temporary and should not be retried. Messages that
    raise this error will be considered as permanent failures and will not be
    retried.
    """
    is_permanent: bool


def transient(*exceptions: Type[Exception]) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator to mark specific exceptions as transient errors.

    When applied to a function, it will catch the specified exceptions and
    raise them as TransientErrors. This indicates that the operation can be
    retried.

    Args:
        *exceptions (Type[Exception]): The exception types to be treated as transient.

    Returns:
        Callable[[Callable[..., T]], Callable[..., T]]: A decorator function.

    Note:
        Messages that raise TransientErrors will be retried.
    """
    ...


def permanent(*exceptions: Type[Exception]) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator to mark specific exceptions as permanent errors.

    When applied to a function, it will catch the specified exceptions and
    raise them as PermanentErrors. This indicates that the operation should not
    be retried.

    Args:
        *exceptions (Type[Exception]): The exception types to be treated as permanent.

    Returns:
        Callable[[Callable[..., T]], Callable[..., T]]: A decorator function.

    Note:
        Messages that raise PermanentErrors will not be retried and will be
        considered as permanent failures.
    """
    ...
