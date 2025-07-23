"""
Type stubs for the Prosody Kafka client library.

This module provides type information and documentation for the Prosody library,
which offers high-performance Python bindings for Kafka message handling.
"""
from datetime import timedelta
from typing import List, Optional, Union, TypeAlias, Dict, Literal
from typing import TypeVar

from prosody import EventHandler

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
            source_system: Optional[str] = None,
            send_timeout: Optional[Duration] = None,
            group_id: Optional[str] = None,
            idempotence_cache_size: Optional[int] = None,
            subscribed_topics: Optional[StringOrList] = None,
            allowed_events: Optional[StringOrList] = None,
            max_concurrency: Optional[int] = None,
            max_uncommitted: Optional[int] = None,
            max_enqueued_per_key: Optional[int] = None,
            stall_threshold: Optional[Duration] = None,
            shutdown_timeout: Optional[Duration] = None,
            poll_interval: Optional[Duration] = None,
            commit_interval: Optional[Duration] = None,
            mode: Optional[Literal['pipeline', 'low-latency', 'best-effort']] = None,
            retry_base: Optional[int] = None,
            max_retries: Optional[int] = None,
            max_retry_delay: Optional[Duration] = None,
            failure_topic: Optional[str] = None,
            probe_port: Optional[int] = None,
            cassandra_nodes: Optional[StringOrList] = None,
            cassandra_keyspace: Optional[str] = None,
            cassandra_datacenter: Optional[str] = None,
            cassandra_rack: Optional[str] = None,
            cassandra_user: Optional[str] = None,
            cassandra_password: Optional[str] = None,
            cassandra_retention: Optional[Duration] = None,
    ) -> None:
        """
        Initialize a new ProsodyClient.

        Args:
            bootstrap_servers: Kafka servers for initial connection.
            mock: Use mock client for testing if True.
            source_system: Identifier for the producing system to prevent loops. Defaults to the group_id if unspecified.
            send_timeout: Timeout for message send operations.
            group_id: Consumer group name.
            idempotence_cache_size: Size of LRU caches for deduplicating messages. Set to 0 to disable.
            subscribed_topics: Topics to subscribe to.
            allowed_events: Allowed event type prefixes. All are allowed if unset.
            max_concurrency: Maximum global concurrency limit.
            max_uncommitted: Max number of uncommitted messages.
            max_enqueued_per_key: Max enqueued messages per key.
            stall_threshold: Threshold determining when message processing has stalled.
            shutdown_timeout: Timeout to wait for in-flight tasks to complete during partition shutdown.
            poll_interval: Time between message polls.
            commit_interval: Time between offset commits.
            mode: Operating mode ('pipeline', 'low-latency', or 'best-effort').
            retry_base: Initial delay for exponential backoff in retries.
            max_retries: Maximum number of retries.
            max_retry_delay: Maximum delay between retries.
            failure_topic: Topic for failed messages in low-latency mode.
            probe_port: Port for the probe server. Set to None to disable.
            cassandra_nodes: List of Cassandra contact nodes (hostnames or IPs with optional ports).
            cassandra_keyspace: Keyspace to use for storing timer data. Defaults to 'prosody'.
            cassandra_datacenter: Preferred datacenter for query routing and load balancing.
            cassandra_rack: Preferred rack identifier for topology-aware routing.
            cassandra_user: Username for authenticating with Cassandra cluster.
            cassandra_password: Password for authenticating with Cassandra cluster.
            cassandra_retention: Retention period for failed/unprocessed timer data. Defaults to 30 days.
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

    async def consumer_state(self) -> Literal['unconfigured', 'configured', 'running']:
        """
        Get the current state of the consumer.

        Returns:
            Literal['unconfigured', 'configured', 'running']: The current state.
        """
        ...

    async def subscribe(self, handler: EventHandler) -> None:
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

    async def assigned_partition_count(self) -> int:
        """
        Returns the number of partitions assigned to the consumer.

        Returns:
            int: The number of assigned partitions. Returns 0 if the consumer
            is not in the Running state.
        """
        ...

    async def is_stalled(self) -> bool:
        """
        Checks if the consumer is stalled.

        Returns:
            bool: True if the consumer is stalled, False otherwise. Returns
            False if the consumer is not in the Running state.
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
