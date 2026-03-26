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
            idempotence_version: Optional[str] = None,
            idempotence_ttl: Optional[Duration] = None,
            subscribed_topics: Optional[StringOrList] = None,
            allowed_events: Optional[StringOrList] = None,
            max_concurrency: Optional[int] = None,
            max_uncommitted: Optional[int] = None,
            stall_threshold: Optional[Duration] = None,
            shutdown_timeout: Optional[Duration] = None,
            poll_interval: Optional[Duration] = None,
            commit_interval: Optional[Duration] = None,
            mode: Optional[Literal['pipeline', 'low-latency', 'best-effort']] = None,
            retry_base: Optional[Duration] = None,
            max_retries: Optional[int] = None,
            max_retry_delay: Optional[Duration] = None,
            failure_topic: Optional[str] = None,
            probe_port: Optional[int] = None,
            slab_size: Optional[Duration] = None,
            cassandra_nodes: Optional[StringOrList] = None,
            cassandra_keyspace: Optional[str] = None,
            cassandra_datacenter: Optional[str] = None,
            cassandra_rack: Optional[str] = None,
            cassandra_user: Optional[str] = None,
            cassandra_password: Optional[str] = None,
            cassandra_retention: Optional[Duration] = None,
            # Scheduler configuration
            scheduler_failure_weight: Optional[float] = None,
            scheduler_max_wait: Optional[Duration] = None,
            scheduler_wait_weight: Optional[float] = None,
            scheduler_cache_size: Optional[int] = None,
            # Monopolization configuration
            monopolization_enabled: Optional[bool] = None,
            monopolization_threshold: Optional[float] = None,
            monopolization_window: Optional[Duration] = None,
            monopolization_cache_size: Optional[int] = None,
            # Defer configuration
            defer_enabled: Optional[bool] = None,
            defer_base: Optional[Duration] = None,
            defer_max_delay: Optional[Duration] = None,
            defer_failure_threshold: Optional[float] = None,
            defer_failure_window: Optional[Duration] = None,
            defer_cache_size: Optional[int] = None,
            defer_seek_timeout: Optional[Duration] = None,
            defer_discard_threshold: Optional[int] = None,
            # Timeout configuration
            timeout: Optional[Duration] = None,
            # Telemetry emitter configuration
            telemetry_topic: Optional[str] = None,
            telemetry_enabled: Optional[bool] = None,
    ) -> None:
        """
        Initialize a new ProsodyClient.

        Args:
            bootstrap_servers: Kafka servers for initial connection.
            mock: Use mock client for testing if True.
            source_system: Identifier for the producing system to prevent loops. Defaults to the group_id if unspecified.
            send_timeout: Timeout for message send operations.
            group_id: Consumer group name.
            idempotence_cache_size: Global shared cache capacity across all partitions. Set to 0 to disable deduplication entirely. Default: 8192.
            idempotence_version: Version string for cache-busting deduplication hashes. Changing this invalidates all previously recorded entries. Default: "1".
            idempotence_ttl: TTL for deduplication records in Cassandra. Default: 7 days.
            subscribed_topics: Topics to subscribe to.
            allowed_events: Allowed event type prefixes. All are allowed if unset.
            max_concurrency: Maximum global concurrency limit.
            max_uncommitted: Max number of uncommitted messages.
            stall_threshold: Threshold determining when message processing has stalled.
            shutdown_timeout: Shutdown budget; handlers complete freely before cancellation fires near the deadline.
            poll_interval: Time between message polls.
            commit_interval: Time between offset commits.
            mode: Operating mode ('pipeline', 'low-latency', or 'best-effort').
            retry_base: Initial delay for exponential backoff in retries.
            max_retries: Maximum number of retries.
            max_retry_delay: Maximum delay between retries.
            failure_topic: Topic for failed messages in low-latency mode.
            probe_port: Port for the probe server. Set to None to disable.
            slab_size: Timer slab partitioning duration. Controls how timers are grouped.
            cassandra_nodes: List of Cassandra contact nodes (hostnames or IPs with optional ports).
            cassandra_keyspace: Keyspace to use for storing timer data. Defaults to 'prosody'.
            cassandra_datacenter: Preferred datacenter for query routing and load balancing.
            cassandra_rack: Preferred rack identifier for topology-aware routing.
            cassandra_user: Username for authenticating with Cassandra cluster.
            cassandra_password: Password for authenticating with Cassandra cluster.
            cassandra_retention: Retention period for failed/unprocessed timer data. Defaults to 30 days.
            scheduler_failure_weight: Target proportion of execution time for failure/retry task processing (0.0 to 1.0).
            scheduler_max_wait: Wait duration at which urgency boost reaches maximum intensity.
            scheduler_wait_weight: Maximum urgency boost (in seconds of virtual time) for waiting tasks.
            scheduler_cache_size: Cache capacity for tracking per-key virtual time in the scheduler.
            monopolization_enabled: Whether monopolization detection is enabled.
            monopolization_threshold: Threshold for monopolization detection (0.0 to 1.0).
            monopolization_window: Rolling window duration for monopolization detection.
            monopolization_cache_size: Cache size for tracking key execution intervals.
            defer_enabled: Whether deferral is enabled for transient failures.
            defer_base: Base exponential backoff delay for deferred retries.
            defer_max_delay: Maximum delay between deferred retries.
            defer_failure_threshold: Failure rate threshold for enabling deferral (0.0 to 1.0).
            defer_failure_window: Sliding window duration for failure rate tracking.
            defer_cache_size: Cache size for defer middleware.
            defer_seek_timeout: Timeout for Kafka seek operations.
            defer_discard_threshold: Messages to read sequentially before seeking.
            timeout: Fixed timeout duration for handler execution. Defaults to 80% of stall threshold.
            telemetry_topic: Kafka topic to produce internal telemetry events to. Defaults to 'prosody.telemetry-events'.
            telemetry_enabled: Whether the telemetry emitter is enabled. Defaults to True.
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

    @property
    def source_system(self) -> str:
        """
        Gets the source system identifier configured for the client.

        The source system identifier is used to identify the originating service
        or component in produced messages, enabling loop detection and message
        attribution.

        Returns:
            str: The source system identifier.
        """
        ...


class AdminClient:
    """
    A client for performing administrative operations on Kafka topics.

    This class provides methods for creating and deleting Kafka topics with
    configurable parameters and settings.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: Optional[StringOrList] = None,
    ) -> None:
        """
        Initialize a new AdminClient.

        Args:
            bootstrap_servers: Kafka servers for initial connection.

        Raises:
            RuntimeError: If the client fails to initialize.
            ValueError: If the configuration is invalid.

        Examples:
            # Single server
            admin = AdminClient(bootstrap_servers="localhost:9094")

            # Multiple servers
            admin = AdminClient(bootstrap_servers=["localhost:9092", "localhost:9093"])

            # Environment variable support (PROSODY_BOOTSTRAP_SERVERS)
            admin = AdminClient()
        """
        ...

    async def create_topic(
        self,
        name: str,
        *,
        partition_count: Optional[int] = None,
        replication_factor: Optional[int] = None,
        cleanup_policy: Optional[str] = None,
        retention: Optional[Duration] = None,
    ) -> None:
        """
        Create a new Kafka topic with the specified configuration.

        Args:
            name: The name of the topic to create.
            partition_count: Number of partitions for the topic. Uses broker default if not specified.
            replication_factor: Replication factor for the topic. Uses broker default if not specified.
            cleanup_policy: Cleanup policy ("delete", "compact", "delete,compact"). Uses cluster default if not specified.
            retention: Message retention time. Can be a timedelta object, float seconds, or duration string.
                      Uses cluster default if not specified.

        Raises:
            RuntimeError: If the topic creation fails.
            ValueError: If the configuration parameters are invalid.

        Examples:
            # Basic topic creation
            await admin.create_topic("my-topic")

            # Topic with specific configuration
            await admin.create_topic(
                "my-topic",
                partition_count=4,
                replication_factor=2,
                cleanup_policy="delete",
                retention=timedelta(days=7)
            )

            # Topic with retention as float seconds
            await admin.create_topic(
                "my-topic",
                retention=604800.0  # 7 days in seconds
            )
        """
        ...

    async def delete_topic(self, name: str) -> None:
        """
        Delete an existing Kafka topic.

        Args:
            name: The name of the topic to delete.

        Raises:
            RuntimeError: If the topic deletion fails.

        Example:
            await admin.delete_topic("my-topic")
        """
        ...
