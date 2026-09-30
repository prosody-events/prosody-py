"""
Type stubs for the Prosody Kafka client library.

This module provides type information and documentation for the Prosody library,
which offers high-performance Python bindings for Kafka message handling.
"""
from datetime import timedelta
from typing import AsyncIterator, Dict, List, Literal, Mapping, Optional, Sequence, TypeAlias, TypeVar, Union
from typing_extensions import Self

from prosody import EventHandler
from prosody._readers import (
    NativeJsonDequeScan as NativeJsonDequeScan,
    NativeJsonMapScan as NativeJsonMapScan,
    NativeMapKeyScan as NativeMapKeyScan,
    NativeMessageDequeScan as NativeMessageDequeScan,
    NativeMessageMapScan as NativeMessageMapScan,
    _NativePublishedDeque as _NativePublishedDeque,
    _NativePublishedMap as _NativePublishedMap,
    _NativePublishedSet as _NativePublishedSet,
    _NativePublishedValue as _NativePublishedValue,
)
from prosody.message import JSONInput, JSONValue
from prosody.request import Outcome
from prosody.state import (
    DequeDefinition,
    MapDefinition,
    SetDefinition,
    MessageDequeDefinition,
    MessageMapDefinition,
    MessageValueDefinition,
    ValueDefinition,
)

P = TypeVar("P")
R = TypeVar("R")
T = TypeVar("T")
V = TypeVar("V")

# Every keyed-state collection definition accepted by ``state_collections``.
StateDefinition: TypeAlias = Union[
    ValueDefinition[object],
    MapDefinition[object],
    SetDefinition,
    DequeDefinition[object],
    MessageValueDefinition[object],
    MessageMapDefinition[object],
    MessageDequeDefinition[object],
]


# Define a Duration type alias for time-related parameters
Duration: TypeAlias = Union[float, timedelta]

# Define a StringOrList type alias for parameters that accept either a string or a list of strings
StringOrList: TypeAlias = Union[str, List[str]]

def flush_telemetry() -> None:
    """Exports buffered telemetry without shutting down the pipeline."""
    ...

def shutdown_telemetry() -> None:
    """Exports buffered telemetry and shuts down the process-global pipeline."""
    ...


class _ProsodyClientApi:
    """
    A client for interacting with Kafka using the Prosody library.

    This class provides methods for sending messages to Kafka topics and
    subscribing to topics for message consumption.
    """

    @classmethod
    async def create(
            cls,
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
            statistics_interval: Optional[Duration] = None,
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
            defer_store_cache_size: Optional[int] = None,
            # Kafka message loader configuration
            loader_cache_size: Optional[int] = None,
            loader_seek_timeout: Optional[Duration] = None,
            loader_discard_threshold: Optional[int] = None,
            # Timeout configuration
            timeout: Optional[Duration] = None,
            # Telemetry emitter configuration
            telemetry_topic: Optional[str] = None,
            telemetry_enabled: Optional[bool] = None,
            # OTel span linking
            message_spans: Optional[Literal['child', 'follows_from']] = None,
            timer_spans: Optional[Literal['child', 'follows_from']] = None,
            # Keyed state configuration
            state_collections: Optional[Sequence[StateDefinition]] = None,
            state_cache_dir: Optional[str] = None,
            state_owned_cache_size: Optional[str] = None,
            state_memtable_size: Optional[str] = None,
            state_read_cache_size: Optional[str] = None,
            state_read_cache: Optional[Union[Duration, Literal[False]]] = None,
            subsystem: Optional[str] = None,
            peer_bind_address: Optional[str] = None,
            peer_advertised_connect: Optional[str] = None,
            peer_network_name: Optional[str] = None,
            peer_cache_capacity: Optional[int] = None,
            peer_registration_ttl: Optional[Duration] = None,
    ) -> Self: ...

    async def send(self, topic: str, key: str, payload: JSONInput) -> None:
        """
        Send a message to a specified topic.

        Args:
            topic (str): The topic to which the message should be sent.
            key (str): The key associated with the message.
            payload (JSONInput): The content of the message (must be JSON-serializable).

        Raises:
            RuntimeError: If there's an error sending the message.
        """
        ...

    async def excise(self, topic: str, key: str) -> None:
        """Send an excise record for a key."""
        ...

    async def request(
        self,
        topic: str,
        key: str,
        payload: JSONInput,
        *,
        subsystems: Sequence[str],
        timeout: Duration,
    ) -> dict[str, Outcome[JSONValue]]:
        """Return one outcome for each subsystem.

        Cancel the task to cancel this request before it completes.

        Raises:
            ValueError: If a subsystem name is invalid.
            RuntimeError: If the request cannot start or the Kafka send fails.
        """
        ...

    async def request_excise(
        self,
        topic: str,
        key: str,
        *,
        subsystems: Sequence[str],
        timeout: Duration,
    ) -> dict[str, Outcome[JSONValue]]:
        """Return one excise outcome for each subsystem."""
        ...

    async def consumer_state(self) -> Literal['shut_down', 'unconfigured', 'configured', 'running']:
        """
        Get the current state of the consumer.

        Returns:
            Literal['shut_down', 'unconfigured', 'configured', 'running']:
            The current state.
        """
        ...

    async def _published(
        self,
        subsystem: str,
        kind: Literal["value", "map", "set", "deque"],
        name: str,
        *,
        read_cache: Optional[Union[Duration, Literal[False]]] = None,
    ) -> Union[
        _NativePublishedValue[JSONValue],
        _NativePublishedMap[JSONValue],
        _NativePublishedSet,
        _NativePublishedDeque[JSONValue],
    ]: ...

    async def subscribe(self, handler: EventHandler[P, R]) -> None:
        """
        Subscribe to messages using the provided handler.

        Args:
            handler (EventHandler): An instance implementing the EventHandler interface.

        Raises:
            TypeError: If the handler does not implement all event methods.
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
        Stop the consumer. You can subscribe again later.

        Raises:
            RuntimeError: If the consumer is not configured or not subscribed.

        """
        ...

    async def shutdown(self) -> None:
        """Shut down all client services.

        Concurrent and repeated calls await the same shutdown operation.

        Raises:
            RuntimeError: If shutdown fails.
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

class _NativeProsodyClient(_ProsodyClientApi): ...


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
            retention: Message retention time as a timedelta or float seconds.
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
