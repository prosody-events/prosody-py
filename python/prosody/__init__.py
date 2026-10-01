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
from prosody.handler import EventHandler, ProsodyHandler
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
        """
        Create a Prosody client without blocking the Python event loop.

        Pass ``None`` to leave an option unset. The option then falls back to
        its environment variable and its default. Some options read ``None``
        as a value, such as ``probe_port=None``, which turns the probe server
        off.

        Args:
            bootstrap_servers: Kafka servers for initial connection.
            mock: Use mock client for testing if True.
            source_system: Identifier for the producing system to prevent loops. Defaults to the group_id if unspecified.
            send_timeout: Timeout for message send operations.
            group_id: Consumer group name.
            idempotence_cache_size: Capacity of the producer idempotence cache and of the consumer deduplication cache. Must be at least 1. Default: 8192.
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
            statistics_interval: Time between librdkafka statistics reports. Env: ``PROSODY_STATISTICS_INTERVAL``. Defaults to 5 seconds.
            mode: Operating mode ('pipeline', 'low-latency', or 'best-effort').
            retry_base: Initial delay for exponential backoff in retries.
            max_retries: Low-latency retries before routing to the failure topic. Zero routes the initial failure without retrying.
            max_retry_delay: Maximum delay between retries.
            failure_topic: Topic for failed messages in low-latency mode.
            probe_port: Port for the probe server. Explicitly pass None to disable.
            slab_size: Timer slab partitioning duration. Controls how timers are grouped.
            cassandra_nodes: List of Cassandra contact nodes (hostnames or IPs with optional ports).
            cassandra_keyspace: Keyspace used for persistent Prosody data. Defaults to 'prosody'.
            cassandra_datacenter: Preferred datacenter for query routing and load balancing.
            cassandra_rack: Preferred rack identifier for topology-aware routing.
            cassandra_user: Username for authenticating with Cassandra cluster.
            cassandra_password: Password for authenticating with Cassandra cluster.
            cassandra_retention: Retention period for persistent timer and deferral data. Defaults to 1 year.
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
            defer_failure_threshold: Failure rate threshold for disabling deferral (0.0 to 1.0).
            defer_failure_window: Sliding window duration for failure rate tracking.
            loader_cache_size: Maximum messages retained by the shared Kafka loader. Env: PROSODY_LOADER_CACHE_SIZE. Defaults to 1024.
            defer_store_cache_size: Maximum deferred store cache entries (default: 8192). Env: PROSODY_DEFER_STORE_CACHE_SIZE.
            loader_seek_timeout: Timeout for Kafka loader seek operations. Env: PROSODY_LOADER_SEEK_TIMEOUT. Defaults to 30 seconds.
            loader_discard_threshold: Sequential-read distance before the loader seeks. Env: PROSODY_LOADER_DISCARD_THRESHOLD. Defaults to 100.
            timeout: Fixed timeout duration for handler execution. Defaults to 80% of stall threshold.
            telemetry_topic: Kafka topic to produce internal telemetry events to. Defaults to 'prosody.telemetry-events'.
            telemetry_enabled: Whether the telemetry emitter is enabled. Defaults to True.
            message_spans: Span linking for message execution ('child' or 'follows_from'). Defaults to 'child'.
            timer_spans: Span linking for timer execution ('child' or 'follows_from'). Defaults to 'follows_from'.
            state_collections: Keyed-state collections to register before subscribe. Pass the definition objects from `value`/`map`/`set`/`deque`/`message_value`/`message_map`/`message_deque`; each serializes into a collection config entry. Duplicate names are rejected.
            state_cache_dir: Directory for the local keyed-state cache. Each consumer opens its cache in a new subdirectory and removes it when the consumer stops, so clients can share the directory. Env: PROSODY_STATE_CACHE_DIR. Defaults to ``<temp>/prosody/keyed-state``.
            state_owned_cache_size: Capacity of the owning keyed-state cache, such as ``"64 MiB"``. Env: ``PROSODY_STATE_OWNED_CACHE_SIZE``. The storage engine selects its default when neither is set.
            state_memtable_size: Bytes of in-memory writes the local keyed-state cache holds for each assigned partition before it flushes them to disk, such as ``"16 MiB"``. Env: ``PROSODY_STATE_MEMTABLE_SIZE``, which applies when the option is omitted. Memory use scales with the number of assigned partitions. The storage engine's default of 64 MiB applies when neither is set.
            state_read_cache_size: Capacity of the published-state read cache, such as ``"1 MiB"``. Env: ``PROSODY_STATE_READ_CACHE_SIZE``. Uses the owned cache size when set, or 1 MiB when both sizes are unset.
            state_read_cache: Default published-read cache TTL, or `False` to bypass the cache. Env: ``PROSODY_STATE_READ_CACHE_TTL``. Defaults to 5 seconds.
            subsystem: Name under which published collections are advertised. Env: ``PROSODY_SUBSYSTEM``. Published collections require it.
            peer_bind_address: Socket address for the peer listener. Prosody reads ``PROSODY_PEER_BIND_ADDRESS`` when absent.
            peer_advertised_connect: Connect URI for remote peers. Prosody reads ``PROSODY_PEER_ADVERTISED_CONNECT`` when absent.
            peer_network_name: Network name for direct routes. Prosody reads ``PROSODY_PEER_NETWORK_NAME`` when absent.
            peer_cache_capacity: Maximum entries in each peer cache. Prosody reads ``PROSODY_PEER_CACHE_CAPACITY`` when absent.
            peer_registration_ttl: Peer registration lease. Prosody reads ``PROSODY_PEER_REGISTRATION_TTL`` when absent.
        Raises:
            ValueError: If the configuration is invalid.
            RuntimeError: If the client fails to initialize.
        """
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
