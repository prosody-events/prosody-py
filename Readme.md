# Prosody: Python Bindings for Kafka

Prosody offers Python bindings to the [Prosody Kafka client](https://github.com/prosody-events/prosody), providing
features for message production and consumption, including configurable retry mechanisms, failure handling
strategies, and integrated OpenTelemetry support for distributed tracing.

## Features

- **Kafka Consumer**: Per-key ordering with cross-key concurrency, offset management, consumer groups
- **Kafka Producer**: Idempotent delivery with configurable retries
- **Timer System**: Persistent scheduled execution backed by Cassandra or in-memory store
- **Quality of Service**: Fair scheduling limits concurrency and prevents failures from starving fresh traffic. Pipeline mode adds deferred retry and monopolization detection
- **Distributed Tracing**: OpenTelemetry integration for tracing message flow across services
- **Backpressure**: Pauses partitions when handlers fall behind
- **Mocking**: In-memory Kafka broker for tests (`mock=True`)
- **Failure Handling**: Pipeline (retry forever), Low-Latency (dead letter), Best-Effort (log and skip)

## Installation

Prosody supports Python 3.10 and above, including free-threaded builds (3.14t). Install from PyPI:

```bash
pip install prosody-events
```

## Quick Start

```python
from prosody import ProsodyClient, EventHandler, Context, Message
import datetime

# Initialize the client with Kafka bootstrap server, consumer group, and topics
client = ProsodyClient(
    # Bootstrap servers should normally be set using the PROSODY_BOOTSTRAP_SERVERS environment variable
    bootstrap_servers="localhost:9092",

    # To allow loopbacks, the source_system must be different from the group_id.
    # Normally, the source_system would be left unspecified, which would default to the group_id.
    source_system="my-application-source",

    # The group_id should be set to the name of your application
    group_id="my-application",

    # Topics the client should subscribe to
    subscribed_topics="my-topic"
)


# Define a custom message handler
class MyHandler(EventHandler):
    async def on_message(self, context: Context, message: Message) -> None:
        # Process the received message
        print(f"Received message: {message}")
        
        # Schedule a timer for delayed processing (requires Cassandra unless mock: True)
        if message.payload.get("schedule_followup"):
            future_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=30)
            await context.schedule(future_time)
    
    async def on_timer(self, context: Context, timer) -> None:
        # Handle timer firing
        print(f"Timer fired for key: {timer.key} at {timer.time}")


# Subscribe to messages using the custom handler
client.subscribe(MyHandler())

# Send a message to a topic
await client.send("my-topic", "message-key", {"content": "Hello, Kafka!"})

# Ensure proper shutdown when done
await client.unsubscribe()
```

## Architecture

Prosody enables efficient, parallel processing of Kafka messages while maintaining order for messages with the same key:

- **Partition-Level Parallelism**: Separate management of each Kafka partition
- **Key-Based Queuing**: Ordered processing for each key within a partition
- **Concurrent Processing**: Simultaneous processing of different keys
- **Backpressure Management**: Pause consumption from backed-up partitions

## Quality of Service

All modes use **fair scheduling** to limit concurrency and distribute execution time. Pipeline mode adds **deferred
retry** and **monopolization detection**.

### Fair Scheduling (All Modes)

The scheduler controls which message runs next and how many run concurrently.

**Virtual Time (VT):** Each key accumulates VT equal to its handler execution time. The scheduler picks the key with the
lowest VT. A key that runs for 500ms accumulates 500ms of VT; a key that hasn't run recently has zero VT and gets
priority.

**Two-Class Split:** Normal messages and failure retries have separate VT pools. The scheduler allocates execution time
between them (default: 70% normal, 30% failure). During a failure spike, retries get at most 30% of execution time—fresh
messages continue processing.

**Starvation Prevention:** Tasks receive a quadratic priority boost based on wait time. A task waiting 2 minutes
(configurable) gets maximum boost, overriding VT disadvantage.

### Deferred Retry (Pipeline Mode)

Moves failing keys to timer-based retry so the partition can continue processing other keys.

On transient failure: store the message offset in Cassandra, schedule a timer, return success. The partition advances.
When the timer fires, reload the message from Kafka and retry.

```python
# Configure defer behavior
client = ProsodyClient(
    group_id="my-consumer-group",
    subscribed_topics="my-topic",
    defer_enabled=True,           # Enable deferral (default: True)
    defer_base=1.0,               # Wait 1s before first retry
    defer_max_delay=86400.0,      # Cap at 24 hours
    defer_failure_threshold=0.9,  # Disable when >90% failing
)
```

**Failure Rate Gating:** When >90% of recent messages fail, deferral disables. The retry middleware blocks the
partition, applying backpressure upstream.

### Monopolization Detection (Pipeline Mode)

Rejects keys that consume too much execution time.

The middleware tracks per-key execution time in 5-minute rolling windows. Keys exceeding 90% of window time are rejected
with a transient error, routing them through defer.

```python
# Configure monopolization detection
client = ProsodyClient(
    group_id="my-consumer-group",
    subscribed_topics="my-topic",
    monopolization_enabled=True,     # Enable detection (default: True)
    monopolization_threshold=0.9,    # Reject keys using >90% of window
    monopolization_window=300.0,     # 5-minute window
)
```

### Handler Timeout

Handlers are automatically cancelled if they exceed a deadline:

```python
client = ProsodyClient(
    group_id="my-consumer-group",
    subscribed_topics="my-topic",
    timeout=30.0,             # Cancel after 30 seconds
    stall_threshold=60.0,     # Report unhealthy after 60 seconds
)
```

When a handler times out, `context.should_cancel()` returns `True` and `await context.on_cancel()` completes. The handler
should exit promptly. If not specified, timeout defaults to 80% of `stall_threshold`.

## Configuration

Configure via constructor options or environment variables. Options fall back to environment variables when unset.

### Core

| Option / Environment Variable           | Description                                       | Default      |
|-----------------------------------------|---------------------------------------------------|--------------|
| `bootstrap_servers` / `PROSODY_BOOTSTRAP_SERVERS` | Kafka servers to connect to             | -            |
| `group_id` / `PROSODY_GROUP_ID`         | Consumer group name                               | -            |
| `subscribed_topics` / `PROSODY_SUBSCRIBED_TOPICS` | Topics to read from                     | -            |
| `allowed_events` / `PROSODY_ALLOWED_EVENTS` | Only process events matching these prefixes   | (all)        |
| `source_system` / `PROSODY_SOURCE_SYSTEM` | Tag for outgoing messages (prevents reprocessing)| `<group_id>` |
| `mock` / `PROSODY_MOCK`                 | Use in-memory Kafka for testing                   | False        |

### Consumer

| Option / Environment Variable           | Description                                          | Default                |
|-----------------------------------------|------------------------------------------------------|------------------------|
| `max_concurrency` / `PROSODY_MAX_CONCURRENCY` | Max messages being processed simultaneously    | 32                     |
| `max_uncommitted` / `PROSODY_MAX_UNCOMMITTED` | Max queued messages before pausing consumption | 64                     |
| `timeout` / `PROSODY_TIMEOUT`           | Cancel handler if it runs longer than this           | 80% of stall threshold |
| `commit_interval` / `PROSODY_COMMIT_INTERVAL` | How often to save progress to Kafka            | 1s                     |
| `poll_interval` / `PROSODY_POLL_INTERVAL` | How often to fetch new messages from Kafka         | 100ms                  |
| `shutdown_timeout` / `PROSODY_SHUTDOWN_TIMEOUT` | Shutdown budget; handlers run freely until cancellation fires near the end of the timeout | 30s |
| `stall_threshold` / `PROSODY_STALL_THRESHOLD` | Report unhealthy if no progress for this long  | 5m                     |
| `probe_port` / `PROSODY_PROBE_PORT`     | HTTP port for health checks (None to disable)        | 8000                   |
| `failure_topic` / `PROSODY_FAILURE_TOPIC` | Send unprocessable messages here (dead letter queue) | -                    |
| `idempotence_cache_size` / `PROSODY_IDEMPOTENCE_CACHE_SIZE` | Global shared cache capacity across all partitions. Set to `0` to disable the entire deduplication middleware (both in-memory and Cassandra tiers). | 8192 |
| `idempotence_version` / `PROSODY_IDEMPOTENCE_VERSION` | Version string for cache-busting dedup hashes | `"1"` |
| `idempotence_ttl` / `PROSODY_IDEMPOTENCE_TTL` | TTL for dedup records in Cassandra | 7d (604800 seconds) |
| `slab_size` / `PROSODY_SLAB_SIZE`       | Timer storage granularity (rarely needs changing)    | 1h                     |
| `message_spans` / `PROSODY_MESSAGE_SPANS` | Span linking for message execution: `child` (child-of) or `follows_from` | `child` |
| `timer_spans` / `PROSODY_TIMER_SPANS`   | Span linking for timer execution: `child` (child-of) or `follows_from`   | `follows_from` |

### Producer

| Option / Environment Variable           | Description                     | Default |
|-----------------------------------------|---------------------------------|---------|
| `send_timeout` / `PROSODY_SEND_TIMEOUT` | Give up sending after this long | 1s      |

### Retry

When a handler fails, retry with exponential backoff:

| Option / Environment Variable           | Description                       | Default |
|-----------------------------------------|-----------------------------------|---------|
| `max_retries` / `PROSODY_MAX_RETRIES`   | Give up after this many attempts  | 3       |
| `retry_base` / `PROSODY_RETRY_BASE`     | Wait this long before first retry | 20ms    |
| `max_retry_delay` / `PROSODY_RETRY_MAX_DELAY` | Never wait longer than this  | 5m      |

### Deferral (Pipeline Mode)

| Option / Environment Variable           | Description                                       | Default |
|-----------------------------------------|---------------------------------------------------|---------|
| `defer_enabled` / `PROSODY_DEFER_ENABLED` | Enable deferral for new messages                | true    |
| `defer_base` / `PROSODY_DEFER_BASE`     | Wait this long before first deferred retry        | 1s      |
| `defer_max_delay` / `PROSODY_DEFER_MAX_DELAY` | Never wait longer than this                 | 24h     |
| `defer_failure_threshold` / `PROSODY_DEFER_FAILURE_THRESHOLD` | Disable deferral when failure rate exceeds this | 0.9 |
| `defer_failure_window` / `PROSODY_DEFER_FAILURE_WINDOW` | Measure failure rate over this time window | 5m     |
| `defer_cache_size` / `PROSODY_DEFER_CACHE_SIZE` | Track this many deferred keys in memory     | 1024    |
| `defer_store_cache_size` / `PROSODY_DEFER_STORE_CACHE_SIZE` | Maximum deferred store cache entries per Cassandra defer store | 8192 |
| `defer_seek_timeout` / `PROSODY_DEFER_SEEK_TIMEOUT` | Timeout when loading deferred messages    | 30s     |
| `defer_discard_threshold` / `PROSODY_DEFER_DISCARD_THRESHOLD` | Read optimization (rarely needs changing) | 100  |

### Monopolization Detection (Pipeline Mode)

| Option / Environment Variable           | Description                             | Default |
|-----------------------------------------|-----------------------------------------|---------|
| `monopolization_enabled` / `PROSODY_MONOPOLIZATION_ENABLED` | Enable hot key protection   | true    |
| `monopolization_threshold` / `PROSODY_MONOPOLIZATION_THRESHOLD` | Max handler time as fraction of window | 0.9 |
| `monopolization_window` / `PROSODY_MONOPOLIZATION_WINDOW` | Measurement window            | 5m      |
| `monopolization_cache_size` / `PROSODY_MONOPOLIZATION_CACHE_SIZE` | Max distinct keys to track  | 8192    |

### Fair Scheduling (All Modes)

| Option / Environment Variable           | Description                                                      | Default |
|-----------------------------------------|------------------------------------------------------------------|---------|
| `scheduler_failure_weight` / `PROSODY_SCHEDULER_FAILURE_WEIGHT` | Fraction of processing time reserved for retries | 0.3    |
| `scheduler_max_wait` / `PROSODY_SCHEDULER_MAX_WAIT` | Messages waiting this long get maximum priority          | 2m      |
| `scheduler_wait_weight` / `PROSODY_SCHEDULER_WAIT_WEIGHT` | Priority boost for waiting messages (higher = more aggressive) | 200.0 |
| `scheduler_cache_size` / `PROSODY_SCHEDULER_CACHE_SIZE` | Max distinct keys to track                             | 8192    |

### Telemetry Emitter

Prosody emits internal lifecycle events (message dispatched/succeeded/failed, timer scheduled/fired, producer sends) to a Kafka topic for observability:

| Option / Environment Variable           | Description                                            | Default                    |
|-----------------------------------------|--------------------------------------------------------|----------------------------|
| `telemetry_topic` / `PROSODY_TELEMETRY_TOPIC` | Kafka topic to produce telemetry events to       | prosody.telemetry-events   |
| `telemetry_enabled` / `PROSODY_TELEMETRY_ENABLED` | Enable or disable the telemetry emitter        | true                       |

### Cassandra

Persistent storage for timers and deferred retries (not needed if `mock=True`):

| Option / Environment Variable           | Description                        | Default |
|-----------------------------------------|------------------------------------|---------|
| `cassandra_nodes` / `PROSODY_CASSANDRA_NODES` | Servers to connect to (host:port) | -      |
| `cassandra_keyspace` / `PROSODY_CASSANDRA_KEYSPACE` | Keyspace name              | prosody |
| `cassandra_user` / `PROSODY_CASSANDRA_USER` | Username                         | -       |
| `cassandra_password` / `PROSODY_CASSANDRA_PASSWORD` | Password                   | -       |
| `cassandra_datacenter` / `PROSODY_CASSANDRA_DATACENTER` | Prefer this datacenter for queries | - |
| `cassandra_rack` / `PROSODY_CASSANDRA_RACK` | Prefer this rack for queries     | -       |
| `cassandra_retention` / `PROSODY_CASSANDRA_RETENTION` | Delete data older than this | 1y     |

### Keyed State

Register keyed-state collections before you subscribe. Persistence is backed by Cassandra and is not needed when `mock=True`. See the [Keyed State](#keyed-state-1) feature section for handler usage; the client-level knobs and per-collection fields are below. Where an option and an environment variable are paired, an explicitly set option wins; otherwise the environment variable applies, then the default.

| Option / Environment Variable                                | Description                                                                                                                                                             | Default             |
|--------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------|
| `state_collections` / -                                      | Keyed-state collections to register before subscribe (list of definition objects; duplicate names are rejected)                                                        | (none)              |
| `state_cache_dir` / `PROSODY_FJALL_CACHE_DIR`                | Root directory for the local committed-value cache; each live client needs its own directory (it is locked exclusively)                                                | per-client temp dir |
| `state_recovery_delay` / `PROSODY_KEYED_STATE_RECOVERY_DELAY` | Delay before the recovery sweep; every collection TTL must strictly exceed it. Whole seconds >= 1 (`timedelta` or float seconds; the env var accepts a duration string like `30s`) | 30s                 |

Each `state_collections` entry has these fields. Prefer the definition constructors (`value` / `map` / `deque` and their `message_*` variants, documented below): they serialize into `state_collections` so you declare each collection once and reuse the same object with `context.state()`.

| Field              | Description                                                                          | Default    |
|--------------------|-------------------------------------------------------------------------------------|------------|
| `name`             | Collection name; non-empty and unique within the client                             | (required) |
| `kind`             | `"value"`, `"map"`, or `"deque"`                                                     | (required) |
| `payload`          | `"json"` (JSON values) or `"message"` (the full Kafka message the handler received) | (required) |
| `ttl`              | Per-write TTL, whole seconds >= 1 (must exceed the recovery delay); `timedelta` or int seconds | (none)     |
| `read_uncommitted` | Opt out of transactional staging (read-uncommitted)                                 | false      |
| `keyset_limit`     | Map-only; ordered-scan bound in `0..=4096` (`0` disables ordered-scan tracking)      | 128        |

## Liveness and Readiness Probes

Prosody includes a built-in probe server for consumer-based applications that provides health check endpoints. The probe
server is tied to the consumer's lifecycle and offers two main endpoints:

1. `/readyz`: A readiness probe that checks if any partitions are assigned to the consumer. Returns a success status
   only when the consumer has at least one partition assigned, indicating it's ready to process messages.

2. `/livez`: A liveness probe that checks if any partitions have stalled (haven't processed a message within a
   configured time threshold).

Configure the probe server using either the client constructor:

```python
client = ProsodyClient(
    group_id="my-consumer-group",
    subscribed_topics="my-topic",
    probe_port=8000,  # Set to None to disable
    stall_threshold=15.0  # Seconds before considering a partition stalled
)
```

Or via environment variables:

```bash
PROSODY_PROBE_PORT=8000  # Set to 'none' to disable
PROSODY_STALL_THRESHOLD=15s  # Default stall detection threshold
```

### Important Notes

1. The probe server starts automatically when the consumer is subscribed and stops when unsubscribed.
2. A partition is considered "stalled" if it hasn't processed a message within the `stall_threshold` duration.
3. The stall threshold should be set based on your application's message processing latency and expected message
   frequency.
4. Setting the threshold too low might cause false positives, while setting it too high could delay detection of actual
   issues.
5. The probe server is only active when consuming messages (not for producer-only usage).

## Advanced Usage

### Pipeline Mode

Pipeline mode is the default mode. Ensures ordered processing, retrying failed operations indefinitely:

```python
# Initialize client in pipeline mode
client = ProsodyClient(
    mode="pipeline",  # Explicitly set pipeline mode (this is the default)
    group_id="my-consumer-group",
    subscribed_topics="my-topic"
)
```

### Low-Latency Mode

Prioritizes quick processing, sending persistently failing messages to a failure topic:

```python
# Initialize client in low-latency mode
client = ProsodyClient(
    mode="low-latency",  # Set low-latency mode
    group_id="my-consumer-group",
    subscribed_topics="my-topic",
    failure_topic="failed-messages"  # Specify a topic for failed messages
)
```

### Best-Effort Mode

Optimized for development environments or services where message processing failures are acceptable:

```python
# Initialize client in best-effort mode
client = ProsodyClient(
    mode="best-effort",  # Set best-effort mode
    group_id="my-consumer-group",
    subscribed_topics="my-topic"
)
```

## Event Type Filtering

Prosody supports filtering messages based on event type prefixes, allowing your consumer to process only specific types of events:

```python
# Process only events with types starting with "user." or "account."
client = ProsodyClient(
    group_id="my-consumer-group",
    subscribed_topics="my-topic",
    allowed_events=["user.", "account."]
)
```

Or via environment variables:

```bash
PROSODY_ALLOWED_EVENTS=user.,account.
```

### Matching Behavior

Prefixes must match exactly from the start of the event type:

✓ Matches:
- `{"type": "user.created"}` matches prefix `user.`
- `{"type": "account.deleted"}` matches prefix `account.`

✗ No Match:
- `{"type": "admin.user.created"}` doesn't match `user.`
- `{"type": "my.account.deleted"}` doesn't match `account.`
- `{"type": "notification"}` doesn't match any prefix

If no prefixes are configured, all messages are processed. Messages without a `type` field are always processed.

## Source System Deduplication

Prosody prevents processing loops in distributed systems by tracking the source of each message:

```python
# Consumer and producer in one application
client = ProsodyClient(
    group_id="my-service",
    source_system="my-service-producer",  # Must differ from groupId to allow loopbacks; defaults to groupId
    subscribed_topics="my-topic"
)
```

Or via environment variable:

```bash
PROSODY_SOURCE_SYSTEM=my-service-producer
```

### How It Works

1. **Producers** add a `source-system` header to all outgoing messages.
2. **Consumers** check this header on incoming messages.
3. If a message's source system matches the consumer's group ID, the message is skipped.

This prevents endless loops where a service consumes its own produced messages.

## Message Deduplication

Prosody automatically deduplicates messages using the `id` field in their JSON payload. Messages with the same ID and
key are processed only once.

Deduplication uses a two-tier architecture:

- **Global in-memory cache**: A single LRU cache shared across all partitions in the process. Because it is shared, it
  survives partition reassignments within the same process, reducing duplicate work during rebalances.
- **Cassandra-backed persistent store**: Deduplication records written to Cassandra survive process restarts and
  cross-instance rebalances, providing durable protection against duplicates.

```python
# Messages with IDs are deduplicated per key
await client.send("my-topic", "key1", {
    "id": "msg-123",  # Message will be processed
    "content": "Hello!"
})

await client.send("my-topic", "key1", {
    "id": "msg-123",  # Message will be skipped (duplicate)
    "content": "Hello again!"
})

await client.send("my-topic", "key2", {
    "id": "msg-123",  # Message will be processed (different key)
    "content": "Hello!"
})
```

The entire deduplication middleware (both the in-memory cache and the Cassandra-backed persistent store) can be disabled by setting `idempotence_cache_size=0`:

```python
client = ProsodyClient(
    group_id="my-consumer-group",
    subscribed_topics="my-topic",
    idempotence_cache_size=0  # Disable deduplication entirely
)
```

Or via environment variable:

```bash
PROSODY_IDEMPOTENCE_CACHE_SIZE=0
```

To invalidate all previously recorded deduplication entries (e.g. after a data migration), change `idempotence_version`:

```python
client = ProsodyClient(
    group_id="my-consumer-group",
    subscribed_topics="my-topic",
    idempotence_version="2"  # All entries recorded under version "1" are ignored
)
```

The `idempotence_ttl` option controls how long deduplication records are retained in Cassandra (default: 7 days). Set
this to match your expected message redelivery window:

```python
from datetime import timedelta

client = ProsodyClient(
    group_id="my-consumer-group",
    subscribed_topics="my-topic",
    idempotence_ttl=timedelta(days=7)  # also accepts seconds as a float (e.g. 604800.0)
)
```

## Timer Functionality

Prosody supports timer-based delayed execution within message handlers. When a timer fires, your handler's `on_timer` method will be called:

```python
import datetime
from prosody import EventHandler, Context, Message

class MyHandler(EventHandler):
    async def on_message(self, context: Context, message: Message) -> None:
        # Schedule a timer to fire in 30 seconds
        future_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=30)
        await context.schedule(future_time)
        
        # Schedule multiple timers
        one_minute = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=1)
        two_minutes = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=2)
        await context.schedule(one_minute)
        await context.schedule(two_minutes)
        
        # Check what's scheduled
        scheduled_times = await context.scheduled()
        print(f"Scheduled timers: {len(scheduled_times)}")
    
    async def on_timer(self, context: Context, timer) -> None:
        print("Timer fired!")
        print(f"Key: {timer.key}")
        print(f"Scheduled time: {timer.time}")
```

### Timer Methods

The context provides timer scheduling methods that allow you to delay execution or implement timeout behavior:

- `schedule(time)`: Schedules a timer to fire at the specified time
- `clear_and_schedule(time)`: Clears all timers and schedules a new one
- `unschedule(time)`: Removes a timer scheduled for the specified time
- `clear_scheduled()`: Removes all scheduled timers
- `scheduled()`: Returns a list of all scheduled timer times

### Timer Object

When a timer fires, the `on_timer` method receives a timer object with these properties:

- `key` (str): The entity key identifying what this timer belongs to
- `time` (datetime): The time when this timer was scheduled to fire

**Note**: Timer precision is limited to seconds due to the underlying storage format. Sub-second precision in scheduled times will be rounded to the nearest second.

### Timer Configuration

Timer functionality requires Cassandra for persistence unless running in mock mode. Configure Cassandra connection via environment variable:

```bash
PROSODY_CASSANDRA_NODES=localhost:9042  # Required for timer persistence
```

Or programmatically when creating the client:

```python
client = ProsodyClient(
    bootstrap_servers="localhost:9092",
    group_id="my-application",
    subscribed_topics="my-topic",
    cassandra_nodes="localhost:9042"  # Required unless mock=True
)
```

For testing, you can use mock mode to avoid Cassandra dependency:

```python
# Mock mode for testing (timers work but aren't persisted)
client = ProsodyClient(
    bootstrap_servers="localhost:9092",
    group_id="my-application",
    subscribed_topics="my-topic",
    mock=True  # No Cassandra required in mock mode
)
```

## Keyed State

Prosody supports keyed state: per-key data that a handler reads and writes and that survives across events. State is partitioned by the message key, so each key has a single writer at a time, and by default writes settle atomically with the event — a handler that throws leaves no partial state. Values are either JSON payloads or the full Kafka `Message` the handler received. Register collections on the client before subscribing, then bind them inside the handler with `context.state(definition)`:

```python
from contextlib import aclosing
from typing import List, cast

from typing_extensions import TypedDict

from prosody import (
    Context,
    EventHandler,
    Message,
    MapDefinition,
    MessageDequeDefinition,
    ProsodyClient,
    Timer,
    ValueDefinition,
    map,
    message_deque,
    value,
)


class Cart(TypedDict):
    items: List[str]


class OrderEvent(TypedDict):
    order_id: str
    total: int


# The type argument is bound through the annotation on the target — the
# constructors are generic, so a bare call defaults to the JSON value type.
CART: ValueDefinition[Cart] = value("cart", ttl=30 * 86400)
TOTALS: MapDefinition[int] = map("totals")  # keys are always str
BACKLOG: MessageDequeDefinition[OrderEvent] = message_deque("backlog")


class OrderHandler(EventHandler):
    async def on_message(self, context: Context, message: Message) -> None:
        payload = cast(OrderEvent, message.payload)

        cart = context.state(CART)  # ValueState[Cart]
        current = await cart.get() or {"items": []}  # Cart | None
        await cart.set({"items": [*current["items"], payload["order_id"]]})

        totals = context.state(TOTALS)  # MapState[int]
        await totals.set(message.key, payload["total"])
        async with aclosing(totals.items()) as scan:
            async for key, total in scan:  # key: str, total: int
                ...

        backlog = context.state(BACKLOG)  # DequeState[Message[OrderEvent]]
        await backlog.append(cast("Message[OrderEvent]", message))
        oldest = await backlog.get(0)  # Message[OrderEvent] | None

        await cart.commit()

    async def on_timer(self, context: Context, timer: Timer) -> None:
        ...


client = ProsodyClient(
    group_id="orders",
    subscribed_topics="orders",
    state_collections=[CART, TOTALS, BACKLOG],
)
```

### Definitions

A definition constructor declares one collection and returns a frozen definition object carrying its `name`, `kind`, and `payload`. Reference that definition both in `state_collections` (registration) and in `context.state()` (binding) — declare each collection once and reuse it. (Reuse is a convenience, not a requirement: binding matches a definition to a registered collection by its `name` / `kind` / `payload` fields, not by object identity, so a structurally-equal definition also works.) Three kinds, each with a JSON variant (values are your JSON payload) and a message variant (values are the full Kafka `Message[P]`):

- `value(name, ...)`: single value. Vends `ValueState[T]`.
- `map(name, ...)`: ordered map with **string** keys. Vends `MapState[V]`.
- `deque(name, ...)`: double-ended queue. Vends `DequeState[T]`.
- `message_value(name, ...)`: single value holding a `Message[P]`. Vends `ValueState[Message[P]]`.
- `message_map(name, ...)`: ordered map of `Message[P]` (string keys). Vends `MapState[Message[P]]`.
- `message_deque(name, ...)`: deque of `Message[P]`. Vends `DequeState[Message[P]]`.

Every constructor accepts `ttl` (whole seconds >= 1, `timedelta` or int) and `read_uncommitted`, plus `keyset_limit` on maps only. The type parameter is annotation-level only: it is a **structural JSON annotation** (TypedDict-oriented). Payloads cross the boundary as plain JSON with no model construction or validation in v1, so `dataclass` / Pydantic types are **not** valid type arguments — the parameter guides your type checker but does not enforce a shape at runtime (an adapter hook is future work). Bind the type argument through the annotation on the assignment target (`CART: ValueDefinition[Cart] = value("cart")`); a bare call defaults to the JSON value type.

### Registration

Put the definitions in `state_collections` when constructing the client, before calling `subscribe`. Each definition serializes into a collection config entry, so passing the definition object is all that is required. Collection names must be unique within a client — duplicate names are rejected. Keyed state needs Cassandra unless the client runs with `mock=True`. See the [Keyed State configuration](#keyed-state) subsection above for the client-level knobs and per-collection fields.

### State Handles

`context.state(definition)` vends a typed handle bound to the collection for the current event attempt. The handle — and any iterator it opens — is valid only within the handler invocation that created it; there is no post-handler read window. Binding an unregistered name raises a `PermanentStateError`; so does a definition whose `kind` or `payload` disagrees with what was durably registered under that name in the consumer group (the collection's stored schema identity, which core validates at first use — this is a schema conflict across deploys, not a Python object-identity check). All handle methods are async.

`ValueState[T]`:

- `get() -> Optional[T]`: reads the current value, or `None` when absent.
- `set(value: T) -> None`: buffers a write. Writing `None` (JSON `null`) is rejected with `NullValueError` (transient) — call `clear()`.
- `clear() -> None`: deletes the stored value.
- `commit() -> None` / `rollback() -> None`: see [Commit and Rollback](#commit-and-rollback).

`MapState[V]` (keys are always `str`):

- `get(key: str) -> Optional[V]`: reads the value for `key`, or `None` when absent.
- `get_many(keys: List[str]) -> List[Optional[V]]`: reads several keys in one isolated batch, returning one entry per key in the same order (`result[i]` is the value for `keys[i]`); a missing key is `None`, and a repeated key is answered at each spot. The whole read happens as one step, so no other change to this event's state slips in partway through.
- `set(key: str, value: V) -> None`: inserts or overwrites. Writing `None` (JSON `null`) is rejected with `NullValueError` (transient) — call `remove(key)`.
- `remove(key: str) -> None`: removes `key` (named `remove` because `del` cannot be async). Deliberately returns `None`, not a boolean "was present" flag (surfacing that would force a hidden read on every remove).
- `clear() -> None`: removes every entry.
- `items(direction=Direction.FORWARD)` / `keys()` / `values()` / `__aiter__`: see [Scan Iteration](#scan-iteration).
- `commit() -> None` / `rollback() -> None`.

`DequeState[T]`:

- `append(item: T) -> None`: appends at the back. Writing `None` (JSON `null`) is rejected.
- `appendleft(item: T) -> None`: prepends at the front. Writing `None` (JSON `null`) is rejected.
- `pop() -> Optional[T]`: removes and returns the back element, or `None` when empty.
- `popleft() -> Optional[T]`: removes and returns the front element, or `None` when empty.
- `size() -> int`: number of live elements (named `size` because `len` cannot be async).
- `is_empty() -> bool`: whether the deque holds no live elements (a method for the same reason).
- `get(index: int) -> Optional[T]`: reads the element at front-relative `index`, or `None` past the end. `index` must be a non-negative integer that fits a native `u32`; a fractional value raises `TypeError` and a negative or oversized one raises `OverflowError` at the native boundary, both of which classify transient at the handler bridge, so the caller mistake retries and stays visible rather than discarding the message.
- `values(direction=Direction.FORWARD)` / `__aiter__`: see [Scan Iteration](#scan-iteration).
- `commit() -> None` / `rollback() -> None`.

### Scan Iteration

Maps expose `items(direction=...)`, `keys()`, and `values()`; deques expose `values(direction=...)`. Each returns an async iterator, so you can drive it with `async for`. `direction` is `Direction.FORWARD` (default) or `Direction.BACKWARD`. On a map, `keys()` and `values()` are always forward-only; only `items()` accepts a direction. Both `MapState` and `DequeState` are themselves async-iterable (`__aiter__` is forward iteration — map: `(key, value)` entries; deque: elements), so the handle itself works in an `async for`.

Iterators are valid only within the attempt that opened them. Exiting an `async for` loop early with a bare `break` does **not** close the underlying cursor — that is harmless by construction (no store permit is held between pulls, the cursor is attempt-epoch fenced, and the native drop closes it on GC). For a deterministic early close, wrap the scan in `contextlib.aclosing(...)`:

```python
from contextlib import aclosing

async with aclosing(context.state(totals).items(Direction.BACKWARD)) as scan:
    async for key, total in scan:
        if total > 1000:
            break  # aclosing closes the cursor on exit
```

### Commit and Rollback

Every handle exposes `commit()` and `rollback()` (both `-> None`). By default a handler's writes are buffered and settle atomically when the event completes; commit and rollback are the explicit mid-handler escape hatch.

- `commit()` durably flushes this collection's buffered operations mid-handler. It is at-least-once: the flush becomes visible even if the event later fails and is redelivered, and it establishes a floor that a later `rollback()` cannot cross.
- `rollback()` discards this collection's buffered uncommitted operations back to the last commit floor. It is infallible.

Both return `None`. The erased core seam deliberately drops the store outcome, so there is **no** applied/noop return value — do not expect one.

### Semantics

- **Per-key single writer.** State is keyed by the message key; only one handler invocation writes a given key at a time.
- **Transactional by default.** A handler's writes settle atomically with the event. A handler that throws leaves no partial state (unless you opted a collection into `read_uncommitted`, or flushed explicitly with `commit()`).
- **At-least-once.** Redelivery re-runs the handler; reads reflect committed prior attempts. Keep handlers idempotent.
- **Attempt-scoped.** The context, the handles it vends, and any iterators those handles open are valid only within the handler invocation that created them. Do not retain them past the handler.
- **Cancellation honesty.** An `asyncio` cancellation may drop the in-flight native future; core's attempt-epoch fence keeps that safe, so a follow-up operation on a fresh attempt still succeeds.

### Error Handling

Keyed-state failures surface as structured errors that flow through the same handler-error bridge as everything else (the transient/permanent category is carried as data, never parsed from the message). Every state error also inherits from the `StateError` brand, so you can catch all of them with `except StateError`:

- `TransientStateError` (subclasses `TransientError`): the default. A temporary store read/write failure (for example a timeout), **and every caller mistake** — a rejected `None`/unrepresentable write (use `clear()` / `remove(key)` instead), an item-shape mismatch, an invalid scan direction, or a malformed definition. Caller mistakes are transient on purpose: a permanent error discards the in-flight message and can silently lose data, so a code error retries and stays visible (logs / metrics / lag) until you fix it.
- `NullValueError` (subclasses `TransientStateError` and `ValueError`): a `None` / JSON-`null` write, which is not a storable value. It reads as a `ValueError` to callers who care about the argument and classifies transient if it propagates.
- `PermanentStateError` (subclasses `PermanentError`): reserved for failures a retry genuinely cannot resolve within the running process — an unregistered or identity-mismatched collection, or a duplicate registration. (A handler may also raise one explicitly to declare its own failure permanent.)

Because they subclass the existing error hierarchy, rethrowing them from a handler classifies the event exactly like a plain `PermanentError` / `TransientError` through the same `is_permanent` bridge.

## OpenTelemetry Tracing

Prosody supports OpenTelemetry tracing, allowing you to monitor and analyze the performance of your Kafka-based
applications. The library will emit traces using the OTLP protocol if the `OTEL_EXPORTER_OTLP_ENDPOINT` environment
variable is defined.

Note: Prosody emits its own traces separately because it uses its own tracing runtime, as it would be expensive to send
all traces to Python.

### Required Packages

To use OpenTelemetry tracing with Prosody, you need to install the following packages:

```
opentelemetry-sdk>=1.26.0
opentelemetry-api>=1.26.0
opentelemetry-exporter-otlp-proto-grpc>=1.26.0
```

### Initializing Tracing

To initialize tracing in your application:

```python
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

traceProvider = TracerProvider()
processor = BatchSpanProcessor(OTLPSpanExporter())
traceProvider.add_span_processor(processor)
trace.set_tracer_provider(traceProvider)

# Creates a tracer from the global tracer provider
tracer = trace.get_tracer(__name__)
```

### Setting OpenTelemetry Environment Variables

Set the following standard OpenTelemetry environment variables:

```
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
OTEL_SERVICE_NAME=my-service-name
```

For more information on these and other OpenTelemetry environment variables, refer to
the [OpenTelemetry specification](https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/#general-sdk-configuration).

### Using Tracing in Your Application

After initializing tracing, you can define spans in your application, and they will be properly propagated through
Kafka:

```python
class MyHandler(EventHandler):
    async def on_message(self, context: Context, message: Message) -> None:
        with tracer.start_as_current_span("test-receive"):
            # Process the received message
            print(f"Received message: {message}")
```

### Span Linking

By default, message execution spans use **`child`** (child-of relationship — the execution span is part of
the same trace as the producer). Timer execution spans use **`follows_from`** (the execution span starts a
new trace with a span link back to the scheduling span, since timer execution is causally related but not part of
the same operation).

Both strategies are configurable via the `message_spans` / `PROSODY_MESSAGE_SPANS` and `timer_spans` /
`PROSODY_TIMER_SPANS` options. Accepted values: `child`, `follows_from`.

## Best Practices

### 🔥 ☢️ DANGER: NEVER SHARE EVENTHANDLER STATE ACROSS CALLS ☢️ 🔥

Your event handler class methods will be called concurrently. NEVER use mutable shared state across event handler calls,
like setting instance variables. Sharing state can introduce subtle data races and corruption that may only appear in
production. If you absolutely must use non-local mutable state, ensure that you know what you're doing and use
appropriate synchronization primitives.

### Ensuring Idempotent Message Handlers

Idempotent message handlers are crucial for maintaining data consistency, fault tolerance, and scalability when working
with distributed, event-based systems. They ensure that processing a message multiple times has the same effect as
processing it once, which is essential for recovering from failures.

Strategies for achieving idempotence:

1. **Natural Idempotence**: Use inherently idempotent operations (e.g., setting a value in a key-value store).

2. **Deduplication with Unique Identifiers**:

- Kafka messages can be uniquely identified by their partition and offset.
- Before processing, check if the message has been handled before.
- Store processed message identifiers with an appropriate TTL.

3. **Database Upserts**: Use upsert operations for database writes (e.g., `INSERT ... ON CONFLICT DO UPDATE` in
   PostgreSQL).

4. **Partition Offset Tracking**:

- Store the latest processed offset for each partition.
- Only process messages with higher offsets than the last processed one.
- Critically, store these offsets transactionally with other state updates to ensure consistency.

5. **Idempotency Keys for External APIs**: Utilize idempotency keys when supported by external APIs.

6. **Check-then-Act Pattern**:

- For non-idempotent external systems, verify if an operation was previously completed before execution.
- Maintain a record of completed operations, keyed by a unique message identifier.

7. **Saga Pattern**:

- Implement a state machine in your database for multi-step operations.
- Each message advances the state machine, allowing for idempotent processing and easy failure recovery.
- Particularly useful for complex, distributed transactions across multiple services.

### Proper Shutdown

Always unsubscribe from topics before exiting your application:

```python
# Ensure proper shutdown
await client.unsubscribe()
```

This ensures:

1. Completion and commitment of all in-flight work
2. Quick rebalancing, allowing other consumers to take over partitions
3. Proper release of resources

Implement shutdown handling in your application using an asyncio event:

```python
import asyncio
import signal
from prosody import ProsodyClient


async def main():
    # Create an event to signal when to shut down
    shutdown_event = asyncio.Event()

    # Set up signal handlers
    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        asyncio.get_running_loop().add_signal_handler(
            sig, lambda s=sig: asyncio.create_task(shutdown(shutdown_event, s))
        )

    client = ProsodyClient(
        bootstrap_servers="localhost:9092",
        group_id="my-consumer-group",
        subscribed_topics="my-topic"
    )

    # Subscribe to messages using your custom handler
    client.subscribe(MyHandler())

    # Wait for the shutdown event
    await shutdown_event.wait()

    # Unsubscribe
    await client.unsubscribe()


async def shutdown(event: asyncio.Event, signal: signal.Signals):
    print(f"Received signal {signal.name}. Initiating shutdown...")
    event.set()


if __name__ == '__main__':
    asyncio.run(main())
```

### Error Handling

Prosody classifies errors as transient (temporary, can be retried) or permanent (won't be resolved by retrying). By
default, all errors are considered transient.

Use the `@permanent` decorator to classify exceptions that should not be retried:

```python
from prosody import EventHandler, Context, Message, permanent


class MyHandler(EventHandler):
    @permanent(TypeError, AttributeError)
    async def on_message(self, context: Context, message: Message):
        # Your message handling logic here
        # TypeError and AttributeError will be treated as permanent
        # All other exceptions will be treated as transient (default behavior)
        pass
```

Best practices:

- Use permanent errors for issues like malformed data or business logic violations.
- Use transient errors for temporary issues like network problems.
- Be cautious with permanent errors as they prevent retries and can result in data loss.
- Consider system reliability and data consistency when classifying errors.

### Handling Task Cancellation

Prosody cancels tasks during partition rebalancing, timeout, or shutdown. During shutdown, handlers run freely for most of the `shutdown_timeout` before the cancellation signal fires — giving in-flight work time to complete. How you handle cancellation is critical:

- Prosody interprets task success based on exception propagation.
- A task that exits without an exception is considered successful.
- Any exception signals task failure.

Best practices:

1. Exit promptly when cancelled to avoid rebalancing delays.
2. Use `try/finally` or context managers for clean resource handling.

Failing to follow these practices can lead to slower message processing due to delayed rebalancing.

## Release Process

Prosody uses an automated release process managed by GitHub Actions. Here's an overview of how releases are handled:

1. **Trigger**: The release process is triggered automatically on pushes to the `main` branch.

2. **Release Please**: The process starts with the "Release Please" action, which:
    - Analyzes commit messages since the last release.
    - Creates or updates a release pull request with changelog updates and version bumps.
    - When the PR is merged, it creates a GitHub release and a git tag.

3. **Build Process**: If a new release is created, the following build jobs are triggered:
    - Linux builds for x86_64 and aarch64 architectures.
    - MuslLinux builds for the same architectures.
    - Windows build for x64 architecture.
    - macOS build for aarch64 architecture.
    - Source distribution (sdist) build.

4. **Artifact Upload**: Each build job uploads its artifacts (wheels or sdist) to GitHub Actions.

5. **Publication**: If all builds are successful, the final step publishes the built artifacts to PyPI.

### Contributing to Releases

To contribute to a release:

1. Make your changes in a feature branch.
2. Use [Conventional Commits](https://www.conventionalcommits.org/) syntax for your commit messages. This helps Release
   Please determine the next version number and generate the changelog.
3. Create a pull request to merge your changes into the `main` branch.
4. Once your PR is approved and merged, Release Please will include your changes in the next release PR.

### Manual Releases

While the process is automated, manual intervention may sometimes be necessary:

- You can manually trigger the release workflow from the GitHub Actions tab if needed.
- If you need to make changes to the release PR created by Release Please, you can do so before merging it.

Remember, all releases are automatically published to PyPI. Ensure you have thoroughly tested your changes before
merging to `main`.

## Administrative Operations

**⚠️ Important Note**: Topic management in production environments should typically be handled through GitOps using Strimzi KafkaTopic manifests. The `AdminClient` is provided for testing scenarios and specific cases where the data team requires manual topic creation and deletion.

### AdminClient

The `AdminClient` provides administrative operations for Kafka topics:

```python
from prosody import AdminClient

# Initialize admin client
admin = AdminClient(bootstrap_servers="localhost:9092")

# Create a topic for testing
await admin.create_topic(
    "test-topic",
    partition_count=4,
    replication_factor=1,
    cleanup_policy="delete",
    retention=datetime.timedelta(days=7)  # or retention=604800.0 (seconds)
)

# Delete a topic
await admin.delete_topic("test-topic")
```

#### Configuration Parameters

The `AdminClient` constructor accepts:

- `bootstrap_servers` (str | list[str]): Kafka bootstrap servers (required)

Or via environment variable:

```bash
PROSODY_BOOTSTRAP_SERVERS=localhost:9092  # Single server
PROSODY_BOOTSTRAP_SERVERS=localhost:9092,localhost:9093  # Multiple servers
```

#### Topic Configuration Options

When creating topics, the following options are supported:

- `partition_count` (int): Number of partitions (optional, uses broker default)
- `replication_factor` (int): Replication factor (optional, uses broker default)
- `cleanup_policy` (str): Cleanup policy such as "delete" or "compact" (optional)
- `retention` (timedelta | float): Message retention time as timedelta object or seconds as float (optional)

These can also be configured via environment variables:

```bash
PROSODY_TOPIC_PARTITIONS=4                    # Number of partitions
PROSODY_TOPIC_REPLICATION_FACTOR=1           # Replication factor
PROSODY_TOPIC_CLEANUP_POLICY=delete          # Cleanup policy
PROSODY_TOPIC_RETENTION=7d                   # Retention as humantime string (7d, 2h 30m, etc.)
```

## API Reference

### ProsodyClient

- `__init__(**config)`: Initialize a new ProsodyClient with the given configuration.
- `send(topic: str, key: str, payload: Any) -> None`: Send a message to a specified topic.
- `consumer_state() -> str`: Get the current state of the consumer.
- `subscribe(handler: EventHandler) -> None`: Subscribe to messages using the provided handler.
- `unsubscribe() -> None`: Unsubscribe from messages and shut down the consumer.

### AdminClient

- `__init__(**config)`: Initialize a new AdminClient with the given configuration.
- `create_topic(name: str, **config) -> None`: Create a Kafka topic with optional configuration parameters.
- `delete_topic(name: str) -> None`: Delete an existing Kafka topic.

### EventHandler

An abstract base class for user-defined handlers:

```python
class EventHandler(ABC):
    @abstractmethod
    async def on_message(self, context: Context, message: Message) -> None:
        # Implement your message handling logic here
        pass
    
    @abstractmethod
    async def on_timer(self, context: Context, timer: Timer) -> None:
        # Implement your timer handling logic here
        pass
```

Note: The on_message method may be called from different threads. Ensure that any handler state is thread-safe. If
library incompatibility becomes an issue, this may be changed in the future so all handler calls originate from the same
thread,

### Message

Represents a Kafka message as a frozen dataclass with the following attributes:

- `topic: str`: The name of the topic.
- `partition: int`: The partition number.
- `offset: int`: The message offset within the partition.
- `timestamp: datetime`: The timestamp when the message was created or sent.
- `key: str`: The message key.
- `payload: JSONValue`: The message payload as a JSON-serializable value.

### Context

Represents the context of a Kafka message, providing timer scheduling methods:

- `schedule(time: datetime) -> None`: Schedules a timer to fire at the specified time
- `clear_and_schedule(time: datetime) -> None`: Clears all timers and schedules a new one
- `unschedule(time: datetime) -> None`: Removes a timer scheduled for the specified time
- `clear_scheduled() -> None`: Removes all scheduled timers
- `scheduled() -> List[datetime]`: Returns a list of all scheduled timer times
- `should_cancel() -> bool`: Check if cancellation has been requested (includes timeout and shutdown)
- `on_cancel() -> Coroutine`: Awaitable that completes when cancellation is signaled
- `state(definition) -> ValueState[T] | MapState[V] | DequeState[T]`: Binds a registered collection for the current event attempt, returning a typed handle (message definitions vend `*State[Message[P]]`). Raises `PermanentStateError` when the name was never registered, or when the definition's `kind` / `payload` disagrees with the collection's durably-registered schema. See the [Keyed State](#keyed-state-2) API reference below.

### Timer

Represents a timer that has fired, provided to the `on_timer` method:

- `key: str`: The entity key identifying what this timer belongs to
- `time: datetime`: The time when this timer was scheduled to fire

### Keyed State

Definition constructors (each returns a frozen definition object used both in `state_collections` and with `context.state()`). Each accepts `ttl` and `read_uncommitted`, plus `keyset_limit` on the map variants:

- `value(name, *, ttl=None, read_uncommitted=None) -> ValueDefinition[T]`
- `map(name, *, ttl=None, read_uncommitted=None, keyset_limit=None) -> MapDefinition[V]`
- `deque(name, *, ttl=None, read_uncommitted=None) -> DequeDefinition[T]`
- `message_value(name, *, ttl=None, read_uncommitted=None) -> MessageValueDefinition[P]`
- `message_map(name, *, ttl=None, read_uncommitted=None, keyset_limit=None) -> MessageMapDefinition[P]`
- `message_deque(name, *, ttl=None, read_uncommitted=None) -> MessageDequeDefinition[P]`

`ValueState[T]`:

- `get() -> Optional[T]`
- `set(value: T) -> None`
- `clear() -> None`
- `commit() -> None`
- `rollback() -> None`

`MapState[V]` (keys are `str`):

- `get(key: str) -> Optional[V]`
- `get_many(keys: List[str]) -> List[Optional[V]]`
- `set(key: str, value: V) -> None`
- `remove(key: str) -> None`
- `clear() -> None`
- `items(direction=Direction.FORWARD)` — async iterator over `(str, V)` entries
- `keys()` — async iterator over `str` keys (forward-only)
- `values()` — async iterator over `V` values (forward-only)
- `__aiter__()` — forward async iteration over `(str, V)` entries
- `commit() -> None`
- `rollback() -> None`

`DequeState[T]`:

- `append(item: T) -> None`
- `appendleft(item: T) -> None`
- `pop() -> Optional[T]`
- `popleft() -> Optional[T]`
- `size() -> int`
- `is_empty() -> bool`
- `get(index: int) -> Optional[T]`
- `values(direction=Direction.FORWARD)` — async iterator over `T` elements
- `__aiter__()` — forward async iteration over `T` elements
- `commit() -> None`
- `rollback() -> None`

`Direction`: an enum with `Direction.FORWARD` and `Direction.BACKWARD`.

Errors:

- `StateError`: brand mixin on every keyed-state error; catch all of them with `except StateError`.
- `TransientStateError` (subclasses `TransientError`): the default — a temporary store read/write failure, or any caller mistake (a `None`/unrepresentable write, item-shape mismatch, invalid scan direction, malformed definition), rejected transient so it retries rather than discarding the message.
- `NullValueError` (subclasses `TransientStateError` and `ValueError`): a `None` / JSON-`null` write; use `clear()` / `remove(key)` to delete instead.
- `PermanentStateError` (subclasses `PermanentError`): reserved for failures a retry cannot resolve in-process (unregistered / identity-mismatched collection, duplicate registration), or one a handler raises explicitly.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
