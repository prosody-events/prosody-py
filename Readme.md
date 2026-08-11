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
- **Type Checking**: PEP 561 type information for mypy and other Python type checkers

## Installation

Prosody supports Python 3.10 and above, including free-threaded builds (3.14t). Install from PyPI:

```bash
pip install prosody-events
```

The wheel includes a `py.typed` marker and type information for the public API,
so applications can type-check normal `prosody` imports without installing a
separate stub package. For example, run `mypy your_application/` after installing
Prosody and mypy. Keyed-state definitions carry their declared value type through
`Context.state(...)`. `EventHandler[Payload]` carries a declared structural JSON
payload type through `on_message`; an unsubscripted handler defaults to
`JSONValue`. See [Keyed State](#keyed-state-cassandra) for typed examples.

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

# Shut down all client services when done
await client.shutdown()
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

For the complete configuration reference, see [CONFIGURATION.md](CONFIGURATION.md).

Constructor options take precedence. Unset options use environment variables, then library defaults.

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
    probe_port=8000,  # Explicitly pass None to disable
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
    source_system="my-service-producer",  # Must differ from group_id to allow loopbacks; defaults to group_id
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

Consumer deduplication is required for keyed-state commits. The client rejects an `idempotence_cache_size` of zero:

```python
client = ProsodyClient(
    group_id="my-consumer-group",
    subscribed_topics="my-topic",
    idempotence_cache_size=0  # Rejected
)
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

Keyed state gives every Kafka key its own durable working memory. Prosody automatically uses the current message or timer key, so a handler can relate the current event to earlier events for that key. State survives restarts and rebalances. By default, changes become visible only when the event succeeds.

Use keyed state for time-aware stream processing: counters, deduplication, rolling aggregates, pending work, and per-key workflows. Keep your relational database as the source of truth for business data and for work that needs joins or ad hoc queries. Reconstructing stream state with repeated database queries can be slow and expensive; keyed state is built for that job.

Most collections should have a TTL. Set it comfortably beyond the longest timer or workflow that uses the state; Prosody validates the minimum supported TTL. Omit it only when keeping inactive keys forever is intentional.

### Published state

Published state lets another client read a JSON value, map, or deque without subscribing to the owner's topics. Use the same typed definition for the owned collection and its read-only view. The owner sets `published=True`, names its `subsystem`, and registers the definition as usual:

```python
CURRENT_ORDER: ValueDefinition[dict[str, str]] = value("current-order", published=True)
owner = ProsodyClient(
    **config,
    subsystem="checkout",
    state_collections=[CURRENT_ORDER],
)

# Inside the owner's handler, the event supplies the user key.
current_order = context.state(CURRENT_ORDER)
await current_order.set({"sku": "book"})
```

Another client opens a reader by naming the subsystem and passing that same definition. The reader is independent of subscriptions and only returns committed state:

```python
order_reader = await client.state("checkout", CURRENT_ORDER)
current_order = await order_reader.get("customer-123")
```

Published readers provide the owned collection's read operations without its mutations. An owned handle gets the user key from the current event; a published reader is outside a handler, so every operation takes that key explicitly. Map and deque iteration is asynchronous and reads in chunks rather than loading the entire collection.

The default cache window is five seconds unless the client configuration changes it. Set `read_cache=timedelta(...)` on a definition to choose a different freshness window, or `read_cache=False` to read durable storage on every operation. To stop publishing a collection, deploy its definition with `published=False` while keeping it registered and retaining `subsystem` for that deployment.

### A counter for each key

Declare each collection once, register it on the client, and ask the event context for the current key's state:

```python
COUNTER: ValueDefinition[int] = value("counter", ttl=timedelta(days=30))


class CountHandler(EventHandler):
    async def on_message(self, context: Context, message: Message) -> None:
        count = context.state(COUNTER)
        await count.set((await count.get() or 0) + 1)


client = ProsodyClient(
    group_id="counters",
    subscribed_topics="events",
    state_collections=[COUNTER],
)
```

Here, counters expire after 30 days without an update.

### Window activity into one notification

This example turns a burst of activity into two useful notifications. It sends the first event immediately, collects later events for five minutes, then sends one summary. Because the user ID is the Kafka key, every user gets an independent window.

```python
WINDOW: ValueDefinition[bool] = value("window", ttl=timedelta(days=1))
PENDING: MessageDequeDefinition[Activity] = message_deque(
    "pending", capacity=100, ttl=timedelta(days=1)
)


class BatchHandler(EventHandler[Activity]):
    async def on_message(self, context: Context, message: Message[Activity]) -> None:
        window = context.state(WINDOW)
        pending = context.state(PENDING)

        if await window.get():
            await pending.append(message)
            return

        await notify(message.key, [message])
        await window.set(True)
        await context.clear_and_schedule(
            datetime.now(timezone.utc) + timedelta(minutes=5)
        )

    async def on_timer(self, context: Context, timer: Timer) -> None:
        pending = context.state(PENDING)
        batch = [message async for message in pending.values()]

        if batch:
            await notify(timer.key, batch)
        await pending.clear()
        await context.state(WINDOW).clear()
```

See the complete, mypy-checked example for imports, types, client setup, and `notify`: [`examples/keyed_state_windowing.py`](examples/keyed_state_windowing.py).

Why this works:

- Register both definitions in `state_collections` before subscribing. Keyed state uses Cassandra unless `mock=True`.
- Use `clear_and_schedule`, not `schedule`, so a retried event does not add another timer for the same key.
- `capacity=100` and the one-day TTL prevent an inactive or unusually busy key from retaining an unlimited backlog. Since this example only appends, overflow drops the oldest saved message.
- A `message_deque` requires the original Kafka messages to remain available for the whole window. Use a plain `deque` of payloads if topic retention or compaction cannot guarantee that.
- Prosody runs one handler at a time for each key, so a user's message and timer handlers cannot overlap.
- Sending a notification is outside Prosody's state transaction and may happen again after a retry. Give notifications a stable idempotency key, or send them through an outbox, when duplicates matter.

### Collections and handles

A definition gives a collection a stable name, kind, and options. Register it once on the client, then pass the same definition to `context.state()` to access the current key. Do not reuse a persisted name for a different collection kind or payload type.

Create handles inside the handler and do not retain them or their iterators afterward.

| Collection | JSON payload | Kafka message | Main operations |
| --- | --- | --- | --- |
| Value | `value` | `message_value` | `get`, `set`, `clear` |
| Ordered string map | `map` | `message_map` | `get`, `get_many`, `contains`, `set`, `remove`, `items`, `keys`, `clear` |
| Deque | `deque` | `message_deque` | `append`, `appendleft`, `pop`, `popleft`, `get`, `size`, `values`, `clear` |

All operations are async. Map and deque scans use `async for`. Map keys are strings. `None` means absence and cannot be stored—use `clear()` or `remove()` instead. Payload annotations guide the type checker but do not validate data at runtime.

### When changes become visible

Reads inside a handler see its earlier writes. The default behavior is the safest choice for most handlers: Prosody buffers those changes and publishes them together when the event succeeds. If the handler raises, none of its pending changes become visible.

Each collection also offers explicit controls for workflows that need different behavior:

- `read_uncommitted=True` writes that collection's changes after the handler succeeds but before the event is recorded as complete. A crash in between can leave the changes visible even though the event is retried. Use it only for idempotent changes, where processing the same event again produces the same stored result.
- `await state.commit()` immediately publishes this collection's pending changes. They remain visible even if the handler later raises and the event is retried.
- `await state.rollback()` discards this collection's pending changes since its last `commit()`. It cannot undo changes that were already committed.

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

Shut down the client before your application exits:

```python
await client.shutdown()
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

    await client.shutdown()


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

## Peer Requests

Peer requests collect one result from each named subsystem. The result order matches the subsystem order.

Return a JSON response from each message handler:

```python
class InventoryHandler(EventHandler):
    async def on_message(self, context, message):
        return {"accepted": message.key}
```

Send a request without a subscription on the requester:

```python
from prosody import Err, Ok

results = await client.request(
    "orders",
    "order-1",
    {"type": "order.created"},
    ["inventory", "billing"],
    2.0,
)

for result in results:
    if isinstance(result, Ok):
        print(result.value)
    elif isinstance(result, Err):
        print(result.error)
```

Each error identifies a handler failure, timeout, format mismatch, or malformed response. Handler failures also include their category and message.

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
- `send(topic: str, key: str, payload: JSONValue) -> None`: Send a JSON-serializable message.
- `request(topic, key, payload, subsystems, timeout, headers=None) -> Sequence[RequestResult[JSONValue]]`: Request one response from each subsystem.
- `consumer_state() -> str`: Get the current state of the consumer.
- `state(subsystem: str, definition: ValueDefinition[T]) -> PublishedValue[T]`: Open a read-only published value.
- `state(subsystem: str, definition: MapDefinition[V]) -> PublishedMap[V]`: Open a read-only published map.
- `state(subsystem: str, definition: DequeDefinition[T]) -> PublishedDeque[T]`: Open a read-only published deque.
- `subscribe(handler: EventHandler[P]) -> None`: Subscribe while preserving the handler's payload specialization.
- `unsubscribe() -> None`: Stop the consumer. You can subscribe again later.
- `shutdown() -> None`: Stop the consumer and all client services.

### AdminClient

- `__init__(**config)`: Initialize a new AdminClient with the given configuration.
- `create_topic(name: str, **config) -> None`: Create a Kafka topic with optional configuration parameters.
- `delete_topic(name: str) -> None`: Delete an existing Kafka topic.

### EventHandler

An abstract base class generic over the message payload type. The payload type
defaults to `JSONValue`, so existing unsubscripted handlers retain their current
typing. Parameterizing the handler gives `on_message` the same payload type:

```python
P = TypeVar("P", default=JSONValue)

class EventHandler(ABC, Generic[P]):
    @abstractmethod
    async def on_message(self, context: Context, message: Message[P]) -> None:
        # Implement your message handling logic here
        pass
    
    @abstractmethod
    async def on_timer(self, context: Context, timer: Timer) -> None:
        # Implement your timer handling logic here
        pass
```

For example, `EventHandler[OrderEvent]` receives `Message[OrderEvent]` when
`OrderEvent` is a `TypedDict`. This is a static contract only: Prosody still
delivers plain JSON and does not construct or validate dataclass or Pydantic
models. Validate the payload explicitly before using such a model.

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
- `payload: P`: The statically typed message payload.

`Message[P]` defaults to `Message[JSONValue]`. Supplying a `TypedDict` payload
specialization gives field-level checking without runtime model construction or
validation.

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

Definition constructors return frozen objects used both in `state_collections` and with `context.state()`. JSON definitions accept `published` and `read_cache` in addition to the options below:

- `value(name, *, ttl=None, read_uncommitted=None, published=None, read_cache=None) -> ValueDefinition[T]`
- `map(name, *, ttl=None, read_uncommitted=None, published=None, read_cache=None, keyset_limit=None) -> MapDefinition[V]`
- `deque(name, *, ttl=None, read_uncommitted=None, published=None, read_cache=None, capacity=None) -> DequeDefinition[T]`
- `message_value(name, *, ttl=None, read_uncommitted=None) -> MessageValueDefinition[P]`
- `message_map(name, *, ttl=None, read_uncommitted=None, keyset_limit=None) -> MessageMapDefinition[P]`
- `message_deque(name, *, ttl=None, read_uncommitted=None, capacity=None) -> MessageDequeDefinition[P]`

`ValueState[T]`:

- `get() -> Optional[T]`
- `set(value: T) -> None`
- `clear() -> None`
- `commit() -> None`
- `rollback() -> None`

`MapState[V]` (keys are `str`):

- `get(key: str, default=None) -> Optional[V] | default` — default only on absence
- `contains(key: str) -> bool` — cheap presence check (no decode/resolver)
- `get_many(keys: List[str]) -> List[Optional[V]]`
- `set(key: str, value: V) -> None`
- `remove(key: str) -> None`
- `clear() -> None`
- `items(direction=Direction.FORWARD)` — async iterator over `(str, V)` entries
- `keys(direction=Direction.FORWARD)` — cheap async iterator over `str` keys
- `values()` — async iterator over `V` values (forward-only)
- `__aiter__()` — forward async iteration over `str` keys (like `dict`)
- `commit() -> None`
- `rollback() -> None`

`DequeState[T]`:

- `append(item: T) -> None`
- `appendleft(item: T) -> None`
- `pop() -> Optional[T]`
- `popleft() -> Optional[T]`
- `peek() -> Optional[T]` — back endpoint, non-destructive
- `peekleft() -> Optional[T]` — front endpoint, non-destructive
- `size() -> int`
- `is_empty() -> bool`
- `clear() -> None`
- `get(index: int) -> Optional[T]`
- `values(direction=Direction.FORWARD)` — async iterator over `T` elements
- `__aiter__()` — forward async iteration over `T` elements
- `commit() -> None`
- `rollback() -> None`

`Direction`: an enum with `Direction.FORWARD` and `Direction.BACKWARD`.

Published readers take the user key as their first argument. `PublishedValue[T]` provides `get`. `PublishedMap[V]` provides `get`, `get_many`, `contains`, `items`, `keys`, and `values`. `PublishedDeque[T]` provides `get`, `size`, `is_empty`, `peek`, `peekleft`, and `values`. `items`, `keys`, and `values` return async iterators directly.

Errors:

- `StateError`: brand mixin on every keyed-state error; catch all of them with `except StateError`.
- `TransientStateError` (subclasses `TransientError`): the default — a temporary store read/write failure, or any caller mistake (a `None`/unrepresentable write, item-shape mismatch, invalid scan direction, malformed definition), rejected transient so it retries rather than discarding the message.
- `NullValueError` (subclasses `TransientStateError` and `ValueError`): a `None` / JSON-`null` write; use `clear()` / `remove(key)` to delete instead.
- `PermanentStateError` (subclasses `PermanentError`): reserved for failures a retry cannot resolve in-process (unregistered / identity-mismatched collection, duplicate registration), or one a handler raises explicitly.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
