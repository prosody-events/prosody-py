# Prosody: Python Bindings for Kafka

Prosody offers Python bindings to the [Prosody Kafka client](https://github.com/cincpro/prosody), providing
features for message production and consumption, including configurable retry mechanisms, failure handling
strategies, and integrated OpenTelemetry support for distributed tracing.

## Features

- Rust-powered Kafka client
- Message production and consumption support
- Configurable modes: pipeline, low-latency, and best-effort
- OpenTelemetry integration for distributed tracing
- Efficient parallel processing with key-based ordering
- Intelligent partition pausing for backpressure management
- Mock Kafka broker support for testing
- Event type filtering for selectively processing messages
- Source system tracking to prevent message processing loops

## Installation

Installation is straightforward using pip, but you will need to have pip point at the Gemfury repository with either 
an argument or an environment variable. Prosody currently supports Python 3.11 to 3.13

```bash
pip install prosody --extra-index-url https://pypi.fury.io/<OUR GEMFURY SECRET>/realgeeks/
```

Or

```bash
export PIP_EXTRA_INDEX_URL=https://pypi.fury.io/<OUR GEMFURY SECRET>/realgeeks/
pip install prosody
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

## Configuration

The `ProsodyClient` constructor accepts these key parameters:

- `bootstrap_servers` (str | list[str]): Kafka bootstrap servers (required)
- `group_id` (str): Consumer group ID (required for consumption)
- `subscribed_topics` (str | list[str]): Topics to subscribe to (required for consumption)
- `source_system` (str): Identifier for the producing system to prevent loops (defaults to group_id)
- `allowed_events` (str | list[str]): Prefixes of event types to process (processes all if unspecified)
- `mode` (str): 'pipeline' (default), 'low-latency', or 'best-effort'

Additional optional parameters control behavior like message committal, polling intervals, and retry logic. Most
parameters can be set via environment variables (e.g., `PROSODY_BOOTSTRAP_SERVERS`).

Refer to the API documentation for detailed information on all parameters and their default values.

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

Prosody automatically deduplicates messages using the `id` field in their JSON payload. Consecutive messages with the
same ID and key are processed only once.

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

Deduplication can be disabled by setting:

```python
client = ProsodyClient(
    group_id="my-consumer-group",
    subscribed_topics="my-topic",
    idempotence_cache_size=0  # Disable deduplication
)
```

Or via environment variable:

```bash
PROSODY_IDEMPOTENCE_CACHE_SIZE=0
```

Note that this deduplication is best-effort and not guaranteed. Because identifiers are cached ephemerally in memory,
duplicates can still occur when instances rebalance or restart.

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

Prosody cancels tasks during partition rebalancing or shutdown. How you handle cancellation is critical:

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
    - Linux builds for x86_64, aarch64, and armv7 architectures.
    - MuslLinux builds for the same architectures.
    - Windows build for x64 architecture.
    - macOS builds for x86_64 and aarch64 architectures.
    - Source distribution (sdist) build.

4. **Artifact Upload**: Each build job uploads its artifacts (wheels or sdist) to GitHub Actions.

5. **Publication**: If all builds are successful, the final step publishes the built artifacts to Gemfury, our private
   Python package index.

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

Remember, all releases are automatically published to Gemfury. Ensure you have thoroughly tested your changes before
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
- `should_shutdown() -> bool`: Check if shutdown has been requested

### Timer

Represents a timer that has fired, provided to the `on_timer` method:

- `key: str`: The entity key identifying what this timer belongs to
- `time: datetime`: The time when this timer was scheduled to fire
