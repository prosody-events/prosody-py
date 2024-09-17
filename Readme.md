# Prosody: Python Bindings for Kafka

Prosody offers Python bindings to the [Prosody Kafka client](https://github.com/RealGeeks/prosody), providing
features for message production and consumption, including configurable retry mechanisms, failure handling
strategies, and integrated OpenTelemetry support for distributed tracing.

## Features

- Rust-powered Kafka client
- Message production and consumption support
- Configurable modes: pipeline and low-latency
- OpenTelemetry integration for distributed tracing
- Efficient parallel processing with key-based ordering
- Intelligent partition pausing for backpressure management
- Mock Kafka broker support for testing

## Installation

```bash
pip install prosody
```

## Quick Start

```python
from prosody import ProsodyClient, EventHandler, Context, Message

# Initialize the client with Kafka bootstrap servers, consumer group, and topics
client = ProsodyClient(
    bootstrap_servers="localhost:9092",
    group_id="my-consumer-group",
    subscribed_topics="my-topic"
)


# Define a custom message handler
class MyHandler(EventHandler):
    async def on_message(self, context: Context, message: Message) -> None:
        # Process the received message
        print(f"Received message: {message}")


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
- `mode` (str): 'pipeline' (default) or 'low-latency'

Additional optional parameters control behavior like message committal, polling intervals, and retry logic. Most
parameters can be set via environment variables (e.g., `PROSODY_BOOTSTRAP_SERVERS`).

Refer to the API documentation for detailed information on all parameters and their default values.

## Advanced Usage

### Pipeline Mode

Pipeline mode is the default mode. Ensures ordered processing, retrying failed operations indefinitely:

```python
# Initialize client in pipeline mode
client = ProsodyClient(
    bootstrap_servers="localhost:9092",
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
    bootstrap_servers="localhost:9092",
    mode="low-latency",  # Set low-latency mode
    group_id="my-consumer-group",
    subscribed_topics="my-topic",
    failure_topic="failed-messages"  # Specify a topic for failed messages
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
- Any exception, including `asyncio.CancelledError`, signals task failure.

Best practices:

1. Exit promptly when cancelled to avoid rebalancing delays.
2. Always propagate `asyncio.CancelledError`.
3. Use `try/finally` or context managers for clean resource handling.

Failing to follow these practices can lead to:

- Slower message processing due to delayed rebalancing.
- Data loss from missed messages when cancellation errors are suppressed.

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

## API Reference

### ProsodyClient

- `__init__(**config)`: Initialize a new ProsodyClient with the given configuration.
- `send(topic: str, key: str, payload: Any) -> None`: Send a message to a specified topic.
- `consumer_state() -> str`: Get the current state of the consumer.
- `subscribe(handler: EventHandler) -> None`: Subscribe to messages using the provided handler.
- `unsubscribe() -> None`: Unsubscribe from messages and shut down the consumer.

### EventHandler

An abstract base class for user-defined handlers:

```python
class EventHandler(ABC):
    @abstractmethod
    async def on_message(self, context: Context, message: Message) -> None:
        # Implement your message handling logic here
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
