# Prosody: High-Performance Python Bindings for Kafka

Prosody offers robust Python bindings to a [Rust-based Kafka client](https://github.com/RealGeeks/prosody), providing
advanced features for message production and consumption, including configurable retry mechanisms, failure handling
strategies, and integrated OpenTelemetry support for distributed tracing.

## Features

- Rust-powered Kafka client for superior performance
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
from prosody import ProsodyClient, AbstractMessageHandler, Context, Message

# Initialize the client with Kafka bootstrap servers, consumer group, and topics
client = ProsodyClient(
    bootstrap_servers="localhost:9092",
    group_id="my-consumer-group",
    subscribed_topics="my-topic"
)


# Define a custom message handler
class MyHandler(AbstractMessageHandler):
    async def handle(self, context: Context, message: Message) -> None:
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

## API Reference

### ProsodyClient

- `__init__(**config)`: Initialize a new ProsodyClient with the given configuration.
- `send(topic: str, key: str, payload: Any) -> None`: Send a message to a specified topic.
- `consumer_state() -> str`: Get the current state of the consumer.
- `subscribe(handler: AbstractMessageHandler) -> None`: Subscribe to messages using the provided handler.
- `unsubscribe() -> None`: Unsubscribe from messages and shut down the consumer.

### AbstractMessageHandler

An abstract base class for user-defined handlers:

```python
class AbstractMessageHandler(ABC):
    @abstractmethod
    async def handle(self, context: Context, message: Message) -> None:
        # Implement your message handling logic here
        pass
```

### Message

Represents a Kafka message with methods to access its properties:

- `topic() -> str`: Message topic
- `partition() -> int`: Message partition
- `offset() -> int`: Message offset
- `timestamp() -> datetime`: Message timestamp
- `key() -> str`: Message key
- `payload() -> Any`: Message payload
