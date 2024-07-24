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

client = ProsodyClient(
    bootstrap_servers="localhost:9092",
    group_id="my-consumer-group",
    subscribed_topics="my-topic"
)


class MyHandler(AbstractMessageHandler):
    async def handle(self, context: Context, message: Message) -> None:
        print(f"Received message: {message}")


client.subscribe(MyHandler())

await client.send("my-topic", "message-key", {"content": "Hello, Kafka!"})

# Ensure proper shutdown
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
client = ProsodyClient(
    bootstrap_servers="localhost:9092",
    mode="pipeline",
    group_id="my-consumer-group",
    subscribed_topics="my-topic"
)
```

### Low-Latency Mode

Prioritizes quick processing, sending persistently failing messages to a failure topic:

```python
client = ProsodyClient(
    bootstrap_servers="localhost:9092",
    mode="low-latency",
    group_id="my-consumer-group",
    subscribed_topics="my-topic",
    failure_topic="failed-messages"
)
```

## Best Practices

### Ensuring Idempotent Message Handlers

Idempotent message handlers are crucial for maintaining data consistency, fault tolerance, and scalability when working
with Kafka and Prosody. They ensure that processing a message multiple times has the same effect as processing it once,
which is essential for handling redeliveries, restarts, and rebalancing.

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
await client.unsubscribe()
```

This ensures:

1. Completion and commitment of all in-flight work
2. Quick rebalancing, allowing other consumers to take over partitions
3. Proper release of resources

Implement shutdown handling in your application:

```python
import signal
import asyncio


async def shutdown(signal, loop):
    print(f"Received exit signal {signal.name}...")
    await client.unsubscribe()
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()


loop = asyncio.get_event_loop()
signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
for s in signals:
    loop.add_signal_handler(
        s, lambda s=s: asyncio.create_task(shutdown(s, loop)))
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
