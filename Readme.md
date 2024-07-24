# Prosody: Python Bindings

Prosody is a high-performance Python library that provides bindings to a Rust-based Kafka client. It offers a robust
interface for producing and consuming Kafka messages with advanced features like configurable retry mechanisms, failure
handling strategies, and integrated OpenTelemetry support for distributed tracing.

## Features

- High-performance Kafka client powered by Rust
- Support for both message production and consumption
- Configurable operational modes: pipeline and low-latency
- Distributed tracing with OpenTelemetry integration
- Efficient parallel processing with key-based ordering
- Backpressure management through intelligent partition pausing
- Mock Kafka broker support for testing

## Installation

```bash
pip install prosody
```

## Quick Start

```python
from prosody import ProsodyClient, AbstractMessageHandler, Context, Message

# Initialize the client
client = ProsodyClient(
    bootstrap_servers="localhost:9092",
    group_id="my-consumer-group",
    subscribed_topics="my-topic"
)


# Define a message handler
class MyHandler(AbstractMessageHandler):
    async def handle(self, context: Context, message: Message) -> None:
        print(f"Received message: {message}")


# Subscribe to messages
client.subscribe(MyHandler())

# Send a message
await client.send("my-topic", "message-key", {"content": "Hello, Kafka!"})

# To stop consuming and ensure all work is completed
await client.unsubscribe()
```

## Configuration

The `ProsodyClient` constructor accepts the following parameters:

- `bootstrap_servers` (`str | list[str]`): Kafka bootstrap servers. Required.
- `group_id` (`str`): Consumer group ID. Required for consumption.
- `subscribed_topics` (`str | list[str]`): Topics to subscribe to. Required for consumption.
- `max_uncommitted` (`int`): Maximum number of uncommitted messages.
- `max_enqueued_per_key` (`int`): Maximum number of enqueued messages per key.
- `partition_shutdown_timeout` (`float | timedelta`): Timeout for partition shutdown.
- `poll_interval` (`float | timedelta`): Interval between poll operations.
- `commit_interval` (`float | timedelta`): Interval between commit operations.
- `send_timeout` (`float | timedelta`): Timeout for send operations in the producer.
- `mock` (`bool`): Use mock Kafka brokers for testing.
- `retry_base` (`int`): Exponential backoff base for retries.
- `max_retries` (`int`): Maximum number of retries.
- `max_retry_delay` (`float | timedelta`): Maximum retry delay.
- `failure_topic` (`str`): Topic for failed messages.
- `mode` (`str`): Operating mode ('pipeline' or 'low-latency'). Default is 'pipeline'.

Key points:

- `bootstrap_servers` is required.
- `group_id` and `subscribed_topics` are required for consumption.
- Time durations (`partition_shutdown_timeout`, `poll_interval`, `commit_interval`, `send_timeout`, `max_retry_delay`)
  can be specified as float (in seconds) or timedelta.
- Most parameters have default values and can be omitted.
- All parameters except for mode can also be set via environment variables (e.g., `PROSODY_BOOTSTRAP_SERVERS`,
  `PROSODY_GROUP_ID`, etc.). Mode cannot be changed through the environment because it's inseparable from the
  application's architecture.

For detailed information about each parameter and its default value, refer to the API documentation.

## Advanced Usage

### Pipeline Mode

Pipeline mode is the default mode. It ensures all messages are processed or sent in order, retrying failed operations
indefinitely:

```python
client = ProsodyClient(
    bootstrap_servers="localhost:9092",
    mode="pipeline",
    group_id="my-consumer-group",
    subscribed_topics="my-topic"
)
```

### Low-Latency Mode

Low-latency mode prioritizes quick processing, sending persistently failing messages to a failure topic:

```python
client = ProsodyClient(
    bootstrap_servers="localhost:9092",
    mode="low-latency",
    group_id="my-consumer-group",
    subscribed_topics="my-topic",
    failure_topic="failed-messages"
)
```

## Architecture

Prosody is designed for efficient, parallel processing of Kafka messages while maintaining order for messages with the
same key. The Python bindings closely mirror the architecture of the underlying Rust library:

- **Partition-Level Parallelism**: Each Kafka partition is managed separately, allowing for parallel processing.
- **Key-Based Queuing**: Messages within a partition are further divided based on their keys, ensuring ordered
  processing for each key.
- **Concurrent Processing**: Different keys can be processed concurrently, even within the same partition.
- **Backpressure Management**: If a partition becomes backed up, Prosody will pause consumption from that specific
  partition.

## Proper Shutdown

Unsubscribe from topics before exiting your application. This ensures that:

1. All in-flight work is completed and committed.
2. Rebalancing is not delayed, allowing other consumers to quickly take over the partitions.
3. Resources are properly released.

To shut down the client, always call the `unsubscribe()` method:

```python
await client.unsubscribe()
```

Implement proper shutdown handling in your application, for example:

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

An abstract base class that user-defined handlers must subclass:

```python
class AbstractMessageHandler(ABC):
    @abstractmethod
    async def handle(self, context: Context, message: Message) -> None:
        pass
```

### Message

Represents a Kafka message with methods to access its properties:

- `topic() -> str`: Returns the topic of the message.
- `partition() -> int`: Returns the partition of the message.
- `offset() -> int`: Returns the offset of the message.
- `timestamp() -> datetime`: Returns the timestamp of the message.
- `key() -> str`: Returns the key of the message.
- `payload() -> Any`: Returns the payload of the message.
