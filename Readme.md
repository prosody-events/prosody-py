# Prosody: Python Bindings for High-Performance Kafka Client

Prosody is a high-performance Python library that provides bindings to a Rust-based Kafka client. It offers a robust
interface for producing and consuming Kafka messages with advanced features like configurable retry mechanisms, failure
handling strategies, and integrated OpenTelemetry support for distributed tracing.

## Features

- High-performance Kafka client powered by Rust
- Support for both message production and consumption
- Configurable operational modes: pipeline and low-latency
- Customizable retry mechanisms and failure handling
- Distributed tracing with OpenTelemetry integration
- Efficient parallel processing with key-based ordering
- Backpressure management through intelligent partition pausing
- Mock Kafka broker support for testing

## Installation

```bash
pip install prosody
```

## Quick Start

### Producing Messages

```python
from prosody import ProsodyClient

# Initialize the client
client = ProsodyClient(bootstrap_servers="localhost:9092")

# Send a message
await client.send("my-topic", "message-key", {"content": "Hello, Kafka!"})
```

### Consuming Messages

```python
from prosody import ProsodyClient, AbstractMessageHandler, Context, Message


class MyHandler(AbstractMessageHandler):
    async def handle(self, context: Context, message: Message) -> None:
        print(f"Received message: {message}")


# Initialize the client
client = ProsodyClient(
    bootstrap_servers="localhost:9092",
    group_id="my-consumer-group",
    subscribed_topics="my-topic"
)

# Subscribe to messages
client.subscribe(MyHandler())

# To stop consuming
await client.unsubscribe()
```

## Configuration

The `ProsodyClient` constructor accepts various configuration options. These can be set either through constructor
parameters or environment variables. Here's a table of available options:

| Parameter                    | Environment Variable                 | Default    | Description                                  |
|------------------------------|--------------------------------------|------------|----------------------------------------------|
| `bootstrap_servers`          | `PROSODY_BOOTSTRAP_SERVERS`          | -          | Kafka bootstrap servers                      |
| `group_id`                   | `PROSODY_GROUP_ID`                   | -          | Consumer group ID                            |
| `subscribed_topics`          | `PROSODY_SUBSCRIBED_TOPICS`          | -          | Topics to subscribe to                       |
| `max_uncommitted`            | `PROSODY_MAX_UNCOMMITTED`            | 32         | Maximum number of uncommitted messages       |
| `max_enqueued_per_key`       | `PROSODY_MAX_ENQUEUED_PER_KEY`       | 8          | Maximum number of enqueued messages per key  |
| `partition_shutdown_timeout` | `PROSODY_PARTITION_SHUTDOWN_TIMEOUT` | 5s         | Timeout for partition shutdown               |
| `poll_interval`              | `PROSODY_POLL_INTERVAL`              | 100ms      | Interval between poll operations             |
| `commit_interval`            | `PROSODY_COMMIT_INTERVAL`            | 1s         | Interval between commit operations           |
| `send_timeout`               | `PROSODY_SEND_TIMEOUT`               | 1s         | Timeout for send operations in the producer  |
| `mock`                       | `PROSODY_MOCK`                       | False      | Use mock Kafka brokers for testing           |
| `retry_base`                 | `PROSODY_RETRY_BASE`                 | 2          | Exponential backoff base for retries         |
| `max_retries`                | `PROSODY_MAX_RETRIES`                | 3          | Maximum number of retries                    |
| `max_retry_delay`            | `PROSODY_RETRY_MAX_DELAY`            | 1m         | Maximum retry delay                          |
| `failure_topic`              | `PROSODY_FAILURE_TOPIC`              | -          | Topic for failed messages                    |
| `mode`                       | -                                    | "pipeline" | Operating mode ('pipeline' or 'low-latency') |

Note: Time durations can be specified as integers (interpreted as milliseconds) or as strings (e.g., "5s", "100ms", "
1m").

## Advanced Usage

### Pipeline Mode

Pipeline mode is the default mode. It ensures all messages are processed or sent in order, retrying failed operations
indefinitely:

```python
client = ProsodyClient(
    bootstrap_servers="localhost:9092",
    retry_base=2,
    max_retries=5,
    max_retry_delay="1m"
)
```

### Low-Latency Mode

Low-latency mode prioritizes quick processing, sending persistently failing messages to a failure topic:

```python
client = ProsodyClient(
    bootstrap_servers="localhost:9092",
    mode="low-latency",
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
- `key() -> str`: Returns the key of the message.
- `payload() -> Any`: Returns the payload of the message.
