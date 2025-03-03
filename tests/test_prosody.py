import asyncio
from typing import List

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from prosody.prosody import AdminClient

from prosody import ProsodyClient, EventHandler, Message, Context, permanent, transient

provider = TracerProvider()

# Sets the global default tracer provider
trace.set_tracer_provider(provider)

# Creates a tracer from the global tracer provider
tracer = trace.get_tracer("prosody-test")


class TestHandler(EventHandler):
    __test__ = False

    def __init__(self):
        self.messages: List[Message] = []
        self.message_count = 0
        self.message_received = asyncio.Event()

    async def on_message(self, context: Context, message: Message) -> None:
        with tracer.start_as_current_span("receive"):
            self.messages.append(message)
            self.message_count += 1
            self.message_received.set()


@pytest.fixture(scope="session", autouse=True)
async def run_before_and_after_tests():
    admin = AdminClient("localhost:9094")
    await admin.create_topic("test-topic", 4, 1)
    yield
    await admin.delete_topic("test-topic")


@pytest.fixture
async def client():
    client = ProsodyClient(
        bootstrap_servers="localhost:9094",
        source_system="test-send",
        group_id="test-group",
        subscribed_topics="test-topic",
        probe_port=None,
    )

    yield client

    if client.consumer_state() == "running":
        await client.unsubscribe()


async def test_client_initialization(client):
    assert isinstance(client, ProsodyClient)
    assert client.consumer_state() == "configured"


async def test_client_subscribe_unsubscribe(client):
    handler = TestHandler()
    client.subscribe(handler)
    assert client.consumer_state() == "running"

    await client.unsubscribe()
    assert client.consumer_state() == "configured"


async def test_send_and_receive_message(client):
    handler = TestHandler()
    client.subscribe(handler)

    # Send a test message
    test_topic = "test-topic"
    test_key = "test-key"
    test_payload = {"content": "Hello, Kafka!"}
    with tracer.start_as_current_span("send"):
        await client.send(test_topic, test_key, test_payload)

    # Wait for the message to be received
    await asyncio.wait_for(handler.message_received.wait(), timeout=30.0)

    # Check if the message was received
    assert len(handler.messages) == 1
    received_message = handler.messages[0]
    assert received_message.topic == test_topic
    assert received_message.key == test_key
    assert received_message.payload == test_payload


async def test_client_configuration():
    client = ProsodyClient(
        bootstrap_servers=["localhost:9092", "localhost:9093"],
        source_system="test-send",
        group_id="test-group",
        subscribed_topics=["topic1", "topic2"],
        max_uncommitted=1000,
        max_enqueued_per_key=100,
        poll_interval=0.1,
        commit_interval=5.0,
        mode="low-latency",
        retry_base=2,
        max_retries=5,
        failure_topic="failed-messages",
        mock=True
    )
    assert isinstance(client, ProsodyClient)


async def test_multiple_messages(client):
    handler = TestHandler()
    client.subscribe(handler)

    # Send multiple test messages
    test_topic = "test-topic"
    messages = [
        ("key1", {"content": "Message 1"}),
        ("key2", {"content": "Message 2"}),
        ("key3", {"content": "Message 3"})
    ]

    with tracer.start_as_current_span("send_multiple"):
        for key, payload in messages:
            await client.send(test_topic, key, payload)

    # Wait for all messages to be received
    async def wait_for_messages():
        while handler.message_count < len(messages):
            await handler.message_received.wait()
            handler.message_received.clear()

    await asyncio.wait_for(wait_for_messages(), timeout=30.0)

    # Check if all messages were received
    assert len(handler.messages) == len(messages)

    # Create sets of expected and received messages for comparison
    expected_messages = set((key, frozenset(payload.items())) for key, payload in messages)
    received_messages = set((msg.key, frozenset(msg.payload.items())) for msg in handler.messages)

    # Check if all sent messages were received, regardless of order
    assert expected_messages == received_messages

    # Verify that all messages have the correct topic
    assert all(msg.topic == test_topic for msg in handler.messages)


async def test_same_key_message_order(client):
    handler = TestHandler()

    # Send multiple test messages with the same key
    test_topic = "test-topic"
    test_key = "same-key"
    messages = [
        {"content": "Message 1", "sequence": 1},
        {"content": "Message 2", "sequence": 2},
        {"content": "Message 3", "sequence": 3},
        {"content": "Message 4", "sequence": 4},
        {"content": "Message 5", "sequence": 5},
    ]

    with tracer.start_as_current_span("send_same_key_messages"):
        for payload in messages:
            await client.send(test_topic, test_key, payload)

    # Subscribe after messages are already on the topic
    client.subscribe(handler)

    # Wait for all messages to be received
    async def wait_for_messages():
        while handler.message_count < len(messages):
            await handler.message_received.wait()
            handler.message_received.clear()

    await asyncio.wait_for(wait_for_messages(), timeout=30.0)

    # Check if all messages were received
    assert len(handler.messages) == len(messages)

    # Check if messages with the same key were received in the order they were sent
    received_messages = [msg for msg in handler.messages if msg.key == test_key]
    received_sequences = [msg.payload["sequence"] for msg in received_messages]
    expected_sequences = [msg["sequence"] for msg in messages]

    assert received_sequences == expected_sequences, f"Expected sequence {expected_sequences}, but got {received_sequences}"

    # Verify that all messages have the correct topic and key
    for msg in received_messages:
        assert msg.topic == test_topic
        assert msg.key == test_key


class TransientErrorHandler(EventHandler):
    def __init__(self):
        self.received_message = False
        self.retry_event = asyncio.Event()

    @transient(ValueError)
    async def on_message(self, context: Context, message: Message) -> None:
        if self.received_message:
            self.retry_event.set()
        else:
            self.received_message = True
            raise ValueError("Transient error occurred")


@pytest.mark.asyncio
async def test_transient_error_decorator(client):
    handler = TransientErrorHandler()
    client.subscribe(handler)

    # Send a test message
    test_topic = "test-topic"
    test_key = "test-key"
    test_payload = {"content": "Trigger transient error"}

    await client.send(test_topic, test_key, test_payload)

    # Wait for the message to be retried
    await asyncio.wait_for(handler.retry_event.wait(), timeout=30.0)


class PermanentErrorHandler(EventHandler):
    def __init__(self):
        self.error_raised = asyncio.Event()
        self.message_count = 0

    @permanent(ValueError)
    async def on_message(self, context: Context, message: Message) -> None:
        self.message_count += 1
        self.error_raised.set()
        raise ValueError("Permanent error occurred")


@pytest.mark.asyncio
async def test_permanent_error_decorator(client):
    handler = PermanentErrorHandler()
    client.subscribe(handler)

    # Send a test message
    test_topic = "test-topic"
    test_key = "test-key"
    test_payload = {"content": "Trigger permanent error"}

    await client.send(test_topic, test_key, test_payload)

    # Wait for the error to be raised
    await asyncio.wait_for(handler.error_raised.wait(), timeout=30.0)

    # Wait a bit to allow for any potential retries
    await asyncio.sleep(5)

    # Check that the message was processed only once
    assert handler.message_count == 1


@pytest.mark.asyncio
async def test_best_effort_mode_does_not_retry(client):
    handler = TransientErrorHandler()

    # Configure client for "best effort" mode, ensuring it doesn't do retries
    client_with_best_effort = ProsodyClient(
        bootstrap_servers="localhost:9094",
        source_system="test-send",
        group_id="test-group",
        subscribed_topics="test-topic",
        mode="best-effort"
    )

    client_with_best_effort.subscribe(handler)

    # Send a test message
    test_topic = "test-topic"
    test_key = "test-key"
    test_payload = {"content": "Trigger transient error"}

    await client_with_best_effort.send(test_topic, test_key, test_payload)

    # Allow time for processing
    await asyncio.sleep(5)

    # Check that the transient error was raised only once
    assert handler.retry_event.is_set() == False

    await client_with_best_effort.unsubscribe()


if __name__ == "__main__":
    pytest.main()
