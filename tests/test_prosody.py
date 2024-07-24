import asyncio
from typing import List

import pytest

from prosody import ProsodyClient, AbstractMessageHandler, Message, Context


class TestHandler(AbstractMessageHandler):
    __test__ = False

    def __init__(self):
        self.messages: List[Message] = []
        self.message_count = 0
        self.message_received = asyncio.Event()

    async def handle(self, context: Context, message: Message) -> None:
        self.messages.append(message)
        self.message_count += 1
        self.message_received.set()


@pytest.fixture
async def client():
    client = ProsodyClient(
        bootstrap_servers="localhost:9094",
        group_id="test-group",
        subscribed_topics="test-topic",
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
    await client.send(test_topic, test_key, test_payload)

    # Wait for the message to be received
    await asyncio.wait_for(handler.message_received.wait(), timeout=5.0)

    # Check if the message was received
    assert len(handler.messages) == 1
    received_message = handler.messages[0]
    assert received_message.topic() == test_topic
    assert received_message.key() == test_key
    assert received_message.payload() == test_payload


async def test_client_configuration():
    client = ProsodyClient(
        bootstrap_servers=["localhost:9092", "localhost:9093"],
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

    for key, payload in messages:
        await client.send(test_topic, key, payload)

    # Wait for all messages to be received
    async def wait_for_messages():
        while handler.message_count < len(messages):
            await handler.message_received.wait()
            handler.message_received.clear()

    await asyncio.wait_for(wait_for_messages(), timeout=5.0)

    # Check if all messages were received
    assert len(handler.messages) == len(messages)
    for i, (key, payload) in enumerate(messages):
        received_message = handler.messages[i]
        assert received_message.topic() == test_topic
        assert received_message.key() == key
        assert received_message.payload() == payload


if __name__ == "__main__":
    pytest.main()
