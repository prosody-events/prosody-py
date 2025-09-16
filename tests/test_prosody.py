import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from typing import List

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from prosody.prosody import AdminClient

from prosody import ProsodyClient, EventHandler, Message, Context, Timer, permanent, transient

# Timer precision tolerance for tests (in seconds)
TIMER_TOLERANCE_SECONDS = 1

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

    async def on_timer(self, context: Context, timer: Timer) -> None:
        # Not used in tests, but required by abstract base class
        pass


@pytest.fixture
async def random_topic_and_group():
    # Generate a random topic and consumer group using a UUID to ensure isolation between tests
    topic = f"test-topic-{uuid.uuid4().hex}"
    group = f"test-group-{uuid.uuid4().hex}"
    admin = AdminClient("localhost:9094")
    await admin.create_topic(topic, 4, 1)
    await asyncio.sleep(1)
    yield topic, group
    await admin.delete_topic(topic)


@pytest.fixture
async def client(random_topic_and_group):
    topic, group = random_topic_and_group
    client = ProsodyClient(
        bootstrap_servers="localhost:9094",
        source_system="test-send",
        group_id=group,
        subscribed_topics=topic,
        probe_port=None,
        cassandra_nodes="localhost:9042",
    )
    yield client
    if await client.consumer_state() == "running":
        await client.unsubscribe()


async def test_client_initialization(client):
    # Check that the client is an instance of ProsodyClient and in configured state
    assert isinstance(client, ProsodyClient)
    assert await client.consumer_state() == "configured"


async def test_client_source_system(client):
    # Check that the source_system property returns the configured value
    assert client.source_system == "test-send"


async def test_client_subscribe_unsubscribe(client):
    # Test subscribing and unsubscribing a handler
    handler = TestHandler()
    await client.subscribe(handler)
    assert await client.consumer_state() == "running"

    await client.unsubscribe()
    assert await client.consumer_state() == "configured"


async def test_send_and_receive_message(client, random_topic_and_group):
    # Test sending and receiving a message using the random topic
    topic, _ = random_topic_and_group
    handler = TestHandler()
    await client.subscribe(handler)

    # Send a test message
    test_key = "test-key"
    test_payload = {"content": "Hello, Kafka!"}
    with tracer.start_as_current_span("send"):
        await client.send(topic, test_key, test_payload)

    # Wait for the message to be received
    await asyncio.wait_for(handler.message_received.wait(), timeout=30.0)

    # Check if the message was received
    assert len(handler.messages) == 1
    received_message = handler.messages[0]
    assert received_message.topic == topic
    assert received_message.key == test_key
    assert received_message.payload == test_payload


async def test_client_configuration(random_topic_and_group):
    # Test creating a client with additional configuration options
    topic, group = random_topic_and_group
    client = ProsodyClient(
        bootstrap_servers=["localhost:9092", "localhost:9093"],
        source_system="test-send",
        group_id=group,
        subscribed_topics=[topic],
        max_uncommitted=1000,
        max_enqueued_per_key=100,
        poll_interval=0.1,
        commit_interval=5.0,
        mode="low-latency",
        retry_base=2,
        max_retries=5,
        failure_topic="failed-messages",
        probe_port=None,
        cassandra_nodes=["localhost:9042", "localhost:9043"],
        mock=True
    )
    assert isinstance(client, ProsodyClient)


async def test_multiple_messages(client, random_topic_and_group):
    # Test sending and receiving multiple messages
    topic, _ = random_topic_and_group
    handler = TestHandler()
    await client.subscribe(handler)

    # Send multiple test messages
    messages = [
        ("key1", {"content": "Message 1"}),
        ("key2", {"content": "Message 2"}),
        ("key3", {"content": "Message 3"})
    ]

    with tracer.start_as_current_span("send_multiple"):
        for key, payload in messages:
            await client.send(topic, key, payload)

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
    assert all(msg.topic == topic for msg in handler.messages)


async def test_same_key_message_order(client, random_topic_and_group):
    # Test that messages with the same key are received in the order they were sent
    topic, _ = random_topic_and_group
    handler = TestHandler()

    # Send multiple test messages with the same key
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
            await client.send(topic, test_key, payload)

    # Subscribe after messages are already on the topic
    await client.subscribe(handler)

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
        assert msg.topic == topic
        assert msg.key == test_key


class TransientErrorHandler(EventHandler):
    def __init__(self):
        self.received_message = False
        self.retry_event = asyncio.Event()

    @transient(ValueError)
    async def on_message(self, context: Context, message: Message) -> None:
        # On first receipt, trigger a transient error; on retry, mark success
        if self.received_message:
            self.retry_event.set()
        else:
            self.received_message = True
            raise ValueError("Transient error occurred")

    async def on_timer(self, context: Context, timer: Timer) -> None:
        # Not used in tests, but required by abstract base class
        pass


@pytest.mark.asyncio
async def test_transient_error_decorator(client, random_topic_and_group):
    # Test that the transient error decorator causes a retry
    topic, _ = random_topic_and_group
    handler = TransientErrorHandler()
    await client.subscribe(handler)

    # Send a test message that triggers a transient error
    test_key = "test-key"
    test_payload = {"content": "Trigger transient error"}

    await client.send(topic, test_key, test_payload)

    # Wait for the message to be retried
    await asyncio.wait_for(handler.retry_event.wait(), timeout=30.0)


class PermanentErrorHandler(EventHandler):
    def __init__(self):
        self.error_raised = asyncio.Event()
        self.message_count = 0

    @permanent(ValueError)
    async def on_message(self, context: Context, message: Message) -> None:
        # Mark that an error occurred and increase the count
        self.message_count += 1
        self.error_raised.set()
        raise ValueError("Permanent error occurred")

    async def on_timer(self, context: Context, timer: Timer) -> None:
        # Not used in tests, but required by abstract base class
        pass


@pytest.mark.asyncio
async def test_permanent_error_decorator(client, random_topic_and_group):
    # Test that the permanent error decorator causes the error to be raised only once
    topic, _ = random_topic_and_group
    handler = PermanentErrorHandler()
    await client.subscribe(handler)

    # Send a test message that triggers a permanent error
    test_key = "test-key"
    test_payload = {"content": "Trigger permanent error"}

    await client.send(topic, test_key, test_payload)

    # Wait for the error to be raised
    await asyncio.wait_for(handler.error_raised.wait(), timeout=30.0)

    # Wait a bit to allow for any potential retries
    await asyncio.sleep(5)

    # Check that the message was processed only once
    assert handler.message_count == 1


@pytest.mark.asyncio
async def test_best_effort_mode_does_not_retry(random_topic_and_group):
    # Test that in best effort mode, the transient error is not retried
    topic, group = random_topic_and_group
    handler = TransientErrorHandler()

    # Configure client for "best effort" mode, ensuring it doesn't do retries
    client_with_best_effort = ProsodyClient(
        bootstrap_servers="localhost:9094",
        source_system="test-send",
        group_id=group,
        subscribed_topics=topic,
        probe_port=None,
        cassandra_nodes="localhost:9042",
        mode="best-effort"
    )

    await client_with_best_effort.subscribe(handler)

    # Send a test message that triggers a transient error
    test_key = "test-key"
    test_payload = {"content": "Trigger transient error"}

    await client_with_best_effort.send(topic, test_key, test_payload)

    # Allow time for processing
    await asyncio.sleep(5)

    # Check that the transient error was raised only once
    assert not handler.retry_event.is_set()

    await client_with_best_effort.unsubscribe()


class TimerTestHandler(EventHandler):
    """Handler for testing timer functionality"""
    __test__ = False

    def __init__(self):
        self.timer_events = asyncio.Queue()
        self.message_events = asyncio.Queue()
        self.scheduled_times = []

    async def on_message(self, context: Context, message: Message) -> None:
        await self.message_events.put({"context": context, "message": message})

    async def on_timer(self, context: Context, timer: Timer) -> None:
        await self.timer_events.put({
            "context": context, 
            "timer": timer
        })


@pytest.mark.asyncio
async def test_timer_scheduling_and_firing(client, random_topic_and_group):
    """Test basic timer scheduling and firing functionality"""
    topic, _ = random_topic_and_group
    handler = TimerTestHandler()
    await client.subscribe(handler)

    # Send a message to trigger timer scheduling
    test_key = "timer-test-key"
    await client.send(topic, test_key, {"action": "schedule_timer"})

    # Wait for message processing
    message_event = await asyncio.wait_for(handler.message_events.get(), timeout=30.0)
    context = message_event["context"]

    # Schedule a timer 2 seconds from now
    scheduled_time = datetime.now(timezone.utc) + timedelta(seconds=2)
    await context.schedule(scheduled_time)

    # Wait for timer to fire (with some tolerance)
    timer_event = await asyncio.wait_for(handler.timer_events.get(), timeout=5.0)
    
    assert timer_event["timer"].key == test_key
    # Allow 1 second tolerance for timer precision
    time_diff = abs((timer_event["timer"].time - scheduled_time).total_seconds())
    assert time_diff <= TIMER_TOLERANCE_SECONDS, f"Timer fired at wrong time, difference: {time_diff} seconds"


@pytest.mark.asyncio 
async def test_timer_unschedule(client, random_topic_and_group):
    """Test unscheduling specific timers"""
    topic, _ = random_topic_and_group
    handler = TimerTestHandler()
    await client.subscribe(handler)

    test_key = "unschedule-test-key"
    await client.send(topic, test_key, {"action": "test_unschedule"})

    # Wait for message processing
    message_event = await asyncio.wait_for(handler.message_events.get(), timeout=30.0)
    context = message_event["context"]

    # Schedule two timers
    timer1_time = datetime.now(timezone.utc) + timedelta(seconds=3)
    timer2_time = datetime.now(timezone.utc) + timedelta(seconds=4)
    
    await context.schedule(timer1_time)
    await context.schedule(timer2_time)

    # Verify both are scheduled
    scheduled = await context.scheduled()
    assert len(scheduled) == 2

    # Unschedule the first timer
    await context.unschedule(timer1_time)

    # Verify only one remains
    scheduled_after = await context.scheduled()
    assert len(scheduled_after) == 1

    # Wait for the remaining timer to fire
    timer_event = await asyncio.wait_for(handler.timer_events.get(), timeout=6.0)
    
    # Should be the second timer
    time_diff = abs((timer_event["timer"].time - timer2_time).total_seconds())
    assert time_diff <= TIMER_TOLERANCE_SECONDS


@pytest.mark.asyncio
async def test_timer_clear_and_schedule(client, random_topic_and_group):
    """Test clearing all timers and scheduling a new one"""
    topic, _ = random_topic_and_group
    handler = TimerTestHandler()
    await client.subscribe(handler)

    test_key = "clear-schedule-test-key"
    await client.send(topic, test_key, {"action": "test_clear_schedule"})

    # Wait for message processing
    message_event = await asyncio.wait_for(handler.message_events.get(), timeout=30.0)
    context = message_event["context"]

    # Schedule multiple timers
    timer1_time = datetime.now(timezone.utc) + timedelta(seconds=5)
    timer2_time = datetime.now(timezone.utc) + timedelta(seconds=6)
    await context.schedule(timer1_time)
    await context.schedule(timer2_time)

    # Verify both are scheduled
    scheduled_before = await context.scheduled()
    assert len(scheduled_before) == 2

    # Clear all and schedule a new one (sooner)
    new_timer_time = datetime.now(timezone.utc) + timedelta(seconds=2)
    await context.clear_and_schedule(new_timer_time)

    # Verify only one timer remains
    scheduled_after = await context.scheduled()
    assert len(scheduled_after) == 1

    # Wait for the new timer to fire
    timer_event = await asyncio.wait_for(handler.timer_events.get(), timeout=4.0)
    
    # Should be the new timer
    time_diff = abs((timer_event["timer"].time - new_timer_time).total_seconds())
    assert time_diff <= TIMER_TOLERANCE_SECONDS


@pytest.mark.asyncio
async def test_timer_clear_scheduled(client, random_topic_and_group):
    """Test clearing all scheduled timers"""
    topic, _ = random_topic_and_group
    handler = TimerTestHandler()
    await client.subscribe(handler)

    test_key = "clear-all-test-key"
    await client.send(topic, test_key, {"action": "test_clear_all"})

    # Wait for message processing
    message_event = await asyncio.wait_for(handler.message_events.get(), timeout=30.0)
    context = message_event["context"]

    # Schedule multiple timers
    timer1_time = datetime.now(timezone.utc) + timedelta(seconds=3)
    timer2_time = datetime.now(timezone.utc) + timedelta(seconds=4)
    timer3_time = datetime.now(timezone.utc) + timedelta(seconds=5)
    
    await context.schedule(timer1_time)
    await context.schedule(timer2_time)
    await context.schedule(timer3_time)

    # Verify all are scheduled
    scheduled_before = await context.scheduled()
    assert len(scheduled_before) == 3

    # Clear all scheduled timers
    await context.clear_scheduled()

    # Verify no timers remain
    scheduled_after = await context.scheduled()
    assert len(scheduled_after) == 0

    # Wait to ensure no timers fire
    try:
        await asyncio.wait_for(handler.timer_events.get(), timeout=6.0)
        assert False, "No timers should have fired after clearing"
    except asyncio.TimeoutError:
        # This is expected - no timers should fire
        pass


@pytest.mark.asyncio
async def test_timer_scheduled_retrieval(client, random_topic_and_group):
    """Test retrieving scheduled timer times"""
    topic, _ = random_topic_and_group
    handler = TimerTestHandler()
    await client.subscribe(handler)

    test_key = "scheduled-retrieval-test"
    await client.send(topic, test_key, {"action": "test_scheduled"})

    # Wait for message processing
    message_event = await asyncio.wait_for(handler.message_events.get(), timeout=30.0)
    context = message_event["context"]

    # Initially should be empty
    scheduled_empty = await context.scheduled()
    assert len(scheduled_empty) == 0

    # Schedule multiple timers with different times
    now = datetime.now(timezone.utc)
    timer_times = [
        now + timedelta(seconds=10),
        now + timedelta(seconds=20),
        now + timedelta(seconds=30)
    ]
    
    for timer_time in timer_times:
        await context.schedule(timer_time)

    # Retrieve scheduled times
    scheduled = await context.scheduled()
    assert len(scheduled) == 3

    # Verify all scheduled times are present (allowing for slight timing differences)
    for expected_time in timer_times:
        found = any(abs((s - expected_time).total_seconds()) <= TIMER_TOLERANCE_SECONDS for s in scheduled)
        assert found, f"Expected time {expected_time} not found in scheduled times"


if __name__ == "__main__":
    pytest.main()
