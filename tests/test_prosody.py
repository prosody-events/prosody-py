import asyncio
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import List

import pytest
import tsasync
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from prosody.prosody import AdminClient

from prosody import ProsodyClient, EventHandler, Message, Context, Timer, permanent, transient

# Use pytest's built-in logging; logs will appear at DEBUG level
logger = logging.getLogger(__name__)

# Timer precision tolerance for tests (in seconds)
TIMER_TOLERANCE_SECONDS = 1

# Default timeout for all async operations (in seconds)
DEFAULT_TIMEOUT = 30.0

provider = TracerProvider()

# Sets the global default tracer provider
trace.set_tracer_provider(provider)

# Creates a tracer from the global tracer provider
tracer = trace.get_tracer("prosody-test")


class TestHandler(EventHandler):
    __test__ = False

    def __init__(self):
        logger.debug("TestHandler.__init__() called")
        self.messages: List[Message] = []
        self.message_count = 0
        self.message_received = tsasync.Event()
        logger.debug("TestHandler.__init__() completed")

    async def on_message(self, context: Context, message: Message) -> None:
        logger.debug(f"TestHandler.on_message() called with key={message.key}")
        with tracer.start_as_current_span("receive"):
            self.messages.append(message)
            self.message_count += 1
            self.message_received.set()
        logger.debug(f"TestHandler.on_message() completed")

    async def on_timer(self, context: Context, timer: Timer) -> None:
        logger.debug(f"TestHandler.on_timer() called")
        pass


@pytest.fixture
async def random_topic_and_group():
    logger.debug("=" * 40)
    logger.debug("FIXTURE random_topic_and_group: STARTING")
    logger.debug(f"FIXTURE: Current event loop: {asyncio.get_running_loop()}")

    topic = f"test-topic-{uuid.uuid4().hex}"
    group = f"test-group-{uuid.uuid4().hex}"
    logger.debug(f"FIXTURE: topic={topic}, group={group}")

    logger.debug("FIXTURE: Creating AdminClient...")
    admin = AdminClient(bootstrap_servers="localhost:9094")
    logger.debug("FIXTURE: AdminClient created")

    logger.debug("FIXTURE: Calling create_topic()...")
    await asyncio.wait_for(admin.create_topic(topic, partition_count=4, replication_factor=1), timeout=DEFAULT_TIMEOUT)
    logger.debug("FIXTURE: create_topic() completed")

    logger.debug("FIXTURE: Sleeping 1 second...")
    await asyncio.sleep(1)
    logger.debug("FIXTURE: Sleep completed")

    logger.debug("FIXTURE: About to yield topic and group")
    yield topic, group

    logger.debug("FIXTURE random_topic_and_group: TEARDOWN STARTING")
    logger.debug("FIXTURE: Calling delete_topic()...")
    await asyncio.wait_for(admin.delete_topic(topic), timeout=DEFAULT_TIMEOUT)
    logger.debug("FIXTURE: delete_topic() completed")
    logger.debug("FIXTURE random_topic_and_group: TEARDOWN COMPLETED")


@pytest.fixture
async def client(random_topic_and_group):
    logger.debug("=" * 40)
    logger.debug("FIXTURE client: STARTING")
    logger.debug(f"FIXTURE client: Current event loop: {asyncio.get_running_loop()}")

    topic, group = random_topic_and_group
    logger.debug(f"FIXTURE client: Got topic={topic}, group={group}")

    logger.debug("FIXTURE client: Creating ProsodyClient...")
    client = ProsodyClient(
        bootstrap_servers="localhost:9094",
        source_system="test-send",
        group_id=group,
        subscribed_topics=topic,
        probe_port=None,
        cassandra_nodes="localhost:9042",
    )
    logger.debug("FIXTURE client: ProsodyClient created")

    logger.debug("FIXTURE client: About to yield client")
    yield client

    logger.debug("FIXTURE client: TEARDOWN STARTING")
    logger.debug("FIXTURE client: Checking consumer_state()...")
    state = await asyncio.wait_for(client.consumer_state(), timeout=DEFAULT_TIMEOUT)
    logger.debug(f"FIXTURE client: consumer_state() = {state}")

    if state == "running":
        logger.debug("FIXTURE client: Calling unsubscribe()...")
        await asyncio.wait_for(client.unsubscribe(), timeout=DEFAULT_TIMEOUT)
        logger.debug("FIXTURE client: unsubscribe() completed")

    logger.debug("FIXTURE client: TEARDOWN COMPLETED")


async def test_client_initialization(client):
    logger.debug("=" * 40)
    logger.debug("TEST test_client_initialization: STARTING")
    assert isinstance(client, ProsodyClient)
    state = await asyncio.wait_for(client.consumer_state(), timeout=DEFAULT_TIMEOUT)
    logger.debug(f"TEST: consumer_state() = {state}")
    assert state == "configured"
    logger.debug("TEST test_client_initialization: PASSED")


async def test_client_source_system(client):
    logger.debug("=" * 40)
    logger.debug("TEST test_client_source_system: STARTING")
    assert client.source_system == "test-send"
    logger.debug("TEST test_client_source_system: PASSED")


async def test_client_subscribe_unsubscribe(client):
    logger.debug("=" * 40)
    logger.debug("TEST test_client_subscribe_unsubscribe: STARTING")
    logger.debug(f"TEST: Current event loop: {asyncio.get_running_loop()}")

    logger.debug("TEST: Creating TestHandler...")
    handler = TestHandler()
    logger.debug("TEST: TestHandler created")

    logger.debug("TEST: Calling client.subscribe(handler)...")
    await asyncio.wait_for(client.subscribe(handler), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: subscribe() completed")

    logger.debug("TEST: Calling consumer_state()...")
    state = await asyncio.wait_for(client.consumer_state(), timeout=DEFAULT_TIMEOUT)
    logger.debug(f"TEST: consumer_state() = {state}")
    assert state == "running"

    logger.debug("TEST: Calling unsubscribe()...")
    await asyncio.wait_for(client.unsubscribe(), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: unsubscribe() completed")

    logger.debug("TEST: Calling consumer_state() again...")
    state = await asyncio.wait_for(client.consumer_state(), timeout=DEFAULT_TIMEOUT)
    logger.debug(f"TEST: consumer_state() = {state}")
    assert state == "configured"

    logger.debug("TEST test_client_subscribe_unsubscribe: PASSED")


async def test_send_and_receive_message(client, random_topic_and_group):
    logger.debug("=" * 40)
    logger.debug("TEST test_send_and_receive_message: STARTING")

    topic, _ = random_topic_and_group
    logger.debug(f"TEST: topic={topic}")

    logger.debug("TEST: Creating TestHandler...")
    handler = TestHandler()

    logger.debug("TEST: Calling subscribe()...")
    await asyncio.wait_for(client.subscribe(handler), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: subscribe() completed")

    test_key = "test-key"
    test_payload = {"content": "Hello, Kafka!"}
    logger.debug(f"TEST: Sending message key={test_key}")
    with tracer.start_as_current_span("send"):
        await asyncio.wait_for(client.send(topic, test_key, test_payload), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: send() completed")

    logger.debug("TEST: Waiting for message...")
    await asyncio.wait_for(handler.message_received.wait(), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: Message received")

    assert len(handler.messages) == 1
    received_message = handler.messages[0]
    assert received_message.topic == topic
    assert received_message.key == test_key
    assert received_message.payload == test_payload
    logger.debug("TEST test_send_and_receive_message: PASSED")


async def test_client_configuration(random_topic_and_group):
    logger.debug("=" * 40)
    logger.debug("TEST test_client_configuration: STARTING")

    topic, group = random_topic_and_group
    logger.debug("TEST: Creating ProsodyClient with mock=True...")
    client = ProsodyClient(
        bootstrap_servers=["localhost:9092", "localhost:9093"],
        source_system="test-send",
        group_id=group,
        subscribed_topics=[topic],
        max_uncommitted=1000,
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
    logger.debug("TEST: ProsodyClient created")
    assert isinstance(client, ProsodyClient)
    logger.debug("TEST test_client_configuration: PASSED")


async def test_deduplication_configuration(random_topic_and_group):
    topic, group = random_topic_and_group

    # idempotence_version and idempotence_ttl as timedelta
    client = ProsodyClient(
        bootstrap_servers="localhost:9092",
        source_system="test-dedup",
        group_id=group,
        subscribed_topics=[topic],
        idempotence_version="2",
        idempotence_ttl=timedelta(days=7),
        mock=True,
    )
    assert isinstance(client, ProsodyClient)

    # idempotence_ttl as float seconds
    client = ProsodyClient(
        bootstrap_servers="localhost:9092",
        source_system="test-dedup",
        group_id=group,
        subscribed_topics=[topic],
        idempotence_ttl=604800.0,
        mock=True,
    )
    assert isinstance(client, ProsodyClient)

    # None values must not raise a TypeError
    client = ProsodyClient(
        bootstrap_servers="localhost:9092",
        source_system="test-dedup",
        group_id=group,
        subscribed_topics=[topic],
        idempotence_version=None,
        idempotence_ttl=None,
        mock=True,
    )
    assert isinstance(client, ProsodyClient)


async def test_multiple_messages(client, random_topic_and_group):
    logger.debug("=" * 40)
    logger.debug("TEST test_multiple_messages: STARTING")

    topic, _ = random_topic_and_group
    logger.debug("TEST: Creating handler and subscribing...")
    handler = TestHandler()
    await asyncio.wait_for(client.subscribe(handler), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: Subscribed")

    messages = [
        ("key1", {"content": "Message 1"}),
        ("key2", {"content": "Message 2"}),
        ("key3", {"content": "Message 3"})
    ]

    logger.debug(f"TEST: Sending {len(messages)} messages...")
    with tracer.start_as_current_span("send_multiple"):
        for key, payload in messages:
            await asyncio.wait_for(client.send(topic, key, payload), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: All messages sent")

    async def wait_for_messages():
        while handler.message_count < len(messages):
            await asyncio.wait_for(handler.message_received.wait(), timeout=DEFAULT_TIMEOUT)
            handler.message_received.clear()

    logger.debug("TEST: Waiting for all messages...")
    await asyncio.wait_for(wait_for_messages(), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: All messages received")

    assert len(handler.messages) == len(messages)
    expected_messages = set((key, frozenset(payload.items())) for key, payload in messages)
    received_messages = set((msg.key, frozenset(msg.payload.items())) for msg in handler.messages)
    assert expected_messages == received_messages
    assert all(msg.topic == topic for msg in handler.messages)
    logger.debug("TEST test_multiple_messages: PASSED")


async def test_same_key_message_order(client, random_topic_and_group):
    logger.debug("=" * 40)
    logger.debug("TEST test_same_key_message_order: STARTING")

    topic, _ = random_topic_and_group
    handler = TestHandler()

    test_key = "same-key"
    messages = [
        {"content": "Message 1", "sequence": 1},
        {"content": "Message 2", "sequence": 2},
        {"content": "Message 3", "sequence": 3},
        {"content": "Message 4", "sequence": 4},
        {"content": "Message 5", "sequence": 5},
    ]

    logger.debug(f"TEST: Sending {len(messages)} messages with same key...")
    with tracer.start_as_current_span("send_same_key_messages"):
        for payload in messages:
            await asyncio.wait_for(client.send(topic, test_key, payload), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: All messages sent")

    logger.debug("TEST: Subscribing...")
    await asyncio.wait_for(client.subscribe(handler), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: Subscribed")

    async def wait_for_messages():
        while handler.message_count < len(messages):
            await asyncio.wait_for(handler.message_received.wait(), timeout=DEFAULT_TIMEOUT)
            handler.message_received.clear()

    logger.debug("TEST: Waiting for all messages...")
    await asyncio.wait_for(wait_for_messages(), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: All messages received")

    assert len(handler.messages) == len(messages)
    received_messages = [msg for msg in handler.messages if msg.key == test_key]
    received_sequences = [msg.payload["sequence"] for msg in received_messages]
    expected_sequences = [msg["sequence"] for msg in messages]

    assert received_sequences == expected_sequences
    for msg in received_messages:
        assert msg.topic == topic
        assert msg.key == test_key
    logger.debug("TEST test_same_key_message_order: PASSED")


class TransientErrorHandler(EventHandler):
    def __init__(self):
        logger.debug("TransientErrorHandler.__init__() called")
        self.received_message = False
        self.retry_event = tsasync.Event()
        logger.debug("TransientErrorHandler.__init__() completed")

    @transient(ValueError)
    async def on_message(self, context: Context, message: Message) -> None:
        logger.debug(f"TransientErrorHandler.on_message() called, received_message={self.received_message}")
        if self.received_message:
            logger.debug("TransientErrorHandler: Setting retry_event")
            self.retry_event.set()
        else:
            self.received_message = True
            logger.debug("TransientErrorHandler: Raising ValueError")
            raise ValueError("Transient error occurred")

    async def on_timer(self, context: Context, timer: Timer) -> None:
        logger.debug("TransientErrorHandler.on_timer() called")
        pass


@pytest.mark.asyncio
async def test_transient_error_decorator(client, random_topic_and_group):
    logger.debug("=" * 40)
    logger.debug("TEST test_transient_error_decorator: STARTING")

    topic, _ = random_topic_and_group
    logger.debug("TEST: Creating TransientErrorHandler...")
    handler = TransientErrorHandler()

    logger.debug("TEST: Subscribing...")
    await asyncio.wait_for(client.subscribe(handler), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: Subscribed")

    test_key = "test-key"
    test_payload = {"content": "Trigger transient error"}
    logger.debug(f"TEST: Sending message key={test_key}")
    await asyncio.wait_for(client.send(topic, test_key, test_payload), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: Message sent")

    logger.debug("TEST: Waiting for retry...")
    await asyncio.wait_for(handler.retry_event.wait(), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST test_transient_error_decorator: PASSED")


class PermanentErrorHandler(EventHandler):
    def __init__(self):
        logger.debug("PermanentErrorHandler.__init__() called")
        self.error_raised = tsasync.Event()
        self.message_count = 0
        logger.debug("PermanentErrorHandler.__init__() completed")

    @permanent(ValueError)
    async def on_message(self, context: Context, message: Message) -> None:
        logger.debug(f"PermanentErrorHandler.on_message() called, count={self.message_count}")
        self.message_count += 1
        self.error_raised.set()
        logger.debug("PermanentErrorHandler: Raising ValueError")
        raise ValueError("Permanent error occurred")

    async def on_timer(self, context: Context, timer: Timer) -> None:
        logger.debug("PermanentErrorHandler.on_timer() called")
        pass


@pytest.mark.asyncio
async def test_permanent_error_decorator(client, random_topic_and_group):
    logger.debug("=" * 40)
    logger.debug("TEST test_permanent_error_decorator: STARTING")

    topic, _ = random_topic_and_group
    logger.debug("TEST: Creating PermanentErrorHandler...")
    handler = PermanentErrorHandler()

    logger.debug("TEST: Subscribing...")
    await asyncio.wait_for(client.subscribe(handler), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: Subscribed")

    test_key = "test-key"
    test_payload = {"content": "Trigger permanent error"}
    logger.debug(f"TEST: Sending message key={test_key}")
    await asyncio.wait_for(client.send(topic, test_key, test_payload), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: Message sent")

    logger.debug("TEST: Waiting for error...")
    await asyncio.wait_for(handler.error_raised.wait(), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: Error raised, sleeping 5 seconds...")

    await asyncio.sleep(5)
    logger.debug(f"TEST: message_count={handler.message_count}")

    assert handler.message_count == 1
    logger.debug("TEST test_permanent_error_decorator: PASSED")


@pytest.mark.asyncio
async def test_best_effort_mode_does_not_retry(random_topic_and_group):
    logger.debug("=" * 40)
    logger.debug("TEST test_best_effort_mode_does_not_retry: STARTING")

    topic, group = random_topic_and_group
    logger.debug("TEST: Creating TransientErrorHandler...")
    handler = TransientErrorHandler()

    logger.debug("TEST: Creating ProsodyClient with mode=best-effort...")
    client_with_best_effort = ProsodyClient(
        bootstrap_servers="localhost:9094",
        source_system="test-send",
        group_id=group,
        subscribed_topics=topic,
        probe_port=None,
        cassandra_nodes="localhost:9042",
        mode="best-effort"
    )
    logger.debug("TEST: Client created")

    logger.debug("TEST: Subscribing...")
    await asyncio.wait_for(client_with_best_effort.subscribe(handler), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: Subscribed")

    test_key = "test-key"
    test_payload = {"content": "Trigger transient error"}
    logger.debug(f"TEST: Sending message key={test_key}")
    await asyncio.wait_for(client_with_best_effort.send(topic, test_key, test_payload), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: Message sent, sleeping 5 seconds...")

    await asyncio.sleep(5)
    logger.debug(f"TEST: retry_event.is_set()={handler.retry_event.is_set()}")

    assert not handler.retry_event.is_set()

    logger.debug("TEST: Unsubscribing...")
    await asyncio.wait_for(client_with_best_effort.unsubscribe(), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST test_best_effort_mode_does_not_retry: PASSED")


class CallbackTimerHandler(EventHandler):
    """Handler that executes a custom callback function for timer testing"""
    __test__ = False

    def __init__(self, message_callback=None):
        logger.debug("CallbackTimerHandler.__init__() called")
        self.timer_events = tsasync.Channel()
        self.results = tsasync.Channel()
        self.message_callback = message_callback
        logger.debug("CallbackTimerHandler.__init__() completed")

    async def on_message(self, context: Context, message: Message) -> None:
        logger.debug(f"CallbackTimerHandler.on_message() called with key={message.key}")
        if self.message_callback:
            try:
                result = await self.message_callback(context, message)
                if result is not None:
                    await self.results.send({"success": True, **result})
            except Exception as e:
                logger.error(f"CallbackTimerHandler.on_message() error: {e}")
                await self.results.send({"success": False, "error": str(e)})
        else:
            await self.results.send({"context": context, "message": message})
        logger.debug("CallbackTimerHandler.on_message() completed")

    async def on_timer(self, context: Context, timer: Timer) -> None:
        logger.debug(f"CallbackTimerHandler.on_timer() called with key={timer.key}")
        await self.timer_events.send({
            "context": context,
            "timer": timer
        })
        logger.debug("CallbackTimerHandler.on_timer() completed")


class TimerTestHandler(CallbackTimerHandler):
    """Backwards compatibility wrapper"""
    __test__ = False

    def __init__(self):
        super().__init__()
        self.message_events = self.results
        self.operation_results = self.results


@pytest.mark.asyncio
async def test_timer_scheduling_and_firing(client, random_topic_and_group):
    logger.debug("=" * 40)
    logger.debug("TEST test_timer_scheduling_and_firing: STARTING")

    topic, _ = random_topic_and_group

    async def schedule_timer_callback(context: Context, message: Message):
        logger.debug("schedule_timer_callback: Scheduling timer...")
        scheduled_time = datetime.now(timezone.utc) + timedelta(seconds=2)
        await asyncio.wait_for(context.schedule(scheduled_time), timeout=DEFAULT_TIMEOUT)
        logger.debug(f"schedule_timer_callback: Timer scheduled for {scheduled_time}")
        return {"scheduled_time": scheduled_time}

    logger.debug("TEST: Creating CallbackTimerHandler...")
    handler = CallbackTimerHandler(schedule_timer_callback)

    logger.debug("TEST: Subscribing...")
    await asyncio.wait_for(client.subscribe(handler), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: Subscribed")

    test_key = "timer-test-key"
    logger.debug(f"TEST: Sending message key={test_key}")
    await asyncio.wait_for(client.send(topic, test_key, {"test": "data"}), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: Message sent")

    logger.debug("TEST: Waiting for result...")
    operation_result = await asyncio.wait_for(handler.results.receive(), timeout=DEFAULT_TIMEOUT)
    logger.debug(f"TEST: Result received: {operation_result}")
    assert operation_result["success"]
    scheduled_time = operation_result["scheduled_time"]

    logger.debug("TEST: Waiting for timer to fire...")
    timer_event = await asyncio.wait_for(handler.timer_events.receive(), timeout=DEFAULT_TIMEOUT)
    logger.debug(f"TEST: Timer fired")

    assert timer_event["timer"].key == test_key
    time_diff = abs((timer_event["timer"].time - scheduled_time).total_seconds())
    assert time_diff <= TIMER_TOLERANCE_SECONDS
    logger.debug("TEST test_timer_scheduling_and_firing: PASSED")


@pytest.mark.asyncio
async def test_timer_unschedule(client, random_topic_and_group):
    logger.debug("=" * 40)
    logger.debug("TEST test_timer_unschedule: STARTING")

    topic, _ = random_topic_and_group

    async def unschedule_timer_callback(context: Context, message: Message):
        logger.debug("unschedule_timer_callback: Scheduling timers...")
        timer1_time = datetime.now(timezone.utc) + timedelta(seconds=3)
        timer2_time = datetime.now(timezone.utc) + timedelta(seconds=4)

        await asyncio.wait_for(context.schedule(timer1_time), timeout=DEFAULT_TIMEOUT)
        await asyncio.wait_for(context.schedule(timer2_time), timeout=DEFAULT_TIMEOUT)

        scheduled = await asyncio.wait_for(context.scheduled(), timeout=DEFAULT_TIMEOUT)
        logger.debug(f"unschedule_timer_callback: {len(scheduled)} timers scheduled")
        assert len(scheduled) == 2

        logger.debug("unschedule_timer_callback: Unscheduling first timer...")
        await asyncio.wait_for(context.unschedule(timer1_time), timeout=DEFAULT_TIMEOUT)

        scheduled_after = await asyncio.wait_for(context.scheduled(), timeout=DEFAULT_TIMEOUT)
        logger.debug(f"unschedule_timer_callback: {len(scheduled_after)} timers remaining")
        return {
            "remaining_timers": len(scheduled_after),
            "expected_timer_time": timer2_time
        }

    logger.debug("TEST: Creating CallbackTimerHandler...")
    handler = CallbackTimerHandler(unschedule_timer_callback)

    logger.debug("TEST: Subscribing...")
    await asyncio.wait_for(client.subscribe(handler), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: Subscribed")

    test_key = "unschedule-test-key"
    logger.debug(f"TEST: Sending message key={test_key}")
    await asyncio.wait_for(client.send(topic, test_key, {"test": "data"}), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: Message sent")

    logger.debug("TEST: Waiting for result...")
    operation_result = await asyncio.wait_for(handler.results.receive(), timeout=DEFAULT_TIMEOUT)
    logger.debug(f"TEST: Result: {operation_result}")
    assert operation_result["success"]
    assert operation_result["remaining_timers"] == 1
    expected_timer_time = operation_result["expected_timer_time"]

    logger.debug("TEST: Waiting for timer to fire...")
    timer_event = await asyncio.wait_for(handler.timer_events.receive(), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: Timer fired")

    time_diff = abs((timer_event["timer"].time - expected_timer_time).total_seconds())
    assert time_diff <= TIMER_TOLERANCE_SECONDS
    logger.debug("TEST test_timer_unschedule: PASSED")


@pytest.mark.asyncio
async def test_timer_clear_and_schedule(client, random_topic_and_group):
    logger.debug("=" * 40)
    logger.debug("TEST test_timer_clear_and_schedule: STARTING")

    topic, _ = random_topic_and_group

    async def clear_and_schedule_callback(context: Context, message: Message):
        logger.debug("clear_and_schedule_callback: Scheduling timers...")
        timer1_time = datetime.now(timezone.utc) + timedelta(seconds=5)
        timer2_time = datetime.now(timezone.utc) + timedelta(seconds=6)
        await asyncio.wait_for(context.schedule(timer1_time), timeout=DEFAULT_TIMEOUT)
        await asyncio.wait_for(context.schedule(timer2_time), timeout=DEFAULT_TIMEOUT)

        scheduled_before = await asyncio.wait_for(context.scheduled(), timeout=DEFAULT_TIMEOUT)
        logger.debug(f"clear_and_schedule_callback: {len(scheduled_before)} timers scheduled")
        assert len(scheduled_before) == 2

        new_timer_time = datetime.now(timezone.utc) + timedelta(seconds=2)
        logger.debug("clear_and_schedule_callback: Calling clear_and_schedule...")
        await asyncio.wait_for(context.clear_and_schedule(new_timer_time), timeout=DEFAULT_TIMEOUT)

        scheduled_after = await asyncio.wait_for(context.scheduled(), timeout=DEFAULT_TIMEOUT)
        logger.debug(f"clear_and_schedule_callback: {len(scheduled_after)} timers remaining")
        return {
            "remaining_timers": len(scheduled_after),
            "new_timer_time": new_timer_time
        }

    logger.debug("TEST: Creating CallbackTimerHandler...")
    handler = CallbackTimerHandler(clear_and_schedule_callback)

    logger.debug("TEST: Subscribing...")
    await asyncio.wait_for(client.subscribe(handler), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: Subscribed")

    test_key = "clear-schedule-test-key"
    logger.debug(f"TEST: Sending message key={test_key}")
    await asyncio.wait_for(client.send(topic, test_key, {"test": "data"}), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: Message sent")

    logger.debug("TEST: Waiting for result...")
    operation_result = await asyncio.wait_for(handler.results.receive(), timeout=DEFAULT_TIMEOUT)
    logger.debug(f"TEST: Result: {operation_result}")
    assert operation_result["success"]
    assert operation_result["remaining_timers"] == 1
    new_timer_time = operation_result["new_timer_time"]

    logger.debug("TEST: Waiting for timer to fire...")
    timer_event = await asyncio.wait_for(handler.timer_events.receive(), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: Timer fired")

    time_diff = abs((timer_event["timer"].time - new_timer_time).total_seconds())
    assert time_diff <= TIMER_TOLERANCE_SECONDS
    logger.debug("TEST test_timer_clear_and_schedule: PASSED")


@pytest.mark.asyncio
async def test_timer_clear_scheduled(client, random_topic_and_group):
    logger.debug("=" * 40)
    logger.debug("TEST test_timer_clear_scheduled: STARTING")

    topic, _ = random_topic_and_group

    async def clear_scheduled_callback(context: Context, message: Message):
        logger.debug("clear_scheduled_callback: Scheduling timers...")
        timer1_time = datetime.now(timezone.utc) + timedelta(seconds=3)
        timer2_time = datetime.now(timezone.utc) + timedelta(seconds=4)
        timer3_time = datetime.now(timezone.utc) + timedelta(seconds=5)

        await asyncio.wait_for(context.schedule(timer1_time), timeout=DEFAULT_TIMEOUT)
        await asyncio.wait_for(context.schedule(timer2_time), timeout=DEFAULT_TIMEOUT)
        await asyncio.wait_for(context.schedule(timer3_time), timeout=DEFAULT_TIMEOUT)

        scheduled_before = await asyncio.wait_for(context.scheduled(), timeout=DEFAULT_TIMEOUT)
        logger.debug(f"clear_scheduled_callback: {len(scheduled_before)} timers scheduled")
        assert len(scheduled_before) == 3

        logger.debug("clear_scheduled_callback: Calling clear_scheduled...")
        await asyncio.wait_for(context.clear_scheduled(), timeout=DEFAULT_TIMEOUT)

        scheduled_after = await asyncio.wait_for(context.scheduled(), timeout=DEFAULT_TIMEOUT)
        logger.debug(f"clear_scheduled_callback: {len(scheduled_after)} timers remaining")
        return {"remaining_timers": len(scheduled_after)}

    logger.debug("TEST: Creating CallbackTimerHandler...")
    handler = CallbackTimerHandler(clear_scheduled_callback)

    logger.debug("TEST: Subscribing...")
    await asyncio.wait_for(client.subscribe(handler), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: Subscribed")

    test_key = "clear-all-test-key"
    logger.debug(f"TEST: Sending message key={test_key}")
    await asyncio.wait_for(client.send(topic, test_key, {"test": "data"}), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: Message sent")

    logger.debug("TEST: Waiting for result...")
    operation_result = await asyncio.wait_for(handler.results.receive(), timeout=DEFAULT_TIMEOUT)
    logger.debug(f"TEST: Result: {operation_result}")
    assert operation_result["success"]
    assert operation_result["remaining_timers"] == 0

    logger.debug("TEST: Verifying no timers fire...")
    try:
        await asyncio.wait_for(handler.timer_events.receive(), timeout=6.0)
        assert False, "No timers should have fired after clearing"
    except asyncio.TimeoutError:
        logger.debug("TEST: Timeout as expected - no timers fired")
        pass

    logger.debug("TEST test_timer_clear_scheduled: PASSED")


@pytest.mark.asyncio
async def test_timer_scheduled_retrieval(client, random_topic_and_group):
    logger.debug("=" * 40)
    logger.debug("TEST test_timer_scheduled_retrieval: STARTING")

    topic, _ = random_topic_and_group

    async def scheduled_retrieval_callback(context: Context, message: Message):
        logger.debug("scheduled_retrieval_callback: Checking initial state...")
        scheduled_empty = await asyncio.wait_for(context.scheduled(), timeout=DEFAULT_TIMEOUT)
        assert len(scheduled_empty) == 0

        now = datetime.now(timezone.utc)
        timer_times = [
            now + timedelta(seconds=10),
            now + timedelta(seconds=20),
            now + timedelta(seconds=30)
        ]

        logger.debug(f"scheduled_retrieval_callback: Scheduling {len(timer_times)} timers...")
        for timer_time in timer_times:
            await asyncio.wait_for(context.schedule(timer_time), timeout=DEFAULT_TIMEOUT)

        scheduled = await asyncio.wait_for(context.scheduled(), timeout=DEFAULT_TIMEOUT)
        logger.debug(f"scheduled_retrieval_callback: {len(scheduled)} timers scheduled")
        return {
            "scheduled_count": len(scheduled),
            "expected_times": timer_times,
            "scheduled_times": scheduled
        }

    logger.debug("TEST: Creating CallbackTimerHandler...")
    handler = CallbackTimerHandler(scheduled_retrieval_callback)

    logger.debug("TEST: Subscribing...")
    await asyncio.wait_for(client.subscribe(handler), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: Subscribed")

    test_key = "scheduled-retrieval-test"
    logger.debug(f"TEST: Sending message key={test_key}")
    await asyncio.wait_for(client.send(topic, test_key, {"test": "data"}), timeout=DEFAULT_TIMEOUT)
    logger.debug("TEST: Message sent")

    logger.debug("TEST: Waiting for result...")
    operation_result = await asyncio.wait_for(handler.results.receive(), timeout=DEFAULT_TIMEOUT)
    logger.debug(f"TEST: Result: {operation_result}")
    assert operation_result["success"]
    assert operation_result["scheduled_count"] == 3

    expected_times = operation_result["expected_times"]
    scheduled_times = operation_result["scheduled_times"]

    for expected_time in expected_times:
        found = False
        for scheduled_time in scheduled_times:
            time_diff = abs((scheduled_time - expected_time).total_seconds())
            if time_diff <= TIMER_TOLERANCE_SECONDS:
                found = True
                break
        assert found, f"Expected time {expected_time} not found in scheduled times"

    logger.debug("TEST test_timer_scheduled_retrieval: PASSED")
