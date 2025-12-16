import asyncio
import logging
import sys
import uuid
from datetime import datetime, timedelta, timezone
from typing import List

import pytest
import tsasync
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from prosody.prosody import AdminClient

from prosody import ProsodyClient, EventHandler, Message, Context, Timer, permanent, transient

# Configure logging at module load time - BEFORE any fixtures run
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    stream=sys.stdout,
    force=True
)
logger = logging.getLogger("prosody_test")
logger.setLevel(logging.DEBUG)
logger.info("=" * 60)
logger.info("TEST MODULE LOADED")
logger.info(f"Python version: {sys.version}")
logger.info(f"Event loop policy: {asyncio.get_event_loop_policy()}")
logger.info("=" * 60)

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
        logger.info("TestHandler.__init__() called")
        self.messages: List[Message] = []
        self.message_count = 0
        self.message_received = tsasync.Event()
        logger.info("TestHandler.__init__() completed")

    async def on_message(self, context: Context, message: Message) -> None:
        logger.info(f"TestHandler.on_message() called with key={message.key}")
        with tracer.start_as_current_span("receive"):
            self.messages.append(message)
            self.message_count += 1
            self.message_received.set()
        logger.info(f"TestHandler.on_message() completed")

    async def on_timer(self, context: Context, timer: Timer) -> None:
        logger.info(f"TestHandler.on_timer() called")
        pass


@pytest.fixture
async def random_topic_and_group():
    logger.info("=" * 40)
    logger.info("FIXTURE random_topic_and_group: STARTING")
    logger.info(f"FIXTURE: Current event loop: {asyncio.get_running_loop()}")

    topic = f"test-topic-{uuid.uuid4().hex}"
    group = f"test-group-{uuid.uuid4().hex}"
    logger.info(f"FIXTURE: topic={topic}, group={group}")

    logger.info("FIXTURE: Creating AdminClient...")
    admin = AdminClient(bootstrap_servers="localhost:9094")
    logger.info("FIXTURE: AdminClient created")

    logger.info("FIXTURE: Calling create_topic()...")
    await admin.create_topic(topic, partition_count=4, replication_factor=1)
    logger.info("FIXTURE: create_topic() completed")

    logger.info("FIXTURE: Sleeping 1 second...")
    await asyncio.sleep(1)
    logger.info("FIXTURE: Sleep completed")

    logger.info("FIXTURE: About to yield topic and group")
    yield topic, group

    logger.info("FIXTURE random_topic_and_group: TEARDOWN STARTING")
    logger.info("FIXTURE: Calling delete_topic()...")
    await admin.delete_topic(topic)
    logger.info("FIXTURE: delete_topic() completed")
    logger.info("FIXTURE random_topic_and_group: TEARDOWN COMPLETED")


@pytest.fixture
async def client(random_topic_and_group):
    logger.info("=" * 40)
    logger.info("FIXTURE client: STARTING")
    logger.info(f"FIXTURE client: Current event loop: {asyncio.get_running_loop()}")

    topic, group = random_topic_and_group
    logger.info(f"FIXTURE client: Got topic={topic}, group={group}")

    logger.info("FIXTURE client: Creating ProsodyClient...")
    client = ProsodyClient(
        bootstrap_servers="localhost:9094",
        source_system="test-send",
        group_id=group,
        subscribed_topics=topic,
        probe_port=None,
        cassandra_nodes="localhost:9042",
    )
    logger.info("FIXTURE client: ProsodyClient created")

    logger.info("FIXTURE client: About to yield client")
    yield client

    logger.info("FIXTURE client: TEARDOWN STARTING")
    logger.info("FIXTURE client: Checking consumer_state()...")
    state = await client.consumer_state()
    logger.info(f"FIXTURE client: consumer_state() = {state}")

    if state == "running":
        logger.info("FIXTURE client: Calling unsubscribe()...")
        await client.unsubscribe()
        logger.info("FIXTURE client: unsubscribe() completed")

    logger.info("FIXTURE client: TEARDOWN COMPLETED")


async def test_client_initialization(client):
    logger.info("=" * 40)
    logger.info("TEST test_client_initialization: STARTING")
    assert isinstance(client, ProsodyClient)
    state = await client.consumer_state()
    logger.info(f"TEST: consumer_state() = {state}")
    assert state == "configured"
    logger.info("TEST test_client_initialization: PASSED")


async def test_client_source_system(client):
    logger.info("=" * 40)
    logger.info("TEST test_client_source_system: STARTING")
    assert client.source_system == "test-send"
    logger.info("TEST test_client_source_system: PASSED")


async def test_client_subscribe_unsubscribe(client):
    logger.info("=" * 40)
    logger.info("TEST test_client_subscribe_unsubscribe: STARTING")
    logger.info(f"TEST: Current event loop: {asyncio.get_running_loop()}")

    logger.info("TEST: Creating TestHandler...")
    handler = TestHandler()
    logger.info("TEST: TestHandler created")

    logger.info("TEST: Calling client.subscribe(handler)...")
    await client.subscribe(handler)
    logger.info("TEST: subscribe() completed")

    logger.info("TEST: Calling consumer_state()...")
    state = await client.consumer_state()
    logger.info(f"TEST: consumer_state() = {state}")
    assert state == "running"

    logger.info("TEST: Calling unsubscribe()...")
    await client.unsubscribe()
    logger.info("TEST: unsubscribe() completed")

    logger.info("TEST: Calling consumer_state() again...")
    state = await client.consumer_state()
    logger.info(f"TEST: consumer_state() = {state}")
    assert state == "configured"

    logger.info("TEST test_client_subscribe_unsubscribe: PASSED")


async def test_send_and_receive_message(client, random_topic_and_group):
    logger.info("=" * 40)
    logger.info("TEST test_send_and_receive_message: STARTING")

    topic, _ = random_topic_and_group
    logger.info(f"TEST: topic={topic}")

    logger.info("TEST: Creating TestHandler...")
    handler = TestHandler()

    logger.info("TEST: Calling subscribe()...")
    await client.subscribe(handler)
    logger.info("TEST: subscribe() completed")

    test_key = "test-key"
    test_payload = {"content": "Hello, Kafka!"}
    logger.info(f"TEST: Sending message key={test_key}")
    with tracer.start_as_current_span("send"):
        await client.send(topic, test_key, test_payload)
    logger.info("TEST: send() completed")

    logger.info("TEST: Waiting for message...")
    await asyncio.wait_for(handler.message_received.wait(), timeout=30.0)
    logger.info("TEST: Message received")

    assert len(handler.messages) == 1
    received_message = handler.messages[0]
    assert received_message.topic == topic
    assert received_message.key == test_key
    assert received_message.payload == test_payload
    logger.info("TEST test_send_and_receive_message: PASSED")


async def test_client_configuration(random_topic_and_group):
    logger.info("=" * 40)
    logger.info("TEST test_client_configuration: STARTING")

    topic, group = random_topic_and_group
    logger.info("TEST: Creating ProsodyClient with mock=True...")
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
    logger.info("TEST: ProsodyClient created")
    assert isinstance(client, ProsodyClient)
    logger.info("TEST test_client_configuration: PASSED")


async def test_multiple_messages(client, random_topic_and_group):
    logger.info("=" * 40)
    logger.info("TEST test_multiple_messages: STARTING")

    topic, _ = random_topic_and_group
    logger.info("TEST: Creating handler and subscribing...")
    handler = TestHandler()
    await client.subscribe(handler)
    logger.info("TEST: Subscribed")

    messages = [
        ("key1", {"content": "Message 1"}),
        ("key2", {"content": "Message 2"}),
        ("key3", {"content": "Message 3"})
    ]

    logger.info(f"TEST: Sending {len(messages)} messages...")
    with tracer.start_as_current_span("send_multiple"):
        for key, payload in messages:
            await client.send(topic, key, payload)
    logger.info("TEST: All messages sent")

    async def wait_for_messages():
        while handler.message_count < len(messages):
            await handler.message_received.wait()
            handler.message_received.clear()

    logger.info("TEST: Waiting for all messages...")
    await asyncio.wait_for(wait_for_messages(), timeout=30.0)
    logger.info("TEST: All messages received")

    assert len(handler.messages) == len(messages)
    expected_messages = set((key, frozenset(payload.items())) for key, payload in messages)
    received_messages = set((msg.key, frozenset(msg.payload.items())) for msg in handler.messages)
    assert expected_messages == received_messages
    assert all(msg.topic == topic for msg in handler.messages)
    logger.info("TEST test_multiple_messages: PASSED")


async def test_same_key_message_order(client, random_topic_and_group):
    logger.info("=" * 40)
    logger.info("TEST test_same_key_message_order: STARTING")

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

    logger.info(f"TEST: Sending {len(messages)} messages with same key...")
    with tracer.start_as_current_span("send_same_key_messages"):
        for payload in messages:
            await client.send(topic, test_key, payload)
    logger.info("TEST: All messages sent")

    logger.info("TEST: Subscribing...")
    await client.subscribe(handler)
    logger.info("TEST: Subscribed")

    async def wait_for_messages():
        while handler.message_count < len(messages):
            await handler.message_received.wait()
            handler.message_received.clear()

    logger.info("TEST: Waiting for all messages...")
    await asyncio.wait_for(wait_for_messages(), timeout=30.0)
    logger.info("TEST: All messages received")

    assert len(handler.messages) == len(messages)
    received_messages = [msg for msg in handler.messages if msg.key == test_key]
    received_sequences = [msg.payload["sequence"] for msg in received_messages]
    expected_sequences = [msg["sequence"] for msg in messages]

    assert received_sequences == expected_sequences
    for msg in received_messages:
        assert msg.topic == topic
        assert msg.key == test_key
    logger.info("TEST test_same_key_message_order: PASSED")


class TransientErrorHandler(EventHandler):
    def __init__(self):
        logger.info("TransientErrorHandler.__init__() called")
        self.received_message = False
        self.retry_event = tsasync.Event()
        logger.info("TransientErrorHandler.__init__() completed")

    @transient(ValueError)
    async def on_message(self, context: Context, message: Message) -> None:
        logger.info(f"TransientErrorHandler.on_message() called, received_message={self.received_message}")
        if self.received_message:
            logger.info("TransientErrorHandler: Setting retry_event")
            self.retry_event.set()
        else:
            self.received_message = True
            logger.info("TransientErrorHandler: Raising ValueError")
            raise ValueError("Transient error occurred")

    async def on_timer(self, context: Context, timer: Timer) -> None:
        logger.info("TransientErrorHandler.on_timer() called")
        pass


@pytest.mark.asyncio
async def test_transient_error_decorator(client, random_topic_and_group):
    logger.info("=" * 40)
    logger.info("TEST test_transient_error_decorator: STARTING")

    topic, _ = random_topic_and_group
    logger.info("TEST: Creating TransientErrorHandler...")
    handler = TransientErrorHandler()

    logger.info("TEST: Subscribing...")
    await client.subscribe(handler)
    logger.info("TEST: Subscribed")

    test_key = "test-key"
    test_payload = {"content": "Trigger transient error"}
    logger.info(f"TEST: Sending message key={test_key}")
    await client.send(topic, test_key, test_payload)
    logger.info("TEST: Message sent")

    logger.info("TEST: Waiting for retry...")
    await asyncio.wait_for(handler.retry_event.wait(), timeout=30.0)
    logger.info("TEST test_transient_error_decorator: PASSED")


class PermanentErrorHandler(EventHandler):
    def __init__(self):
        logger.info("PermanentErrorHandler.__init__() called")
        self.error_raised = tsasync.Event()
        self.message_count = 0
        logger.info("PermanentErrorHandler.__init__() completed")

    @permanent(ValueError)
    async def on_message(self, context: Context, message: Message) -> None:
        logger.info(f"PermanentErrorHandler.on_message() called, count={self.message_count}")
        self.message_count += 1
        self.error_raised.set()
        logger.info("PermanentErrorHandler: Raising ValueError")
        raise ValueError("Permanent error occurred")

    async def on_timer(self, context: Context, timer: Timer) -> None:
        logger.info("PermanentErrorHandler.on_timer() called")
        pass


@pytest.mark.asyncio
async def test_permanent_error_decorator(client, random_topic_and_group):
    logger.info("=" * 40)
    logger.info("TEST test_permanent_error_decorator: STARTING")

    topic, _ = random_topic_and_group
    logger.info("TEST: Creating PermanentErrorHandler...")
    handler = PermanentErrorHandler()

    logger.info("TEST: Subscribing...")
    await client.subscribe(handler)
    logger.info("TEST: Subscribed")

    test_key = "test-key"
    test_payload = {"content": "Trigger permanent error"}
    logger.info(f"TEST: Sending message key={test_key}")
    await client.send(topic, test_key, test_payload)
    logger.info("TEST: Message sent")

    logger.info("TEST: Waiting for error...")
    await asyncio.wait_for(handler.error_raised.wait(), timeout=30.0)
    logger.info("TEST: Error raised, sleeping 5 seconds...")

    await asyncio.sleep(5)
    logger.info(f"TEST: message_count={handler.message_count}")

    assert handler.message_count == 1
    logger.info("TEST test_permanent_error_decorator: PASSED")


@pytest.mark.asyncio
async def test_best_effort_mode_does_not_retry(random_topic_and_group):
    logger.info("=" * 40)
    logger.info("TEST test_best_effort_mode_does_not_retry: STARTING")

    topic, group = random_topic_and_group
    logger.info("TEST: Creating TransientErrorHandler...")
    handler = TransientErrorHandler()

    logger.info("TEST: Creating ProsodyClient with mode=best-effort...")
    client_with_best_effort = ProsodyClient(
        bootstrap_servers="localhost:9094",
        source_system="test-send",
        group_id=group,
        subscribed_topics=topic,
        probe_port=None,
        cassandra_nodes="localhost:9042",
        mode="best-effort"
    )
    logger.info("TEST: Client created")

    logger.info("TEST: Subscribing...")
    await client_with_best_effort.subscribe(handler)
    logger.info("TEST: Subscribed")

    test_key = "test-key"
    test_payload = {"content": "Trigger transient error"}
    logger.info(f"TEST: Sending message key={test_key}")
    await client_with_best_effort.send(topic, test_key, test_payload)
    logger.info("TEST: Message sent, sleeping 5 seconds...")

    await asyncio.sleep(5)
    logger.info(f"TEST: retry_event.is_set()={handler.retry_event.is_set()}")

    assert not handler.retry_event.is_set()

    logger.info("TEST: Unsubscribing...")
    await client_with_best_effort.unsubscribe()
    logger.info("TEST test_best_effort_mode_does_not_retry: PASSED")


class CallbackTimerHandler(EventHandler):
    """Handler that executes a custom callback function for timer testing"""
    __test__ = False

    def __init__(self, message_callback=None):
        logger.info("CallbackTimerHandler.__init__() called")
        self.timer_events = tsasync.Channel()
        self.results = tsasync.Channel()
        self.message_callback = message_callback
        logger.info("CallbackTimerHandler.__init__() completed")

    async def on_message(self, context: Context, message: Message) -> None:
        logger.info(f"CallbackTimerHandler.on_message() called with key={message.key}")
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
        logger.info("CallbackTimerHandler.on_message() completed")

    async def on_timer(self, context: Context, timer: Timer) -> None:
        logger.info(f"CallbackTimerHandler.on_timer() called with key={timer.key}")
        await self.timer_events.send({
            "context": context,
            "timer": timer
        })
        logger.info("CallbackTimerHandler.on_timer() completed")


class TimerTestHandler(CallbackTimerHandler):
    """Backwards compatibility wrapper"""
    __test__ = False

    def __init__(self):
        super().__init__()
        self.message_events = self.results
        self.operation_results = self.results


@pytest.mark.asyncio
async def test_timer_scheduling_and_firing(client, random_topic_and_group):
    logger.info("=" * 40)
    logger.info("TEST test_timer_scheduling_and_firing: STARTING")

    topic, _ = random_topic_and_group

    async def schedule_timer_callback(context: Context, message: Message):
        logger.info("schedule_timer_callback: Scheduling timer...")
        scheduled_time = datetime.now(timezone.utc) + timedelta(seconds=2)
        await context.schedule(scheduled_time)
        logger.info(f"schedule_timer_callback: Timer scheduled for {scheduled_time}")
        return {"scheduled_time": scheduled_time}

    logger.info("TEST: Creating CallbackTimerHandler...")
    handler = CallbackTimerHandler(schedule_timer_callback)

    logger.info("TEST: Subscribing...")
    await client.subscribe(handler)
    logger.info("TEST: Subscribed")

    test_key = "timer-test-key"
    logger.info(f"TEST: Sending message key={test_key}")
    await client.send(topic, test_key, {"test": "data"})
    logger.info("TEST: Message sent")

    logger.info("TEST: Waiting for result...")
    operation_result = await asyncio.wait_for(handler.results.receive(), timeout=30.0)
    logger.info(f"TEST: Result received: {operation_result}")
    assert operation_result["success"]
    scheduled_time = operation_result["scheduled_time"]

    logger.info("TEST: Waiting for timer to fire...")
    timer_event = await asyncio.wait_for(handler.timer_events.receive(), timeout=5.0)
    logger.info(f"TEST: Timer fired")

    assert timer_event["timer"].key == test_key
    time_diff = abs((timer_event["timer"].time - scheduled_time).total_seconds())
    assert time_diff <= TIMER_TOLERANCE_SECONDS
    logger.info("TEST test_timer_scheduling_and_firing: PASSED")


@pytest.mark.asyncio
async def test_timer_unschedule(client, random_topic_and_group):
    logger.info("=" * 40)
    logger.info("TEST test_timer_unschedule: STARTING")

    topic, _ = random_topic_and_group

    async def unschedule_timer_callback(context: Context, message: Message):
        logger.info("unschedule_timer_callback: Scheduling timers...")
        timer1_time = datetime.now(timezone.utc) + timedelta(seconds=3)
        timer2_time = datetime.now(timezone.utc) + timedelta(seconds=4)

        await context.schedule(timer1_time)
        await context.schedule(timer2_time)

        scheduled = await context.scheduled()
        logger.info(f"unschedule_timer_callback: {len(scheduled)} timers scheduled")
        assert len(scheduled) == 2

        logger.info("unschedule_timer_callback: Unscheduling first timer...")
        await context.unschedule(timer1_time)

        scheduled_after = await context.scheduled()
        logger.info(f"unschedule_timer_callback: {len(scheduled_after)} timers remaining")
        return {
            "remaining_timers": len(scheduled_after),
            "expected_timer_time": timer2_time
        }

    logger.info("TEST: Creating CallbackTimerHandler...")
    handler = CallbackTimerHandler(unschedule_timer_callback)

    logger.info("TEST: Subscribing...")
    await client.subscribe(handler)
    logger.info("TEST: Subscribed")

    test_key = "unschedule-test-key"
    logger.info(f"TEST: Sending message key={test_key}")
    await client.send(topic, test_key, {"test": "data"})
    logger.info("TEST: Message sent")

    logger.info("TEST: Waiting for result...")
    operation_result = await asyncio.wait_for(handler.results.receive(), timeout=30.0)
    logger.info(f"TEST: Result: {operation_result}")
    assert operation_result["success"]
    assert operation_result["remaining_timers"] == 1
    expected_timer_time = operation_result["expected_timer_time"]

    logger.info("TEST: Waiting for timer to fire...")
    timer_event = await asyncio.wait_for(handler.timer_events.receive(), timeout=6.0)
    logger.info("TEST: Timer fired")

    time_diff = abs((timer_event["timer"].time - expected_timer_time).total_seconds())
    assert time_diff <= TIMER_TOLERANCE_SECONDS
    logger.info("TEST test_timer_unschedule: PASSED")


@pytest.mark.asyncio
async def test_timer_clear_and_schedule(client, random_topic_and_group):
    logger.info("=" * 40)
    logger.info("TEST test_timer_clear_and_schedule: STARTING")

    topic, _ = random_topic_and_group

    async def clear_and_schedule_callback(context: Context, message: Message):
        logger.info("clear_and_schedule_callback: Scheduling timers...")
        timer1_time = datetime.now(timezone.utc) + timedelta(seconds=5)
        timer2_time = datetime.now(timezone.utc) + timedelta(seconds=6)
        await context.schedule(timer1_time)
        await context.schedule(timer2_time)

        scheduled_before = await context.scheduled()
        logger.info(f"clear_and_schedule_callback: {len(scheduled_before)} timers scheduled")
        assert len(scheduled_before) == 2

        new_timer_time = datetime.now(timezone.utc) + timedelta(seconds=2)
        logger.info("clear_and_schedule_callback: Calling clear_and_schedule...")
        await context.clear_and_schedule(new_timer_time)

        scheduled_after = await context.scheduled()
        logger.info(f"clear_and_schedule_callback: {len(scheduled_after)} timers remaining")
        return {
            "remaining_timers": len(scheduled_after),
            "new_timer_time": new_timer_time
        }

    logger.info("TEST: Creating CallbackTimerHandler...")
    handler = CallbackTimerHandler(clear_and_schedule_callback)

    logger.info("TEST: Subscribing...")
    await client.subscribe(handler)
    logger.info("TEST: Subscribed")

    test_key = "clear-schedule-test-key"
    logger.info(f"TEST: Sending message key={test_key}")
    await client.send(topic, test_key, {"test": "data"})
    logger.info("TEST: Message sent")

    logger.info("TEST: Waiting for result...")
    operation_result = await asyncio.wait_for(handler.results.receive(), timeout=30.0)
    logger.info(f"TEST: Result: {operation_result}")
    assert operation_result["success"]
    assert operation_result["remaining_timers"] == 1
    new_timer_time = operation_result["new_timer_time"]

    logger.info("TEST: Waiting for timer to fire...")
    timer_event = await asyncio.wait_for(handler.timer_events.receive(), timeout=4.0)
    logger.info("TEST: Timer fired")

    time_diff = abs((timer_event["timer"].time - new_timer_time).total_seconds())
    assert time_diff <= TIMER_TOLERANCE_SECONDS
    logger.info("TEST test_timer_clear_and_schedule: PASSED")


@pytest.mark.asyncio
async def test_timer_clear_scheduled(client, random_topic_and_group):
    logger.info("=" * 40)
    logger.info("TEST test_timer_clear_scheduled: STARTING")

    topic, _ = random_topic_and_group

    async def clear_scheduled_callback(context: Context, message: Message):
        logger.info("clear_scheduled_callback: Scheduling timers...")
        timer1_time = datetime.now(timezone.utc) + timedelta(seconds=3)
        timer2_time = datetime.now(timezone.utc) + timedelta(seconds=4)
        timer3_time = datetime.now(timezone.utc) + timedelta(seconds=5)

        await context.schedule(timer1_time)
        await context.schedule(timer2_time)
        await context.schedule(timer3_time)

        scheduled_before = await context.scheduled()
        logger.info(f"clear_scheduled_callback: {len(scheduled_before)} timers scheduled")
        assert len(scheduled_before) == 3

        logger.info("clear_scheduled_callback: Calling clear_scheduled...")
        await context.clear_scheduled()

        scheduled_after = await context.scheduled()
        logger.info(f"clear_scheduled_callback: {len(scheduled_after)} timers remaining")
        return {"remaining_timers": len(scheduled_after)}

    logger.info("TEST: Creating CallbackTimerHandler...")
    handler = CallbackTimerHandler(clear_scheduled_callback)

    logger.info("TEST: Subscribing...")
    await client.subscribe(handler)
    logger.info("TEST: Subscribed")

    test_key = "clear-all-test-key"
    logger.info(f"TEST: Sending message key={test_key}")
    await client.send(topic, test_key, {"test": "data"})
    logger.info("TEST: Message sent")

    logger.info("TEST: Waiting for result...")
    operation_result = await asyncio.wait_for(handler.results.receive(), timeout=30.0)
    logger.info(f"TEST: Result: {operation_result}")
    assert operation_result["success"]
    assert operation_result["remaining_timers"] == 0

    logger.info("TEST: Verifying no timers fire...")
    try:
        await asyncio.wait_for(handler.timer_events.receive(), timeout=6.0)
        assert False, "No timers should have fired after clearing"
    except asyncio.TimeoutError:
        logger.info("TEST: Timeout as expected - no timers fired")
        pass

    logger.info("TEST test_timer_clear_scheduled: PASSED")


@pytest.mark.asyncio
async def test_timer_scheduled_retrieval(client, random_topic_and_group):
    logger.info("=" * 40)
    logger.info("TEST test_timer_scheduled_retrieval: STARTING")

    topic, _ = random_topic_and_group

    async def scheduled_retrieval_callback(context: Context, message: Message):
        logger.info("scheduled_retrieval_callback: Checking initial state...")
        scheduled_empty = await context.scheduled()
        assert len(scheduled_empty) == 0

        now = datetime.now(timezone.utc)
        timer_times = [
            now + timedelta(seconds=10),
            now + timedelta(seconds=20),
            now + timedelta(seconds=30)
        ]

        logger.info(f"scheduled_retrieval_callback: Scheduling {len(timer_times)} timers...")
        for timer_time in timer_times:
            await context.schedule(timer_time)

        scheduled = await context.scheduled()
        logger.info(f"scheduled_retrieval_callback: {len(scheduled)} timers scheduled")
        return {
            "scheduled_count": len(scheduled),
            "expected_times": timer_times,
            "scheduled_times": scheduled
        }

    logger.info("TEST: Creating CallbackTimerHandler...")
    handler = CallbackTimerHandler(scheduled_retrieval_callback)

    logger.info("TEST: Subscribing...")
    await client.subscribe(handler)
    logger.info("TEST: Subscribed")

    test_key = "scheduled-retrieval-test"
    logger.info(f"TEST: Sending message key={test_key}")
    await client.send(topic, test_key, {"test": "data"})
    logger.info("TEST: Message sent")

    logger.info("TEST: Waiting for result...")
    operation_result = await asyncio.wait_for(handler.results.receive(), timeout=30.0)
    logger.info(f"TEST: Result: {operation_result}")
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

    logger.info("TEST test_timer_scheduled_retrieval: PASSED")
