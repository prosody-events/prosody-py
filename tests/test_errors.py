"""Transient and permanent handler failures and the retry that follows."""

import asyncio

from prosody import (
    Context,
    ExciseMessage,
    EventHandler,
    Message,
    Timer,
    permanent,
    transient,
)
import pytest
import tsasync

from support import DEFAULT_TIMEOUT



class TransientErrorHandler(EventHandler):
    """Fails the first delivery with a transient error, then signals the retry.

    ``fail`` selects the transient failure: raise a ``@transient`` error, or
    return a result that has no JSON form (a caller mistake).
    """

    async def on_excise(self, context: Context, message: ExciseMessage) -> None:
        return None

    def __init__(self, fail: str = "raise"):
        self.fail = fail
        self.received_message = False
        self.retry_event = tsasync.Event()

    @transient(ValueError)
    async def on_message(self, context: Context, message: Message) -> object:
        if self.received_message:
            self.retry_event.set()
            return None
        self.received_message = True
        if self.fail == "unencodable":
            return object()
        raise ValueError("Transient error occurred")

    async def on_timer(self, context: Context, timer: Timer) -> None:
        pass

@pytest.mark.parametrize("fail", ["raise", "unencodable"])
async def test_transient_failure_retries(client, random_topic_and_group, fail):
    topic, _ = random_topic_and_group
    handler = TransientErrorHandler(fail)
    await asyncio.wait_for(client.subscribe(handler), timeout=DEFAULT_TIMEOUT)

    payload = {"content": "Trigger transient error"}
    await asyncio.wait_for(client.send(topic, "test-key", payload), timeout=DEFAULT_TIMEOUT)
    await asyncio.wait_for(handler.retry_event.wait(), timeout=DEFAULT_TIMEOUT)

class PermanentErrorHandler(EventHandler):
    async def on_excise(self, context: Context, message: ExciseMessage) -> None:
        return None

    def __init__(self):
        self.error_raised = tsasync.Event()
        self.message_count = 0

    @permanent(ValueError)
    async def on_message(self, context: Context, message: Message) -> None:
        self.message_count += 1
        self.error_raised.set()
        raise ValueError("Permanent error occurred")

    async def on_timer(self, context: Context, timer: Timer) -> None:
        pass

async def test_permanent_error_decorator(client, random_topic_and_group):

    topic, _ = random_topic_and_group
    handler = PermanentErrorHandler()

    await asyncio.wait_for(client.subscribe(handler), timeout=DEFAULT_TIMEOUT)

    test_key = "test-key"
    test_payload = {"content": "Trigger permanent error"}
    await asyncio.wait_for(client.send(topic, test_key, test_payload), timeout=DEFAULT_TIMEOUT)

    await asyncio.wait_for(handler.error_raised.wait(), timeout=DEFAULT_TIMEOUT)

    await asyncio.sleep(5)

    assert handler.message_count == 1

async def test_best_effort_mode_does_not_retry(random_topic_and_group, client_factory):

    topic, group = random_topic_and_group
    handler = TransientErrorHandler()

    client_with_best_effort = await client_factory(
        bootstrap_servers="localhost:9094",
        source_system="test-send",
        group_id=group,
        subscribed_topics=topic,
        probe_port=None,
        cassandra_nodes="localhost:9042",
        mode="best-effort",
    )

    await asyncio.wait_for(client_with_best_effort.subscribe(handler), timeout=DEFAULT_TIMEOUT)

    test_key = "test-key"
    test_payload = {"content": "Trigger transient error"}
    await asyncio.wait_for(client_with_best_effort.send(topic, test_key, test_payload), timeout=DEFAULT_TIMEOUT)

    await asyncio.sleep(5)

    assert not handler.retry_event.is_set()

