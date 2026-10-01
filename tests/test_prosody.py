"""Client lifecycle: create, subscribe, unsubscribe, and shutdown."""

import asyncio
from datetime import datetime, timezone
import uuid

from prosody import ExciseMessage, EventHandler, Message, Outcome, ProsodyClient
import pytest

from support import DEFAULT_TIMEOUT, TestHandler



def test_event_handler_is_runtime_subscriptable():
    assert EventHandler[dict] is not None

def test_outcome_alias_is_available_at_runtime():
    assert Outcome[dict] is not None

def test_excise_message_has_no_payload():
    message = ExciseMessage("events", 0, 1, datetime.now(timezone.utc), "key")

    assert not hasattr(message, "payload")

def test_source_system_and_response_flag_stay_out_of_equality():
    now = datetime.now(timezone.utc)
    extra = {"source_system": "producer", "response_requested": True}

    assert ExciseMessage("t", 0, 1, now, "k") == ExciseMessage("t", 0, 1, now, "k", **extra)
    assert Message("t", 0, 1, now, "k", {}) == Message("t", 0, 1, now, "k", {}, **extra)

async def test_create_starts_native_construction_when_awaited(monkeypatch):
    calls = []

    class NativeClient:
        @staticmethod
        def create(**configuration):
            calls.append(configuration)

            async def finish():
                return object()

            return finish()

    monkeypatch.setattr("prosody._NativeProsodyClient", NativeClient)
    pending = ProsodyClient.create(mock=True)
    assert calls == []

    assert isinstance(await pending, ProsodyClient)
    assert calls == [{"mock": True}]

def test_missing_native_client_reports_attribute_error():
    client = object.__new__(ProsodyClient)

    with pytest.raises(AttributeError):
        getattr(client, "missing")

@pytest.mark.parametrize("missing", ["on_message", "on_excise", "on_timer"])
async def test_subscribe_rejects_non_callable_handler_before_consumption(missing):
    class CompleteHandler(EventHandler):
        async def on_message(self, context, message):
            return None

        async def on_excise(self, context, message):
            return None

        async def on_timer(self, context, timer):
            return None

    client = await ProsodyClient.create(
        mock=True,
        bootstrap_servers="localhost:9094",
        group_id=f"handler-validation-{uuid.uuid4()}",
        subscribed_topics="events",
    )
    handler = CompleteHandler()
    setattr(handler, missing, None)
    try:
        with pytest.raises(TypeError, match=f"handler.{missing} must be callable"):
            await client.subscribe(handler)
        assert await client.consumer_state() == "configured"
    finally:
        await client.shutdown()

async def test_client_initialization(client):
    assert isinstance(client, ProsodyClient)
    state = await asyncio.wait_for(client.consumer_state(), timeout=DEFAULT_TIMEOUT)
    assert state == "configured"

async def test_shutdown_is_idempotent(client):
    await asyncio.gather(client.shutdown(), client.shutdown())

async def test_async_with_shuts_the_client_down(random_topic_and_group):
    topic, group = random_topic_and_group
    with pytest.raises(LookupError):
        try:
            async with await ProsodyClient.create(
                bootstrap_servers="localhost:9094",
                source_system="test-scope",
                group_id=group,
                subscribed_topics=topic,
                probe_port=None,
                cassandra_nodes="localhost:9042",
            ) as client:
                assert await client.consumer_state() == "configured"
                raise LookupError("the block exits with an error")
        finally:
            # The exit shut the client down before the error left the block.
            assert await client.consumer_state() == "shut_down"

async def test_client_source_system(client):
    assert client.source_system == "test-send"

async def test_client_raises_after_fork(client_factory):
    import os

    client = await client_factory(
        bootstrap_servers="localhost:9092",
        source_system="fork-test",
        mock=True,
    )

    rd, wr = os.pipe()
    pid = os.fork()

    if pid == 0:
        # Child process
        os.close(rd)
        try:
            asyncio.run(client.consumer_state())
            os.write(wr, b"ok")
        except RuntimeError as e:
            os.write(wr, f"error:{e}".encode())
        except Exception as e:
            os.write(wr, f"unexpected:{e}".encode())
        finally:
            os.close(wr)
            os._exit(0)

    # Parent process
    os.close(wr)
    chunks = []
    while chunk := os.read(rd, 4096):
        chunks.append(chunk)
    os.close(rd)
    os.waitpid(pid, 0)

    result = b"".join(chunks).decode()
    assert result.startswith("error:"), f"expected RuntimeError in child, got: {result!r}"
    assert "after fork" in result, f"unexpected error message: {result!r}"

async def test_client_subscribe_unsubscribe(client):

    handler = TestHandler()

    await asyncio.wait_for(client.subscribe(handler), timeout=DEFAULT_TIMEOUT)

    state = await asyncio.wait_for(client.consumer_state(), timeout=DEFAULT_TIMEOUT)
    assert state == "running"

    await asyncio.wait_for(client.unsubscribe(), timeout=DEFAULT_TIMEOUT)

    state = await asyncio.wait_for(client.consumer_state(), timeout=DEFAULT_TIMEOUT)
    assert state == "configured"

