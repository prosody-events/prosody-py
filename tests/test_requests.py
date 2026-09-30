"""Requests that a subsystem answers with its handler result."""

import asyncio
from datetime import timedelta

from prosody import (
    Context,
    ExciseMessage,
    EventHandler,
    Failure,
    HandlerError,
    Message,
    PermanentError,
    Success,
    Timer,
)

from support import DEFAULT_TIMEOUT


class RequestHandler(EventHandler):
    async def on_message(self, context: Context, message: Message):
        return {"key": message.key, "requested": message.response_requested}

    async def on_excise(self, context: Context, message: ExciseMessage):
        return {"key": message.key, "requested": message.response_requested}

    async def on_timer(self, context: Context, timer: Timer):
        return None

class RejectingRequestHandler(EventHandler):
    async def on_message(self, context: Context, message: Message):
        raise PermanentError("request rejected")

    async def on_excise(self, context: Context, message: ExciseMessage):
        raise PermanentError("request rejected")

    async def on_timer(self, context: Context, timer: Timer):
        raise PermanentError("request rejected")

async def test_request_returns_the_local_handler_response(random_topic_and_group, client_factory):
    topic, group = random_topic_and_group
    client = await client_factory(
        bootstrap_servers="localhost:9094",
        source_system="request-test",
        group_id=group,
        subscribed_topics=topic,
        probe_port=None,
        cassandra_nodes="localhost:9042",
        subsystem="inventory",
    )
    await asyncio.wait_for(client.subscribe(RequestHandler()), timeout=DEFAULT_TIMEOUT)
    results = await asyncio.wait_for(
        client.request(
            topic,
            "order-1",
            {"type": "order.created"},
            subsystems=["inventory"],
            timeout=timedelta(seconds=DEFAULT_TIMEOUT),
        ),
        timeout=DEFAULT_TIMEOUT,
    )
    assert len(results) == 1
    assert results["inventory"] == Success({"key": "order-1", "requested": True})

async def test_excise_request_returns_the_local_handler_response(
    random_topic_and_group, client_factory
):
    topic, group = random_topic_and_group
    client = await client_factory(
        bootstrap_servers="localhost:9094",
        source_system="request-excise-test",
        group_id=group,
        subscribed_topics=topic,
        probe_port=None,
        cassandra_nodes="localhost:9042",
        subsystem="inventory",
    )
    await asyncio.wait_for(client.subscribe(RequestHandler()), timeout=DEFAULT_TIMEOUT)
    results = await asyncio.wait_for(
        client.request_excise(
            topic,
            "order-1",
            subsystems=["inventory"],
            timeout=timedelta(seconds=DEFAULT_TIMEOUT),
        ),
        timeout=DEFAULT_TIMEOUT,
    )

    assert results == {
        "inventory": Success({"key": "order-1", "requested": True})
    }

async def test_request_returns_handler_failure(random_topic_and_group, client_factory):
    topic, group = random_topic_and_group
    client = await client_factory(
        bootstrap_servers="localhost:9094",
        source_system="request-test",
        group_id=group,
        subscribed_topics=topic,
        probe_port=None,
        cassandra_nodes="localhost:9042",
        subsystem="inventory",
    )
    await asyncio.wait_for(
        client.subscribe(RejectingRequestHandler()), timeout=DEFAULT_TIMEOUT
    )
    results = await asyncio.wait_for(
        client.request(
            topic,
            "order-1",
            {"type": "order.created"},
            subsystems=["inventory"],
            timeout=timedelta(seconds=DEFAULT_TIMEOUT),
        ),
        timeout=DEFAULT_TIMEOUT,
    )

    outcome = results["inventory"]
    assert isinstance(outcome, Failure)
    assert isinstance(outcome.error, HandlerError)
    assert "request rejected" in outcome.error.message
