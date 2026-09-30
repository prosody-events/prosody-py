"""Client and admin option handling at the Python boundary."""

import ast
import asyncio
import pathlib
import uuid
from datetime import timedelta

import pytest
import tsasync

import prosody
from prosody import Context, EventHandler, ExciseMessage, Message, ProsodyClient, Timer
from prosody.prosody import AdminClient

from support import DEFAULT_TIMEOUT


def _stub_options(method: str) -> list[str]:
    """Return the keyword-only option names of ``method`` in ``prosody.pyi``."""
    stub = pathlib.Path(prosody.__file__).with_name("prosody.pyi")
    for node in ast.walk(ast.parse(stub.read_text())):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == method:
            return [argument.arg for argument in node.args.kwonlyargs]
    raise AssertionError(f"{method} is not in prosody.pyi")


async def test_none_leaves_every_create_option_unset(client_factory):
    options = dict.fromkeys(_stub_options("create"))
    assert len(options) > 50, "the stub parse found too few options"
    options.update(
        bootstrap_servers="localhost:9092",
        group_id=f"test-group-{uuid.uuid4().hex}",
        subscribed_topics="none-options",
        source_system="test-none",
        mock=True,
    )

    client = await client_factory(**options)

    assert isinstance(client, ProsodyClient)


async def test_none_leaves_every_create_topic_option_unset():
    options = dict.fromkeys(_stub_options("create_topic"))
    assert options, "the stub parse found no options"
    admin = AdminClient(bootstrap_servers="localhost:9094")
    topic = f"test-topic-{uuid.uuid4().hex}"

    await asyncio.wait_for(admin.create_topic(topic, **options), DEFAULT_TIMEOUT)
    await asyncio.wait_for(admin.delete_topic(topic), DEFAULT_TIMEOUT)


class _Recorder(EventHandler):
    """Records each delivered message and signals when ``until`` arrives."""

    def __init__(self, until: str) -> None:
        self.until = until
        self.messages: list[Message] = []
        self.done = tsasync.Event()

    async def on_message(self, context: Context, message: Message) -> None:
        self.messages.append(message)
        if message.payload["id"] == self.until:
            self.done.set()

    async def on_excise(self, context: Context, message: ExciseMessage) -> None:
        pass

    async def on_timer(self, context: Context, timer: Timer) -> None:
        pass


async def test_statistics_interval_reaches_the_consumer(client_factory):
    options = dict(
        bootstrap_servers="localhost:9092",
        group_id=f"test-group-{uuid.uuid4().hex}",
        subscribed_topics="statistics",
        source_system="test-statistics",
        probe_port=None,
        mock=True,
    )
    accepted = await client_factory(**options, statistics_interval=timedelta(minutes=1))
    await accepted.subscribe(_Recorder("none"))

    rejected = await client_factory(**options, statistics_interval=0.0)
    with pytest.raises(RuntimeError, match="statistics_interval"):
        await rejected.subscribe(_Recorder("none"))


async def test_idempotence_cache_size_sizes_the_producer_cache(client_factory):
    admin = AdminClient(bootstrap_servers="localhost:9094")
    topic = f"test-topic-{uuid.uuid4().hex}"
    await asyncio.wait_for(admin.create_topic(topic, partition_count=1), DEFAULT_TIMEOUT)
    try:
        client = await client_factory(
            bootstrap_servers="localhost:9094",
            group_id=f"test-group-{uuid.uuid4().hex}",
            subscribed_topics=topic,
            source_system="test-idempotence",
            probe_port=None,
            cassandra_nodes="localhost:9042",
            idempotence_cache_size=1,
        )
        recorder = _Recorder("last")
        await client.subscribe(recorder)

        # A one-entry producer cache forgets "first" after the fillers, so
        # the producer sends the repeat, and "last" lands one offset later.
        ids = ["first", *(f"filler-{index}" for index in range(8)), "first", "last"]
        for event_id in ids:
            await asyncio.wait_for(client.send(topic, "key", {"id": event_id}), DEFAULT_TIMEOUT)
        await asyncio.wait_for(recorder.done.wait(), DEFAULT_TIMEOUT)

        assert recorder.messages[-1].offset == len(ids) - 1
    finally:
        await asyncio.wait_for(admin.delete_topic(topic), DEFAULT_TIMEOUT)


async def test_client_configuration(random_topic_and_group, client_factory):

    topic, group = random_topic_and_group
    client = await client_factory(
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
        mock=True,
    )
    assert isinstance(client, ProsodyClient)

async def test_deduplication_configuration(random_topic_and_group, client_factory):
    topic, group = random_topic_and_group

    # idempotence_version and idempotence_ttl as timedelta
    client = await client_factory(
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
    client = await client_factory(
        bootstrap_servers="localhost:9092",
        source_system="test-dedup",
        group_id=group,
        subscribed_topics=[topic],
        idempotence_ttl=604800.0,
        mock=True,
    )
    assert isinstance(client, ProsodyClient)

async def test_span_configuration(random_topic_and_group, client_factory):
    topic, group = random_topic_and_group

    # valid message_spans and timer_spans
    client = await client_factory(
        bootstrap_servers="localhost:9092",
        source_system="test-spans",
        group_id=group,
        subscribed_topics=[topic],
        message_spans="child",
        timer_spans="follows_from",
        mock=True,
    )
    assert isinstance(client, ProsodyClient)
