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

DEFAULT_TIMEOUT = 30.0


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

