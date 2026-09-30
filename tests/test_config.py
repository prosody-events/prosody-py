"""Client and admin option handling at the Python boundary."""

import ast
import asyncio
import pathlib
import uuid

import prosody
from prosody import ProsodyClient
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
