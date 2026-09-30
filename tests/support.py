"""Shared test scaffolding: constants, handlers, and await helpers.

The fixtures that use these live in ``conftest.py``.
"""

import asyncio
from typing import List
import uuid

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from prosody import (
    Context,
    ExciseMessage,
    EventHandler,
    Message,
    Timer,
    value,
    map,
    set as set_definition,
    deque,
    message_value,
    message_map,
    message_deque,
)
import tsasync


DEFAULT_TIMEOUT = 30.0

BOOTSTRAP = "localhost:9094"

CASSANDRA_NODES = "localhost:9042"

CASSANDRA_KEYSPACE = "prosody_test"

# Canonical registered set: one of every kind x payload. ``state()`` binds these
# same frozen definitions.
STATE_DEFS = {
    "cart": value("cart"),
    "totals": map("totals", keyset_limit=256),
    "tags": set_definition("tags"),
    "backlog": deque("backlog"),
    "bounded": deque("bounded", capacity=3),
    "last_msg": message_value("last-msg"),
    "msg_index": message_map("msg-index"),
    "msg_log": message_deque("msg-log"),
}

STATE_COLLECTIONS = list(STATE_DEFS.values())

# Number of map entries used by the chunk-boundary scan tests; > the 256-item
# native ready-chunk cap so a scan must flatten across at least two pulls.
CHUNK_SPAN = 300

provider = TracerProvider()

# Sets the global default tracer provider
trace.set_tracer_provider(provider)

# Creates a tracer from the global tracer provider
tracer = trace.get_tracer("prosody-test")

def nonce() -> str:
    return uuid.uuid4().hex

async def _wait(awaitable):
    """Await ``awaitable`` bounded by ``DEFAULT_TIMEOUT`` (used in handlers and
    test bodies alike so a wedged op can never hang the suite)."""
    return await asyncio.wait_for(awaitable, timeout=DEFAULT_TIMEOUT)

async def _collect(scan):
    """Drain an async scan into a list. ``async for`` itself cannot take a
    ``wait_for``; wrapping the whole drain in one coroutine lets the caller bound
    it (and every underlying ``__anext__``) under a single ``asyncio.wait_for``."""
    out = []
    async for item in scan:
        out.append(item)
    return out

def _msg_fields(m):
    return {
        "topic": m.topic,
        "partition": m.partition,
        "offset": m.offset,
        "key": m.key,
        "timestamp": m.timestamp,
        "payload": m.payload,
    }

class TestHandler(EventHandler):
    async def on_excise(self, context: Context, message: ExciseMessage) -> None:
        self.messages.append(message)
        self.message_received.set()

    __test__ = False

    def __init__(self):
        self.messages: List[Message | ExciseMessage] = []
        self.message_count = 0
        self.message_received = tsasync.Event()

    async def on_message(self, context: Context, message: Message) -> None:
        with tracer.start_as_current_span("receive"):
            self.messages.append(message)
            self.message_count += 1
            self.message_received.set()

    async def on_timer(self, context: Context, timer: Timer) -> None:
        pass

class StateHandler(EventHandler):
    """Injectable-callback handler. Assertions run inside ``on_message``; the
    callback reports observation dicts over ``results`` (a ``tsasync.Channel``,
    thread-safe for the Rust->Python signalling), or raises to drive retry."""

    async def on_excise(self, context, message) -> None:
        return None

    __test__ = False

    def __init__(self, on_msg, on_tmr=None):
        self.on_msg = on_msg
        self.on_tmr = on_tmr
        self.results = tsasync.Channel()

    async def on_message(self, context, message) -> None:
        await self.on_msg(context, message, self.results)

    async def on_timer(self, context, timer) -> None:
        if self.on_tmr is not None:
            await self.on_tmr(context, timer, self.results)

async def _make_state_client(topic, group, client_factory):
    return await client_factory(
        bootstrap_servers=BOOTSTRAP,
        source_system="test-state",
        group_id=group,
        subscribed_topics=topic,
        probe_port=None,
        cassandra_nodes=CASSANDRA_NODES,
        cassandra_keyspace=CASSANDRA_KEYSPACE,
        state_collections=STATE_COLLECTIONS,
        # >= 2 so the async-bridging test can observe two keys interleaving.
        max_concurrency=4,
    )


class NativeScan:
    """A native cursor stub over a fixed list of items."""

    def __init__(self, items):
        self._items = iter(list(items))
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._items)
        except StopIteration:
            raise StopAsyncIteration from None

    async def aclose(self):
        self.closed = True


class NativeRecorder:
    """A native handle stub that records each call.

    An async method returns ``results[name]``. ``scan`` yields ``items``, and
    ``keys`` yields the first element of each item.
    """

    def __init__(self, results=None, items=()):
        self.calls = []
        self.scans = []
        self._results = results or {}
        self._items = list(items)

    @property
    def queries(self):
        return [args[-1] for name, args in self.calls if name in ("scan", "keys")]

    def scan(self, *args):
        return self._open("scan", args, self._items)

    def keys(self, *args):
        return self._open("keys", args, [item[0] for item in self._items])

    def _open(self, name, args, items):
        self.calls.append((name, args))
        self.scans.append(NativeScan(items))
        return self.scans[-1]

    def __getattr__(self, name):
        async def call(*args):
            self.calls.append((name, args))
            return self._results.get(name)

        return call
