"""Pure-Python unit tests for the typed keyed-state surface.

No FFI / no Kafka: the wrappers are exercised against recording stubs so the
delegation, transforms, and definition/config shaping are locked without a live
consumer. The native ``Context.state`` dispatcher (Rust) is exercised by the
infra-backed FFI scenarios in ``test_keyed_state.py``.
"""

import contextlib
import dataclasses
from datetime import datetime, timedelta, timezone

import pytest

import prosody
from prosody import (
    Direction,
    Message,
    value,
    map,
    deque,
    message_value,
    message_map,
    message_deque,
    ValueState,
    MapState,
    DequeState,
    StateError,
    PermanentStateError,
    TransientStateError,
    NullValueError,
    PermanentError,
    TransientError,
)


# --- definitions / to_config ---


def test_value_to_config():
    assert value("cart").to_config() == {
        "name": "cart",
        "kind": "value",
        "payload": "json",
        "ttl_seconds": None,
        "read_uncommitted": None,
        "keyset_limit": None,
        "capacity": None,
    }


def test_ttl_timedelta_and_int():
    assert value("c", ttl=timedelta(days=30)).to_config()["ttl_seconds"] == 2592000
    assert value("c", ttl=60).to_config()["ttl_seconds"] == 60


def test_map_keyset_limit():
    assert map("s", keyset_limit=256).to_config()["keyset_limit"] == 256
    assert map("s").to_config()["keyset_limit"] is None


def test_deque_capacity_to_config():
    assert deque("d", capacity=100).to_config()["capacity"] == 100
    assert message_deque("md", capacity=50).to_config()["capacity"] == 50
    assert deque("d").to_config()["capacity"] is None
    # capacity is deque-only: value/map definitions carry it as None.
    assert value("v").to_config()["capacity"] is None
    assert map("m").to_config()["capacity"] is None


def test_read_uncommitted_passthrough():
    assert value("c", read_uncommitted=True).to_config()["read_uncommitted"] is True


def test_kinds_and_payloads():
    assert deque("d").to_config()["kind"] == "deque"
    mv = message_value("mv").to_config()
    assert (mv["kind"], mv["payload"]) == ("value", "message")
    mm = message_map("mm").to_config()
    assert (mm["kind"], mm["payload"]) == ("map", "message")
    md = message_deque("md").to_config()
    assert (md["kind"], md["payload"]) == ("deque", "message")


def test_message_map_keyset_limit():
    assert message_map("mm", keyset_limit=128).to_config()["keyset_limit"] == 128


def test_definitions_frozen():
    d = value("cart")
    with pytest.raises(dataclasses.FrozenInstanceError):
        d.name = "other"


# --- Direction tokens (must equal P2 parse_direction) ---


def test_direction_tokens():
    assert Direction.FORWARD.value == "forward"
    assert Direction.BACKWARD.value == "backward"


# --- error hierarchy ---


def test_permanent_state_error():
    e = PermanentStateError("x")
    assert e.is_permanent is True
    assert isinstance(e, (StateError, PermanentError))


def test_transient_state_error():
    e = TransientStateError("x")
    assert e.is_permanent is False
    assert isinstance(e, (StateError, TransientError))


def test_null_value_error_mro():
    e = NullValueError("x")
    assert e.is_permanent is False
    assert isinstance(e, ValueError)
    assert isinstance(e, TransientStateError)
    assert isinstance(e, StateError)


def test_state_error_is_catchable_brand():
    # The documented `except StateError` form requires StateError to derive
    # from BaseException; a bare mixin raises TypeError at the except clause.
    for exc in (
        PermanentStateError("p"),
        TransientStateError("t"),
        NullValueError("n"),
    ):
        with pytest.raises(StateError):
            raise exc
        try:
            raise exc
        except StateError as caught:
            assert caught is exc


# --- generic Message ---


def test_message_generic_subscript_and_payload():
    assert Message[dict] is not None  # subscriptable
    m = Message("t", 0, 0, datetime.now(timezone.utc), "k", {"a": 1})
    assert m.payload == {"a": 1}
    assert (m.topic, m.partition, m.offset, m.key) == ("t", 0, 0, "k")


# --- exports smoke ---


def test_exports_present():
    for n in (
        "Direction",
        "value",
        "map",
        "deque",
        "message_value",
        "message_map",
        "message_deque",
        "ValueDefinition",
        "MapDefinition",
        "DequeDefinition",
        "MessageValueDefinition",
        "MessageMapDefinition",
        "MessageDequeDefinition",
        "ValueState",
        "MapState",
        "DequeState",
        "StateError",
        "PermanentStateError",
        "TransientStateError",
        "NullValueError",
    ):
        assert hasattr(prosody, n), n


# --- wrapper delegation via recording stubs ---


class _StubScan:
    def __init__(self, items):
        self._items = list(items)
        self._i = 0
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._i >= len(self._items):
            raise StopAsyncIteration
        item = self._items[self._i]
        self._i += 1
        return item

    async def aclose(self):
        self.closed = True


class _StubNative:
    def __init__(self, scan_items=()):
        self.calls = []
        self._scan_items = scan_items
        self.scans = []

    def scan(self, direction):
        self.calls.append(("scan", direction))
        s = _StubScan(self._scan_items)
        self.scans.append(s)
        return s

    def keys(self, direction):
        # The cheap key-only scan: yields bare keys, mirroring the native path
        # that never decodes a value.
        self.calls.append(("keys", direction))
        s = _StubScan([k for k, _ in self._scan_items])
        self.scans.append(s)
        return s

    def __getattr__(self, name):
        async def coro(*args):
            self.calls.append((name, args))
            return ("R", name, args)

        return coro


@pytest.mark.asyncio
async def test_value_delegation():
    n = _StubNative()
    v = ValueState(n)
    await v.get()
    await v.set(1)
    await v.clear()
    await v.commit()
    await v.rollback()
    assert [c[0] for c in n.calls] == ["get", "set", "clear", "commit", "rollback"]


@pytest.mark.asyncio
async def test_map_delegation():
    n = _StubNative()
    m = MapState(n)
    await m.get("k")
    await m.get_many(["a", "b"])
    await m.set("k", 1)
    await m.remove("k")
    await m.clear()
    await m.commit()
    await m.rollback()
    assert [c[0] for c in n.calls] == [
        "get",
        "get_many",
        "set",
        "remove",
        "clear",
        "commit",
        "rollback",
    ]
    assert n.calls[0][1] == ("k",)
    assert n.calls[1][1] == (["a", "b"],)
    assert n.calls[2][1] == ("k", 1)


@pytest.mark.asyncio
async def test_deque_method_mapping():
    n = _StubNative()
    d = DequeState(n)
    await d.append(1)
    await d.appendleft(2)
    await d.pop()
    await d.popleft()
    await d.get(3)
    await d.size()
    await d.is_empty()
    assert [c[0] for c in n.calls] == [
        "push_back",
        "push_front",
        "pop_back",
        "pop_front",
        "get",
        "len",
        "is_empty",
    ]
    assert n.calls[0][1] == (1,)  # append forwards item to push_back
    assert n.calls[4][1] == (3,)  # get(index) forwards index


@pytest.mark.asyncio
async def test_map_scan_transforms():
    entries = [("a", 1), ("b", 2)]
    m = MapState(_StubNative(entries))
    assert [e async for e in m.items()] == [("a", 1), ("b", 2)]
    assert [k async for k in m.keys()] == ["a", "b"]
    assert [v async for v in m.values()] == [1, 2]
    assert [k async for k in m] == ["a", "b"]  # __aiter__ = keys (dict-like)


@pytest.mark.asyncio
async def test_map_items_direction_token():
    n = _StubNative([("a", 1)])
    async for _ in MapState(n).items(Direction.BACKWARD):
        pass
    assert n.calls[0] == ("scan", "backward")  # Direction -> token


@pytest.mark.asyncio
async def test_map_keys_direction_token():
    # keys() drives the cheap native `keys` cursor (not the pair `scan`) and
    # threads the direction through.
    n = _StubNative([("a", 1)])
    async for _ in MapState(n).keys():
        pass
    assert n.calls[0] == ("keys", "forward")

    n_back = _StubNative([("a", 1)])
    async for _ in MapState(n_back).keys(Direction.BACKWARD):
        pass
    assert n_back.calls[0] == ("keys", "backward")


@pytest.mark.asyncio
async def test_map_values_forward_token():
    # values() stays a forward pair-scan projection.
    n = _StubNative([("a", 1)])
    async for _ in MapState(n).values():
        pass
    assert n.calls[0] == ("scan", "forward")


@pytest.mark.asyncio
async def test_map_contains_delegates():
    n = _StubNative()
    await MapState(n).contains("k")
    assert n.calls == [("contains_key", ("k",))]


class _GetStub:
    """A map native whose ``get`` returns a fixed value regardless of key, to
    exercise :meth:`MapState.get`'s absent-vs-present-falsy branch."""

    def __init__(self, value):
        self._value = value

    async def get(self, key):
        return self._value


@pytest.mark.asyncio
async def test_map_get_default():
    # Absent (native None) returns the default...
    assert await MapState(_GetStub(None)).get("k", "fallback") == "fallback"
    # ...and None when no default is given.
    assert await MapState(_GetStub(None)).get("k") is None
    # A present-but-falsy value returns as-is, NEVER the default (this is the
    # exact bug a `value or default` implementation would introduce).
    for falsy in (0, False, "", []):
        assert await MapState(_GetStub(falsy)).get("k", "fallback") == falsy
    # A present truthy value returns as-is.
    assert await MapState(_GetStub(7)).get("k", "fallback") == 7


@pytest.mark.asyncio
async def test_deque_peek_mapping():
    n = _StubNative()
    d = DequeState(n)
    await d.peek()
    await d.peekleft()
    assert [c[0] for c in n.calls] == ["peek_back", "peek_front"]


@pytest.mark.asyncio
async def test_deque_values_and_aiter():
    n = _StubNative([1, 2, 3])
    assert [x async for x in DequeState(n).values()] == [1, 2, 3]
    assert [x async for x in DequeState(_StubNative([9]))] == [9]  # __aiter__


@pytest.mark.asyncio
async def test_deque_values_direction_token():
    n = _StubNative([1])
    async for _ in DequeState(n).values(Direction.BACKWARD):
        pass
    assert n.calls[0] == ("scan", "backward")


@pytest.mark.asyncio
async def test_aclosing_closes_scan():
    n = _StubNative([1, 2, 3])
    it = DequeState(n).values()
    async with contextlib.aclosing(it):
        pass
    assert n.scans[0].closed is True
