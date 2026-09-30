"""Pure-Python unit tests for the typed keyed-state surface.

No Kafka: recording stubs stand in for the native handles, so these tests
check delegation and transforms without a live consumer. The
``test_keyed_state*.py`` files check the native layer against a live consumer.
"""

import contextlib
from datetime import datetime, timezone
import importlib

from prosody import (
    Direction,
    Message,
    value,
    ValueState,
    MapState,
    DequeState,
    StateError,
    PermanentStateError,
    TransientStateError,
    PermanentError,
    TransientError,
    PublishedDeque,
    PublishedMap,
)
from prosody.query import _KeyQuery, _PositionQuery
import pytest


def test_permanent_state_error():
    e = PermanentStateError("x")
    assert e.is_permanent is True
    assert isinstance(e, (StateError, PermanentError))

def test_transient_state_error():
    e = TransientStateError("x")
    assert e.is_permanent is False
    assert isinstance(e, (StateError, TransientError))

def test_state_error_is_catchable_brand():
    # The documented `except StateError` form requires StateError to derive
    # from BaseException; a bare mixin raises TypeError at the except clause.
    for exc in (
        PermanentStateError("p"),
        TransientStateError("t"),
    ):
        try:
            raise exc
        except StateError as caught:
            assert caught is exc
        with pytest.raises(StateError):
            raise exc

def test_message_generic_subscript_and_payload():
    assert Message[dict] is not None  # subscriptable
    m = Message("t", 0, 0, datetime.now(timezone.utc), "k", {"a": 1})
    assert m.payload == {"a": 1}
    assert (m.topic, m.partition, m.offset, m.key) == ("t", 0, 0, "k")

def test_exports_present():
    prosody = importlib.import_module("prosody")
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
        "SetState",
        "DequeState",
        "StoreOutcome",
        "set",
        "SetDefinition",
        "PublishedSet",
        "Demand",
        "DemandKind",
        "StateError",
        "PermanentStateError",
        "TransientStateError",
        "flush_telemetry",
        "shutdown_telemetry",
    ):
        assert hasattr(prosody, n), n

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

    def scan(self, query):
        self.calls.append(("scan", query))
        s = _StubScan(self._scan_items)
        self.scans.append(s)
        return s

    def keys(self, query):
        # The cheap key-only scan: yields bare keys, mirroring the native path
        # that never decodes a value.
        self.calls.append(("keys", query))
        s = _StubScan([k for k, _ in self._scan_items])
        self.scans.append(s)
        return s

    async def commit(self):
        self.calls.append(("commit", ()))
        return "applied"

    async def rollback(self):
        self.calls.append(("rollback", ()))
        return "no_op"

    def __getattr__(self, name):
        async def coro(*args):
            self.calls.append((name, args))
            return ("R", name, args)

        return coro

async def test_value_delegation():
    n = _StubNative()
    v = ValueState(n)
    await v.get()
    await v.set(1)
    await v.clear()
    await v.commit()
    await v.rollback()
    assert [c[0] for c in n.calls] == ["get", "set", "clear", "commit", "rollback"]

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
    await d.clear()
    assert [c[0] for c in n.calls] == [
        "push_back",
        "push_front",
        "pop_back",
        "pop_front",
        "get",
        "len",
        "is_empty",
        "clear",
    ]
    assert n.calls[0][1] == (1,)  # append forwards item to push_back
    assert n.calls[4][1] == (3,)  # get(index) forwards index

async def test_map_scan_transforms():
    entries = [("a", 1), ("b", 2)]
    m = MapState(_StubNative(entries))
    assert [e async for e in m.items()] == [("a", 1), ("b", 2)]
    assert [k async for k in m.keys()] == ["a", "b"]
    assert [v async for v in m.values()] == [1, 2]
    assert [k async for k in m] == ["a", "b"]  # __aiter__ = keys (dict-like)

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

async def test_deque_peek_mapping():
    n = _StubNative()
    d = DequeState(n)
    await d.peek()
    await d.peekleft()
    assert [c[0] for c in n.calls] == ["peek_back", "peek_front"]

async def test_deque_values_and_aiter():
    n = _StubNative([1, 2, 3])
    assert [x async for x in DequeState(n).values()] == [1, 2, 3]
    assert [x async for x in DequeState(_StubNative([9]))] == [9]  # __aiter__

async def test_aclosing_closes_scan():
    n = _StubNative([1, 2, 3])
    it = DequeState(n).values()
    async with contextlib.aclosing(it):
        pass
    assert n.scans[0].closed is True


async def test_published_scans_reuse_typed_state_scan_adapter():
    class NativeMap:
        async def contains_key(self, key, map_key):
            assert (key, map_key) == ("user-1", "a")
            return True

        def scan(self, key, query):
            assert (key, query) == ("user-1", _KeyQuery())
            return _StubScan([("a", 1), ("b", 2)])

        def keys(self, key, query):
            assert (key, query) == ("user-1", _KeyQuery(backward=True))
            return _StubScan(["b", "a"])

    class NativeDeque:
        async def len(self, key):
            assert key == "user-1"
            return 2

        async def is_empty(self, key):
            assert key == "user-1"
            return False

        async def peek_front(self, key):
            assert key == "user-1"
            return 1

        async def peek_back(self, key):
            assert key == "user-1"
            return 2

        async def get(self, key, index):
            assert key == "user-1"
            return [1, 2][index] if index < 2 else None

        def scan(self, key, query):
            assert (key, query) == ("user-1", _PositionQuery(backward=True))
            return _StubScan([2, 1])

    native_map = NativeMap()
    published_map = PublishedMap(native_map)
    item_scan = published_map.items("user-1")
    items = [
        item
        async for item in item_scan
    ]
    keys = [
        key
        async for key in published_map.keys(
            "user-1", Direction.BACKWARD
        )
    ]
    map_values = [
        value async for value in published_map.values("user-1")
    ]
    native_deque = NativeDeque()
    published_deque = PublishedDeque(native_deque)
    deque_scan = published_deque.values("user-1", Direction.BACKWARD)
    values = [
        item
        async for item in deque_scan
    ]
    assert items == [("a", 1), ("b", 2)]
    assert keys == ["b", "a"]
    assert map_values == [1, 2]
    assert await published_map.contains("user-1", "a")
    assert await published_deque.size("user-1") == 2
    assert not await published_deque.is_empty("user-1")
    assert await published_deque.peekleft("user-1") == 1
    assert await published_deque.peek("user-1") == 2
    assert await published_deque.get("user-1", -1) == 2
    assert await published_deque.get("user-1", -3) is None
    assert values == [2, 1]
