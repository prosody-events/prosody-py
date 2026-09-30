"""Pure-Python unit tests for the typed keyed-state surface.

No Kafka: recording stubs stand in for the native handles, so these tests
check delegation and transforms without a live consumer. The
``test_keyed_state*.py`` files check the native layer against a live consumer.
"""

import contextlib
from datetime import datetime, timezone
import importlib

from prosody import (
    Message,
    ValueState,
    MapState,
    SetState,
    DequeState,
    StateError,
    PermanentStateError,
    TransientStateError,
    PermanentError,
    TransientError,
    PublishedDeque,
    PublishedMap,
    PublishedSet,
    PublishedValue,
)
import pytest

from support import NativeRecorder


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

WRITES = {"set", "clear", "remove", "insert", "push_back", "push_front"}

# (wrapper, call, native method, native arguments) for every read or write
# whose Python name or arguments differ from, or match, the native call.
MAPPINGS = [
    (ValueState, lambda s: s.get(), "get", ()),
    (ValueState, lambda s: s.set(1), "set", (1,)),
    (ValueState, lambda s: s.clear(), "clear", ()),
    (MapState, lambda s: s.get("k"), "get", ("k",)),
    (MapState, lambda s: s.get_many(["a", "b"]), "get_many", (["a", "b"],)),
    (MapState, lambda s: s.contains("k"), "contains_key", ("k",)),
    (MapState, lambda s: s.contains_many(["a"]), "contains_many", (["a"],)),
    (MapState, lambda s: s.is_empty(), "is_empty", ()),
    (MapState, lambda s: s.set("k", 1), "set", ("k", 1)),
    (MapState, lambda s: s.remove("k"), "remove", ("k",)),
    (MapState, lambda s: s.clear(), "clear", ()),
    (SetState, lambda s: s.add("a"), "insert", ("a",)),
    (SetState, lambda s: s.discard("b"), "remove", ("b",)),
    (SetState, lambda s: s.contains("a"), "contains", ("a",)),
    (SetState, lambda s: s.contains_many(["a"]), "contains_many", (["a"],)),
    (SetState, lambda s: s.is_empty(), "is_empty", ()),
    (SetState, lambda s: s.clear(), "clear", ()),
    (DequeState, lambda s: s.append(1), "push_back", (1,)),
    (DequeState, lambda s: s.appendleft(2), "push_front", (2,)),
    (DequeState, lambda s: s.pop(), "pop_back", ()),
    (DequeState, lambda s: s.popleft(), "pop_front", ()),
    (DequeState, lambda s: s.peek(), "peek_back", ()),
    (DequeState, lambda s: s.peekleft(), "peek_front", ()),
    (DequeState, lambda s: s.get(3), "get", (3,)),
    (DequeState, lambda s: s.size(), "len", ()),
    (DequeState, lambda s: s.is_empty(), "is_empty", ()),
    (DequeState, lambda s: s.clear(), "clear", ()),
    (PublishedValue, lambda s: s.get("u"), "get", ("u",)),
    (PublishedMap, lambda s: s.get("u", "a"), "get", ("u", "a")),
    (PublishedMap, lambda s: s.get_many("u", ["a"]), "get_many", ("u", ["a"])),
    (PublishedMap, lambda s: s.contains("u", "a"), "contains_key", ("u", "a")),
    (PublishedMap, lambda s: s.contains_many("u", ["a"]), "contains_many", ("u", ["a"])),
    (PublishedMap, lambda s: s.is_empty("u"), "is_empty", ("u",)),
    (PublishedSet, lambda s: s.contains("u", "a"), "contains", ("u", "a")),
    (PublishedSet, lambda s: s.contains_many("u", ["a"]), "contains_many", ("u", ["a"])),
    (PublishedSet, lambda s: s.is_empty("u"), "is_empty", ("u",)),
    (PublishedDeque, lambda s: s.get("u", 1), "get", ("u", 1)),
    (PublishedDeque, lambda s: s.size("u"), "len", ("u",)),
    (PublishedDeque, lambda s: s.is_empty("u"), "is_empty", ("u",)),
    (PublishedDeque, lambda s: s.peek("u"), "peek_back", ("u",)),
    (PublishedDeque, lambda s: s.peekleft("u"), "peek_front", ("u",)),
]


@pytest.mark.parametrize(("wrapper", "call", "name", "args"), MAPPINGS)
async def test_each_method_calls_its_native_operation(wrapper, call, name, args):
    native = NativeRecorder({name: "result"})
    result = await call(wrapper(native))
    assert native.calls == [(name, args)]
    assert result == (None if name in WRITES else "result")


async def test_deque_get_resolves_a_negative_index():
    native = NativeRecorder({"len": 2, "get": "last"})
    for get in (DequeState(native).get, lambda i: PublishedDeque(native).get("u", i)):
        native.calls.clear()
        assert await get(-1) == "last"
        assert native.calls[-1][0] == "get"
        assert native.calls[-1][1][-1] == 1
        native.calls.clear()
        assert await get(-3) is None
        assert [name for name, _ in native.calls] == ["len"]


async def test_scan_transforms():
    entries = [("a", 1), ("b", 2)]
    handle = MapState(NativeRecorder(items=entries))
    published = PublishedMap(NativeRecorder(items=entries))
    assert [e async for e in handle.items()] == entries
    assert [e async for e in published.items("u")] == entries
    assert [k async for k in handle.keys()] == ["a", "b"]
    assert [k async for k in published.keys("u")] == ["a", "b"]
    assert [v async for v in handle.values()] == [1, 2]
    assert [v async for v in published.values("u")] == [1, 2]
    assert [k async for k in handle] == ["a", "b"]  # __aiter__ = keys (dict-like)
    members = [("a",), ("b",)]
    assert [m async for m in SetState(NativeRecorder(items=members))] == ["a", "b"]
    deque = DequeState(NativeRecorder(items=[1, 2, 3]))
    assert [x async for x in deque] == [1, 2, 3]


async def test_map_get_default():
    def handle(value):
        return MapState(NativeRecorder({"get": value}))

    # Absent (native None) returns the default...
    assert await handle(None).get("k", "fallback") == "fallback"
    # ...and None when no default is given.
    assert await handle(None).get("k") is None
    # A present-but-falsy value returns as-is, NEVER the default (this is the
    # exact bug a `value or default` implementation would introduce).
    for falsy in (0, False, "", []):
        assert await handle(falsy).get("k", "fallback") == falsy
    # A present truthy value returns as-is.
    assert await handle(7).get("k", "fallback") == 7


async def test_aclosing_closes_scan():
    native = NativeRecorder(items=[1, 2, 3])
    async with contextlib.aclosing(DequeState(native).values()):
        pass
    assert native.scans[0].closed is True
