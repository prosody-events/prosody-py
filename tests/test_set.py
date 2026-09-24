"""Pure-Python tests for the set surface.

Recording stubs stand in for the native handles. ``test_keyed_state.py``
checks the same wiring against a live consumer.
"""

import pytest

from prosody import (
    MapState,
    PublishedSet,
    SetState,
    set as set_definition,
)
from prosody.query import _KeyQuery


class _Native:
    """Records each native call and returns a fixed result per method."""

    def __init__(self, results):
        self.calls = []
        self._results = results

    def __getattr__(self, name):
        async def call(*args):
            self.calls.append((name, args))
            return self._results.get(name)

        return call


def test_set_definition_to_config():
    assert set_definition(
        "tags", ttl=60, read_uncommitted=True, published=True, keyset_limit=64
    ).to_config() == {
        "name": "tags",
        "kind": "set",
        "payload": "presence",
        "ttl_seconds": 60,
        "read_uncommitted": True,
        "published": True,
        "read_cache": None,
        "keyset_limit": 64,
        "capacity": None,
    }
    assert set_definition("tags", read_cache=False).read_cache is False


@pytest.mark.asyncio
async def test_set_methods_map_to_native_set_operations():
    native = _Native({"contains": True, "contains_many": [True, False]})
    tags = SetState(native)

    await tags.add("a")
    await tags.discard("b")
    assert await tags.contains("a") is True
    assert await tags.contains_many(["a", "b"]) == [True, False]
    await tags.is_empty()
    await tags.clear()

    assert native.calls == [
        ("insert", ("a",)),
        ("remove", ("b",)),
        ("contains", ("a",)),
        ("contains_many", (["a", "b"],)),
        ("is_empty", ()),
        ("clear", ()),
    ]


@pytest.mark.asyncio
async def test_set_iteration_opens_a_member_query():
    queries = []

    class Native:
        def keys(self, query):
            queries.append(query)
            return _Scan(["a", "b"])

    tags = SetState(Native())
    assert [member async for member in tags] == ["a", "b"]
    assert [member async for member in tags.members(prefix="a")] == ["a", "b"]
    assert queries == [_KeyQuery("forward"), _KeyQuery("forward", prefix="a")]


@pytest.mark.asyncio
async def test_published_set_reads_map_to_native_reads():
    native = _Native({"contains": True, "contains_many": [False], "is_empty": True})
    tags = PublishedSet(native)

    assert await tags.contains("user", "a") is True
    assert await tags.contains_many("user", ["b"]) == [False]
    assert await tags.is_empty("user") is True
    assert native.calls == [
        ("contains", ("user", "a")),
        ("contains_many", ("user", ["b"])),
        ("is_empty", ("user",)),
    ]


@pytest.mark.asyncio
async def test_map_presence_reads_map_to_native_reads():
    native = _Native({"contains_many": [True, False], "is_empty": False})
    totals = MapState(native)

    assert await totals.contains_many(["a", "b"]) == [True, False]
    assert await totals.is_empty() is False
    assert native.calls == [("contains_many", (["a", "b"],)), ("is_empty", ())]


class _Scan:
    def __init__(self, items):
        self._items = list(items)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._items:
            raise StopAsyncIteration
        return self._items.pop(0)

    async def aclose(self):
        pass
