"""Keyed-state definitions and the config dicts they build.

No Kafka: these tests check the pure-Python definition layer.
"""

import dataclasses
from datetime import timedelta

from prosody import (
    value,
    map,
    set as set_definition,
    deque,
    message_value,
    message_map,
    message_deque,
    ProsodyClient,
    PublishedDeque,
    PublishedMap,
    PublishedSet,
    PublishedValue,
)
import pytest


def _config(name, kind, payload="json", **options):
    """The expected config dict: every option unset unless named."""
    config = dict.fromkeys(
        ("ttl_seconds", "read_uncommitted", "published", "read_cache", "keyset_limit", "capacity")
    )
    return {"name": name, "kind": kind, "payload": payload, **config, **options}


@pytest.mark.parametrize(
    ("definition", "expected"),
    [
        (value("cart"), _config("cart", "value")),
        (value("c", ttl=timedelta(days=30)), _config("c", "value", ttl_seconds=2592000)),
        (value("c", ttl=60), _config("c", "value", ttl_seconds=60)),
        (value("c", read_uncommitted=True), _config("c", "value", read_uncommitted=True)),
        (value("c", published=True), _config("c", "value", published=True)),
        (map("s"), _config("s", "map")),
        (map("s", keyset_limit=256), _config("s", "map", keyset_limit=256)),
        (
            set_definition(
                "tags", ttl=60, read_uncommitted=True, published=True, keyset_limit=64
            ),
            _config(
                "tags",
                "set",
                None,
                ttl_seconds=60,
                read_uncommitted=True,
                published=True,
                keyset_limit=64,
            ),
        ),
        (deque("d"), _config("d", "deque")),
        (deque("d", capacity=100), _config("d", "deque", capacity=100)),
        (message_value("mv"), _config("mv", "value", "message")),
        (message_map("mm"), _config("mm", "map", "message")),
        (
            message_map("mm", keyset_limit=128),
            _config("mm", "map", "message", keyset_limit=128),
        ),
        (message_deque("md"), _config("md", "deque", "message")),
        (
            message_deque("md", capacity=50),
            _config("md", "deque", "message", capacity=50),
        ),
    ],
)
def test_to_config(definition, expected):
    assert definition.to_config() == expected


@pytest.mark.parametrize("read_cache", [None, False, 2.0, timedelta(seconds=2)])
def test_read_cache_is_in_the_config(read_cache):
    for define in (value, map, set_definition, deque):
        definition = define("c", read_cache=read_cache)
        assert definition.read_cache == read_cache
        assert definition.to_config()["read_cache"] == read_cache


@pytest.mark.parametrize(
    ("definition", "reader"),
    (
        (value("value", read_cache=False), PublishedValue),
        (map("map", read_cache=2.0), PublishedMap),
        (set_definition("set", read_cache=1.0), PublishedSet),
        (deque("deque"), PublishedDeque),
    ),
)
async def test_client_state_dispatches_by_definition_type(definition, reader):
    class StubClient:
        def __getattr__(self, name):
            async def open_state(*args, **kwargs):
                return name, args, kwargs

            return open_state

    result = await ProsodyClient.state(StubClient(), "checkout", definition)
    assert type(result) is reader
    assert result._native == (
        "_published",
        ("checkout", definition.kind, definition.name),
        {"read_cache": definition.read_cache},
    )

def test_definitions_frozen():
    d = value("cart")
    with pytest.raises(dataclasses.FrozenInstanceError):
        d.name = "other"
