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


def test_value_to_config():
    assert value("cart").to_config() == {
        "name": "cart",
        "kind": "value",
        "payload": "json",
        "ttl_seconds": None,
        "read_uncommitted": None,
        "published": None,
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

def test_publication_and_read_cache_share_the_descriptor():
    definition = value("cart", published=True, read_cache=timedelta(seconds=2))
    assert definition.to_config()["published"] is True
    assert definition.read_cache == timedelta(seconds=2)

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
