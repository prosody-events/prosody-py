"""Keyed-state configuration validation at the Python and Prosody boundaries.

Python rejects values that cannot be represented by Prosody's Rust types.
Prosody validates collection names, identities, and semantic limits after the
definitions are mapped. Mock clients exercise both boundaries without external
infrastructure.
"""

from datetime import timedelta

import pytest

from prosody import (
    ProsodyClient,
    deque,
    map,
    set as set_definition,
    value,
)
from support import STATE_COLLECTIONS


BASE = dict(
    bootstrap_servers="localhost:9092",
    source_system="cfg",
    group_id="g",
    subscribed_topics="t",
    mock=True,
)

NOT_WHOLE = (2.5, 5.0, float("nan"), float("inf"))

SIZE_FIELDS = ("state_owned_cache_size", "state_memtable_size", "state_read_cache_size")

PAYLOAD_MISMATCHES = [
    ("value", "bogus", "expected"),
    ("set", "presence", "expected"),
    ("set", "json", "a set collection takes no payload"),
    ("set", "message", "a set collection takes no payload"),
    ("value", None, "missing"),
    ("map", None, "missing"),
]


class RawDef:
    """A minimal definition whose ``to_config()`` feeds an arbitrary dict straight
    to the Rust guard, bypassing the typed helpers' coercions."""

    def __init__(self, cfg):
        self._cfg = cfg

    def to_config(self):
        return self._cfg


def raw(name="v", kind="value", payload="json", **options):
    return RawDef({"name": name, "kind": kind, "payload": payload, **options})


def collections(*definitions):
    return {"state_collections": list(definitions)}


def make_client(**overrides):
    return ProsodyClient.create(**BASE, **overrides)


REJECTIONS = [
    *(({field: size}, field) for field in SIZE_FIELDS for size in ("-1 MiB", "nonsense")),
    (collections(value("v", ttl=-1)), "ttl_seconds: must be a whole number"),
    *(
        (collections(raw(ttl_seconds=ttl)), "ttl_seconds: must be a whole number")
        for ttl in NOT_WHOLE
    ),
    *(
        (
            collections(map("m", keyset_limit=limit)),
            "keyset_limit: must be a non-negative whole number",
        )
        for limit in (*NOT_WHOLE, -1)
    ),
    (collections(raw(keyset_limit=5)), "keyset_limit: only valid for map and set"),
    *(
        (collections(deque("d", capacity=size)), "capacity: must be a positive whole number")
        for size in (*NOT_WHOLE, 0, -1)
    ),
    (collections(raw(capacity=5)), "capacity: only valid for deque"),
    (collections(raw(kind="bogus")), "kind: expected"),
    *(
        (collections(raw(kind=kind, payload=payload)), f"payload: {error}")
        for kind, payload, error in PAYLOAD_MISMATCHES
    ),
]


@pytest.mark.parametrize(("options", "match"), REJECTIONS)
async def test_invalid_option_is_rejected(options, match):
    with pytest.raises(ValueError, match=match):
        await make_client(**options)


@pytest.mark.parametrize(
    ("kind", "field"), [("value", "ttl_seconds"), ("map", "keyset_limit"), ("deque", "capacity")]
)
async def test_non_numeric_whole_number_raises_type_error(kind, field):
    with pytest.raises(TypeError, match=f"{field}: must be"):
        await make_client(**collections(raw(kind=kind, **{field: "5"})))


@pytest.mark.parametrize("read_cache", [True, -1, "soon"])
async def test_invalid_read_cache_is_rejected(read_cache, client_factory):
    with pytest.raises(ValueError, match="state_read_cache"):
        await make_client(state_read_cache=read_cache)
    client = await client_factory(**BASE)
    with pytest.raises(ValueError, match="read_cache"):
        await client.state("owner", value("v", read_cache=read_cache))


@pytest.mark.parametrize(
    "definitions",
    [
        STATE_COLLECTIONS,
        # 0 disables ordered-scan tracking and is a valid whole number.
        [map("m", keyset_limit=0), set_definition("s", keyset_limit=64)],
        [deque("d", capacity=100)],
        [value("v", ttl=timedelta(days=30))],
    ],
)
async def test_valid_collections_are_accepted(definitions, client_factory):
    await client_factory(**BASE, state_collections=definitions)
