"""Infra-free keyed-state configuration-validation tests (Appendix 1 item 11).

The state-collection validation table (Appendix 2) is enforced synchronously in
Rust at ``ProsodyClient(...)`` construction (``build_keyed_state_config`` ->
``register_state_collection`` -> ``whole_number_field``). ``mock=True`` needs the
built extension but no Kafka/Cassandra, so every rule below is exercised at
construction and the offending field is named in the ``ValueError``.

Divergences from the JS reference recorded here (live prosody-py behaviour):

* The TTL ceiling is Cassandra's ``630_720_000`` seconds (20y), not ``u32::MAX``.
* ``ttl <= state_recovery_delay`` is DELEGATED to core and is NOT validated under
  ``mock=True`` (core defers it to the real consumer build). There is therefore
  no Python-side guard to assert here — inventing one would violate the north
  star. The rule is covered by core's own tests.
* The ``value()``/``map()``/``deque()`` helpers coerce a ``ttl`` through
  ``int(...)`` before Rust sees it, so a fractional/NaN/inf ``ttl_seconds`` can
  only reach the Rust whole-number guard through a raw definition stub (mirroring
  the JS raw ``{name, kind, payload, ...}`` objects). ``keyset_limit`` is passed
  through unchanged, so the helper suffices for it.
"""

import pytest

from prosody import ProsodyClient, value, map, deque, message_value, message_map, message_deque


BASE = dict(
    bootstrap_servers="localhost:9092",
    source_system="cfg",
    group_id="g",
    subscribed_topics="t",
    mock=True,
)


def make_client(**overrides):
    return ProsodyClient(**BASE, **overrides)


class RawDef:
    """A minimal definition whose ``to_config()`` feeds an arbitrary dict straight
    to the Rust guard, bypassing the typed helpers' coercions."""

    def __init__(self, cfg):
        self._cfg = cfg

    def to_config(self):
        return self._cfg


def raw(
    name="v",
    kind="value",
    payload="json",
    ttl_seconds=None,
    read_uncommitted=None,
    keyset_limit=None,
):
    return RawDef(
        {
            "name": name,
            "kind": kind,
            "payload": payload,
            "ttl_seconds": ttl_seconds,
            "read_uncommitted": read_uncommitted,
            "keyset_limit": keyset_limit,
        }
    )


STATE_COLLECTIONS = [
    value("cart"),
    map("totals", keyset_limit=256),
    deque("backlog"),
    message_value("last-msg"),
    message_map("msg-index"),
    message_deque("msg-log"),
]


# --- name rules -----------------------------------------------------------


def test_rejects_empty_name():
    with pytest.raises(ValueError, match="name: must not be empty"):
        make_client(state_collections=[value("")])


def test_rejects_duplicate_name():
    with pytest.raises(ValueError, match="duplicate collection name"):
        make_client(state_collections=[value("dup"), map("dup")])


# --- ttl rules ------------------------------------------------------------


def test_rejects_ttl_zero():
    with pytest.raises(
        ValueError, match=r"ttl_seconds: must be a whole number in 1..=630720000"
    ):
        make_client(state_collections=[value("v", ttl=0)])


def test_rejects_ttl_negative():
    with pytest.raises(ValueError, match=r"ttl_seconds: must be a whole number"):
        make_client(state_collections=[value("v", ttl=-1)])


def test_rejects_ttl_over_ceiling():
    with pytest.raises(ValueError, match=r"ttl_seconds: must be a whole number"):
        make_client(state_collections=[value("v", ttl=630720001)])


def test_accepts_ttl_at_ceiling():
    # 630_720_000 (twenty years) is the inclusive Cassandra ceiling.
    make_client(state_collections=[value("v", ttl=630720000)])


@pytest.mark.parametrize("ttl_seconds", [2.5, float("nan"), float("inf")])
def test_rejects_ttl_fractional_or_nonfinite(ttl_seconds):
    # A fractional/NaN/inf ttl only reaches the Rust guard through a raw stub;
    # the typed helpers would truncate/raise on int() first.
    with pytest.raises(ValueError, match=r"ttl_seconds: must be a whole number"):
        make_client(state_collections=[raw(ttl_seconds=ttl_seconds)])


# --- keyset_limit rules ---------------------------------------------------


def test_rejects_keyset_over_ceiling():
    with pytest.raises(
        ValueError, match=r"keyset_limit: must be a whole number in 0..=4096"
    ):
        make_client(state_collections=[map("m", keyset_limit=5000)])


@pytest.mark.parametrize("keyset_limit", [2.5, -1, float("nan"), float("inf")])
def test_rejects_keyset_non_whole(keyset_limit):
    with pytest.raises(ValueError, match=r"keyset_limit: must be a whole number"):
        make_client(state_collections=[map("m", keyset_limit=keyset_limit)])


def test_accepts_keyset_zero():
    # 0 disables ordered-scan tracking and is a valid whole number.
    make_client(state_collections=[map("m", keyset_limit=0)])


def test_rejects_keyset_on_non_map():
    # The value() helper has no keyset param, so a raw stub carries it onto a
    # value collection to reach the map-only guard.
    with pytest.raises(ValueError, match=r"keyset_limit: only valid for map"):
        make_client(state_collections=[raw(kind="value", keyset_limit=5)])


# --- kind / payload tokens ------------------------------------------------


def test_rejects_unknown_kind():
    with pytest.raises(ValueError, match=r"kind: expected"):
        make_client(state_collections=[raw(kind="bogus")])


def test_rejects_unknown_payload():
    with pytest.raises(ValueError, match=r"payload: expected"):
        make_client(state_collections=[raw(payload="bogus")])


# --- recovery_delay rules -------------------------------------------------


def test_rejects_recovery_delay_zero():
    with pytest.raises(ValueError, match="state_recovery_delay: must be >= 1 second"):
        make_client(state_recovery_delay=0, state_collections=[value("v")])


def test_rejects_recovery_delay_fractional():
    with pytest.raises(
        ValueError, match="state_recovery_delay: must be a whole number of seconds"
    ):
        make_client(state_recovery_delay=2.5, state_collections=[value("v")])


# --- happy path -----------------------------------------------------------


def test_accepts_canonical_collection_set():
    make_client(state_collections=STATE_COLLECTIONS)
