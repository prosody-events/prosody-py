"""Keyed-state configuration validation at the Python and Prosody boundaries.

Python rejects values that cannot be represented by Prosody's Rust types.
Prosody validates collection names, identities, and semantic limits after the
definitions are mapped. Mock clients exercise both boundaries without external
infrastructure.
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


@pytest.mark.parametrize("state_owned_cache_size", ["0", "-1 MiB", "nonsense"])
def test_invalid_state_owned_cache_size_is_rejected(state_owned_cache_size):
    with pytest.raises(ValueError, match="state_owned_cache_size"):
        make_client(state_owned_cache_size=state_owned_cache_size)


@pytest.mark.parametrize("state_read_cache_size", ["0", "-1 MiB", "nonsense"])
def test_invalid_state_read_cache_size_is_rejected(state_read_cache_size):
    with pytest.raises(ValueError, match="state_read_cache_size"):
        make_client(state_read_cache_size=state_read_cache_size)


@pytest.mark.parametrize("state_read_cache", [True, -1])
def test_invalid_state_read_cache_is_rejected(state_read_cache):
    with pytest.raises(ValueError, match="state_read_cache"):
        make_client(state_read_cache=state_read_cache)


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
    capacity=None,
):
    return RawDef(
        {
            "name": name,
            "kind": kind,
            "payload": payload,
            "ttl_seconds": ttl_seconds,
            "read_uncommitted": read_uncommitted,
            "keyset_limit": keyset_limit,
            "capacity": capacity,
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


# --- ttl rules ------------------------------------------------------------


def test_rejects_ttl_negative():
    with pytest.raises(ValueError, match=r"ttl_seconds: must be a whole number"):
        make_client(state_collections=[value("v", ttl=-1)])


@pytest.mark.parametrize("ttl_seconds", [2.5, float("nan"), float("inf")])
def test_rejects_ttl_fractional_or_nonfinite(ttl_seconds):
    with pytest.raises(ValueError, match=r"ttl_seconds: must be a whole number"):
        make_client(state_collections=[raw(ttl_seconds=ttl_seconds)])


# --- keyset_limit rules ---------------------------------------------------


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


# --- capacity rules (deque-only) ------------------------------------------


def test_accepts_deque_capacity():
    make_client(state_collections=[deque("d", capacity=100)])


def test_rejects_capacity_zero():
    with pytest.raises(
        ValueError, match=r"capacity: must be a whole number in 1..=4294967295"
    ):
        make_client(state_collections=[deque("d", capacity=0)])


@pytest.mark.parametrize("capacity", [2.5, -1, float("nan"), float("inf")])
def test_rejects_capacity_non_whole(capacity):
    # The deque() helper passes capacity through unchanged, so it reaches the
    # Rust whole-number guard directly.
    with pytest.raises(ValueError, match=r"capacity: must be a whole number"):
        make_client(state_collections=[deque("d", capacity=capacity)])


def test_rejects_capacity_on_non_deque():
    with pytest.raises(ValueError, match=r"capacity: only valid for deque"):
        make_client(state_collections=[raw(kind="value", capacity=5)])


# --- kind / payload tokens ------------------------------------------------


def test_rejects_unknown_kind():
    with pytest.raises(ValueError, match=r"kind: expected"):
        make_client(state_collections=[raw(kind="bogus")])


def test_rejects_unknown_payload():
    with pytest.raises(ValueError, match=r"payload: expected"):
        make_client(state_collections=[raw(payload="bogus")])


# --- recovery_delay rules -------------------------------------------------


def test_rejects_recovery_delay_fractional():
    with pytest.raises(
        ValueError, match="state_recovery_delay: must be a whole number of seconds"
    ):
        make_client(state_recovery_delay=2.5, state_collections=[value("v")])


def test_rejects_recovery_delay_negative():
    with pytest.raises(ValueError, match=r"state_recovery_delay"):
        make_client(state_recovery_delay=-5, state_collections=[value("v")])


@pytest.mark.parametrize("delay", [float("nan"), float("inf")])
def test_rejects_recovery_delay_nonfinite(delay):
    with pytest.raises(ValueError, match=r"state_recovery_delay"):
        make_client(state_recovery_delay=delay, state_collections=[value("v")])


# --- happy path -----------------------------------------------------------


def test_accepts_canonical_collection_set():
    make_client(state_collections=STATE_COLLECTIONS)
