"""Pure-Python tests for keyed-state query options.

Each scan method resolves its options into one native query value. These tests
check that translation for every option, on every handle and published reader,
against recording stubs. ``test_keyed_state.py`` checks that the native layer
applies the same values against a live store.
"""

import pytest

from prosody import (
    DequeState,
    Direction,
    MapState,
    PublishedDeque,
    PublishedMap,
    PublishedSet,
    SetState,
)
from prosody.query import _KeyQuery, _PositionQuery


class _Recorder:
    """A native stub that records every query it receives."""

    def __init__(self):
        self.queries = []

    def _record(self, *args):
        self.queries.append(args[-1])
        return _Empty()

    scan = _record
    keys = _record


class _Empty:
    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration

    async def aclose(self):
        pass


def _key_scans(native):
    """Every key-ordered scan method, as a call that takes the options."""
    handle = MapState(native)
    members = SetState(native)
    published = PublishedMap(native)
    published_set = PublishedSet(native)
    return [
        lambda direction, **options: handle.items(direction, **options),
        lambda direction, **options: handle.keys(direction, **options),
        lambda direction, **options: handle.values(direction=direction, **options),
        lambda direction, **options: members.members(direction, **options),
        lambda direction, **options: published.items("user", direction, **options),
        lambda direction, **options: published.keys("user", direction, **options),
        lambda direction, **options: published.values(
            "user", direction=direction, **options
        ),
        lambda direction, **options: published_set.members(
            "user", direction, **options
        ),
    ]


def _position_scans(native):
    """Every deque scan method, as a call that takes the options."""
    handle = DequeState(native)
    published = PublishedDeque(native)
    return [
        lambda direction, **options: handle.values(direction, **options),
        lambda direction, **options: published.values("user", direction, **options),
    ]


KEY_CASES = [
    ({}, _KeyQuery("backward")),
    ({"prefix": "ord-"}, _KeyQuery("backward", prefix="ord-")),
    ({"from_": "b"}, _KeyQuery("backward", start=("b", True))),
    ({"after": "b"}, _KeyQuery("backward", start=("b", False))),
    ({"to": "y"}, _KeyQuery("backward", end=("y", True))),
    ({"before": "y"}, _KeyQuery("backward", end=("y", False))),
    ({"limit": 3}, _KeyQuery("backward", limit=3)),
    (
        {"prefix": "p", "after": "p1", "before": "p9", "limit": 2},
        _KeyQuery("backward", "p", ("p1", False), ("p9", False), 2),
    ),
]


@pytest.mark.parametrize(("options", "expected"), KEY_CASES)
def test_key_options_translate_on_every_scan(options, expected):
    native = _Recorder()
    for scan in _key_scans(native):
        scan(Direction.BACKWARD, **options)
    assert native.queries == [expected] * len(native.queries)
    assert len(native.queries) == 8


@pytest.mark.parametrize(
    ("options", "error"),
    [
        ({"from_": "a", "after": "a"}, ValueError),
        ({"to": "z", "before": "z"}, ValueError),
        ({"limit": 0}, ValueError),
        ({"limit": -1}, ValueError),
        ({"limit": 1.5}, TypeError),
        ({"limit": "2"}, TypeError),
        ({"limit": True}, TypeError),
    ],
)
def test_key_options_reject_values_without_a_native_form(options, error):
    native = _Recorder()
    for scan in _key_scans(native):
        with pytest.raises(error):
            scan(Direction.FORWARD, **options)
    assert native.queries == []


POSITION_CASES = [
    ({}, _PositionQuery("forward")),
    ({"from_": 2}, _PositionQuery("forward", start=(2, True))),
    ({"after": 2}, _PositionQuery("forward", start=(2, False))),
    ({"to": 7}, _PositionQuery("forward", end=(7, True))),
    ({"before": 7}, _PositionQuery("forward", end=(7, False))),
    ({"limit": 4}, _PositionQuery("forward", limit=4)),
    ({"range": range(2, 5)}, _PositionQuery("forward", range=(2, 5))),
    ({"range": range(3, 3)}, _PositionQuery("forward", range=(3, 3))),
    ({"range": slice(2, 5)}, _PositionQuery("forward", range=(2, 5))),
    ({"range": slice(None, 4)}, _PositionQuery("forward", range=(0, 4))),
    ({"range": slice(6, None)}, _PositionQuery("forward", range=(6, None))),
    ({"range": slice(1, 3, 1)}, _PositionQuery("forward", range=(1, 3))),
    (
        {"from_": 1, "before": 9, "range": slice(2, None), "limit": 2},
        _PositionQuery("forward", (1, True), (9, False), (2, None), 2),
    ),
]


@pytest.mark.parametrize(("options", "expected"), POSITION_CASES)
def test_position_options_translate_on_every_scan(options, expected):
    native = _Recorder()
    for scan in _position_scans(native):
        scan(Direction.FORWARD, **options)
    assert native.queries == [expected, expected]


@pytest.mark.parametrize(
    ("options", "error"),
    [
        ({"from_": 1, "after": 1}, ValueError),
        ({"to": 4, "before": 4}, ValueError),
        ({"from_": -1}, ValueError),
        ({"before": -2}, ValueError),
        ({"after": 1.0}, TypeError),
        ({"range": range(-3, 2)}, ValueError),
        ({"range": range(0, 6, 2)}, ValueError),
        ({"range": range(5, 2)}, ValueError),
        ({"range": slice(-3, None)}, ValueError),
        ({"range": slice(None, -1)}, ValueError),
        ({"range": slice(0, 4, 2)}, ValueError),
        ({"range": slice("a", "b")}, TypeError),
        ({"range": [1, 2]}, TypeError),
        ({"limit": 0}, ValueError),
    ],
)
def test_position_options_reject_values_without_a_native_form(options, error):
    native = _Recorder()
    for scan in _position_scans(native):
        with pytest.raises(error):
            scan(Direction.FORWARD, **options)
    assert native.queries == []
