"""Pure-Python tests for the set definition, store outcomes, and demand values.

Recording stubs stand in for the native handles.
"""

import dataclasses

import pytest

from prosody import (
    Demand,
    DemandKind,
    DequeState,
    MapState,
    SetState,
    StoreOutcome,
    ValueState,
    set as set_definition,
)

from support import NativeRecorder


def test_set_definition_to_config():
    assert set_definition(
        "tags", ttl=60, read_uncommitted=True, published=True, keyset_limit=64
    ).to_config() == {
        "name": "tags",
        "kind": "set",
        "payload": None,
        "ttl_seconds": 60,
        "read_uncommitted": True,
        "published": True,
        "keyset_limit": 64,
        "capacity": None,
    }
    assert set_definition("tags", read_cache=False).read_cache is False


@pytest.mark.parametrize("handle", [ValueState, MapState, SetState, DequeState])
@pytest.mark.parametrize(
    ("token", "outcome"),
    [("applied", StoreOutcome.APPLIED), ("no_op", StoreOutcome.NO_OP)],
)
async def test_commit_and_rollback_return_the_store_outcome(handle, token, outcome):
    state = handle(NativeRecorder({"commit": token, "rollback": token}))
    assert await state.commit() is outcome
    assert await state.rollback() is outcome


def test_demand_is_a_frozen_value():
    demand = Demand(DemandKind.FAILURE, 1)
    assert demand == Demand(DemandKind.FAILURE, 1)
    assert (demand.kind, demand.retry) == (DemandKind.FAILURE, 1)
    assert DemandKind.NORMAL.value == "normal"
    with pytest.raises(dataclasses.FrozenInstanceError):
        demand.retry = 2
