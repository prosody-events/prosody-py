"""Pure-Python tests for store outcomes and demand values.

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
)

from support import NativeRecorder


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
