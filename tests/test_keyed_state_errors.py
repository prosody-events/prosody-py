"""Error categories that keyed-state operations raise at the Python boundary."""

import asyncio

from prosody import value, PermanentStateError, TransientStateError
import pytest
import tsasync

from support import STATE_DEFS, nonce, _wait, StateHandler, observe, outcome


async def test_json_collection_rejects_delivered_message(state_client):
    client, topic, _ = state_client

    async def cb(ctx, msg):
        try:
            await _wait(ctx.state(STATE_DEFS["cart"]).set(msg))
        except TransientStateError as error:
            return {"message": str(error)}
        return {"message": None}

    obs = await observe(client, topic, cb)
    assert "cannot be stored in a JSON collection" in obs["message"]

async def test_unregistered_name_is_permanent(state_client):
    client, topic, _ = state_client

    async def cb(ctx, _msg):
        definition = value("never-registered-" + nonce())
        return {"outcome": await outcome(lambda: ctx.state(definition))}

    assert (await observe(client, topic, cb))["outcome"] == "permanent"

async def test_malformed_definition_at_vend_is_transient(state_client):
    client, topic, _ = state_client

    class BadDef:
        def to_config(self):
            return {"name": "x", "kind": "bogus", "payload": "json"}

    async def cb(ctx, _msg):
        return {"outcome": await outcome(lambda: ctx.state(BadDef()))}

    assert (await observe(client, topic, cb))["outcome"] == "transient"

async def test_rethrown_permanent_state_error_no_retry(state_client):
    client, topic, _ = state_client
    state = {"count": 0}
    handled = tsasync.Event()

    async def cb(ctx, msg, results):
        state["count"] += 1
        handled.set()
        raise PermanentStateError("permanent state boom")

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, nonce(), {"go": True}))
    await _wait(handled.wait())
    await asyncio.sleep(5)  # a retry would bump the counter
    assert state["count"] == 1

async def test_rethrown_transient_state_error_retries(state_client):
    client, topic, _ = state_client
    state = {"count": 0}
    retried = tsasync.Event()

    async def cb(ctx, msg, results):
        state["count"] += 1
        if state["count"] == 1:
            raise TransientStateError("transient state later")
        retried.set()

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, nonce(), {"go": True}))
    await _wait(retried.wait())
    assert state["count"] == 2  # no state error is ever terminal -> it retried

async def test_deque_index_type_and_negative_index(state_client):
    client, topic, _ = state_client

    async def cb(ctx, _msg):
        d = ctx.state(STATE_DEFS["backlog"])
        await _wait(d.append("x"))
        # Fractional indices remain invalid. Negative indices follow Python's
        # sequence convention. An index past the u32 range is past the end.
        return {
            "frac": await outcome(lambda: d.get(1.5)),
            "neg": await _wait(d.get(-1)),
            "ok": await _wait(d.get(0)),
            "far": await _wait(d.get(2**32)),
        }

    obs = await observe(client, topic, cb)
    assert obs == {"frac": "TypeError", "neg": "x", "ok": "x", "far": None}

# No test here registers one name with two kinds across two runs. Core retries
# the identity check inside the partition and does not fail the vend, so a
# handler has nothing to observe. Do not add a Python-side guard for it: core
# owns and tests that rule.


async def test_null_write_surfaces_core_permanent_error(state_client):
    """Core rejects a JSON null write. The client maps it to PermanentStateError."""
    client, topic, _ = state_client

    async def cb(ctx, _msg):
        writes = {
            "value": lambda: ctx.state(STATE_DEFS["cart"]).set(None),
            "map": lambda: ctx.state(STATE_DEFS["totals"]).set("k", None),
            "deque": lambda: ctx.state(STATE_DEFS["backlog"]).append(None),
        }
        return {kind: await outcome(write) for kind, write in writes.items()}

    obs = await observe(client, topic, cb)
    assert obs == {"value": "permanent", "map": "permanent", "deque": "permanent"}

@pytest.mark.parametrize("bad", [object(), lambda: 1], ids=["object", "lambda"])
async def test_unrepresentable_write_rejects_transient(state_client, bad):
    client, topic, _ = state_client
    v = nonce()

    async def cb(ctx, _msg):
        c = ctx.state(STATE_DEFS["cart"])
        await _wait(c.set({"v": v}))
        await _wait(c.commit())
        rejected = await outcome(lambda: c.set(bad))
        return {"outcome": rejected, "after": (await _wait(c.get()))["v"]}

    obs = await observe(client, topic, cb)
    assert obs["outcome"] == "transient"
    assert obs["after"] == v  # the rejected write left the committed value intact
