"""Error categories that keyed-state operations raise at the Python boundary."""

import asyncio

from prosody import value, PermanentStateError, TransientStateError
from prosody.query import _KeyQuery
import pytest
import tsasync

from support import STATE_DEFS, nonce, _wait, StateHandler


async def test_json_collection_rejects_delivered_message(state_client):
    client, topic, _ = state_client

    async def cb(ctx, msg, results):
        cart = ctx.state(STATE_DEFS["cart"])
        try:
            await _wait(cart.set(msg))
            await results.send({"threw": False})
        except Exception as error:
            await results.send(
                {
                    "threw": True,
                    "transient": isinstance(error, TransientStateError),
                    "message": str(error),
                }
            )

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, nonce(), {"go": True}))
    obs = await _wait(handler.results.receive())

    assert obs["threw"] is True
    assert obs["transient"] is True
    assert "cannot be stored in a JSON collection" in obs["message"]

async def test_unregistered_name_is_permanent(state_client):
    client, topic, _ = state_client

    async def cb(ctx, msg, results):
        try:
            ctx.state(value("never-registered-" + nonce()))
            await results.send({"threw": False, "permanent": False})
        except Exception as e:
            await results.send(
                {"threw": True, "permanent": isinstance(e, PermanentStateError)}
            )

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, nonce(), {"go": True}))
    obs = await _wait(handler.results.receive())

    assert obs["threw"] is True
    assert obs["permanent"] is True

async def test_bad_direction_token_is_transient(state_client):
    client, topic, _ = state_client

    async def cb(ctx, msg, results):
        m = ctx.state(STATE_DEFS["totals"])
        try:
            # The typed API only passes Direction.value tokens, so drive the
            # native handle directly to reach parse_direction's guard.
            m._native.scan(_KeyQuery("sideways"))
            await results.send({"threw": False})
        except Exception as e:
            await results.send(
                {
                    "threw": True,
                    "transient": isinstance(e, TransientStateError),
                    "msg": str(e),
                }
            )

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, nonce(), {"go": True}))
    obs = await _wait(handler.results.receive())

    assert obs["threw"] is True
    assert obs["transient"] is True
    assert "forward" in obs["msg"] and "backward" in obs["msg"]

async def test_malformed_definition_at_vend_is_transient(state_client):
    client, topic, _ = state_client

    class BadDef:
        def to_config(self):
            return {"name": "x", "kind": "bogus", "payload": "json"}

    async def cb(ctx, msg, results):
        try:
            ctx.state(BadDef())
            await results.send({"threw": False})
        except Exception as e:
            await results.send(
                {
                    "threw": True,
                    "transient": isinstance(e, TransientStateError),
                    "permanent": isinstance(e, PermanentStateError),
                }
            )

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, nonce(), {"go": True}))
    obs = await _wait(handler.results.receive())

    assert obs["threw"] is True
    assert obs["transient"] is True
    assert obs["permanent"] is False

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

    async def cb(ctx, msg, results):
        d = ctx.state(STATE_DEFS["backlog"])
        await _wait(d.append("x"))
        # Fractional indices remain invalid. Negative indices follow Python's
        # sequence convention.
        frac = None
        try:
            await _wait(d.get(1.5))
        except Exception as e:
            frac = type(e).__name__
        neg = await _wait(d.get(-1))
        ok = await _wait(d.get(0))
        # An index past the u32 range is past the end, like any other.
        far = await _wait(d.get(2**32))
        await results.send({"frac": frac, "neg": neg, "ok": ok, "far": far})

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, nonce(), {"go": True}))
    obs = await _wait(handler.results.receive())

    assert obs["frac"] == "TypeError"
    assert obs["neg"] == "x"
    assert obs["ok"] == "x"
    assert obs["far"] is None

# No test here registers one name with two kinds across two runs. Core retries
# the identity check inside the partition and does not fail the vend, so a
# handler has nothing to observe. Do not add a Python-side guard for it: core
# owns and tests that rule.


async def test_null_write_surfaces_core_permanent_error(state_client):
    """Core rejects a JSON null write. The client maps it to PermanentStateError."""
    client, topic, _ = state_client

    async def cb(ctx, msg, results):
        writes = {
            "value": lambda: ctx.state(STATE_DEFS["cart"]).set(None),
            "map": lambda: ctx.state(STATE_DEFS["totals"]).set("k", None),
            "deque": lambda: ctx.state(STATE_DEFS["backlog"]).append(None),
        }
        outcome = {}
        for kind, write in writes.items():
            try:
                await _wait(write())
                outcome[kind] = "accepted"
            except PermanentStateError:
                outcome[kind] = "permanent"
            except Exception as error:  # pragma: no cover - reported, not raised
                outcome[kind] = f"{type(error).__name__}: {error}"
        await results.send(outcome)

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, nonce(), {"go": True}))
    obs = await _wait(handler.results.receive())

    assert obs == {"value": "permanent", "map": "permanent", "deque": "permanent"}

@pytest.mark.parametrize("bad", [object(), lambda: 1], ids=["object", "lambda"])
async def test_unrepresentable_write_rejects_transient(state_client, bad):
    client, topic, _ = state_client
    v = nonce()

    async def cb(ctx, msg, results):
        c = ctx.state(STATE_DEFS["cart"])
        try:
            await _wait(c.set({"v": v}))
            await _wait(c.commit())
            try:
                await _wait(c.set(bad))
                outcome = {"threw": False}
            except Exception as e:
                outcome = {
                    "threw": True,
                    "transient": isinstance(e, TransientStateError),
                    "permanent": isinstance(e, PermanentStateError),
                }
            await results.send({"outcome": outcome, "after": (await _wait(c.get()))["v"]})
        except Exception as e:  # pragma: no cover
            await results.send({"error": str(e)})

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, nonce(), {"go": True}))
    obs = await _wait(handler.results.receive())

    assert obs.get("error") is None
    assert obs["outcome"]["threw"] is True
    assert obs["outcome"]["transient"] is True
    assert obs["outcome"]["permanent"] is False
    assert obs["after"] == v  # the rejected write left the committed value intact
