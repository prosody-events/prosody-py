"""Commit, rollback, and the end of a handler attempt for keyed state."""

from prosody import StateError, TransientStateError
import pytest

from support import STATE_DEFS, nonce, _wait, StateHandler, observe


async def test_commit_floor_survives_failed_attempt(state_client):
    client, topic, _ = state_client
    v = nonce()
    state = {"attempt": 0}

    async def cb(ctx, msg, results):
        state["attempt"] += 1
        c = ctx.state(STATE_DEFS["cart"])
        if state["attempt"] == 1:
            await _wait(c.set({"v": v}))
            await _wait(c.commit())
            raise TransientStateError("fail after commit")
        await results.send({"attempt": state["attempt"], "got": await _wait(c.get())})

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, nonce(), {"go": True}))
    obs = await _wait(handler.results.receive())

    assert obs["attempt"] == 2  # the transient raise drove a redelivery
    assert obs["got"] == {"v": v}  # the committed floor survived the failed attempt

async def test_rollback_discards_uncommitted(state_client):
    client, topic, _ = state_client
    a = nonce()
    b = nonce()

    async def cb(ctx, msg):
        c = ctx.state(STATE_DEFS["cart"])
        await _wait(c.set({"v": a}))
        await _wait(c.commit())
        await _wait(c.set({"v": b}))
        before = await _wait(c.get())
        await _wait(c.rollback())
        after = await _wait(c.get())
        return {"before": before, "after": after}

    obs = await observe(client, topic, cb)

    assert obs["before"] == {"v": b}  # uncommitted overwrite visible before rollback
    assert obs["after"] == {"v": a}  # rollback reverts to the committed floor

async def test_map_commit_floor_survives_rollback(state_client):
    client, topic, _ = state_client

    async def cb(ctx, msg):
        m = ctx.state(STATE_DEFS["totals"])
        await _wait(m.set("kept", 1))
        await _wait(m.commit())
        await _wait(m.set("kept", 2))
        await _wait(m.set("dropped", 9))
        before = {"kept": await _wait(m.get("kept")), "dropped": await _wait(m.get("dropped"))}
        await _wait(m.rollback())
        after = {"kept": await _wait(m.get("kept")), "dropped": await _wait(m.get("dropped"))}
        return {"before": before, "after": after}

    obs = await observe(client, topic, cb)

    assert obs["before"] == {"kept": 2, "dropped": 9}
    assert obs["after"] == {"kept": 1, "dropped": None}

async def test_leaked_handle_after_failed_attempt_rejects_transient(state_client):
    client, topic, _ = state_client
    state = {"attempt": 0, "leaked": None}

    async def cb(ctx, msg, results):
        state["attempt"] += 1
        c = ctx.state(STATE_DEFS["cart"])
        if state["attempt"] == 1:
            state["leaked"] = c
            await _wait(c.set({"v": nonce()}))
            raise TransientStateError("fail attempt 1")
        try:
            leaked = {"status": "resolved", "value": await _wait(state["leaked"].get())}
        except TransientStateError:
            leaked = {"status": "rejected", "transient": True}
        except Exception as e:  # pragma: no cover
            leaked = {"status": "rejected", "transient": False, "type": type(e).__name__}
        await results.send({"leaked": leaked, "fresh": await _wait(c.get())})

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, nonce(), {"go": True}))
    obs = await _wait(handler.results.receive())

    assert obs["leaked"]["status"] == "rejected"
    assert obs["leaked"]["transient"] is True
    # the failed attempt's uncommitted write is invisible -> no store effect.
    assert obs["fresh"] is None

async def test_leaked_context_cannot_bind_after_failed_attempt(state_client):
    client, topic, _ = state_client
    state = {"attempt": 0, "ctx": None}

    async def cb(ctx, msg, results):
        state["attempt"] += 1
        if state["attempt"] == 1:
            state["ctx"] = ctx
            raise TransientStateError("fail attempt 1")
        try:
            m = state["ctx"].state(STATE_DEFS["totals"])
            result = {"status": "resolved", "value": await _wait(m.get("x"))}
        except StateError as e:
            result = {
                "status": "rejected",
                "state_error": True,
                "transient": isinstance(e, TransientStateError),
            }
        await results.send(result)

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, nonce(), {"go": True}))
    obs = await _wait(handler.results.receive())

    assert obs["status"] == "rejected"
    assert obs["state_error"] is True
    assert obs["transient"] is True

async def test_leaked_handle_after_successful_handler_rejects(state_client):
    client, topic, _ = state_client
    k = nonce()
    state = {"leaked": None}

    async def cb(ctx, msg, results):
        try:
            if msg.payload["step"] == 1:
                state["leaked"] = ctx.state(STATE_DEFS["cart"])
                await _wait(state["leaked"].set({"v": nonce()}))
                await results.send({"ev": "captured"})
                return
            await results.send({"ev": "sentinel-started"})
        except Exception as e:  # pragma: no cover
            await results.send({"ev": "error", "error": str(e)})

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    # Two same-key sends: per-key serialization guarantees event 1 fully tore
    # down before the sentinel, so calling the leaked handle now is post-handler.
    await _wait(client.send(topic, k, {"step": 1}))
    o1 = await _wait(handler.results.receive())
    assert o1["ev"] == "captured"
    await _wait(client.send(topic, k, {"step": 2}))
    o2 = await _wait(handler.results.receive())
    assert o2["ev"] == "sentinel-started"

    with pytest.raises(TransientStateError):
        await _wait(state["leaked"].get())
