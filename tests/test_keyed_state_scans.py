"""Keyed-state scans: ordering, chunk boundaries, close, and cancellation."""

import asyncio
import contextlib
import gc

from prosody import PermanentStateError, TransientStateError
import pytest

from support import STATE_DEFS, CHUNK_SPAN, nonce, _wait, _collect, StateHandler


async def test_gc_traversal_never_overcounts_the_shared_env(state_client):
    """A handle and its cursors share one reference to each env object.

    ``tp_traverse`` may visit that reference at most once in total, or the
    cyclic GC subtracts more references than exist.
    """
    client, topic, _ = state_client

    async def cb(ctx, msg, results):
        totals = ctx.state(STATE_DEFS["totals"])
        scans = [totals.items(), totals.keys(), totals.values()]
        natives = [totals._native, *(scan._native for scan in scans)]
        visits = sum(
            gc.get_referents(native).count(PermanentStateError) for native in natives
        )
        await results.send({"visits": visits})

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, nonce(), {"go": True}))
    obs = await _wait(handler.results.receive())

    assert obs["visits"] <= 1

async def test_break_without_aclosing_is_harmless(state_client):
    client, topic, _ = state_client

    async def cb(ctx, msg, results):
        m = ctx.state(STATE_DEFS["totals"])
        try:
            for k, v in {"a": 1, "b": 2, "c": 3}.items():
                await _wait(m.set(k, v))

            async def scan_break():
                async for _ in m.items():
                    break

            await _wait(scan_break())  # plain break, no aclosing
            await _wait(m.set("after", 99))
            await results.send({"ok": (await _wait(m.get("after"))) == 99})
        except Exception as e:  # pragma: no cover
            await results.send({"error": str(e)})

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, nonce(), {"go": True}))
    obs = await _wait(handler.results.receive())

    assert obs.get("error") is None
    assert obs["ok"] is True

async def test_aclosing_early_exit_then_followup_succeeds(state_client):
    client, topic, _ = state_client

    async def cb(ctx, msg, results):
        m = ctx.state(STATE_DEFS["totals"])
        try:
            for k, v in {"a": 1, "b": 2, "c": 3}.items():
                await _wait(m.set(k, v))

            async def scan_aclose():
                it = m.items()
                async with contextlib.aclosing(it):
                    async for _ in it:
                        break

            await _wait(scan_aclose())  # deterministic close via aclosing
            await _wait(m.set("after", 7))
            await results.send({"ok": (await _wait(m.get("after"))) == 7})
        except Exception as e:  # pragma: no cover
            await results.send({"error": str(e)})

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, nonce(), {"go": True}))
    obs = await _wait(handler.results.receive())

    assert obs.get("error") is None
    assert obs["ok"] is True

async def test_post_handler_iteration_is_terminated(state_client):
    client, topic, _ = state_client
    k = nonce()
    state = {"scan": None}

    async def cb(ctx, msg, results):
        try:
            if msg.payload["step"] == 1:
                m = ctx.state(STATE_DEFS["totals"])
                await _wait(m.set("a", 1))
                state["scan"] = m.items()  # captured, NOT iterated
                await results.send({"ev": "captured"})
                return
            await results.send({"ev": "sentinel-started"})
        except Exception as e:  # pragma: no cover
            await results.send({"ev": "error", "error": str(e)})

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, k, {"step": 1}))
    assert (await _wait(handler.results.receive()))["ev"] == "captured"
    await _wait(client.send(topic, k, {"step": 2}))
    assert (await _wait(handler.results.receive()))["ev"] == "sentinel-started"

    with pytest.raises(TransientStateError):
        await _wait(state["scan"].__anext__())

async def test_scan_ordered_distinct_across_chunk_boundary(state_client):
    client, topic, _ = state_client

    async def cb(ctx, msg, results):
        m = ctx.state(STATE_DEFS["totals"])
        try:
            for i in range(CHUNK_SPAN):
                await _wait(m.set(f"k{i:03d}", i))
            collected = await _wait(_collect(m.items()))
            keys = [k for k, _ in collected]
            await results.send(
                {"len": len(collected), "distinct": len(set(keys)), "ordered": keys == sorted(keys)}
            )
        except Exception as e:  # pragma: no cover
            await results.send({"error": str(e)})

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, nonce(), {"go": True}))
    obs = await _wait(handler.results.receive())

    assert obs.get("error") is None
    # flatten + order preserved across the 256->300 native ready-chunk boundary.
    assert obs["len"] == CHUNK_SPAN
    assert obs["distinct"] == CHUNK_SPAN
    assert obs["ordered"] is True

async def test_concurrent_anext_serialize_ordered_distinct(state_client):
    client, topic, _ = state_client

    async def cb(ctx, msg, results):
        m = ctx.state(STATE_DEFS["totals"])
        try:
            for i in range(CHUNK_SPAN):
                await _wait(m.set(f"k{i:03d}", i))
            it = m.items()
            # Six concurrent pulls: the Rust mutex serializes them across the
            # retained/native boundary, so they collectively drain the first six
            # ascending entries with no duplicate or loss.
            results_list = await asyncio.gather(
                *[_wait(it.__anext__()) for _ in range(6)]
            )
            keys = sorted(k for k, _ in results_list)
            await results.send({"keys": keys, "distinct": len(set(keys))})
        except Exception as e:  # pragma: no cover
            await results.send({"error": str(e)})

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, nonce(), {"go": True}))
    obs = await _wait(handler.results.receive())

    assert obs.get("error") is None
    assert obs["distinct"] == 6
    assert obs["keys"] == [f"k{i:03d}" for i in range(6)]

async def test_aclose_after_anext_closes_once(state_client):
    client, topic, _ = state_client

    async def cb(ctx, msg, results):
        m = ctx.state(STATE_DEFS["totals"])
        try:
            for i in range(3):
                await _wait(m.set(f"k{i}", i))
            it = m.items()
            # Task creation does not prove that the native pull acquired its
            # mutex before close. Establish the required order explicitly.
            pulled = asyncio.Event()

            async def pull():
                await _wait(it.__anext__())
                pulled.set()

            async def close_after_pull():
                await _wait(pulled.wait())
                await _wait(it.aclose())

            await asyncio.gather(pull(), close_after_pull())
            await _wait(m.set("after", 1))
            await results.send({"ok": (await _wait(m.get("after"))) == 1})
        except Exception as e:  # pragma: no cover
            await results.send({"error": str(e)})

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, nonce(), {"go": True}))
    obs = await _wait(handler.results.receive())

    assert obs.get("error") is None
    assert obs["ok"] is True

async def test_cancel_pull_then_followup(state_client):
    client, topic, _ = state_client

    async def cb(ctx, msg, results):
        m = ctx.state(STATE_DEFS["totals"])
        try:
            await _wait(m.set("a", 1))
            # Cancel a scan pull, then a follow-up op on the same collection must
            # still succeed. Cancellation safety of the native future is core-
            # owned; the binding-observable proxy is that the cancelled pull
            # leaves the handle usable. (An empty scan does not block at the FFI,
            # so the cancel is issued before the native future is driven —
            # cancelling a genuinely in-flight pull mid-attempt is exercised by
            # core's own tests, not manufacturable cleanly through asyncio task
            # cancellation here.)
            it = m.items()
            t = asyncio.ensure_future(it.__anext__())
            t.cancel()
            with contextlib.suppress(asyncio.CancelledError, StopAsyncIteration):
                await t
            await _wait(m.set("after", 2))
            await results.send({"ok": (await _wait(m.get("after"))) == 2})
        except Exception as e:  # pragma: no cover
            await results.send({"error": str(e)})

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, nonce(), {"go": True}))
    obs = await _wait(handler.results.receive())

    assert obs.get("error") is None
    assert obs["ok"] is True
