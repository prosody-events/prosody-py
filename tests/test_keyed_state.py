"""Infra-backed keyed-state FFI scenarios: value/map/deque and their message
variants exercised end-to-end against a live consumer (round-trips, absence,
clear, scan chunk-boundary flattening, direction, attempt fencing, exhaustion).

Mirrors the completed prosody-js ``__test__/index.spec.js`` C1..C12 checklist in
the established prosody-py idiom: the ``random_topic_and_group`` fixture, a client
registering the canonical ``STATE_COLLECTIONS`` against Cassandra, an
injectable-callback handler that runs its assertions INSIDE ``on_message`` and
reports observation dicts back over a ``tsasync.Channel``, and
``asyncio.wait_for(..., DEFAULT_TIMEOUT)`` around every await.

Runtime guards that live in Rust in prosody-py (null/kind/direction/index guards,
scan flattening/serialization, the attempt fence) are green-is-correct at the
integration level: a live vended handle is required to reach them, so these
assertions ride on the live consumer rather than on fake cursors. The pure-Python
wrapper layer (Direction tokens, ``_StateScan`` transforms, wrapper->native method
mapping, error MRO) is locked separately by ``test_state.py`` and not duplicated
here.
"""

import asyncio
import contextlib
import uuid

import pytest
import tsasync

from prosody import (
    ProsodyClient,
    EventHandler,
    Direction,
    value,
    map,
    deque,
    message_value,
    message_map,
    message_deque,
    StateError,
    PermanentStateError,
    TransientStateError,
    NullValueError,
)
from prosody.prosody import AdminClient


DEFAULT_TIMEOUT = 30.0

BOOTSTRAP = "localhost:9094"
CASSANDRA_NODES = "localhost:9042"
CASSANDRA_KEYSPACE = "prosody_test"

# Canonical registered set: one of every kind x payload. ``state()`` binds these
# same frozen definitions.
STATE_DEFS = {
    "cart": value("cart"),
    "totals": map("totals", keyset_limit=256),
    "backlog": deque("backlog"),
    "bounded": deque("bounded", capacity=3),
    "last_msg": message_value("last-msg"),
    "msg_index": message_map("msg-index"),
    "msg_log": message_deque("msg-log"),
}
STATE_COLLECTIONS = list(STATE_DEFS.values())

# Number of map entries used by the chunk-boundary scan tests; > the 256-item
# native ready-chunk cap so a scan must flatten across at least two pulls.
CHUNK_SPAN = 300


def nonce() -> str:
    return uuid.uuid4().hex


async def _wait(awaitable):
    """Await ``awaitable`` bounded by ``DEFAULT_TIMEOUT`` (used in handlers and
    test bodies alike so a wedged op can never hang the suite)."""
    return await asyncio.wait_for(awaitable, timeout=DEFAULT_TIMEOUT)


async def _collect(scan):
    """Drain an async scan into a list. ``async for`` itself cannot take a
    ``wait_for``; wrapping the whole drain in one coroutine lets the caller bound
    it (and every underlying ``__anext__``) under a single ``asyncio.wait_for``."""
    out = []
    async for item in scan:
        out.append(item)
    return out


def _msg_fields(m):
    return {
        "topic": m.topic,
        "partition": m.partition,
        "offset": m.offset,
        "key": m.key,
        "timestamp": m.timestamp,
        "payload": m.payload,
    }


class StateHandler(EventHandler):
    """Injectable-callback handler. Assertions run inside ``on_message``; the
    callback reports observation dicts over ``results`` (a ``tsasync.Channel``,
    thread-safe for the Rust->Python signalling), or raises to drive retry."""

    __test__ = False

    def __init__(self, on_msg, on_tmr=None):
        self.on_msg = on_msg
        self.on_tmr = on_tmr
        self.results = tsasync.Channel()

    async def on_message(self, context, message) -> None:
        await self.on_msg(context, message, self.results)

    async def on_timer(self, context, timer) -> None:
        if self.on_tmr is not None:
            await self.on_tmr(context, timer, self.results)


@pytest.fixture
async def random_topic_and_group():
    topic = f"test-topic-{uuid.uuid4().hex}"
    group = f"test-group-{uuid.uuid4().hex}"
    admin = AdminClient(bootstrap_servers=BOOTSTRAP)
    await _wait(admin.create_topic(topic, partition_count=4, replication_factor=1))
    await asyncio.sleep(1)
    yield topic, group
    await _wait(admin.delete_topic(topic))


def _make_state_client(topic, group):
    return ProsodyClient(
        bootstrap_servers=BOOTSTRAP,
        source_system="test-state",
        group_id=group,
        subscribed_topics=topic,
        probe_port=None,
        cassandra_nodes=CASSANDRA_NODES,
        cassandra_keyspace=CASSANDRA_KEYSPACE,
        state_collections=STATE_COLLECTIONS,
        # >= 2 so the async-bridging test can observe two keys interleaving.
        max_concurrency=4,
    )


@pytest.fixture
async def state_client(random_topic_and_group):
    topic, group = random_topic_and_group
    client = _make_state_client(topic, group)
    yield client, topic, group
    with contextlib.suppress(Exception):
        if await _wait(client.consumer_state()) == "running":
            await _wait(client.unsubscribe())


# ===========================================================================
# item 1 -- Value (C1)
# ===========================================================================


async def test_value_roundtrip_and_absent(state_client):
    client, topic, _ = state_client
    rich = {
        "s": "café 😀",
        "n": 3.5,
        "b": True,
        "arr": [1, "x", None],
        "nested": {"z": [True, 2]},
    }

    async def cb(ctx, msg, results):
        c = ctx.state(STATE_DEFS["cart"])
        try:
            before = await _wait(c.get())  # never written -> None
            await _wait(c.set(rich))
            after = await _wait(c.get())  # read-your-writes
            await _wait(c.clear())
            cleared = await _wait(c.get())
            await results.send({"before": before, "after": after, "cleared": cleared})
        except Exception as e:  # pragma: no cover - reported, not raised
            await results.send({"error": str(e)})

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, nonce(), {"go": True}))
    obs = await _wait(handler.results.receive())

    assert obs.get("error") is None
    assert obs["before"] is None
    assert obs["after"] == rich  # nested null preserved through the serde bridge
    assert obs["cleared"] is None


# ===========================================================================
# item 2 -- Map (C2/C2b)
# ===========================================================================


async def test_map_set_remove_scan_order_getmany(state_client):
    client, topic, _ = state_client
    absent = nonce()

    async def cb(ctx, msg, results):
        m = ctx.state(STATE_DEFS["totals"])
        try:
            for k, v in {"k1": 1, "café": 9, "😀": 7, "k2": 5}.items():
                await _wait(m.set(k, v))
            await _wait(m.remove("k2"))
            fwd = await _wait(_collect(m.items()))
            bwd = await _wait(_collect(m.items(Direction.BACKWARD)))
            await results.send(
                {
                    "fwd": fwd,
                    "bwd": bwd,
                    "k2": await _wait(m.get("k2")),
                    "cafe": await _wait(m.get("café")),
                    "emoji": await _wait(m.get("😀")),
                    "many": await _wait(
                        m.get_many(["k1", "absent", absent, "café", "k1"])
                    ),
                    "empty": await _wait(m.get_many([])),
                }
            )
        except Exception as e:  # pragma: no cover
            await results.send({"error": str(e)})

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, nonce(), {"go": True}))
    obs = await _wait(handler.results.receive())

    assert obs.get("error") is None
    # removed key reads absent; unicode keys round-trip with their values.
    assert obs["k2"] is None
    assert obs["cafe"] == 9
    assert obs["emoji"] == 7
    fwd_keys = [k for k, _ in obs["fwd"]]
    bwd_keys = [k for k, _ in obs["bwd"]]
    # forward yields ascending key order; backward is its exact reverse.
    assert fwd_keys == sorted(fwd_keys)
    assert bwd_keys == list(reversed(fwd_keys))
    assert "k2" not in fwd_keys
    # get_many is positional: one result per key, in order, absent -> None, and
    # a repeated key is NOT deduped (same value at each of its positions).
    assert obs["many"] == [1, None, None, 9, 1]
    assert obs["empty"] == []


# ===========================================================================
# item 3 -- Deque (C3)
# ===========================================================================


async def test_deque_push_len_get_pop_scan_and_empty(state_client):
    client, topic, _ = state_client
    d_full = nonce()
    d_empty = nonce()

    async def cb(ctx, msg, results):
        d = ctx.state(STATE_DEFS["backlog"])
        try:
            if msg.key == d_full:
                await _wait(d.append("a"))
                await _wait(d.append("b"))
                await _wait(d.appendleft("z"))
                fwd = await _wait(_collect(d.values()))
                bwd = await _wait(_collect(d.values(Direction.BACKWARD)))
                await results.send(
                    {
                        "tag": "full",
                        "size": await _wait(d.size()),
                        "empty": await _wait(d.is_empty()),
                        "head": await _wait(d.get(0)),
                        "fwd": fwd,
                        "bwd": bwd,
                        "pf": await _wait(d.popleft()),
                        "pb": await _wait(d.pop()),
                    }
                )
            else:
                await results.send(
                    {
                        "tag": "empty",
                        "len": await _wait(d.size()),
                        "empty": await _wait(d.is_empty()),
                        "pf": await _wait(d.popleft()),
                        "pb": await _wait(d.pop()),
                    }
                )
        except Exception as e:  # pragma: no cover
            await results.send({"tag": "error", "error": str(e)})

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, d_full, {"go": True}))
    await _wait(client.send(topic, d_empty, {"go": True}))
    obs = {}
    for _ in range(2):
        o = await _wait(handler.results.receive())
        obs[o["tag"]] = o

    assert "error" not in obs
    full = obs["full"]
    assert full["size"] == 3
    assert full["empty"] is False
    assert full["head"] == "z"  # appendleft put z at index 0
    assert full["fwd"] == ["z", "a", "b"]
    assert full["bwd"] == ["b", "a", "z"]
    assert full["pf"] == "z"  # popleft removes the front
    assert full["pb"] == "b"  # pop removes the back
    empty = obs["empty"]
    assert empty["len"] == 0
    assert empty["empty"] is True
    assert empty["pf"] is None
    assert empty["pb"] is None


# ===========================================================================
# Cheap presence/key paths and the capacity-bounded deque (parity operators)
# ===========================================================================


async def test_map_contains_and_keys_cheap_paths(state_client):
    client, topic, _ = state_client
    absent = nonce()

    async def cb(ctx, msg, results):
        m = ctx.state(STATE_DEFS["totals"])
        try:
            for k, v in {"k1": 1, "café": 9, "k2": 5}.items():
                await _wait(m.set(k, v))
            await _wait(m.remove("k2"))
            await _wait(m.set("k3", 0))  # a falsy value is still present
            await results.send(
                {
                    # read-your-writes presence: set -> True, removed -> False,
                    # never-written -> False, falsy-but-present -> True.
                    "present": await _wait(m.contains("k1")),
                    "removed": await _wait(m.contains("k2")),
                    "never": await _wait(m.contains(absent)),
                    "falsy": await _wait(m.contains("k3")),
                    # the cheap key-only scan, both directions.
                    "fwd_keys": await _wait(_collect(m.keys())),
                    "bwd_keys": await _wait(_collect(m.keys(Direction.BACKWARD))),
                }
            )
        except Exception as e:  # pragma: no cover
            await results.send({"error": str(e)})

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, nonce(), {"go": True}))
    obs = await _wait(handler.results.receive())

    assert obs.get("error") is None
    assert obs["present"] is True
    assert obs["removed"] is False
    assert obs["never"] is False
    assert obs["falsy"] is True
    # keys() yields bare strings (not pairs), the live set, ascending, and
    # backward is its exact reverse.
    assert obs["fwd_keys"] == sorted(obs["fwd_keys"])
    assert obs["bwd_keys"] == list(reversed(obs["fwd_keys"]))
    assert set(obs["fwd_keys"]) == {"k1", "café", "k3"}
    assert "k2" not in obs["fwd_keys"]


async def test_deque_peek_and_capacity(state_client):
    client, topic, _ = state_client
    populated = nonce()
    empty = nonce()

    async def cb(ctx, msg, results):
        try:
            if msg.key == populated:
                # A capacity-3 deque: appending five items lazily evicts from the
                # front on each over-capacity push, leaving the last three.
                d = ctx.state(STATE_DEFS["bounded"])
                for item in ("a", "b", "c", "d", "e"):
                    await _wait(d.append(item))
                await results.send(
                    {
                        "tag": "full",
                        "size": await _wait(d.size()),
                        "head": await _wait(d.get(0)),
                        # peeks read the endpoints without removing them.
                        "peek": await _wait(d.peek()),
                        "peekleft": await _wait(d.peekleft()),
                        "size_after_peek": await _wait(d.size()),
                    }
                )
            else:
                d = ctx.state(STATE_DEFS["backlog"])
                await results.send(
                    {
                        "tag": "empty",
                        "peek": await _wait(d.peek()),
                        "peekleft": await _wait(d.peekleft()),
                    }
                )
        except Exception as e:  # pragma: no cover
            await results.send({"tag": "error", "error": str(e)})

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, populated, {"go": True}))
    await _wait(client.send(topic, empty, {"go": True}))
    obs = {}
    for _ in range(2):
        o = await _wait(handler.results.receive())
        obs[o["tag"]] = o

    assert "error" not in obs
    full = obs["full"]
    # capacity 3: only the last three appends survive; the front is "c".
    assert full["size"] == 3
    assert full["head"] == "c"
    assert full["peekleft"] == "c"  # front endpoint
    assert full["peek"] == "e"  # back endpoint
    assert full["size_after_peek"] == 3  # a peek does not remove
    empty_obs = obs["empty"]
    assert empty_obs["peek"] is None
    assert empty_obs["peekleft"] is None


# ===========================================================================
# item 4 -- Message collections (C4/C4b/C4c)
# ===========================================================================


async def test_message_value_roundtrip(state_client):
    client, topic, _ = state_client
    mk = nonce()

    async def cb(ctx, msg, results):
        lm = ctx.state(STATE_DEFS["last_msg"])
        try:
            if msg.payload["step"] == 1:
                await _wait(lm.set(msg))
                await results.send({"tag": "orig", **_msg_fields(msg)})
            else:
                got = await _wait(lm.get())
                await results.send({"tag": "got", **_msg_fields(got)})
        except Exception as e:  # pragma: no cover
            await results.send({"tag": "error", "error": str(e)})

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, mk, {"step": 1}))
    await _wait(client.send(topic, mk, {"step": 2}))
    obs = {}
    for _ in range(2):
        o = await _wait(handler.results.receive())
        obs[o["tag"]] = o

    assert "error" not in obs
    orig = {k: v for k, v in obs["orig"].items() if k != "tag"}
    got = {k: v for k, v in obs["got"].items() if k != "tag"}
    # event2's offset differs from event1's, so equality proves the store
    # returned the recorded message rather than the live one.
    assert got == orig
    assert orig["payload"] == {"step": 1}


async def test_message_deque_roundtrip(state_client):
    client, topic, _ = state_client
    md = nonce()

    async def cb(ctx, msg, results):
        dl = ctx.state(STATE_DEFS["msg_log"])
        try:
            await _wait(dl.append(msg))
            head = await _wait(dl.get(0))
            scanned = await _wait(_collect(dl.values()))
            await results.send(
                {
                    "orig": _msg_fields(msg),
                    "head": _msg_fields(head),
                    "scanned_len": len(scanned),
                    "scanned_first_payload": scanned[0].payload,
                }
            )
        except Exception as e:  # pragma: no cover
            await results.send({"error": str(e)})

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, md, {"marker": md}))
    obs = await _wait(handler.results.receive())

    assert obs.get("error") is None
    assert obs["head"] == obs["orig"]
    assert obs["scanned_len"] == 1
    assert obs["scanned_first_payload"] == {"marker": md}


async def test_message_map_roundtrip(state_client):
    client, topic, _ = state_client
    mm = nonce()

    async def cb(ctx, msg, results):
        mi = ctx.state(STATE_DEFS["msg_index"])
        try:
            if msg.payload["step"] == 1:
                await _wait(mi.set("primary", msg))
                await _wait(mi.set("café", msg))
                await results.send({"tag": "orig", **_msg_fields(msg)})
            else:
                got = await _wait(mi.get("primary"))
                cafe = await _wait(mi.get("café"))
                missing = await _wait(mi.get("absent"))
                scanned_keys = [k for k, _ in await _wait(_collect(mi.items()))]
                many = await _wait(mi.get_many(["primary", "absent", "café"]))
                await results.send(
                    {
                        "tag": "got",
                        **_msg_fields(got),
                        "cafe_payload": cafe.payload,
                        "missing": missing,
                        "scanned_keys": scanned_keys,
                        "many": [
                            None if m is None else {"offset": m.offset, "pl": m.payload}
                            for m in many
                        ],
                    }
                )
        except Exception as e:  # pragma: no cover
            await results.send({"tag": "error", "error": str(e)})

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, mm, {"step": 1}))
    await _wait(client.send(topic, mm, {"step": 2}))
    obs = {}
    for _ in range(2):
        o = await _wait(handler.results.receive())
        obs[o["tag"]] = o

    assert "error" not in obs
    orig = {k: v for k, v in obs["orig"].items() if k != "tag"}
    got = obs["got"]
    got_fields = {
        k: got[k]
        for k in ("topic", "partition", "offset", "key", "timestamp", "payload")
    }
    assert got_fields == orig
    assert orig["payload"] == {"step": 1}
    assert got["cafe_payload"] == {"step": 1}
    assert got["missing"] is None
    # forward scan yields both keys ascending.
    assert got["scanned_keys"] == sorted(got["scanned_keys"])
    assert "primary" in got["scanned_keys"] and "café" in got["scanned_keys"]
    # get_many: one entry per key, exactly one absent -> None; both present
    # entries carry the recorded payload at the recorded offset.
    assert len(got["many"]) == 3
    assert sum(1 for m in got["many"] if m is None) == 1
    for m in [m for m in got["many"] if m is not None]:
        assert m == {"offset": orig["offset"], "pl": {"step": 1}}


# ===========================================================================
# item 5 -- commit/rollback (C5a/C5b/C5c)
# ===========================================================================


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

    async def cb(ctx, msg, results):
        c = ctx.state(STATE_DEFS["cart"])
        try:
            await _wait(c.set({"v": a}))
            await _wait(c.commit())
            await _wait(c.set({"v": b}))
            before = await _wait(c.get())
            await _wait(c.rollback())
            after = await _wait(c.get())
            await results.send({"before": before, "after": after})
        except Exception as e:  # pragma: no cover
            await results.send({"error": str(e)})

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, nonce(), {"go": True}))
    obs = await _wait(handler.results.receive())

    assert obs.get("error") is None
    assert obs["before"] == {"v": b}  # uncommitted overwrite visible before rollback
    assert obs["after"] == {"v": a}  # rollback reverts to the committed floor


async def test_map_commit_floor_survives_rollback(state_client):
    client, topic, _ = state_client

    async def cb(ctx, msg, results):
        m = ctx.state(STATE_DEFS["totals"])
        try:
            await _wait(m.set("kept", 1))
            await _wait(m.commit())
            await _wait(m.set("kept", 2))
            await _wait(m.set("dropped", 9))
            before = {"kept": await _wait(m.get("kept")), "dropped": await _wait(m.get("dropped"))}
            await _wait(m.rollback())
            after = {"kept": await _wait(m.get("kept")), "dropped": await _wait(m.get("dropped"))}
            await results.send({"before": before, "after": after})
        except Exception as e:  # pragma: no cover
            await results.send({"error": str(e)})

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, nonce(), {"go": True}))
    obs = await _wait(handler.results.receive())

    assert obs.get("error") is None
    assert obs["before"] == {"kept": 2, "dropped": 9}
    assert obs["after"] == {"kept": 1, "dropped": None}


# ===========================================================================
# item 6 -- leaked handle / context / post-success (C6a/C6b/C6c)
# ===========================================================================


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


# ===========================================================================
# item 7 -- iterators (C7a + strong properties)
# ===========================================================================


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


# ===========================================================================
# item 8 -- errors
# ===========================================================================


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
            m._native.scan("sideways")
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


async def test_deque_index_guard_type_and_overflow(state_client):
    client, topic, _ = state_client

    async def cb(ctx, msg, results):
        d = ctx.state(STATE_DEFS["backlog"])
        await _wait(d.append("x"))
        # Live prosody-py divergence from JS: the deque index is a PyO3 u32, so a
        # fractional index raises TypeError and a negative one raises
        # OverflowError (both classify transient at the handler bridge) rather
        # than TransientStateError. Documented in state.py's DequeState.get.
        frac = None
        try:
            await _wait(d.get(1.5))
        except Exception as e:
            frac = type(e).__name__
        neg = None
        try:
            await _wait(d.get(-1))
        except Exception as e:
            neg = type(e).__name__
        ok = await _wait(d.get(0))
        await results.send({"frac": frac, "neg": neg, "ok": ok})

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, nonce(), {"go": True}))
    obs = await _wait(handler.results.receive())

    assert obs["frac"] == "TypeError"
    assert obs["neg"] == "OverflowError"
    assert obs["ok"] == "x"


# NOTE: identity mismatch across two runs (register the same name with a
# different kind) is NOT covered as an FFI scenario here. Live core does not
# surface it as an observable PermanentStateError at the Python boundary: the
# partition's keyed-state manager retries descriptor-identity acquisition
# internally ("keyed-state descriptor identity acquisition failed; retrying")
# rather than failing the vend or subscribe, so a handler-level assertion has
# nothing to observe. Fabricating a Python-side guard would violate the north
# star (invent no surface core does not provide); the completed prosody-js
# reference likewise omits an identity-mismatch integration test. The rule is
# owned and covered by core (see ../prosody/tests/keyed_state.rs).


# ===========================================================================
# item 9 -- null rejection (C10a) + unrepresentable (C10c)
# ===========================================================================


async def test_null_write_rejects_transient_store_untouched(state_client):
    client, topic, _ = state_client
    v = nonce()

    async def cb(ctx, msg, results):
        c = ctx.state(STATE_DEFS["cart"])
        d = ctx.state(STATE_DEFS["backlog"])
        try:
            await _wait(c.set({"v": v}))
            await _wait(c.commit())

            try:
                await _wait(c.set(None))
                value_outcome = {"threw": False}
            except Exception as e:
                value_outcome = {
                    "threw": True,
                    "null": isinstance(e, NullValueError),
                    "transient": isinstance(e, TransientStateError),
                    "value_error": isinstance(e, ValueError),
                    "msg": str(e),
                }

            try:
                await _wait(d.append(None))
                deque_outcome = {"threw": False}
            except Exception as e:
                deque_outcome = {
                    "threw": True,
                    "transient": isinstance(e, TransientStateError),
                }

            await results.send(
                {
                    "value": value_outcome,
                    "deque": deque_outcome,
                    "after": (await _wait(c.get()))["v"],
                }
            )
        except Exception as e:  # pragma: no cover
            await results.send({"error": str(e)})

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, nonce(), {"go": True}))
    obs = await _wait(handler.results.receive())

    assert obs.get("error") is None
    assert obs["value"]["threw"] is True
    assert obs["value"]["null"] is True
    assert obs["value"]["transient"] is True
    assert obs["value"]["value_error"] is True
    assert "clear" in obs["value"]["msg"]  # names the deletion verb
    assert obs["deque"]["threw"] is True
    assert obs["deque"]["transient"] is True
    assert obs["after"] == v  # store untouched


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


# ===========================================================================
# item 13 -- async bridging (C12)
# ===========================================================================


async def test_blocked_handler_does_not_block_other_key(state_client):
    client, topic, group = state_client

    # Probe with a bare client on the SAME group so committed offsets keep the
    # state client from re-seeing probe messages. Five keys over four partitions
    # guarantee two distinct keys share a partition.
    probe = ProsodyClient(
        bootstrap_servers=BOOTSTRAP,
        source_system="probe",
        group_id=group,
        subscribed_topics=topic,
        probe_port=None,
        cassandra_nodes=CASSANDRA_NODES,
        cassandra_keyspace=CASSANDRA_KEYSPACE,
    )
    async def probe_cb(ctx, msg, results):
        await results.send({"key": msg.key, "partition": msg.partition})

    probe_h = StateHandler(probe_cb)
    await _wait(probe.subscribe(probe_h))
    probe_keys = [f"probe-{nonce()}-{i}" for i in range(5)]
    for k in probe_keys:
        await _wait(probe.send(topic, k, {"probe": True}))
    probes = []
    for _ in range(5):
        probes.append(await _wait(probe_h.results.receive()))
    await _wait(probe.unsubscribe())

    seen = {}
    key_a = key_b = None
    for p in probes:
        if p["partition"] in seen:
            key_a = seen[p["partition"]]
            key_b = p["key"]
            break
        seen[p["partition"]] = p["key"]
    assert key_a is not None and key_b is not None

    gate = tsasync.Event()
    events = tsasync.Channel()
    state = {"a_started": False, "a_finished": False}

    async def cb(ctx, msg, results):
        if msg.key == key_a:
            state["a_started"] = True
            await events.send({"tag": "A-blocked"})
            try:
                await _wait(gate.wait())
            finally:
                state["a_finished"] = True
                await events.send({"tag": "A-done"})
            return
        if msg.key == key_b:
            c = ctx.state(STATE_DEFS["cart"])
            await _wait(c.set({"n": 2}))
            await _wait(c.get())
            await events.send(
                {
                    "tag": "B-done",
                    "a_started": state["a_started"],
                    "a_finished": state["a_finished"],
                }
            )

    # This handler reports over the `events` channel captured in the closure.
    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    try:
        await _wait(client.send(topic, key_a, {"n": 1}))
        # wait for A-blocked
        while True:
            ev = await _wait(events.receive())
            if ev["tag"] == "A-blocked":
                break
        await _wait(client.send(topic, key_b, {"n": 2}))
        # wait for B-done
        while True:
            ev = await _wait(events.receive())
            if ev["tag"] == "B-done":
                b_info = ev
                break
        # B progressed while A parked -> the state op released the GIL/runtime.
        assert b_info["a_started"] is True
        assert b_info["a_finished"] is False
    finally:
        gate.set()
    # drain A-done
    while True:
        ev = await _wait(events.receive())
        if ev["tag"] == "A-done":
            break
