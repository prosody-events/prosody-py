"""Keyed-state value, map, and deque handles against a live consumer.

Each test runs its checks inside ``on_message`` and reports what it saw over
the handler's channel. The fixtures register every collection kind against
Cassandra.
"""

from prosody import Direction
import tsasync

from support import (
    BOOTSTRAP,
    CASSANDRA_NODES,
    CASSANDRA_KEYSPACE,
    STATE_DEFS,
    nonce,
    _wait,
    _collect,
    StateHandler,
)


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

# Cheap presence/key paths and the capacity-bounded deque (parity operators)


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

async def test_blocked_handler_does_not_block_other_key(state_client, client_factory):
    client, topic, group = state_client

    # Probe with a bare client on the SAME group so committed offsets keep the
    # state client from re-seeing probe messages. Five keys over four partitions
    # guarantee two distinct keys share a partition.
    probe = await client_factory(
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
    seen = {}
    key_a = key_b = None
    for p in probes:
        if p["partition"] in seen:
            key_a = seen[p["partition"]]
            key_b = p["key"]
            break
        seen[p["partition"]] = p["key"]
    assert key_a is not None and key_b is not None
    await _wait(probe.unsubscribe())

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
