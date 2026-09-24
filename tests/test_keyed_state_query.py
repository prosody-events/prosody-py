"""Infra-backed checks that query options, sets, store outcomes, and the demand
cross the native boundary intact.

Each scenario runs inside a live handler with the fixtures and collections of
``test_keyed_state.py``. Core owns the query semantics; these tests check that
each Python option reaches core as the matching query setting.
"""

from prosody import (
    Demand,
    DemandKind,
    Direction,
    StoreOutcome,
    TransientStateError,
    deque as deque_definition,
    map as map_definition,
    set as set_definition,
)

from test_keyed_state import (  # noqa: F401 (fixtures)
    BOOTSTRAP,
    CASSANDRA_KEYSPACE,
    CASSANDRA_NODES,
    STATE_DEFS,
    StateHandler,
    _collect,
    _wait,
    nonce,
    random_topic_and_group,
    state_client,
)


def _client_config(topic, group):
    return dict(
        bootstrap_servers=BOOTSTRAP,
        source_system="test-state-query",
        group_id=group,
        subscribed_topics=topic,
        probe_port=None,
        cassandra_nodes=CASSANDRA_NODES,
        cassandra_keyspace=CASSANDRA_KEYSPACE,
    )


async def _observe(client, topic, callback):
    """Run ``callback`` in a handler for one message and return its report."""

    async def cb(ctx, msg, results):
        try:
            await results.send(await callback(ctx))
        except Exception as e:  # pragma: no cover - reported, not raised
            await results.send({"error": f"{type(e).__name__}: {e}"})

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, nonce(), {"go": True}))
    obs = await _wait(handler.results.receive())
    assert obs.get("error") is None
    return obs


async def test_map_query_options_reach_core(state_client):
    client, topic, _ = state_client

    async def scans(ctx):
        m = ctx.state(STATE_DEFS["totals"])
        empty_before = await _wait(m.is_empty())
        for number, key in enumerate(["a1", "a2", "a3", "b1", "b2"]):
            await _wait(m.set(key, number))

        async def keys(*args, **options):
            return await _wait(_collect(m.keys(*args, **options)))

        return {
            "empty_before": empty_before,
            "empty_after": await _wait(m.is_empty()),
            "contains_many": await _wait(m.contains_many(["a2", "zz", "b1"])),
            "prefix": await keys(prefix="a"),
            "prefix_reverse": await keys(Direction.BACKWARD, prefix="a"),
            "from": await keys(from_="a2"),
            "after": await keys(after="a2"),
            "to": await keys(to="b1"),
            "before": await keys(before="b1"),
            "reverse_from": await keys(Direction.BACKWARD, from_="b1", limit=2),
            "limit": await keys(limit=2),
            "page": await keys(after="a2", limit=2),
            "items": await _wait(_collect(m.items(prefix="b", limit=1))),
            "values": await _wait(
                _collect(m.values(direction=Direction.BACKWARD, prefix="a"))
            ),
        }

    obs = await _observe(client, topic, scans)
    assert obs["empty_before"] is True
    assert obs["empty_after"] is False
    assert obs["contains_many"] == [True, False, True]
    assert obs["prefix"] == ["a1", "a2", "a3"]
    assert obs["prefix_reverse"] == ["a3", "a2", "a1"]
    assert obs["from"] == ["a2", "a3", "b1", "b2"]
    assert obs["after"] == ["a3", "b1", "b2"]
    assert obs["to"] == ["a1", "a2", "a3", "b1"]
    assert obs["before"] == ["a1", "a2", "a3"]
    assert obs["reverse_from"] == ["b1", "a3"]
    assert obs["limit"] == ["a1", "a2"]
    assert obs["page"] == ["a3", "b1"]
    assert obs["items"] == [("b1", 3)]
    assert obs["values"] == [2, 1, 0]


async def test_set_operations_reach_core(state_client):
    client, topic, _ = state_client

    async def operations(ctx):
        tags = ctx.state(STATE_DEFS["tags"])
        empty_before = await _wait(tags.is_empty())
        for member in ["red", "green", "blue", "gold"]:
            await _wait(tags.add(member))
        await _wait(tags.discard("green"))
        await _wait(tags.discard("absent"))
        report = {
            "empty_before": empty_before,
            "contains": await _wait(tags.contains("red")),
            "discarded": await _wait(tags.contains("green")),
            "contains_many": await _wait(tags.contains_many(["blue", "green"])),
            "members": await _wait(_collect(tags)),
            "reverse_limit": await _wait(
                _collect(tags.members(Direction.BACKWARD, limit=2))
            ),
            "prefix": await _wait(_collect(tags.members(prefix="g"))),
            "after": await _wait(_collect(tags.members(after="blue", before="red"))),
        }
        await _wait(tags.clear())
        report["empty_after_clear"] = await _wait(tags.is_empty())
        return report

    obs = await _observe(client, topic, operations)
    assert obs["empty_before"] is True
    assert obs["contains"] is True
    assert obs["discarded"] is False
    assert obs["contains_many"] == [True, False]
    assert obs["members"] == ["blue", "gold", "red"]
    assert obs["reverse_limit"] == ["red", "gold"]
    assert obs["prefix"] == ["gold"]
    assert obs["after"] == ["gold"]
    assert obs["empty_after_clear"] is True


async def test_deque_position_options_reach_core(state_client):
    client, topic, _ = state_client

    async def scans(ctx):
        d = ctx.state(STATE_DEFS["backlog"])
        for number in range(10):
            await _wait(d.append(number))

        async def values(*args, **options):
            return await _wait(_collect(d.values(*args, **options)))

        return {
            "from_to": await values(from_=2, to=5),
            "after_before": await values(after=2, before=5),
            "range": await values(range=range(2, 5)),
            "slice": await values(range=slice(7, None)),
            "reverse_range": await values(Direction.BACKWARD, range=slice(None, 3)),
            "tail": await values(Direction.BACKWARD, limit=3),
            "reverse_from": await values(Direction.BACKWARD, from_=5, to=3),
            "page": await values(after=6, limit=2),
        }

    obs = await _observe(client, topic, scans)
    assert obs["from_to"] == [2, 3, 4, 5]
    assert obs["after_before"] == [3, 4]
    assert obs["range"] == [2, 3, 4]
    assert obs["slice"] == [7, 8, 9]
    assert obs["reverse_range"] == [2, 1, 0]
    assert obs["tail"] == [9, 8, 7]
    assert obs["reverse_from"] == [5, 4, 3]
    assert obs["page"] == [7, 8]


async def test_commit_and_rollback_report_store_outcomes(state_client):
    client, topic, _ = state_client

    async def outcomes(ctx):
        report = {}
        writes = {
            "cart": lambda s: s.set({"v": 1}),
            "totals": lambda s: s.set("k", 1),
            "tags": lambda s: s.add("m"),
            "backlog": lambda s: s.append(1),
        }
        for name, write in writes.items():
            state = ctx.state(STATE_DEFS[name])
            await _wait(write(state))
            committed = await _wait(state.commit())
            idle_commit = await _wait(state.commit())
            await _wait(write(state))
            rolled_back = await _wait(state.rollback())
            idle_rollback = await _wait(state.rollback())
            report[name] = [committed, idle_commit, rolled_back, idle_rollback]
        return report

    obs = await _observe(client, topic, outcomes)
    expected = [
        StoreOutcome.APPLIED,
        StoreOutcome.NO_OP,
        StoreOutcome.APPLIED,
        StoreOutcome.NO_OP,
    ]
    assert obs == {name: expected for name in ["cart", "totals", "tags", "backlog"]}


async def test_context_demand_reports_the_retry_ordinal(state_client):
    client, topic, _ = state_client
    demands = []

    async def cb(ctx, msg, results):
        demands.append(ctx.demand)
        if len(demands) == 1:
            raise TransientStateError("fail the first attempt")
        await results.send(list(demands))

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, nonce(), {"go": True}))
    obs = await _wait(handler.results.receive())

    assert obs == [Demand(DemandKind.NORMAL, 0), Demand(DemandKind.FAILURE, 1)]


async def test_published_readers_accept_query_options(
    random_topic_and_group, client_factory
):
    topic, group = random_topic_and_group
    subsystem = f"query-{nonce()}"
    totals = map_definition("pub-totals", published=True, read_cache=False)
    tags = set_definition("pub-tags", published=True, read_cache=False)
    backlog = deque_definition("pub-backlog", published=True, read_cache=False)
    client = await client_factory(
        **_client_config(topic, group),
        subsystem=subsystem,
        state_collections=[totals, tags, backlog],
    )

    async def cb(ctx, msg, results):
        try:
            if msg.payload["step"] == "write":
                for number, key in enumerate(["a1", "a2", "b1"]):
                    await _wait(ctx.state(totals).set(key, number))
                    await _wait(ctx.state(tags).add(key))
                    await _wait(ctx.state(backlog).append(number))
                return

            key = msg.key
            m = await _wait(client.state(subsystem, totals))
            s = await _wait(client.state(subsystem, tags))
            d = await _wait(client.state(subsystem, backlog))
            await results.send(
                {
                    "map_empty": await _wait(m.is_empty(key)),
                    "map_contains_many": await _wait(m.contains_many(key, ["a2", "zz"])),
                    "map_keys": await _wait(_collect(m.keys(key, prefix="a"))),
                    "map_items": await _wait(_collect(m.items(key, after="a1", limit=1))),
                    "map_values": await _wait(
                        _collect(m.values(key, direction=Direction.BACKWARD))
                    ),
                    "set_contains": await _wait(s.contains(key, "b1")),
                    "set_contains_many": await _wait(s.contains_many(key, ["a1", "zz"])),
                    "set_empty": await _wait(s.is_empty(key)),
                    "set_members": await _wait(
                        _collect(s.members(key, Direction.BACKWARD, before="a1"))
                    ),
                    "deque_values": await _wait(_collect(d.values(key, range=range(1, 3)))),
                }
            )
        except Exception as e:  # pragma: no cover - reported, not raised
            await results.send({"error": f"{type(e).__name__}: {e}"})

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    key = nonce()
    await _wait(client.send(topic, key, {"step": "write"}))
    await _wait(client.send(topic, key, {"step": "read"}))
    obs = await _wait(handler.results.receive())

    assert obs.get("error") is None
    assert obs["map_empty"] is False
    assert obs["map_contains_many"] == [True, False]
    assert obs["map_keys"] == ["a1", "a2"]
    assert obs["map_items"] == [("a2", 1)]
    assert obs["map_values"] == [2, 1, 0]
    assert obs["set_contains"] is True
    assert obs["set_contains_many"] == [True, False]
    assert obs["set_empty"] is False
    assert obs["set_members"] == ["b1", "a2"]
    assert obs["deque_values"] == [1, 2]
