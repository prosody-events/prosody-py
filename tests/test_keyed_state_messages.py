"""Message collections: store a delivered message and read it back."""

from prosody import Message, PermanentStateError, TransientStateError

from support import STATE_DEFS, nonce, _wait, _collect, _msg_fields, StateHandler


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

async def test_only_a_delivered_message_is_storable(state_client):
    """A message collection stores where a message sits in Kafka, so only a
    message prosody delivered can go into one.

    Every delivered message qualifies, including one read back out of a
    collection: the wrapper holds the message it came from, which is both what
    the write needs and what keeps the loader's permit held while Python can
    still reach it. A ``Message`` built in Python has no Kafka position behind it
    and is rejected transiently, keeping the event visible rather than
    discarding it.
    """
    client, topic, _ = state_client
    mk = nonce()

    async def cb(ctx, msg, results):
        lm = ctx.state(STATE_DEFS["last_msg"])
        outcomes = {}
        try:
            forged = Message(
                msg.topic, msg.partition, msg.offset, msg.timestamp, msg.key, msg.payload
            )
            outcomes["forged"] = await _store_outcome(lm, forged)

            outcomes["delivered"] = await _store_outcome(lm, msg)
            await _wait(lm.commit())

            # A message read back out of the collection is storable too, and
            # round-trips to the same Kafka position.
            reread = await _wait(lm.get())
            outcomes["reread"] = await _store_outcome(lm, reread)
            await _wait(lm.commit())
            again = await _wait(lm.get())
            outcomes["same_offset"] = again.offset == msg.offset
            outcomes["same_payload"] = again.payload == msg.payload
            await results.send(outcomes)
        except Exception as e:  # pragma: no cover
            await results.send({"error": str(e)})

    handler = StateHandler(cb)
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, mk, {"marker": mk}))
    obs = await _wait(handler.results.receive())

    assert obs.get("error") is None
    assert obs["forged"] == "transient"
    assert obs["delivered"] == "stored"
    assert obs["reread"] == "stored"
    assert obs["same_offset"] is True
    assert obs["same_payload"] is True

async def _store_outcome(handle, item):
    """Classifies what storing ``item`` did: ``"stored"`` or the error category."""
    try:
        await _wait(handle.set(item))
    except TransientStateError:
        return "transient"
    except PermanentStateError:
        return "permanent"
    return "stored"

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
