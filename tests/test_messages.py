"""Message and excise delivery through a live consumer."""

import asyncio

from prosody import ExciseMessage

from support import DEFAULT_TIMEOUT, tracer, TestHandler



async def test_send_and_receive_message(client, random_topic_and_group):

    topic, _ = random_topic_and_group

    handler = TestHandler()

    await asyncio.wait_for(client.subscribe(handler), timeout=DEFAULT_TIMEOUT)

    test_key = "test-key"
    test_payload = {"content": "Hello, Kafka!"}
    with tracer.start_as_current_span("send"):
        await asyncio.wait_for(client.send(topic, test_key, test_payload), timeout=DEFAULT_TIMEOUT)

    await asyncio.wait_for(handler.message_received.wait(), timeout=DEFAULT_TIMEOUT)

    assert len(handler.messages) == 1
    received_message = handler.messages[0]
    assert received_message.topic == topic
    assert received_message.key == test_key
    assert received_message.payload == test_payload
    assert received_message.source_system == "test-send"
    assert received_message.response_requested is False

async def test_send_and_receive_excise(client, random_topic_and_group):
    topic, _ = random_topic_and_group
    handler = TestHandler()
    await asyncio.wait_for(client.subscribe(handler), timeout=DEFAULT_TIMEOUT)

    await asyncio.wait_for(client.excise(topic, "obsolete-key"), timeout=DEFAULT_TIMEOUT)
    await asyncio.wait_for(handler.message_received.wait(), timeout=DEFAULT_TIMEOUT)

    excise = handler.messages[0]
    assert isinstance(excise, ExciseMessage)
    assert excise.key == "obsolete-key"
    assert excise.source_system == "test-send"
    assert excise.response_requested is False

async def test_multiple_messages(client, random_topic_and_group):

    topic, _ = random_topic_and_group
    handler = TestHandler()
    await asyncio.wait_for(client.subscribe(handler), timeout=DEFAULT_TIMEOUT)

    messages = [
        ("key1", {"content": "Message 1"}),
        ("key2", {"content": "Message 2"}),
        ("key3", {"content": "Message 3"})
    ]

    with tracer.start_as_current_span("send_multiple"):
        for key, payload in messages:
            await asyncio.wait_for(client.send(topic, key, payload), timeout=DEFAULT_TIMEOUT)

    async def wait_for_messages():
        while handler.message_count < len(messages):
            await asyncio.wait_for(handler.message_received.wait(), timeout=DEFAULT_TIMEOUT)
            handler.message_received.clear()

    await asyncio.wait_for(wait_for_messages(), timeout=DEFAULT_TIMEOUT)

    assert len(handler.messages) == len(messages)
    expected_messages = set((key, frozenset(payload.items())) for key, payload in messages)
    received_messages = set((msg.key, frozenset(msg.payload.items())) for msg in handler.messages)
    assert expected_messages == received_messages
    assert all(msg.topic == topic for msg in handler.messages)

async def test_same_key_message_order(client, random_topic_and_group):

    topic, _ = random_topic_and_group
    handler = TestHandler()

    test_key = "same-key"
    messages = [
        {"content": "Message 1", "sequence": 1},
        {"content": "Message 2", "sequence": 2},
        {"content": "Message 3", "sequence": 3},
        {"content": "Message 4", "sequence": 4},
        {"content": "Message 5", "sequence": 5},
    ]

    with tracer.start_as_current_span("send_same_key_messages"):
        for payload in messages:
            await asyncio.wait_for(client.send(topic, test_key, payload), timeout=DEFAULT_TIMEOUT)

    await asyncio.wait_for(client.subscribe(handler), timeout=DEFAULT_TIMEOUT)

    async def wait_for_messages():
        while handler.message_count < len(messages):
            await asyncio.wait_for(handler.message_received.wait(), timeout=DEFAULT_TIMEOUT)
            handler.message_received.clear()

    await asyncio.wait_for(wait_for_messages(), timeout=DEFAULT_TIMEOUT)

    assert len(handler.messages) == len(messages)
    received_messages = [msg for msg in handler.messages if msg.key == test_key]
    received_sequences = [msg.payload["sequence"] for msg in received_messages]
    expected_sequences = [msg["sequence"] for msg in messages]

    assert received_sequences == expected_sequences
    for msg in received_messages:
        assert msg.topic == topic
        assert msg.key == test_key
