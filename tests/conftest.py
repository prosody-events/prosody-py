import asyncio
import os
import uuid

import pytest

from prosody.prosody import AdminClient

from support import BOOTSTRAP, CASSANDRA_NODES, _make_state_client, _wait

os.environ["PROSODY_PEER_BIND_ADDRESS"] = "127.0.0.1:0"


@pytest.fixture
async def client_factory():
    """Build clients and shut down each client after the test."""
    from prosody import ProsodyClient

    clients = []

    async def create(**configuration):
        client = await ProsodyClient.create(**configuration)
        clients.append(client)
        return client

    yield create

    outcomes = await asyncio.gather(
        *(client.shutdown() for client in clients), return_exceptions=True
    )
    errors = [outcome for outcome in outcomes if isinstance(outcome, BaseException)]
    if errors:
        details = "; ".join(str(error) for error in errors)
        raise RuntimeError(f"client shutdown failed: {details}") from errors[0]


@pytest.fixture
async def random_topic_and_group():
    """Create a four-partition topic and a fresh group name for one test."""
    topic = f"test-topic-{uuid.uuid4().hex}"
    group = f"test-group-{uuid.uuid4().hex}"
    admin = AdminClient(bootstrap_servers=BOOTSTRAP)
    await _wait(admin.create_topic(topic, partition_count=4, replication_factor=1))
    yield topic, group
    await _wait(admin.delete_topic(topic))


@pytest.fixture
async def client(random_topic_and_group, client_factory):
    """A live client subscribed to the test topic, with source system test-send."""
    topic, group = random_topic_and_group
    return await client_factory(
        bootstrap_servers=BOOTSTRAP,
        source_system="test-send",
        group_id=group,
        subscribed_topics=topic,
        probe_port=None,
        cassandra_nodes=CASSANDRA_NODES,
    )


@pytest.fixture
async def state_client(random_topic_and_group, client_factory):
    """A live client that registers every keyed-state collection kind."""
    topic, group = random_topic_and_group
    client = await _make_state_client(topic, group, client_factory)
    yield client, topic, group
