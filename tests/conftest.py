import asyncio
import os

import pytest

os.environ["PROSODY_PEER_BIND_ADDRESS"] = "127.0.0.1:0"


@pytest.fixture
async def client_factory():
    """Build clients and shut down each client after the test."""
    from prosody import ProsodyClient

    clients = []

    def create(**configuration):
        client = ProsodyClient(**configuration)
        clients.append(client)
        return client

    yield create

    await asyncio.gather(*(client.shutdown() for client in clients))
