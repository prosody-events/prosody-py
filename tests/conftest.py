import asyncio
import os

import pytest

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
