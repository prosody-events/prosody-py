"""Shutdown handling in ``ProsodyHandler`` dispatch, without a broker."""

import asyncio
from types import SimpleNamespace

import pytest
import tsasync

from prosody.handler import EventHandler, ProsodyHandler


class RecordingHandler(EventHandler):
    """Record whether the handler body started."""

    def __init__(self) -> None:
        self.started = False

    async def on_message(self, context, message):
        self.started = True

    async def on_excise(self, context, message):
        pass

    async def on_timer(self, context, timer):
        pass


class SyncHandler(RecordingHandler):
    """Return no coroutine, so the handler task cannot be created."""

    def on_message(self, context, message):
        pass


async def test_set_shutdown_event_cancels_dispatch_before_the_handler_starts() -> None:
    handler = RecordingHandler()
    event = tsasync.Event()
    event.set()
    before = asyncio.all_tasks()

    with pytest.raises(asyncio.CancelledError):
        await ProsodyHandler(handler).on_message(object(), object(), {}, event)

    assert not handler.started
    assert asyncio.all_tasks() == before


async def test_handler_that_cannot_start_cancels_the_shutdown_waiter() -> None:
    waiter = asyncio.get_running_loop().create_future()
    event = SimpleNamespace(wait=lambda: waiter)

    with pytest.raises(TypeError):
        await ProsodyHandler(SyncHandler()).on_message(object(), object(), {}, event)

    assert waiter.cancelled()
