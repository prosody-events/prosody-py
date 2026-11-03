"""Shutdown handling in ``ProsodyHandler`` dispatch, without a broker."""

import asyncio
import threading

import pytest
import tsasync

from prosody.handler import EventHandler, ProsodyHandler


class RecordingHandler(EventHandler):
    """Record whether the handler body started."""

    def __init__(self) -> None:
        self.started = False

    async def on_message(self, context, message):
        self.started = True
        await asyncio.Event().wait()

    async def on_excise(self, context, message):
        pass

    async def on_timer(self, context, timer):
        pass


class LateSetEvent:
    """Model an event that Rust sets between ``is_set()`` and ``wait()``."""

    def __init__(self, inner: tsasync.Event) -> None:
        self.inner = inner

    def is_set(self) -> bool:
        return False

    def wait(self):
        return self.inner.wait()


def set_event_off_loop() -> tsasync.Event:
    """Create and set an event on a thread with no event loop, as Rust does."""
    events: list[tsasync.Event] = []

    def create() -> None:
        event = tsasync.Event()
        event.set()
        events.append(event)

    thread = threading.Thread(target=create)
    thread.start()
    thread.join()
    return events[0]


@pytest.mark.parametrize("late", [False, True], ids=["set-before-dispatch", "set-before-wait"])
async def test_set_shutdown_event_cancels_dispatch_and_leaves_no_task(late: bool) -> None:
    handler = RecordingHandler()
    event = set_event_off_loop()
    shutdown_event = LateSetEvent(event) if late else event
    before = asyncio.all_tasks()

    with pytest.raises(asyncio.CancelledError):
        await ProsodyHandler(handler).on_message(object(), object(), {}, shutdown_event)

    assert asyncio.all_tasks() == before
    if not late:
        assert not handler.started
