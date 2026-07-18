"""Type-checked burst-batching example (README §3.2, in Python).

Exercised by ``mypy`` as a CI gate so the fuller README example stays honest: a
per-key ``value`` flag plus a capacity-bounded ``message_deque`` batch a burst of
activity events per user and flush one summary when a per-key timer fires. The
type parameters are structural JSON annotations (no runtime model validation).
The generic event handler carries that payload type through each message.
"""

from datetime import datetime, timedelta, timezone
from typing import List

from typing_extensions import TypedDict

from prosody import (
    Context,
    EventHandler,
    Message,
    MessageDequeDefinition,
    Timer,
    ValueDefinition,
    message_deque,
    value,
)


class Activity(TypedDict):
    actor: str
    action: str


# Your own delivery function (push, email, …) — the only thing here you write.
async def notify(user_id: str, activities: List["Message[Activity]"]) -> None:
    ...


# Declare the collections once, at module scope; register both on the client via
# ``state_collections=[WINDOW, PENDING]``.
WINDOW: ValueDefinition[bool] = value("window")  # is a batch open for this user?
PENDING: MessageDequeDefinition[Activity] = message_deque(
    "pending", capacity=100
)  # keep the latest 100 messages


class BatchHandler(EventHandler[Activity]):
    async def on_message(
        self, context: Context, message: Message[Activity]
    ) -> None:
        # message.key = userId; message.payload = {actor, action}
        window = context.state(WINDOW)  # bind THIS user's handles for THIS event
        pending = context.state(PENDING)
        if not await window.get():
            # no batch open → this is the first event: send it right away
            await notify(message.key, [message])
            await window.set(True)
            # clear_and_schedule (not schedule): timers are NOT rolled back with
            # state, so a retried event must not stack a second timer.
            await context.clear_and_schedule(
                datetime.now(timezone.utc) + timedelta(minutes=5)
            )
        else:
            # a batch is open → just save it
            await pending.append(message)

    async def on_timer(self, context: Context, timer: Timer) -> None:
        # fires ~5 minutes later, for timer.key
        window = context.state(WINDOW)
        pending = context.state(PENDING)
        # the scan resolves the saved messages concurrently
        batch = [msg async for msg in pending.values()]
        if batch:
            await notify(timer.key, batch)  # one summary of the saved messages
        await pending.clear()  # empty the buffer
        await window.clear()  # close the batch; the next event opens a fresh one
