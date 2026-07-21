"""Expected mypy errors for the public API.

Each ignore names one diagnostic code. ``warn_unused_ignores`` makes this file
self-falsifying: if a signature becomes too permissive, mypy reports the now
unused ignore and the type-check gate fails.
"""

from typing_extensions import TypedDict

from prosody import (
    Context,
    MapDefinition,
    Message,
    MessageDequeDefinition,
    ProsodyClient,
    map,
    message_deque,
)


class Event(TypedDict):
    amount: int


TOTALS: MapDefinition[int] = map("negative-totals")
EVENTS: MessageDequeDefinition[Event] = message_deque("negative-events")


async def expected_errors(
    client: ProsodyClient, context: Context, message: Message[Event]
) -> None:
    await client.send("events", "key", object())  # type: ignore[arg-type]

    totals = context.state(TOTALS)
    await totals.set("key", "not-an-int")  # type: ignore[arg-type]

    events = context.state(EVENTS)
    await events.append(message.payload)  # type: ignore[arg-type]
