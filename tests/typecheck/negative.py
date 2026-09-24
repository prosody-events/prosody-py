"""Expected mypy errors for the public API.

Each ignore names one diagnostic code. ``warn_unused_ignores`` makes this file
self-falsifying: if a signature becomes too permissive, mypy reports the now
unused ignore and the type-check gate fails.
"""

from typing_extensions import TypedDict

from prosody import (
    Context,
    Direction,
    MapDefinition,
    Message,
    MessageDequeDefinition,
    ProsodyClient,
    SetDefinition,
    map,
    message_deque,
    set,
)


class Event(TypedDict):
    amount: int


TOTALS: MapDefinition[int] = map("negative-totals")
EVENTS: MessageDequeDefinition[Event] = message_deque("negative-events")
TAGS: SetDefinition = set("negative-tags")


async def expected_errors(
    client: ProsodyClient, context: Context, message: Message[Event]
) -> None:
    await client.send("events", "key", object())  # type: ignore[arg-type]

    totals = context.state(TOTALS)
    await totals.set("key", "not-an-int")  # type: ignore[arg-type]

    totals.keys(limit="10")  # type: ignore[arg-type]
    totals.values(Direction.BACKWARD)  # type: ignore[call-arg]

    events = context.state(EVENTS)
    await events.append(message.payload)  # type: ignore[arg-type]
    events.values(range=[0, 1])  # type: ignore[arg-type]

    tags = context.state(TAGS)
    await tags.add(1)  # type: ignore[arg-type]

    await ProsodyClient.create(unknown_option=True)  # type: ignore[call-arg]
