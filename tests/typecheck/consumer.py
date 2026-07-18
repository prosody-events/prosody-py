"""Representative downstream code, checked only against the installed wheel."""

from typing import Optional, cast

from typing_extensions import TypedDict, assert_type

from prosody import (
    Context,
    EventHandler,
    MapDefinition,
    Message,
    MessageDequeDefinition,
    Timer,
    map,
    message_deque,
    transient,
)


class Event(TypedDict):
    amount: int


TOTALS: MapDefinition[int] = map("totals")
EVENTS: MessageDequeDefinition[Event] = message_deque("events", capacity=10)


@transient(ValueError)
def parse_amount(raw: str) -> int:
    return int(raw)


assert_type(parse_amount("1"), int)


class Handler(EventHandler):
    async def on_message(self, context: Context, message: Message) -> None:
        totals = context.state(TOTALS)
        assert_type(await totals.get(message.key), Optional[int])
        assert_type(await totals.get(message.key, 0), int)
        assert_type(await totals.contains(message.key), bool)
        async for key in totals:
            assert_type(key, str)

        events = context.state(EVENTS)
        await events.append(cast("Message[Event]", message))
        event = await events.peek()
        assert_type(event, Optional[Message[Event]])
        if event is not None:
            assert_type(event.payload["amount"], int)

    async def on_timer(self, context: Context, timer: Timer) -> None:
        assert_type(timer.key, str)
