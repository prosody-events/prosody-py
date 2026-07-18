"""Representative downstream code, checked only against the installed wheel."""

from typing import Optional

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
from prosody.message import JSONValue


class Event(TypedDict):
    amount: int


TOTALS: MapDefinition[int] = map("totals")
EVENTS: MessageDequeDefinition[Event] = message_deque("events", capacity=10)


@transient(ValueError)
def parse_amount(raw: str) -> int:
    return int(raw)


assert_type(parse_amount("1"), int)


class DefaultHandler(EventHandler):
    """An unsubscripted handler retains the JSONValue default."""

    async def on_message(self, context: Context, message: Message) -> None:
        assert_type(message, Message[JSONValue])

    async def on_timer(self, context: Context, timer: Timer) -> None:
        pass


class Handler(EventHandler[Event]):
    async def on_message(self, context: Context, message: Message[Event]) -> None:
        totals = context.state(TOTALS)
        assert_type(await totals.get(message.key), Optional[int])
        assert_type(await totals.get(message.key, 0), int)
        assert_type(await totals.contains(message.key), bool)
        async for key in totals:
            assert_type(key, str)

        events = context.state(EVENTS)
        await events.append(message)
        event = await events.peek()
        assert_type(event, Optional[Message[Event]])
        if event is not None:
            assert_type(event.payload["amount"], int)

    async def on_timer(self, context: Context, timer: Timer) -> None:
        assert_type(timer.key, str)
