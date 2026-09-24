"""Representative downstream code, checked only against the installed wheel."""

from datetime import timedelta
from typing import Optional

from typing_extensions import TypedDict, assert_type

from prosody import (
    Context,
    Direction,
    EventHandler,
    ExciseMessage,
    Failure,
    MapDefinition,
    Message,
    MessageDequeDefinition,
    Outcome,
    ProsodyClient,
    ProsodyHandler,
    PublishedSet,
    SetDefinition,
    SetState,
    StoreOutcome,
    Success,
    Timer,
    map,
    message_deque,
    set,
    transient,
)
from prosody.message import JSONValue


class Event(TypedDict):
    amount: int


class Response(TypedDict):
    accepted: bool


TOTALS: MapDefinition[int] = map("totals")
EVENTS: MessageDequeDefinition[Event] = message_deque("events", capacity=10)
TAGS: SetDefinition = set("tags", keyset_limit=64)


@transient(ValueError)
def parse_amount(raw: str) -> int:
    return int(raw)


assert_type(parse_amount("1"), int)


class DefaultHandler(EventHandler):
    """An unsubscripted handler retains the JSONValue default."""

    async def on_excise(self, context: Context, message: ExciseMessage) -> None:
        assert_type(message.key, str)
        message.payload  # type: ignore[attr-defined]

    async def on_message(self, context: Context, message: Message) -> None:
        assert_type(message, Message[JSONValue])

    async def on_timer(self, context: Context, timer: Timer) -> None:
        pass


class Handler(EventHandler[Event, Response]):
    async def on_excise(self, context: Context, message: ExciseMessage) -> Response:
        assert_type(message, ExciseMessage)
        return {"accepted": True}

    async def on_message(self, context: Context, message: Message[Event]) -> Response:
        totals = context.state(TOTALS)
        assert_type(await totals.get(message.key), Optional[int])
        assert_type(await totals.get(message.key, 0), int)
        assert_type(await totals.contains(message.key), bool)
        async for key in totals:
            assert_type(key, str)
        async for entry in totals.items(Direction.BACKWARD, prefix="a", after="a1", limit=5):
            assert_type(entry, tuple[str, int])
        assert_type(await totals.contains_many(["a"]), list[bool])
        assert_type(await totals.is_empty(), bool)
        assert_type(await totals.commit(), StoreOutcome)

        tags = context.state(TAGS)
        assert_type(tags, SetState)
        await tags.add("a")
        await tags.discard("a")
        assert_type(await tags.contains_many(["a"]), list[bool])
        async for member in tags.members(from_="a", before="z"):
            assert_type(member, str)
        assert_type(await tags.rollback(), StoreOutcome)

        events = context.state(EVENTS)
        await events.append(message)
        async for item in events.values(range=slice(0, 5), limit=2):
            assert_type(item, Message[Event])
        event = await events.peek()
        assert_type(event, Optional[Message[Event]])
        if event is not None:
            assert_type(event.payload["amount"], int)
        return {"accepted": True}

    async def on_timer(self, context: Context, timer: Timer) -> None:
        assert_type(timer.key, str)


wrapped_handler = ProsodyHandler(Handler())
assert_type(wrapped_handler, ProsodyHandler[Event, Response])
assert_type(wrapped_handler.handler, EventHandler[Event, Response])


async def read_published_set(client: ProsodyClient) -> None:
    reader = await client.state("checkout", TAGS)
    assert_type(reader, PublishedSet)
    assert_type(await reader.contains("user", "a"), bool)
    async for member in reader.members("user", limit=10):
        assert_type(member, str)


async def subscribe_specialized(client: ProsodyClient) -> None:
    await client.subscribe(Handler())


async def request_typed(client: ProsodyClient) -> None:
    results = await client.request(
        "orders",
        "order-1",
        {},
        subsystems=["inventory"],
        timeout=timedelta(seconds=2),
    )
    excise_results = await client.request_excise(
        "orders",
        "order-1",
        subsystems=["inventory"],
        timeout=timedelta(seconds=2),
    )
    assert_type(excise_results, dict[str, Outcome[JSONValue]])
    assert_type(results, dict[str, Outcome[JSONValue]])
    for outcome in results.values():
        if isinstance(outcome, Success):
            assert_type(outcome.value, JSONValue)
        elif isinstance(outcome, Failure):
            assert_type(outcome, Failure)
