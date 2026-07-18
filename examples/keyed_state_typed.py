"""Type-checked keyed-state example.

Exercised by ``mypy`` as a CI gate to keep ``py.typed`` honest: it drives the
generic definition -> handle -> operation flow over both JSON and message
collections and must type-check cleanly. The type parameters are structural JSON
annotations (TypedDict here) — there is no runtime model validation, so the
example casts the incoming payload to reflect that boundary honestly.
"""

from contextlib import aclosing
from datetime import timedelta
from typing import List, Optional, cast

from typing_extensions import TypedDict

from prosody import (
    Context,
    EventHandler,
    MapDefinition,
    Message,
    MessageDequeDefinition,
    Timer,
    ValueDefinition,
    map,
    message_deque,
    value,
)


class Cart(TypedDict):
    items: List[str]


class OrderEvent(TypedDict):
    order_id: str
    total: int


# JSON value collection, JSON map collection, and a message deque collection.
# The type argument is bound through the annotation on the target — the
# constructors are generic, so a bare call would default to ``JSONValue``.
CART: ValueDefinition[Cart] = value("cart", ttl=timedelta(days=30))
TOTALS: MapDefinition[int] = map("totals")  # keys are always str
BACKLOG: MessageDequeDefinition[OrderEvent] = message_deque("backlog", capacity=100)


class OrderHandler(EventHandler):
    async def on_message(self, context: Context, message: Message) -> None:
        # The type parameter is a structural annotation with no runtime
        # validation, so casting the erased JSON payload to the declared shape
        # is the honest way to recover the type at the boundary.
        payload = cast(OrderEvent, message.payload)

        cart = context.state(CART)  # ValueState[Cart]
        current: Cart = await cart.get() or {"items": []}
        await cart.set({"items": [*current["items"], payload["order_id"]]})

        totals = context.state(TOTALS)  # MapState[int]
        await totals.set(message.key, payload["total"])

        # Cheap presence check and default-get: `contains` is a bool, and the
        # `get` overload widens the return to `int` when a default is supplied.
        _present: bool = await totals.contains(message.key)
        running: int = await totals.get(message.key, 0)
        _no_default: Optional[int] = await totals.get(message.key)

        async with aclosing(totals.items()) as scan:
            async for key, total in scan:
                # key: str, total: int
                if total > running:
                    break

        # __aiter__ now yields keys (like `dict`), typed `str`.
        async for _k in totals:
            _key: str = _k

        backlog = context.state(BACKLOG)  # DequeState[Message[OrderEvent]]
        await backlog.append(cast("Message[OrderEvent]", message))
        oldest = await backlog.get(0)  # Optional[Message[OrderEvent]]
        if oldest is not None:
            _order_id: str = oldest.payload["order_id"]
        newest = await backlog.peek()  # Optional[Message[OrderEvent]]
        front = await backlog.peekleft()  # Optional[Message[OrderEvent]]
        if newest is not None and front is not None:
            _actions: str = newest.payload["order_id"] + front.payload["order_id"]

        await cart.commit()

    async def on_timer(self, context: Context, timer: Timer) -> None:
        _key: str = timer.key


async def unparameterized(m: Message) -> None:
    """A bare ``Message`` (defaulting to ``Message[JSONValue]``) type-checks."""
    _topic: str = m.topic
    _payload = m.payload
