"""Query options and the async iterator shared by every keyed-state scan.

The query builders check the caller's options and resolve them into one plain
value that the native scan accepts. Core applies every option in storage. The
builders reject only values that have no native form.
"""

import enum
from dataclasses import dataclass
from typing import (
    Awaitable,
    Callable,
    Generic,
    Optional,
    Protocol,
    Tuple,
    TypeVar,
    Union,
)

X = TypeVar("X")
Y = TypeVar("Y")
K = TypeVar("K")


class Direction(enum.Enum):
    """Scan direction over an ordered collection.

    The string values are the tokens the native scan accepts.
    """

    FORWARD = "forward"
    BACKWARD = "backward"


@dataclass(frozen=True)
class _KeyQuery:
    """Resolved map or set query options.

    An edge is a ``(key, inclusive)`` pair in iteration order.
    """

    direction: str
    prefix: Optional[str] = None
    start: Optional[Tuple[str, bool]] = None
    end: Optional[Tuple[str, bool]] = None
    limit: Optional[int] = None


@dataclass(frozen=True)
class _PositionQuery:
    """Resolved deque query options.

    An edge is a ``(position, inclusive)`` pair in iteration order. ``range``
    is an ascending half-open span of positions; its end is ``None`` when the
    span has no upper bound.
    """

    direction: str
    start: Optional[Tuple[int, bool]] = None
    end: Optional[Tuple[int, bool]] = None
    range: Optional[Tuple[int, Optional[int]]] = None
    limit: Optional[int] = None


def _edge(
    inclusive_name: str,
    inclusive: Optional[K],
    exclusive_name: str,
    exclusive: Optional[K],
) -> Optional[Tuple[K, bool]]:
    """Resolve one inclusive and exclusive edge pair into a single edge."""
    if inclusive is not None and exclusive is not None:
        raise ValueError(f"set {inclusive_name} or {exclusive_name}, not both")
    if inclusive is not None:
        return (inclusive, True)
    if exclusive is not None:
        return (exclusive, False)
    return None


def _whole(name: str, number: object) -> int:
    """Return ``number`` when it is an ``int`` and not a ``bool``."""
    if isinstance(number, bool) or not isinstance(number, int):
        raise TypeError(f"{name}: expected an int, got {type(number).__name__}")
    return number


def _limit(limit: Optional[int]) -> Optional[int]:
    if limit is None:
        return None
    if _whole("limit", limit) < 1:
        raise ValueError(f"limit: must be a positive int, got {limit}")
    return limit


def _position(name: str, position: Optional[int]) -> Optional[int]:
    """Check a front-relative deque position.

    Scan positions count from the front and cannot be negative. To read from
    the back, iterate in reverse.
    """
    if position is None:
        return None
    if _whole(name, position) < 0:
        raise ValueError(f"{name}: positions count from the front, got {position}")
    return position


def _span(span: Union[range, slice, None]) -> Optional[Tuple[int, Optional[int]]]:
    """Resolve a ``range`` or ``slice`` of positions into an ascending span."""
    if span is None:
        return None
    if isinstance(span, range):
        start, stop, step = span.start, span.stop, span.step
    elif isinstance(span, slice):
        start = 0 if span.start is None else span.start
        stop = span.stop
        step = 1 if span.step is None else span.step
    else:
        raise TypeError(f"range: expected a range or a slice, got {type(span).__name__}")

    if step != 1:
        raise ValueError(f"range: the step must be 1, got {step}")
    low = _position("range start", start) or 0
    high = _position("range stop", stop)
    if high is not None and high < low:
        raise ValueError(f"range: the stop {high} precedes the start {low}")
    return (low, high)


def _key_query(
    direction: Direction,
    prefix: Optional[str],
    from_: Optional[str],
    after: Optional[str],
    to: Optional[str],
    before: Optional[str],
    limit: Optional[int],
) -> _KeyQuery:
    """Resolve map or set query options."""
    return _KeyQuery(
        direction.value,
        prefix,
        _edge("from_", from_, "after", after),
        _edge("to", to, "before", before),
        _limit(limit),
    )


def _position_query(
    direction: Direction,
    from_: Optional[int],
    after: Optional[int],
    to: Optional[int],
    before: Optional[int],
    span: Union[range, slice, None],
    limit: Optional[int],
) -> _PositionQuery:
    """Resolve deque query options."""
    return _PositionQuery(
        direction.value,
        _edge(
            "from_", _position("from_", from_), "after", _position("after", after)
        ),
        _edge("to", _position("to", to), "before", _position("before", before)),
        _span(span),
        _limit(limit),
    )


async def _deque_index(
    index: int,
    size: Callable[[], Awaitable[int]],
) -> Optional[int]:
    """Resolve a Python sequence index without reading size for non-negatives."""
    if index >= 0:
        return index
    position = index + await size()
    return position if position >= 0 else None


def _identity(item: X) -> X:
    return item


class _NativeScan(Protocol[X]):
    async def __anext__(self) -> X:
        raise NotImplementedError

    async def aclose(self) -> None:
        raise NotImplementedError


class _StateScan(Generic[Y]):
    """Async iterator over a native scan cursor, applying a per-flavour transform.

    The native cursor already handles retained-chunk flattening, serialization,
    and ``StopAsyncIteration`` at exhaustion, so this is a thin adapter: each
    ``__anext__`` awaits the native pull and reshapes the item (map entries to
    keys/values/pairs; deque items pass through).

    Iterating with ``async for`` and then ``break`` does NOT call ``aclose()``.
    That is harmless by construction — no store permit is held between pulls
    and native ``Drop`` closes it on GC. Owned cursors are attempt-fenced;
    published cursors follow their standalone reader. For deterministic early
    close use ``contextlib.aclosing(...)``.
    """

    def __init__(self, native: _NativeScan[X], transform: Callable[[X], Y]) -> None:
        self._native = native
        self._transform = transform

    def __aiter__(self) -> "_StateScan[Y]":
        return self

    async def __anext__(self) -> Y:
        # Re-raises the native StopAsyncIteration at exhaustion (never coerced
        # by PEP 479 since it crosses no generator boundary here).
        return self._transform(await self._native.__anext__())

    async def aclose(self) -> None:
        await self._native.aclose()
