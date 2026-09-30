"""Type stubs for the query options and scan iterator of keyed state."""

import enum
from typing import Generic, Optional, Tuple

from typing_extensions import TypeVar

_Y = TypeVar("_Y")  # yielded item type of a scan


class Direction(enum.Enum):
    """Scan direction over an ordered collection."""

    FORWARD = "forward"
    BACKWARD = "backward"


class _KeyQuery:
    """Resolved map or set query options for a native scan."""

    backward: bool
    prefix: Optional[str]
    start: Optional[Tuple[str, bool]]
    end: Optional[Tuple[str, bool]]
    range: Optional[Tuple[Optional[str], Optional[str]]]
    limit: Optional[int]


class _PositionQuery:
    """Resolved deque query options for a native scan."""

    backward: bool
    start: Optional[Tuple[int, bool]]
    end: Optional[Tuple[int, bool]]
    range: Optional[Tuple[int, Optional[int]]]
    limit: Optional[int]


class _StateScan(Generic[_Y]):
    """Async iterator over a native scan cursor.

    Every scan method and every ``__aiter__`` returns one. Drive it with
    ``async for``.

    A ``break`` out of the loop does not call :meth:`aclose`. This is safe:
    the cursor holds no store permit between pulls, it stops working when the
    handler attempt ends, and garbage collection closes it. To close it at a
    known point, wrap it in ``contextlib.aclosing(...)``.
    """

    def __aiter__(self) -> "_StateScan[_Y]": ...
    async def __anext__(self) -> _Y: ...
    async def aclose(self) -> None:
        """Close the underlying native cursor (idempotent)."""
        ...
