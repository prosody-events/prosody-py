"""Type stubs for the query options and scan iterator of keyed state."""

import enum
from typing import Generic, Optional, Tuple

from typing_extensions import TypeVar

_Y = TypeVar("_Y")  # yielded item type of a scan


class Direction(enum.Enum):
    """Scan direction over an ordered collection.

    The string values are the tokens the native scan accepts.
    """

    FORWARD = "forward"
    BACKWARD = "backward"


class _KeyQuery:
    """Resolved map or set query options for a native scan."""

    direction: str
    prefix: Optional[str]
    start: Optional[Tuple[str, bool]]
    end: Optional[Tuple[str, bool]]
    limit: Optional[int]


class _PositionQuery:
    """Resolved deque query options for a native scan."""

    direction: str
    start: Optional[Tuple[int, bool]]
    end: Optional[Tuple[int, bool]]
    range: Optional[Tuple[int, Optional[int]]]
    limit: Optional[int]


class _StateScan(Generic[_Y]):
    """Async iterator over a native scan cursor.

    Every scan method and every ``__aiter__`` returns one. The native cursor
    owns retained-chunk flattening, serialization, and ``StopAsyncIteration``
    at exhaustion; this adapter only reshapes each item.

    Drive it with ``async for``. Exiting the loop early with a bare ``break``
    does **not** call :meth:`aclose` — that is harmless by construction (no store
    permit is held between pulls, the cursor is attempt-epoch fenced, and native
    ``Drop`` closes it on GC). For a deterministic early close wrap it in
    ``contextlib.aclosing(...)``.

    The generic parameter restores the yielded type even though the runtime
    class is one non-generic adapter.
    """

    def __aiter__(self) -> "_StateScan[_Y]": ...
    async def __anext__(self) -> _Y: ...
    async def aclose(self) -> None:
        """Close the underlying native cursor (idempotent)."""
        ...
