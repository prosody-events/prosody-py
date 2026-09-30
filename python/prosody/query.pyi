import enum
from typing import Generic, Optional, Tuple

from typing_extensions import TypeVar

_Y = TypeVar("_Y")  # yielded item type of a scan


class Direction(enum.Enum):
    FORWARD = "forward"
    BACKWARD = "backward"


class _KeyQuery:
    backward: bool
    prefix: Optional[str]
    start: Optional[Tuple[str, bool]]
    end: Optional[Tuple[str, bool]]
    range: Optional[Tuple[Optional[str], Optional[str]]]
    limit: Optional[int]


class _PositionQuery:
    backward: bool
    start: Optional[Tuple[int, bool]]
    end: Optional[Tuple[int, bool]]
    range: Optional[Tuple[int, Optional[int]]]
    limit: Optional[int]


class _StateScan(Generic[_Y]):
    def __aiter__(self) -> "_StateScan[_Y]": ...
    async def __anext__(self) -> _Y: ...
    async def aclose(self) -> None: ...
