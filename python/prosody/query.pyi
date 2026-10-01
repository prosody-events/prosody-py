import enum
from typing import Generic

from typing_extensions import TypeVar

_Y = TypeVar("_Y")  # yielded item type of a scan


class Direction(enum.Enum):
    FORWARD = "forward"
    BACKWARD = "backward"


class _StateScan(Generic[_Y]):
    def __aiter__(self) -> "_StateScan[_Y]": ...
    async def __anext__(self) -> _Y: ...
    async def aclose(self) -> None: ...
