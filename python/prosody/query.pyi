import enum
from typing import Generic, Optional, Union

from typing_extensions import TypedDict, TypeVar

_Y = TypeVar("_Y")  # yielded item type of a scan


class Direction(enum.Enum):
    FORWARD = "forward"
    BACKWARD = "backward"


# The scan options of a map or a set. The functional form permits the key
# ``range`` and keeps the builtin ``range`` visible to the deque options.
_KeyOptions = TypedDict(
    "_KeyOptions",
    {
        "prefix": Optional[str],
        "from_": Optional[str],
        "after": Optional[str],
        "to": Optional[str],
        "before": Optional[str],
        "range": Optional[slice],
        "limit": Optional[int],
    },
    total=False,
)

# The scan options of a deque.
_PositionOptions = TypedDict(
    "_PositionOptions",
    {
        "from_": Optional[int],
        "after": Optional[int],
        "to": Optional[int],
        "before": Optional[int],
        "range": Union[range, slice, None],
        "limit": Optional[int],
    },
    total=False,
)


class _StateScan(Generic[_Y]):
    def __aiter__(self) -> "_StateScan[_Y]": ...
    async def __anext__(self) -> _Y: ...
    async def aclose(self) -> None: ...
