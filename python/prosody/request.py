from dataclasses import dataclass
from typing import ClassVar, Generic, TypeAlias, TypeVar, Union
from typing_extensions import TypeAliasType

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class HandlerError:
    """The remote handler answered with an error."""

    message: str


@dataclass(frozen=True, slots=True)
class Timeout:
    """No response arrived before the deadline."""

    message: ClassVar[str] = "no response arrived before the deadline"


@dataclass(frozen=True, slots=True)
class FormatMismatch:
    """The responder answered in another format."""

    message: ClassVar[str] = "the responder answered in another format"


@dataclass(frozen=True, slots=True)
class MalformedResponse:
    """The response did not decode."""

    message: ClassVar[str] = "the response did not decode"


ResponseError: TypeAlias = Union[HandlerError, Timeout, FormatMismatch, MalformedResponse]


@dataclass(frozen=True, slots=True)
class Success(Generic[T]):
    """Contains one successful subsystem response."""

    value: T


@dataclass(frozen=True, slots=True)
class Failure:
    """Contains one subsystem failure."""

    error: ResponseError


Outcome = TypeAliasType("Outcome", Union[Success[T], Failure], type_params=(T,))

__all__ = [
    "Failure",
    "FormatMismatch",
    "HandlerError",
    "MalformedResponse",
    "Outcome",
    "ResponseError",
    "Success",
    "Timeout",
]
