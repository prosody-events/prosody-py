from dataclasses import dataclass
from typing import ClassVar, Generic, TypeAlias, TypeVar

T = TypeVar("T", covariant=True)

@dataclass(frozen=True, slots=True)
class HandlerError:
    message: str

@dataclass(frozen=True, slots=True)
class Timeout:
    message: ClassVar[str]

@dataclass(frozen=True, slots=True)
class FormatMismatch:
    message: ClassVar[str]

@dataclass(frozen=True, slots=True)
class MalformedResponse:
    message: ClassVar[str]

ResponseError: TypeAlias = HandlerError | Timeout | FormatMismatch | MalformedResponse

@dataclass(frozen=True, slots=True)
class Success(Generic[T]):
    value: T

@dataclass(frozen=True, slots=True)
class Failure:
    error: ResponseError

Outcome: TypeAlias = Success[T] | Failure
