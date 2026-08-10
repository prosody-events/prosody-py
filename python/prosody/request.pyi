from typing import Generic, Literal, Protocol, TypeAlias, TypeVar

T = TypeVar("T", covariant=True)

class Ok(Protocol, Generic[T]):
    @property
    def value(self) -> T: ...

class Err(Protocol):
    @property
    def error(self) -> ResponseError: ...

class HandlerResponseError(Protocol):
    @property
    def category(self) -> Literal["transient", "permanent", "terminal"]: ...
    @property
    def message(self) -> str: ...

class ResponseTimeoutError(Protocol): ...
class ResponseFormatMismatchError(Protocol): ...
class MalformedResponseError(Protocol): ...

ResponseError: TypeAlias = (
    HandlerResponseError
    | ResponseTimeoutError
    | ResponseFormatMismatchError
    | MalformedResponseError
)
RequestResult: TypeAlias = Ok[T] | Err
