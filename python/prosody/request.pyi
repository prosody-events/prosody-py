from typing import Generic, Literal, TypeAlias, TypeVar

T = TypeVar("T", covariant=True)

class Ok(Generic[T]):
    @property
    def value(self) -> T: ...

class Err:
    @property
    def error(self) -> ResponseError: ...

class HandlerResponseError:
    @property
    def category(self) -> Literal["transient", "permanent", "terminal"]: ...
    @property
    def message(self) -> str: ...

class ResponseTimeoutError: ...
class ResponseFormatMismatchError: ...
class MalformedResponseError: ...

ResponseError: TypeAlias = (
    HandlerResponseError
    | ResponseTimeoutError
    | ResponseFormatMismatchError
    | MalformedResponseError
)
RequestResult: TypeAlias = Ok[T] | Err
