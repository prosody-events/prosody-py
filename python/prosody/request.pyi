from typing import Generic, Literal, Protocol, TypeAlias, TypeVar, runtime_checkable

T = TypeVar("T", covariant=True)

@runtime_checkable
class Ok(Protocol, Generic[T]):
    @property
    def value(self) -> T: ...

@runtime_checkable
class Err(Protocol):
    @property
    def error(self) -> ResponseError: ...

@runtime_checkable
class HandlerResponseError(Protocol):
    @property
    def category(self) -> Literal["transient", "permanent", "terminal"]: ...
    @property
    def message(self) -> str: ...

@runtime_checkable
class ResponseTimeoutError(Protocol): ...
@runtime_checkable
class ResponseFormatMismatchError(Protocol): ...
@runtime_checkable
class MalformedResponseError(Protocol): ...

ResponseError: TypeAlias = (
    HandlerResponseError
    | ResponseTimeoutError
    | ResponseFormatMismatchError
    | MalformedResponseError
)
RequestResult: TypeAlias = Ok[T] | Err
