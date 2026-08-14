from typing import TypeVar, Union
from typing_extensions import TypeAliasType

from prosody.prosody import (
    HandlerResponseError,
    MalformedResponseError,
    ResponseError,
    ResponseFormatMismatchError,
    ResponseTimeoutError,
)

T = TypeVar("T")

RequestResult = TypeAliasType("RequestResult", Union[T, ResponseError], type_params=(T,))

__all__ = [
    "HandlerResponseError",
    "MalformedResponseError",
    "RequestResult",
    "ResponseError",
    "ResponseFormatMismatchError",
    "ResponseTimeoutError",
]
