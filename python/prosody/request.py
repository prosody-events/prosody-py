from typing import TypeVar, Union
from typing_extensions import TypeAliasType

from prosody.prosody import (
    Err,
    HandlerResponseError,
    MalformedResponseError,
    Ok,
    ResponseFormatMismatchError,
    ResponseTimeoutError,
)

T = TypeVar("T")

ResponseError = TypeAliasType(
    "ResponseError",
    Union[
        HandlerResponseError,
        ResponseTimeoutError,
        ResponseFormatMismatchError,
        MalformedResponseError,
    ],
)
RequestResult = TypeAliasType("RequestResult", Union[Ok, Err], type_params=(T,))

__all__ = [
    "Err",
    "HandlerResponseError",
    "MalformedResponseError",
    "Ok",
    "RequestResult",
    "ResponseError",
    "ResponseFormatMismatchError",
    "ResponseTimeoutError",
]
