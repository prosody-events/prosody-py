from typing import Callable, Type

from typing_extensions import ParamSpec, TypeVar

P = ParamSpec("P")
R = TypeVar("R")

class EventHandlerError(Exception):
    @property
    def is_permanent(self) -> bool: ...

class TransientError(EventHandlerError):
    is_permanent: bool

class PermanentError(EventHandlerError):
    is_permanent: bool

class StateError(Exception): ...
class PermanentStateError(StateError, PermanentError): ...
class TransientStateError(StateError, TransientError): ...
class NullValueError(TransientStateError, ValueError): ...

def transient(
    *exceptions: Type[Exception],
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...
def permanent(
    *exceptions: Type[Exception],
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...
