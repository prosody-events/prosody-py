import asyncio
from abc import ABC, abstractmethod
from functools import wraps


class EventHandlerError(Exception, ABC):
    @property
    @abstractmethod
    def is_permanent(self):
        raise NotImplementedError("Subclasses must implement this property")


class TransientError(EventHandlerError):
    is_permanent = False


class PermanentError(EventHandlerError):
    is_permanent = True


def create_error_decorator(error_class, exception_types):
    def decorator(func):
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exception_types as e:
                raise error_class(str(e)) from e

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except exception_types as e:
                raise error_class(str(e)) from e

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


def transient(*exceptions):
    return create_error_decorator(TransientError, exceptions)


def permanent(*exceptions):
    return create_error_decorator(PermanentError, exceptions)
