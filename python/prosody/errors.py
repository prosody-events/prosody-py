import asyncio
from abc import ABC, abstractmethod
from functools import wraps


class EventHandlerError(Exception, ABC):
    """
    Abstract base class for event handler errors.

    This class defines the structure for errors that can occur during event handling.
    Subclasses must implement the `is_permanent` property to indicate whether
    the error is permanent and should not be retried.
    """

    @property
    @abstractmethod
    def is_permanent(self):
        """
        Indicates whether the error is permanent and should not be retried.

        Returns:
            bool: True if the error is permanent, False if it's transient.
        """
        raise NotImplementedError("Subclasses must implement this property")


class TransientError(EventHandlerError):
    """
    Represents a transient error in event handling.

    Transient errors are temporary and can be retried. Messages that raise
    this error will be attempted again.
    """
    is_permanent = False


class PermanentError(EventHandlerError):
    """
    Represents a permanent error in event handling.

    Permanent errors are not temporary and should not be retried. Messages that
    raise this error will be considered as permanent failures and will not be
    retried.
    """
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
    """
    Decorator to mark specific exceptions as transient errors.

    When applied to a function, it will catch the specified exceptions and
    raise them as TransientErrors. This indicates that the operation can be
    retried.

    Args:
        *exceptions (Type[Exception]): The exception types to be treated as transient.

    Returns:
        Callable[[Callable[..., T]], Callable[..., T]]: A decorator function.

    Note:
        Messages that raise TransientErrors will be retried.
    """
    return create_error_decorator(TransientError, exceptions)


def permanent(*exceptions):
    """
    Decorator to mark specific exceptions as permanent errors.

    When applied to a function, it will catch the specified exceptions and
    raise them as PermanentErrors. This indicates that the operation should not
    be retried.

    Args:
        *exceptions (Type[Exception]): The exception types to be treated as permanent.

    Returns:
        Callable[[Callable[..., T]], Callable[..., T]]: A decorator function.

    Note:
        Messages that raise PermanentErrors will not be retried and will be
        considered as permanent failures.
    """
    return create_error_decorator(PermanentError, exceptions)
