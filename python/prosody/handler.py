import asyncio
import logging
import os
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Mapping
from typing import Generic, Protocol

from typing_extensions import TypeVar

from opentelemetry import trace
from opentelemetry.propagate import extract

from prosody.context import Context
from prosody.message import ExciseMessage, JSONValue, Message
from prosody.timer import Timer

_log = logging.getLogger(__name__)

_sentry = ...  # uninitialized sentinel; None means "DSN absent or package missing"


def _get_sentry():
    global _sentry
    if _sentry is not ...:
        return _sentry
    _sentry = None
    if not os.environ.get("SENTRY_DSN"):
        return None
    try:
        import sentry_sdk
        from sentry_sdk.integrations.logging import LoggingIntegration
        if not sentry_sdk.is_initialized():
            sentry_sdk.init(
                dsn=os.environ["SENTRY_DSN"],
                integrations=[LoggingIntegration(event_level=None)],
            )
        _sentry = sentry_sdk
    except ImportError:
        _log.error("SENTRY_DSN is set but sentry-sdk is not installed. Run: pip install 'prosody[sentry]'")
    return _sentry


def _capture_handler_exception(event_type: str, context: dict, exc: Exception) -> None:
    sentry = _get_sentry()
    if sentry is None:
        return
    with sentry.isolation_scope() as scope:
        scope.set_tag("prosody.event_type", event_type)
        scope.set_context("prosody", context)
        sentry.capture_exception(exc.__cause__ or exc)


P = TypeVar("P", default=JSONValue)
Response = TypeVar("Response", default=JSONValue)
Result = TypeVar("Result")


class _ShutdownEvent(Protocol):
    async def wait(self) -> None: ...


class EventHandler(ABC, Generic[P, Response]):
    """
    Abstract base class for event handlers, generic over payload and response.

    Subclasses must implement `on_message`, `on_excise`, and `on_timer`.
    An unsubscripted handler uses ``JSONValue`` for both types.
    Use structural JSON types such as ``TypedDict`` for precise
    payload and response contracts. These annotations do not validate values.
    """

    @abstractmethod
    async def on_message(self, context: Context, message: Message[P]) -> Response:
        """
        Handle a Kafka message.

        Args:
            context (Context): The context of the message.
            message (Message[P]): The Kafka message to be processed.

        Returns:
            Response: The response for requests.

        Notes:
            - This method may be cancelled at any time. Implement it to respond quickly to cancellation.
            - Use `try/finally` blocks or context managers for proper resource cleanup.
            - This method may be called from different threads. Ensure that any handler state is thread-safe.

        Raises:
            asyncio.CancelledError: If the task is cancelled.
        """
        pass

    @abstractmethod
    async def on_excise(self, context: Context, message: ExciseMessage) -> Response:
        """Handle an excise record."""
        pass

    @abstractmethod
    async def on_timer(self, context: Context, timer: Timer) -> None:
        """
        Handle a timer event.

        Args:
            context (Context): The context of the timer event.
            timer (Timer): The timer event to be processed.

        Returns:
            None: No result.

        Notes:
            - This method may be cancelled at any time. Implement it to respond quickly to cancellation.
            - Use `try/finally` blocks or context managers for proper resource cleanup.
            - This method may be called from different threads. Ensure that any handler state is thread-safe.

        Raises:
            asyncio.CancelledError: If the task is cancelled.
        """
        pass


class ProsodyHandler(Generic[P, Response]):
    def __init__(self, handler: EventHandler[P, Response]):
        self.handler = handler
        self.tracer = trace.get_tracer(__name__)

    async def _dispatch(
        self,
        call: Callable[[], Awaitable[Result]],
        span_name: str,
        event_type: str,
        details: dict[str, object],
        opentelemetry_context: Mapping[str, str],
        shutdown_event: _ShutdownEvent,
    ) -> Result:
        otel_context = extract(carrier=opentelemetry_context)
        with self.tracer.start_as_current_span(span_name, context=otel_context):
            handler_task = asyncio.create_task(call())
            shutdown_task = asyncio.create_task(shutdown_event.wait())
            try:
                done, _ = await asyncio.wait(
                    {handler_task, shutdown_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if shutdown_task in done:
                    handler_task.cancel("partition has been revoked")
                try:
                    return await handler_task
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    _capture_handler_exception(event_type, details, exc)
                    raise
            finally:
                for task in {handler_task, shutdown_task}:
                    if not task.done():
                        task.cancel("task is shutting down")

    async def on_message(
        self,
        context: Context,
        message: Message[P],
        opentelemetry_context: Mapping[str, str],
        shutdown_event: _ShutdownEvent,
    ) -> Response:
        return await self._dispatch(
            lambda: self.handler.on_message(context, message),
            "on_message",
            "message",
            {
                "topic": getattr(message, "topic", None),
                "partition": getattr(message, "partition", None),
                "key": getattr(message, "key", None),
                "offset": getattr(message, "offset", None),
            },
            opentelemetry_context,
            shutdown_event,
        )

    async def on_excise(
        self,
        context: Context,
        message: ExciseMessage,
        opentelemetry_context: Mapping[str, str],
        shutdown_event: _ShutdownEvent,
    ) -> Response:
        return await self._dispatch(
            lambda: self.handler.on_excise(context, message),
            "on_excise",
            "excise",
            {
                "topic": getattr(message, "topic", None),
                "partition": getattr(message, "partition", None),
                "key": getattr(message, "key", None),
                "offset": getattr(message, "offset", None),
            },
            opentelemetry_context,
            shutdown_event,
        )

    async def on_timer(
        self,
        context: Context,
        timer: Timer,
        opentelemetry_context: Mapping[str, str],
        shutdown_event: _ShutdownEvent,
    ) -> None:
        await self._dispatch(
            lambda: self.handler.on_timer(context, timer),
            "on_timer",
            "timer",
            {
                "key": getattr(timer, "key", None),
                "time": getattr(timer, "time", None),
            },
            opentelemetry_context,
            shutdown_event,
        )
