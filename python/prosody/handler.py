import asyncio
from abc import ABC, abstractmethod

from opentelemetry import trace
from opentelemetry.propagate import extract

from prosody.context import Context
from prosody.message import Message
from prosody.timer import Timer


def _capture_handler_exception(event_type: str, context: dict, exc: Exception) -> None:
    try:
        import sentry_sdk
    except ImportError:
        return
    if not sentry_sdk.is_initialized():
        return
    with sentry_sdk.isolation_scope() as scope:
        scope.set_tag("prosody.event_type", event_type)
        scope.set_context("prosody", context)
        sentry_sdk.capture_exception(exc)


class EventHandler(ABC):
    """
    Abstract base class for event handlers.

    Subclasses must implement the `on_message` method to define custom message
    processing logic. Subclasses may optionally implement the `on_timer` method
    to handle timer events.
    """

    @abstractmethod
    async def on_message(self, context: Context, message: Message) -> None:
        """
        Handle a Kafka message.

        Args:
            context (Context): The context of the message.
            message (Message): The Kafka message to be processed.

        Notes:
            - This method may be cancelled at any time. Implement it to respond quickly to cancellation.
            - Use `try/finally` blocks or context managers for proper resource cleanup.
            - This method may be called from different threads. Ensure that any handler state is thread-safe.

        Raises:
            asyncio.CancelledError: If the task is cancelled.
        """
        pass

    @abstractmethod
    async def on_timer(self, context: Context, timer: Timer) -> None:
        """
        Handle a timer event.

        Args:
            context (Context): The context of the timer event.
            timer (Timer): The timer event to be processed.

        Notes:
            - This method may be cancelled at any time. Implement it to respond quickly to cancellation.
            - Use `try/finally` blocks or context managers for proper resource cleanup.
            - This method may be called from different threads. Ensure that any handler state is thread-safe.

        Raises:
            asyncio.CancelledError: If the task is cancelled.
        """
        pass


class ProsodyHandler:
    def __init__(self, handler: EventHandler):
        self.handler = handler
        self.tracer = trace.get_tracer(__name__)

    async def on_message(self, context, message, opentelemetry_context, shutdown_event):
        otel_context = extract(carrier=opentelemetry_context)

        with self.tracer.start_as_current_span("on_message", context=otel_context):
            handler_task = asyncio.create_task(self.handler.on_message(context, message))
            shutdown_task = asyncio.create_task(shutdown_event.wait())

            try:
                done, _ = await asyncio.wait(
                    {handler_task, shutdown_task},
                    return_when=asyncio.FIRST_COMPLETED
                )

                if shutdown_task in done:
                    handler_task.cancel("partition has been revoked")

                try:
                    await handler_task
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    _capture_handler_exception("message", {
                        "topic": getattr(message, "topic", None),
                        "partition": getattr(message, "partition", None),
                        "key": getattr(message, "key", None),
                        "offset": getattr(message, "offset", None),
                    }, exc)
                    raise

            finally:
                for task in {handler_task, shutdown_task}:
                    if not task.done():
                        task.cancel("task is shutting down")

    async def on_timer(self, context, timer, opentelemetry_context, shutdown_event):
        otel_context = extract(carrier=opentelemetry_context)

        with self.tracer.start_as_current_span("on_timer", context=otel_context):
            handler_task = asyncio.create_task(self.handler.on_timer(context, timer))
            shutdown_task = asyncio.create_task(shutdown_event.wait())

            try:
                done, _ = await asyncio.wait(
                    {handler_task, shutdown_task},
                    return_when=asyncio.FIRST_COMPLETED
                )

                if shutdown_task in done:
                    handler_task.cancel("partition has been revoked")

                try:
                    await handler_task
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    _capture_handler_exception("timer", {
                        "key": getattr(timer, "key", None),
                        "time": getattr(timer, "time", None),
                    }, exc)
                    raise

            finally:
                for task in {handler_task, shutdown_task}:
                    if not task.done():
                        task.cancel("task is shutting down")
