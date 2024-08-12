import asyncio
from abc import ABC, abstractmethod

from opentelemetry import trace
from opentelemetry.propagate import extract


class EventHandler(ABC):
    @abstractmethod
    async def on_message(self, context, message):
        pass


class ProsodyHandler:
    def __init__(self, handler: EventHandler):
        self.handler = handler
        self.tracer = trace.get_tracer(__name__)

    async def on_message(self, context, message, opentelemetry_context, shutdown_event):
        otel_context = extract(carrier=opentelemetry_context)

        with self.tracer.start_as_current_span("python-receive", context=otel_context):
            handler_task = asyncio.create_task(self.handler.on_message(context, message))
            shutdown_task = asyncio.create_task(shutdown_event.wait())

            try:
                done, _ = await asyncio.wait(
                    {handler_task, shutdown_task},
                    return_when=asyncio.FIRST_COMPLETED
                )

                if shutdown_task in done:
                    handler_task.cancel("partition has been revoked")

                await handler_task

            finally:
                for task in {handler_task, shutdown_task}:
                    if not task.done():
                        task.cancel("task is shutting down")
