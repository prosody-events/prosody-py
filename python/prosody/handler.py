import asyncio
from abc import ABC, abstractmethod

from opentelemetry import trace
from opentelemetry.propagate import extract


class AbstractMessageHandler(ABC):
    @abstractmethod
    async def handle(self, context, message):
        pass


class TracingHandler:
    def __init__(self, handler: AbstractMessageHandler):
        self.handler = handler
        self.tracer = trace.get_tracer(__name__)

    async def handle(self, context, message, opentelemetry_context, shutdown_event):
        otel_context = extract(carrier=opentelemetry_context)

        with self.tracer.start_as_current_span("python-receive", context=otel_context):
            handler_task = asyncio.create_task(self.handler.handle(context, message))
            shutdown_task = asyncio.create_task(shutdown_event.wait())

            try:
                done, _ = await asyncio.wait(
                    {handler_task, shutdown_task},
                    return_when=asyncio.FIRST_COMPLETED
                )

                if shutdown_task in done:
                    handler_task.cancel()

                await handler_task

            finally:
                for task in {handler_task, shutdown_task}:
                    if not task.done():
                        task.cancel()
