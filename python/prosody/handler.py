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

    async def handle(self, context, message, opentelemetry_context):
        # Convert the serialized context back into an OpenTelemetry Context object
        otel_context = extract(carrier=opentelemetry_context)

        # Create a new span
        with self.tracer.start_as_current_span("python-receive", context=otel_context):
            # Call the wrapped handler's handle method
            await self.handler.handle(context, message)
