import asyncio
import os
from datetime import datetime

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from prosody import ProsodyClient, EventHandler, Message, Context

# Set up OpenTelemetry
resource = Resource(attributes={
    "service.name": "prosody-example"
})

# Configure the OTLP exporter to send traces to Tempo
os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = "http://localhost:4317"
os.environ["OTEL_EXPORTER_OTLP_PROTOCOL"] = "grpc"

provider = TracerProvider(resource=resource)
processor = BatchSpanProcessor(OTLPSpanExporter())
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)

tracer = trace.get_tracer(__name__)


class ExampleHandler(EventHandler):
    async def on_message(self, context: Context, message: Message) -> None:
        with tracer.start_as_current_span("receive_message"):
            print(f"Received message: {message.payload} at {datetime.now()}")


async def send_message(client: ProsodyClient, key: str, content: str):
    with tracer.start_as_current_span(f"send_message_{key}"):
        await client.send("example-topic", key, {"content": content})


async def send_messages(client: ProsodyClient):
    while True:
        with tracer.start_as_current_span("send_messages_batch"):
            timestamp = datetime.now().isoformat()

            # Send both messages concurrently
            await asyncio.gather(
                send_message(client, "key1", f"Message 1 at {timestamp}"),
                send_message(client, "key2", f"Message 2 at {timestamp}")
            )

            print(f"Sent messages at {timestamp}")

        await asyncio.sleep(10)


async def main():
    client = ProsodyClient(
        bootstrap_servers="localhost:9094",
        group_id="example-group",
        subscribed_topics="example-topic"
    )

    handler = ExampleHandler()
    client.subscribe(handler)

    try:
        send_task = asyncio.create_task(send_messages(client))
        await asyncio.sleep(60)  # Run for 1 minute
    finally:
        send_task.cancel()
        await client.unsubscribe()


if __name__ == "__main__":
    asyncio.run(main())
