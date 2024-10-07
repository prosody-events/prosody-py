import asyncio
from datetime import datetime

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prosody import ProsodyClient, EventHandler, Message, Context

# Ensure these environment variables are set before running:
# OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
# OTEL_EXPORTER_OTLP_PROTOCOL=grpc
# OTEL_SERVICE_NAME=prosody-example

# Set up OpenTelemetry
# Resource.create() automatically reads from OTEL_SERVICE_NAME and other ENV vars
resource = Resource.create()
provider = TracerProvider(resource=resource)

# BatchSpanProcessor batches spans before sending them to the exporter
processor = BatchSpanProcessor(OTLPSpanExporter())
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)

# Create a tracer for this module
tracer = trace.get_tracer(__name__)


class ExampleHandler(EventHandler):
    async def on_message(self, context: Context, message: Message) -> None:
        # Start a new span for each received message
        with tracer.start_as_current_span("receive-message") as span:
            # Add an event to the span with message details
            span.add_event("Message received", {
                "content": message.payload["content"],
                "timestamp": datetime.now().isoformat()
            })


async def send_messages(client: ProsodyClient):
    while True:
        # Start a new span for each batch of messages
        with tracer.start_as_current_span("send-messages-batch") as span:
            timestamp = datetime.now().isoformat()

            # Send both messages concurrently
            # The ProsodyClient.send method creates its own spans internally
            await asyncio.gather(
                client.send("example-topic", "key1", {"content": f"Message 1 at {timestamp}"}),
                client.send("example-topic", "key2", {"content": f"Message 2 at {timestamp}"})
            )

            # Add an event to the span to log the batch completion
            span.add_event("Messages sent", {
                "timestamp": timestamp,
                "count": 2
            })

        # Wait for 10 seconds before sending the next batch
        await asyncio.sleep(10)


async def main():
    # Initialize the Prosody client
    client = ProsodyClient(
        bootstrap_servers="localhost:9094",
        group_id="example-group",
        subscribed_topics="example-topic"
    )

    # Create and subscribe the message handler
    handler = ExampleHandler()
    client.subscribe(handler)

    try:
        # Start the message sending task
        send_task = asyncio.create_task(send_messages(client))

        # Run the example for 1 minute
        await asyncio.sleep(60)
    finally:
        # Ensure we cancel our sending task and unsubscribe the client
        send_task.cancel()
        await client.unsubscribe()


if __name__ == "__main__":
    asyncio.run(main())

# To run this example:
# 1. Install dependencies:
#    pip install -r requirements.txt
# 2. Set the required environment variables (see top of file)
# 3. Ensure Kafka and Tempo are running (use provided docker-compose.yaml)
# 4. Create the prosody-example topic
# 5. Run the script:
#    python tracing.py
#
# To view the traces:
# 1. Open your browser and go to http://localhost:3000 to access Grafana
# 2. Click on the Explore icon (compass) in the left sidebar
# 3. Select "Tempo" from the data source dropdown at the top
# 4. In the TraceQL query field, enter: `{.service.name="prosody-example"}`
# 5. Click the "Run query" button
# 6. In the list of spans, look for and click on a "send-messages-batch" span
# 7. This will show you the details of the batch, including child spans for individual message sends
# 8. Expand the "Events" section within a span to see detailed information about events,
#    such as "Message received" or "Messages sent". This provides insights into the timing
#    and content of specific actions within each span.
#
# You can explore other spans like "receive-message" to see the full lifecycle of messages in the system.
# The trace view allows you to see the hierarchical relationship between spans and their durations.
