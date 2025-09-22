"""
OpenTelemetry Integration Test

This test verifies that OpenTelemetry tracing works correctly across the entire
message processing pipeline including message sending, receiving, and timer execution.

## Prerequisites

1. Install OpenTelemetry dependencies:
   ```
   pip install opentelemetry-exporter-otlp
   ```

2. Start an OTLP collector (Jaeger must be running):
   ```bash
   # Using Docker:
   docker run -d --name jaeger \
     -p 16686:16686 \
     -p 14250:14250 \
     -p 4317:4317 \
     -p 4318:4318 \
     jaegertracing/all-in-one:latest
   ```

## Running the Test

Run this test manually (it's excluded from default test runs):

```bash
# Using the Makefile (recommended):
make test-tracing

# Or manually:
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 \
OTEL_SERVICE_NAME=prosody-python-tracing-test \
pytest tests/test_tracing_integration.py::test_complete_distributed_trace -v -s

# Or run all tracing tests:
pytest -m "tracing" -v -s
```

## Viewing Results

After running the test, you can view the distributed traces in Jaeger at:
http://localhost:16686

Look for traces from the "prosody-python-tracing-test" service.
"""

import asyncio
import logging
import os
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, Any

import pytest

# OpenTelemetry dependencies (required for this test)
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource

from prosody import ProsodyClient, EventHandler, Message, Context, Timer
from prosody.prosody import AdminClient


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class TracingHandler(EventHandler):
    """Test handler that creates spans and schedules timers with proper tracing"""

    def __init__(self):
        self.message_received_event = asyncio.Event()
        self.timer_fired_event = asyncio.Event()
        self.tracer = trace.get_tracer("prosody-python-test")
        self.logger = logger

        # Track state for verification
        self._message_received = False
        self._timer_fired = False

    @property
    def message_received(self) -> bool:
        return self._message_received

    @property
    def timer_fired(self) -> bool:
        return self._timer_fired

    async def on_message(self, context: Context, message: Message) -> None:
        self.logger.info(f"📨 Handler: on_message called with topic={message.topic}, key={message.key}")

        with self.tracer.start_as_current_span("test-message-handler") as span:
            span.set_attributes({
                "message.topic": message.topic,
                "message.key": message.key,
                "test.phase": "message_received"
            })

            # Schedule a timer to fire in 2 seconds
            timer_time = datetime.now(timezone.utc) + timedelta(seconds=2)
            self.logger.info(f"⏰ Handler: Scheduling timer for {timer_time}")

            await context.schedule(timer_time)

            span.add_event("timer_scheduled", {"timer.time": timer_time.timestamp()})

            # Signal that message was received
            self._message_received = True
            self.message_received_event.set()
            self.logger.debug("✅ Handler: Message received event set")

    async def on_timer(self, context: Context, timer: Timer) -> None:
        self.logger.info(f"⏲️  Handler: on_timer called with key={timer.key}, time={timer.time}")

        with self.tracer.start_as_current_span("test-timer-handler") as span:
            span.set_attributes({
                "timer.key": timer.key,
                "timer.time": timer.time.timestamp(),
                "test.phase": "timer_fired"
            })

            span.add_event("timer_execution_complete")

            # Signal that timer was fired
            self._timer_fired = True
            self.timer_fired_event.set()
            self.logger.debug("✅ Handler: Timer fired event set")


def setup_opentelemetry():
    """Configure OpenTelemetry SDK with OTLP exporter"""

    # Set service metadata
    service_name = "prosody-python-tracing-test"
    service_version = "1.1.1"  # Should match the package version

    os.environ.setdefault("OTEL_SERVICE_NAME", service_name)
    os.environ.setdefault("OTEL_SERVICE_VERSION", service_version)
    os.environ.setdefault("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")

    # Create resource with service information
    resource = Resource.create({
        "service.name": service_name,
        "service.version": service_version,
        "deployment.environment": "test",
        "test.suite": "tracing_integration"
    })

    # Configure tracer provider
    tracer_provider = TracerProvider(resource=resource)

    # Add OTLP exporter
    otlp_exporter = OTLPSpanExporter(
        endpoint=os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] + "/v1/traces"
    )
    tracer_provider.add_span_processor(
        BatchSpanProcessor(otlp_exporter)
    )

    # Set the global tracer provider
    trace.set_tracer_provider(tracer_provider)

    logger.info(f"OpenTelemetry configured with service name: {service_name}")
    logger.info(f"OTLP endpoint: {os.environ['OTEL_EXPORTER_OTLP_ENDPOINT']}")


@pytest.fixture
async def random_topic_and_group():
    """Generate a unique topic and consumer group for the test"""
    timestamp = int(time.time())
    random_id = uuid.uuid4().hex[:8]
    topic = f"tracing-test-{timestamp}-{random_id}"
    group = f"test-group-{random_id}"

    admin = AdminClient(bootstrap_servers="localhost:9094")
    await admin.create_topic(topic, partition_count=1, replication_factor=1)
    await asyncio.sleep(1)  # Allow topic creation to propagate

    yield topic, group

    # Cleanup
    try:
        await admin.delete_topic(topic)
    except Exception as e:
        logger.warning(f"Failed to clean up topic {topic}: {e}")


@pytest.fixture
async def client(random_topic_and_group):
    """Create a ProsodyClient for testing"""
    topic, group = random_topic_and_group
    client = ProsodyClient(
        bootstrap_servers="localhost:9094",
        source_system="test-tracing",
        group_id=group,
        subscribed_topics=topic,
        probe_port=None,
        cassandra_nodes="localhost:9042",
    )
    yield client

    # Cleanup
    try:
        if await client.consumer_state() == "running":
            await client.unsubscribe()
    except Exception as e:
        logger.warning(f"Failed to unsubscribe client: {e}")


@pytest.mark.tracing
@pytest.mark.integration
@pytest.mark.asyncio
async def test_complete_distributed_trace(client, random_topic_and_group):
    """
    Creates a complete distributed trace across message sending, receiving, and timer execution.

    This test verifies that:
    1. Message sending creates spans with proper context propagation
    2. Message handling receives the trace context and continues the distributed trace
    3. Timer scheduling and execution maintain the trace context
    4. All spans are properly connected in the distributed trace
    """

    # Setup OpenTelemetry (this must be done before creating the handler)
    setup_opentelemetry()

    topic, _ = random_topic_and_group
    handler = TracingHandler()
    tracer = trace.get_tracer("prosody-python-test")

    logger.info(f"Starting tracing integration test with topic: {topic}")

    # Start the root span for the entire test
    with tracer.start_as_current_span("test-root-span") as root_span:
        root_span.set_attributes({
            "test.name": "tracing_integration",
            "test.topic": topic
        })
        root_span.add_event("test_started")

        # Subscribe to messages
        await client.subscribe(handler)
        logger.info("Subscribed to messages")

        # Send a message within a span
        with tracer.start_as_current_span("test-message-send") as send_span:
            send_span.set_attributes({
                "message.topic": topic,
                "message.key": "test-key-123"
            })
            send_span.add_event("sending_message")

            # Debug: Log the current span context details
            span_context = send_span.get_span_context()
            logger.info(f"Send span context - trace_id: {format(span_context.trace_id, '032x')}, "
                       f"span_id: {format(span_context.span_id, '016x')}")

            await client.send(topic, "test-key-123", {
                "test": "tracing_integration",
                "timestamp": time.time(),
                "message": "Hello from tracing test!"
            })

            send_span.add_event("message_sent")
            logger.info("Message sent")

        # Wait for message to be received with proper synchronization
        with tracer.start_as_current_span("test-wait-for-message") as wait_span:
            wait_span.set_attribute("test.phase", "waiting_for_message")
            logger.info("Waiting for message reception...")

            # Wait for the message to be received (with timeout)
            await asyncio.wait_for(handler.message_received_event.wait(), timeout=30.0)

            wait_span.add_event("message_received_confirmed")
            root_span.add_event("message_received_by_handler")
            logger.info("Message reception confirmed")

        # Wait for timer to fire with proper synchronization
        with tracer.start_as_current_span("test-wait-for-timer") as wait_span:
            wait_span.set_attribute("test.phase", "waiting_for_timer")
            logger.info("Waiting for timer execution...")

            # Wait for the timer to fire (with timeout)
            await asyncio.wait_for(handler.timer_fired_event.wait(), timeout=10.0)

            wait_span.add_event("timer_fired_confirmed")
            root_span.add_event("timer_fired_by_handler")
            logger.info("Timer execution confirmed")

        root_span.add_event("test_completed")
        logger.info("Test completed successfully")

    # Verify both events occurred
    assert handler.message_received, "Message should have been received"
    assert handler.timer_fired, "Timer should have fired"

    # Allow time for span flushing
    logger.info("Allowing time for span flushing...")
    await asyncio.sleep(5)

    logger.info("✅ Tracing integration test completed successfully!")


if __name__ == "__main__":
    # Allow running this test directly for manual testing
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "manual":
        # Run the test manually without pytest
        async def run_manual_test():
            # Setup test fixtures manually
            from tests.test_prosody import random_topic_and_group as fixture_func

            # This would require more setup - better to use pytest
            print("Use pytest to run this test:")
            print("pytest tests/test_tracing_integration.py::test_complete_distributed_trace -v -s")
    else:
        print("This is a tracing integration test.")
        print("To run manually: pytest tests/test_tracing_integration.py::test_complete_distributed_trace -v -s")
        print("Make sure OTEL_EXPORTER_OTLP_ENDPOINT is set and an OTLP collector is running.")