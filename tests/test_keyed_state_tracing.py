"""Collector-backed keyed-state trace-graph audit (Appendix 1 item 12).

Follows ``../prosody/docs/keyed-state/clients/02-lgtm-trace-audit.md``: a fixed op
set runs inside one handler span, spans are exported through the live LGTM OTLP
collector, and the complete trace graph is queried back from Tempo and audited
for exact per-op semantic-span counts, parentage to the handler span, and the
ABSENCE of any binding/chunk wrapper span (state.rs activates the carrier with
``with_context``/``attach`` and opens NO span of its own — core owns the one
semantic span per op).

Marked ``tracing``/``integration`` so it is deselected by default; run explicitly:

    pytest tests/test_keyed_state_tracing.py -m tracing -v -s
"""

import asyncio
import os
import time
import uuid

import pytest
import requests
import tsasync
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from prosody import ProsodyClient, EventHandler, value, map
from prosody.prosody import AdminClient


DEFAULT_TIMEOUT = 30.0
BOOTSTRAP = "localhost:9094"
CASSANDRA_NODES = "localhost:9042"
CASSANDRA_KEYSPACE = "prosody_test"
OTLP_ENDPOINT = "http://127.0.0.1:4318"
TEMPO = "http://127.0.0.1:3200"

STATE_COLLECTIONS = [value("cart"), map("totals", keyset_limit=256)]

HANDLER_SPAN = "test.state_handler"
# Expected core semantic spans for the fixed op set below (one message).
EXPECTED_COUNTS = {"value.set": 1, "value.get": 1, "map.set": 2, "map.stream": 1}
# pyclass names that must NOT appear as spans (no binding/chunk wrapper span).
FORBIDDEN_SPAN_NAMES = {"NativeValueState", "NativeMapState", "NativeStateScan"}


async def _wait(awaitable):
    return await asyncio.wait_for(awaitable, timeout=DEFAULT_TIMEOUT)


class TracingStateHandler(EventHandler):
    __test__ = False

    def __init__(self):
        self.tracer = trace.get_tracer("prosody-py-state-trace-audit")
        self.done = tsasync.Event()
        self.error = None

    async def on_message(self, context, message) -> None:
        with self.tracer.start_as_current_span(HANDLER_SPAN):
            try:
                c = context.state(STATE_COLLECTIONS[0])
                await _wait(c.set({"v": 1}))
                await _wait(c.get())
                m = context.state(STATE_COLLECTIONS[1])
                await _wait(m.set("a", 1))
                await _wait(m.set("b", 2))
                collected = []
                async for entry in m.items():
                    collected.append(entry)
            except Exception as e:  # pragma: no cover
                self.error = str(e)
            finally:
                self.done.set()

    async def on_timer(self, context, timer) -> None:
        pass


def _search_traces(service):
    r = requests.get(
        f"{TEMPO}/api/search",
        params={"q": f'{{ resource.service.name = "{service}" }}', "limit": 100},
        timeout=10,
    )
    r.raise_for_status()
    return r.json().get("traces", [])


def _fetch_trace(trace_id):
    r = requests.get(f"{TEMPO}/api/traces/{trace_id}", timeout=10)
    r.raise_for_status()
    return r.json()


def _flatten_spans(trace_json):
    """Flatten every resource/scope span into (span, scope_name) records indexed
    by spanId. Tempo returns the v1 trace shape under ``batches``."""
    spans = {}
    scopes = {}
    for batch in trace_json.get("batches", []):
        for scope_spans in batch.get("scopeSpans", []):
            scope_name = scope_spans.get("scope", {}).get("name", "")
            for span in scope_spans.get("spans", []):
                sid = span.get("spanId")
                spans[sid] = span
                scopes[sid] = scope_name
    return spans, scopes


def _attr(span, key):
    for a in span.get("attributes", []):
        if a.get("key") == key:
            v = a.get("value", {})
            return v.get("stringValue") or v.get("intValue") or v.get("boolValue")
    return None


@pytest.fixture
async def random_topic_and_group():
    topic = f"trace-topic-{uuid.uuid4().hex}"
    group = f"trace-group-{uuid.uuid4().hex}"
    admin = AdminClient(bootstrap_servers=BOOTSTRAP)
    await _wait(admin.create_topic(topic, partition_count=1, replication_factor=1))
    await asyncio.sleep(1)
    yield topic, group
    with_suppress = getattr(asyncio, "TimeoutError", Exception)
    try:
        await _wait(admin.delete_topic(topic))
    except with_suppress:
        pass


@pytest.mark.tracing
@pytest.mark.integration
@pytest.mark.asyncio
async def test_state_op_trace_graph(random_topic_and_group):
    topic, group = random_topic_and_group

    # The native prosody OTLP exporter (which emits the core semantic spans)
    # initializes from OTEL_EXPORTER_OTLP_ENDPOINT at import; without it there are
    # no core spans to audit. Share ONE unique service between the native
    # exporter (OTEL_SERVICE_NAME env) and the Python handler span so a single
    # Tempo query isolates this run.
    if not os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
        pytest.skip(
            "set OTEL_EXPORTER_OTLP_ENDPOINT (and a unique OTEL_SERVICE_NAME) so "
            "the native exporter emits core spans; see 02-lgtm-trace-audit.md"
        )
    service = os.environ.get(
        "OTEL_SERVICE_NAME", f"prosody-py-state-trace-audit-{uuid.uuid4().hex}"
    )

    provider = TracerProvider(resource=Resource.create({"service.name": service}))
    provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{OTLP_ENDPOINT}/v1/traces"))
    )
    trace.set_tracer_provider(provider)

    client = ProsodyClient(
        bootstrap_servers=BOOTSTRAP,
        source_system="test-state-trace",
        group_id=group,
        subscribed_topics=topic,
        probe_port=None,
        cassandra_nodes=CASSANDRA_NODES,
        cassandra_keyspace=CASSANDRA_KEYSPACE,
        state_collections=STATE_COLLECTIONS,
    )
    handler = TracingStateHandler()
    await _wait(client.subscribe(handler))
    await _wait(client.send(topic, uuid.uuid4().hex, {"go": True}))
    await _wait(handler.done.wait())
    assert handler.error is None, handler.error
    with_suppress = getattr(asyncio, "TimeoutError", Exception)
    try:
        await _wait(client.unsubscribe())
    except with_suppress:
        pass

    # Flush/shutdown the exporter; a shutdown error fails the audit.
    assert provider.force_flush(timeout_millis=15000)
    provider.shutdown()

    # Poll Tempo until the CORE op spans are ingested (the native exporter and
    # the Python SDK batch/flush on separate timers, so a trace can appear with
    # only the handler span before the core spans land). Bounded at 60s.
    inspected_ids = []
    spans = {}
    scopes = {}
    deadline = time.time() + 60
    while time.time() < deadline:
        spans = {}
        scopes = {}
        inspected_ids = []
        for t in _search_traces(service):
            tid = t.get("traceID")
            inspected_ids.append(tid)
            s, sc = _flatten_spans(_fetch_trace(tid))
            spans.update(s)
            scopes.update(sc)
        names = {sp.get("name") for sp in spans.values()}
        if HANDLER_SPAN in names and EXPECTED_COUNTS.keys() <= names:
            break
        time.sleep(2)
    assert spans, f"no traces ingested for service {service}"

    # Locate the handler span.
    handler_ids = [sid for sid, sp in spans.items() if sp.get("name") == HANDLER_SPAN]
    assert len(handler_ids) == 1, f"expected one {HANDLER_SPAN}, got {len(handler_ids)}"
    handler_id = handler_ids[0]

    # Every non-root parentSpanId resolves within the flattened set (allowing the
    # producer/receive ancestry that lives above the handler span); the parent
    # chain from each op span reaches the handler span.
    def reaches_handler(sid):
        seen = set()
        cur = sid
        while cur and cur not in seen:
            seen.add(cur)
            if cur == handler_id:
                return True
            cur = spans.get(cur, {}).get("parentSpanId")
        return False

    # Exact per-op semantic-span counts among descendants of the handler span.
    op_spans = [
        sp
        for sid, sp in spans.items()
        if sp.get("name") in EXPECTED_COUNTS and reaches_handler(sp.get("parentSpanId"))
    ]
    counts = {}
    for sp in op_spans:
        counts[sp["name"]] = counts.get(sp["name"], 0) + 1
    assert counts == EXPECTED_COUNTS, f"op-span counts {counts} != {EXPECTED_COUNTS}"

    # Instrumentation-scope check (02-lgtm-trace-audit.md step 4): the handler
    # span is produced by the Python SDK tracer; every core op span must
    # originate from a different (native/core) scope. A same-named Python-side
    # impostor span would share the Python scope and be caught here.
    assert scopes.get(handler_id) == "prosody-py-state-trace-audit"
    op_scopes = {scopes.get(sp.get("spanId")) for sp in op_spans}
    assert len(op_scopes) == 1, f"core op spans span multiple scopes: {op_scopes}"
    (core_scope,) = op_scopes
    assert core_scope and core_scope != scopes[handler_id], (
        f"op spans share the Python handler scope: {core_scope!r}"
    )

    # Each op span is parented directly by the handler span (no same-named
    # binding wrapper span interposed) and carries its collection attribute.
    for sp in op_spans:
        assert sp.get("parentSpanId") == handler_id, (
            f"{sp['name']} parent {sp.get('parentSpanId')} != handler {handler_id}"
        )
        collection = _attr(sp, "collection")
        expected = "cart" if sp["name"].startswith("value") else "totals"
        assert collection == expected, f"{sp['name']} collection={collection}"

    # No binding/chunk wrapper span exists anywhere in the inspected traces.
    all_names = {sp.get("name") for sp in spans.values()}
    assert not (all_names & FORBIDDEN_SPAN_NAMES), (
        f"unexpected binding span(s): {all_names & FORBIDDEN_SPAN_NAMES}"
    )
    # The duplicate-wrapper bug: an op span parented directly by an identically
    # named span for one call. Assert no such pair exists.
    for sp in op_spans:
        parent = spans.get(sp.get("parentSpanId"), {})
        assert parent.get("name") != sp.get("name"), (
            f"duplicate-wrapper span for {sp['name']}"
        )

    # Retain evidence for the audit record.
    print("\n=== LGTM trace-audit evidence ===")
    print(f"service: {service}")
    print(f"inspected trace ids: {inspected_ids}")
    print(f"op-span counts: {counts}")
    print(f"handler span id: {handler_id}")
    print(f"all span names: {sorted(all_names)}")
    print(f"no binding/chunk spans: {not (all_names & FORBIDDEN_SPAN_NAMES)}")
