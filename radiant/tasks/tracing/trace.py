import os

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

_tracer_initialized = False


def initialize_tracer():
    global _tracer_initialized
    if not _tracer_initialized:
        resource = Resource(attributes={SERVICE_NAME: "Radiant"})
        trace.set_tracer_provider(TracerProvider(resource=resource))
        _endpoint = os.getenv("OTLP_COLLECTOR_HTTP_ENDPOINT", "http://localhost:8889")
        span_processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{_endpoint}/v1/traces"))
        trace.get_tracer_provider().add_span_processor(span_processor)
        _tracer_initialized = True


def get_tracer(name: str):
    if os.getenv("OTLP_COLLECTOR_ENABLED", "false") == "true":
        initialize_tracer()
        return trace.get_tracer(name)
    else:
        # If OpenTelemetry is disabled, return a no-op tracer
        return trace.NoOpTracer()
