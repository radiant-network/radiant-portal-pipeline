from opentelemetry import trace

from radiant.tasks.tracing.trace import get_tracer


def test_tracer_enabled(monkeypatch):
    monkeypatch.setenv("RADIANT_OTLP_COLLECTOR_ENABLED", "True")
    monkeypatch.setenv("RADIANT_OTLP_COLLECTOR_HTTP_ENDPOINT", "http://custom_endpoint:12345")
    tracer = get_tracer(__name__)

    assert tracer.resource.attributes["service.name"] == "Radiant"
    assert (
        tracer.span_processor._span_processors[0].span_exporter._endpoint == "http://custom_endpoint:12345/v1/traces"
    )


def test_tracer_disabled_by_default(monkeypatch):
    tracer = get_tracer(__name__)

    assert isinstance(tracer, type(trace.NoOpTracer()))
