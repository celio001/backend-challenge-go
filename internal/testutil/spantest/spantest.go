// Package spantest records the spans a test produces, by installing an in-memory tracer provider for the length of the test.
package spantest

import (
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace/noop"
)

type Recorder struct {
	rec *tracetest.SpanRecorder
}

// Install makes the global tracer provider record every span (and honor W3C trace context) until the test ends.
// Tests using it must not run in parallel: the provider is process-wide.
func Install(t *testing.T) *Recorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() {
		otel.SetTracerProvider(noop.NewTracerProvider())
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
	})
	return &Recorder{rec: rec}
}

// Ended returns the finished spans in the order they ended.
func (r *Recorder) Ended() []sdktrace.ReadOnlySpan { return r.rec.Ended() }

// Named returns the finished spans with the given name.
func (r *Recorder) Named(name string) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range r.rec.Ended() {
		if s.Name() == name {
			out = append(out, s)
		}
	}
	return out
}

// Attrs flattens a span's attributes into a map of printable values.
func Attrs(s sdktrace.ReadOnlySpan) map[string]string {
	out := map[string]string{}
	for _, kv := range s.Attributes() {
		out[string(kv.Key)] = kv.Value.Emit()
	}
	return out
}
