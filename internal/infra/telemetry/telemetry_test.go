package telemetry

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// restoreGlobals resets the process-wide providers to inert ones, so a test that installs its own cannot leak into the next.
// (The original defaults cannot be put back: OpenTelemetry refuses to set its own delegating default as a delegate.)
func restoreGlobals(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		otel.SetTracerProvider(noop.NewTracerProvider())
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
	})
}

func TestEnabled(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want bool
	}{
		{name: "nothing set is off", env: map[string]string{}},
		{name: "an endpoint turns it on", env: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://jaeger:4318"}, want: true},
		{name: "a traces-only endpoint turns it on", env: map[string]string{"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://jaeger:4318/v1/traces"}, want: true},
		{name: "the SDK can be disabled explicitly", env: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://jaeger:4318", "OTEL_SDK_DISABLED": "true"}},
		{name: "the exporter can be set to none", env: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://jaeger:4318", "OTEL_TRACES_EXPORTER": "none"}},
		{name: "only the other signals' endpoints do not enable traces", env: map[string]string{"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "http://x"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Enabled(func(k string) string { return tt.env[k] }); got != tt.want {
				t.Fatalf("Enabled = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDisabledSetupKeepsTheProviderButStillPropagatesTraceContext(t *testing.T) {
	restoreGlobals(t)
	otel.SetTracerProvider(noop.NewTracerProvider())
	before := otel.GetTracerProvider()

	shutdown, err := Setup(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if otel.GetTracerProvider() != before {
		t.Fatal("a disabled setup replaced the tracer provider")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	// An incoming traceparent is still understood, so a caller's trace id survives through this service into its logs.
	const traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	ctx := otel.GetTextMapPropagator().Extract(context.Background(), propagation.MapCarrier{"traceparent": traceparent})
	if got := trace.SpanContextFromContext(ctx).TraceID().String(); got != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("extracted trace id = %q", got)
	}
}

func TestInstallBuildsTheResourceFromTheDefaultAndTheEnvironment(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want string
	}{
		{name: "the default service name", want: DefaultServiceName},
		{name: "OTEL_SERVICE_NAME wins", env: map[string]string{"OTEL_SERVICE_NAME": "wallet-eu"}, want: "wallet-eu"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			restoreGlobals(t)
			t.Setenv("OTEL_SERVICE_NAME", "")
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			exp := tracetest.NewInMemoryExporter()
			shutdown, err := install(context.Background(), exp)
			if err != nil {
				t.Fatal(err)
			}
			_, span := otel.Tracer("test").Start(context.Background(), "work")
			span.End()
			// Shutting an in-memory exporter down clears it, so what it holds is read after a flush and before the shutdown.
			if err := otel.GetTracerProvider().(*sdktrace.TracerProvider).ForceFlush(context.Background()); err != nil {
				t.Fatal(err)
			}

			spans := exp.GetSpans()
			if len(spans) != 1 {
				t.Fatalf("exported %d spans", len(spans))
			}
			var name string
			for _, kv := range spans[0].Resource.Attributes() {
				if string(kv.Key) == "service.name" {
					name = kv.Value.AsString()
				}
			}
			if err := shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
			if name != tt.want {
				t.Fatalf("service.name = %q, want %q", name, tt.want)
			}
		})
	}
}

// The exporter is configured only through the standard environment: pointing the endpoint at a collector is enough.
func TestSetupExportsOverOTLPHTTPToTheConfiguredEndpoint(t *testing.T) {
	restoreGlobals(t)
	var mu sync.Mutex
	var path, contentType string
	var body []byte
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		mu.Lock()
		defer mu.Unlock()
		path, contentType, body = r.URL.Path, r.Header.Get("Content-Type"), append(body, raw...)
		w.WriteHeader(http.StatusOK)
	}))
	defer collector.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", collector.URL)
	t.Setenv("OTEL_SERVICE_NAME", "")

	shutdown, err := Setup(context.Background(), Enabled(func(k string) string { return map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": collector.URL}[k] }))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); !ok {
		t.Fatalf("tracer provider = %T, want the SDK's", otel.GetTracerProvider())
	}
	_, span := otel.Tracer("test").Start(context.Background(), "wager.execute")
	span.End()
	if err := shutdown(context.Background()); err != nil { // flushes the batch
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if path != "/v1/traces" || contentType != "application/x-protobuf" {
		t.Fatalf("request = %s (%s)", path, contentType)
	}
	for _, want := range []string{"wager.execute", DefaultServiceName} {
		if !bytes.Contains(body, []byte(want)) {
			t.Fatalf("the exported payload lacks %q", want)
		}
	}
}
