// Package telemetry sets up OpenTelemetry tracing. It is configured by the standard OTEL_* environment variables and is off
// unless an OTLP endpoint is given, so a plain run pays nothing for it.
package telemetry

import (
	"context"
	"fmt"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// DefaultServiceName is used unless OTEL_SERVICE_NAME says otherwise.
const DefaultServiceName = "wallet-service"

// Enabled reports whether the environment asks for traces to be exported: any OTLP endpoint turns tracing on, and
// OTEL_SDK_DISABLED=true or OTEL_TRACES_EXPORTER=none turns it off again.
func Enabled(getenv func(string) string) bool {
	if strings.EqualFold(getenv("OTEL_SDK_DISABLED"), "true") || strings.EqualFold(getenv("OTEL_TRACES_EXPORTER"), "none") {
		return false
	}
	return getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != ""
}

// Shutdown flushes buffered spans; call it after the components that create spans have stopped.
type Shutdown func(ctx context.Context) error

// Setup installs the global propagator (W3C trace context and baggage, so an incoming traceparent is honored even
// when export is off) and, when enabled, a tracer provider that exports over OTLP/HTTP. Endpoint, headers, protocol
// details, sampler and resource attributes come from the standard OTEL_* variables.
func Setup(ctx context.Context, enabled bool) (Shutdown, error) {
	otel.SetTextMapPropagator(Propagator())
	if !enabled {
		return func(context.Context) error { return nil }, nil
	}
	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("create otlp trace exporter: %w", err)
	}
	return install(ctx, exporter)
}

// install is Setup's second half, separate so tests can use an exporter that keeps spans in memory.
func install(ctx context.Context, exporter sdktrace.SpanExporter) (Shutdown, error) {
	// The name given here is the default; OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES, read afterwards, override it.
	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(DefaultServiceName)),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithFromEnv(),
	)
	if err != nil {
		return nil, fmt.Errorf("build otel resource: %w", err)
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

func Propagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
}

// FromEnv is Enabled over the process environment.
func FromEnv() bool { return Enabled(os.Getenv) }
