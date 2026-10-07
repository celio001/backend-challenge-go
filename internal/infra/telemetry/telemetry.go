// Package telemetry sets up OpenTelemetry tracing from the standard OTEL_* variables; it is off unless an OTLP endpoint is given.
package telemetry

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

const DefaultServiceName = "wallet-service"

// Enabled is true when an OTLP endpoint is set, unless OTEL_SDK_DISABLED=true or OTEL_TRACES_EXPORTER=none say otherwise.
func Enabled(getenv func(string) string) bool {
	if strings.EqualFold(getenv("OTEL_SDK_DISABLED"), "true") || strings.EqualFold(getenv("OTEL_TRACES_EXPORTER"), "none") {
		return false
	}
	return getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != ""
}

type Shutdown func(ctx context.Context) error

// Setup always installs the W3C propagator, so an incoming traceparent is honored even with export off, and when enabled an OTLP/HTTP tracer provider.
func Setup(ctx context.Context, enabled bool) (Shutdown, error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
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
