package app

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.uber.org/fx"

	"github.com/celio001/backend-challenge-go/internal/infra/telemetry"
)

// TelemetryModule is listed first so its stop hook runs last: every component that creates spans has stopped, and the
// spans still buffered are flushed to the collector before the process ends.
var TelemetryModule = fx.Module("telemetry", fx.Invoke(startTelemetry))

func startTelemetry(lc fx.Lifecycle, cfg Config, log *slog.Logger) {
	var shutdown telemetry.Shutdown
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			var err error
			if shutdown, err = telemetry.Setup(ctx, cfg.TracingEnabled); err != nil {
				return fmt.Errorf("start tracing: %w", err)
			}
			if cfg.TracingEnabled {
				otel.SetErrorHandler(throttledErrors(log, 30*time.Second))
				log.Info("tracing enabled")
			}
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if shutdown == nil {
				return nil
			}
			if err := shutdown(ctx); err != nil {
				return fmt.Errorf("flush traces: %w", err)
			}
			return nil
		},
	})
}

// throttledErrors reports exporter failures through the service's logger, at most once per interval: a collector that is
// down would otherwise produce one line per batch.
func throttledErrors(log *slog.Logger, every time.Duration) otel.ErrorHandlerFunc {
	var mu sync.Mutex
	var last time.Time
	return func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if time.Since(last) < every {
			return
		}
		last = time.Now()
		log.Warn("opentelemetry error", "error", err)
	}
}
