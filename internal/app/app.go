// Package app is the only place that knows Uber Fx: it wires configuration, connections, use cases and servers.
package app

import (
	"log/slog"
	"os"
	"time"

	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
)

const (
	StartTimeout = 60 * time.Second
	StopTimeout  = 30 * time.Second
)

var Module = fx.Options(
	fx.WithLogger(newFxLogger),
	fx.Provide(
		configFromEnv,
		func() *slog.Logger { return slog.New(slog.NewJSONHandler(os.Stdout, nil)) },
	),
	SystemModule,
	ObservabilityModule,
	PostgresModule,
	OIDCModule,
	SQSModule,
	UseCaseModule,
	HTTPModule,
	OutboxModule,
	ConsumerModule,
	RefWorkerModule,
)

// Fx wiring events are debug noise in production; its failures still surface at error level.
func newFxLogger(l *slog.Logger) fxevent.Logger {
	fl := &fxevent.SlogLogger{Logger: l}
	fl.UseLogLevel(slog.LevelDebug)
	fl.UseErrorLevel(slog.LevelError)
	return fl
}

func New(opts ...fx.Option) *fx.App {
	return fx.New(append([]fx.Option{Module, fx.StartTimeout(StartTimeout), fx.StopTimeout(StopTimeout)}, opts...)...)
}
