package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"go.uber.org/fx"

	"github.com/celio001/backend-challenge-go/internal/adapter/httpapi"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/celio001/backend-challenge-go/internal/auth"
	"github.com/celio001/backend-challenge-go/internal/usecase"
)

var HTTPModule = fx.Module("http",
	fx.Provide(newHealth, newHandler),
	fx.Invoke(runServer),
)

func newHealth(pool *pgxpool.Pool, events *eventsQueue) *httpapi.Health {
	return httpapi.NewHealth(
		httpapi.Check{Name: "postgres", Fn: pool.Ping},
		httpapi.Check{Name: "sqs", Fn: events.Ping},
	)
}

type handlerParams struct {
	fx.In
	OpenWallet   httpapi.OpenWallet
	ProcessWager httpapi.ProcessWager
	Queries      httpapi.WalletQueries
	TxQueries    httpapi.TransactionQueries
	Verifier     auth.Verifier
	IDs          usecase.IDGenerator
	Health       *httpapi.Health
	Log          *slog.Logger
}

func newHandler(p handlerParams) http.Handler {
	return httpapi.New(httpapi.Deps{OpenWallet: p.OpenWallet, ProcessWager: p.ProcessWager, Queries: p.Queries, TxQueries: p.TxQueries, Verifier: p.Verifier, IDs: p.IDs, Health: p.Health, Log: p.Log})
}

// Listen is synchronous so a busy port fails the boot instead of a background goroutine.
func runServer(lc fx.Lifecycle, cfg Config, h http.Handler, health *httpapi.Health, log *slog.Logger) {
	srv := &http.Server{
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	done := make(chan struct{})
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			ln, err := net.Listen("tcp", cfg.HTTPAddr)
			if err != nil {
				return fmt.Errorf("listen %s: %w", cfg.HTTPAddr, err)
			}
			go func() {
				defer close(done)
				if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
					log.Error("http server stopped", "error", err)
				}
			}()
			log.Info("http server listening", "addr", ln.Addr().String())
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if err := srv.Shutdown(ctx); err != nil {
				return fmt.Errorf("shutdown http server: %w", err)
			}
			select {
			case <-done:
			case <-ctx.Done():
				return ctx.Err()
			}
			log.Info("http server stopped")
			return nil
		},
	})
	// Fx stops hooks in reverse order, so this runs before the server shuts down.
	lc.Append(fx.Hook{OnStop: func(context.Context) error {
		health.Drain()
		return nil
	}})
}
