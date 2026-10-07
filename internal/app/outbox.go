package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/celio001/backend-challenge-go/internal/infra/observability"
	"github.com/celio001/backend-challenge-go/internal/infra/outbox"
	"github.com/celio001/backend-challenge-go/internal/infra/postgres"
	"github.com/celio001/backend-challenge-go/internal/infra/system"
)

var OutboxModule = fx.Module("outbox", fx.Invoke(runOutbox))

// runOutbox registers after the pool and the queue, so Fx stops it first: no event is half relayed when they close.
func runOutbox(lc fx.Lifecycle, cfg Config, pool *pgxpool.Pool, pub outbox.Publisher, m *observability.Metrics, log *slog.Logger) {
	owner := replicaID()
	worker := outbox.New(postgres.NewOutboxStore(pool), pub, outbox.Config{Owner: owner, Lease: cfg.OutboxLease, Observer: m}, log)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				defer close(done)
				worker.Run(ctx)
			}()
			log.Info("outbox relay started", "owner", owner)
			return nil
		},
		OnStop: func(stopCtx context.Context) error {
			cancel()
			select {
			case <-done:
				log.Info("outbox relay stopped", "owner", owner)
				return nil
			case <-stopCtx.Done():
				return fmt.Errorf("outbox relay did not stop in time: %w", stopCtx.Err())
			}
		},
	})
}

// replicaID names the lease holder: the host tells operators which replica, the suffix keeps restarts of the same host apart.
func replicaID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "replica"
	}
	return host + "-" + system.IDs{}.NewID()[:8]
}
