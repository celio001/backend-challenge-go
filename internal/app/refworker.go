package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/celio001/backend-challenge-go/internal/infra/observability"
	"github.com/celio001/backend-challenge-go/internal/infra/postgres"
	"github.com/celio001/backend-challenge-go/internal/infra/refworker"
	"github.com/celio001/backend-challenge-go/internal/usecase/resolvereference"
)

var RefWorkerModule = fx.Module("refworker", fx.Invoke(runRefWorker))

// runRefWorker registers after the pool and the use case, so Fx stops it first: no resolution is half done when the pool closes.
func runRefWorker(lc fx.Lifecycle, cfg Config, pool *pgxpool.Pool, resolver *resolvereference.UseCase, m *observability.Metrics, log *slog.Logger) {
	owner := replicaID()
	worker := refworker.New(postgres.NewPendingStore(pool), resolver, refworker.Config{Owner: owner, Lease: cfg.ReferenceLease, Observer: m.References()}, log)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				defer close(done)
				worker.Run(ctx)
			}()
			log.Info("reference resolver started", "owner", owner)
			return nil
		},
		OnStop: func(stopCtx context.Context) error {
			cancel()
			select {
			case <-done:
				log.Info("reference resolver stopped", "owner", owner)
				return nil
			case <-stopCtx.Done():
				return fmt.Errorf("reference resolver did not stop in time: %w", stopCtx.Err())
			}
		},
	})
}
