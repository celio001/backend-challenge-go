package app

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/celio001/backend-challenge-go/internal/infra/postgres"
	"github.com/celio001/backend-challenge-go/internal/usecase"
	"github.com/celio001/backend-challenge-go/internal/usecase/queries"
)

var PostgresModule = fx.Module("postgres",
	fx.Provide(
		newPool,
		fx.Annotate(postgres.NewUnitOfWork, fx.As(new(usecase.UnitOfWork))),
		fx.Annotate(func(p *pgxpool.Pool) *postgres.WalletReader { return postgres.NewWalletReader(p) }, fx.As(new(queries.WalletReader))),
		fx.Annotate(func(p *pgxpool.Pool) *postgres.LedgerReader { return postgres.NewLedgerReader(p) }, fx.As(new(queries.LedgerReader))),
		fx.Annotate(func(p *pgxpool.Pool) *postgres.TransactionReader { return postgres.NewTransactionReader(p) }, fx.As(new(queries.TransactionReader))),
	),
)

// The pool is built here but only checked on start, and closed on stop after everything that uses it has stopped.
func newPool(lc fx.Lifecycle, cfg Config, log *slog.Logger) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	pc.ConnConfig.RuntimeParams["lock_timeout"] = "2000"
	pc.ConnConfig.RuntimeParams["statement_timeout"] = "5000"
	pool, err := pgxpool.NewWithConfig(context.Background(), pc)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return retryUntil(ctx, log, "postgres", pool.Ping)
		},
		OnStop: func(context.Context) error {
			pool.Close()
			return nil
		},
	})
	return pool, nil
}

// retryUntil keeps calling fn until it succeeds or ctx ends, so dependencies that boot slower than this process are tolerated.
// Fx reports only the context error when the start timeout expires, so each failed attempt is logged with its cause.
func retryUntil(ctx context.Context, log *slog.Logger, dependency string, fn func(context.Context) error) error {
	delay := 250 * time.Millisecond
	for {
		err := fn(ctx)
		if err == nil {
			return nil
		}
		log.Warn("dependency not ready", "dependency", dependency, "retryIn", delay.String(), "error", err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w (last error: %v)", dependency, ctx.Err(), err)
		case <-time.After(delay):
		}
		delay = min(delay*2, 2*time.Second)
	}
}
