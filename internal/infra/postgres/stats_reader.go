package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// StatsReader answers the aggregate questions behind the backlog metrics.
type StatsReader struct {
	pool *pgxpool.Pool
}

func NewStatsReader(pool *pgxpool.Pool) *StatsReader { return &StatsReader{pool: pool} }

func (r *StatsReader) OutboxBacklog(ctx context.Context) (int64, time.Duration, error) {
	var pending int64
	var oldestSeconds float64
	// Age is measured from when the event happened, on the database clock, so a replica with a skewed clock cannot distort it.
	err := r.pool.QueryRow(ctx,
		`SELECT count(*), coalesce(extract(epoch FROM now() - min(occurred_at)), 0)::float8
		   FROM outbox_events WHERE published_at IS NULL`).Scan(&pending, &oldestSeconds)
	if err != nil {
		return 0, 0, fmt.Errorf("read outbox backlog: %w", err)
	}
	return pending, time.Duration(oldestSeconds * float64(time.Second)), nil
}

func (r *StatsReader) PendingReferences(ctx context.Context) (int64, error) {
	var open int64
	if err := r.pool.QueryRow(ctx, `SELECT count(*) FROM wager_transactions WHERE status = 'PENDING_REFERENCE'`).Scan(&open); err != nil {
		return 0, fmt.Errorf("count pending references: %w", err)
	}
	return open, nil
}
