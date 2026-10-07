package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgconn"
)

// execer is satisfied by both *pgxpool.Pool and pgx.Tx.
type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}
