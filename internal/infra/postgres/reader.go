package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// querier is satisfied by *pgxpool.Pool and pgx.Tx.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// dbtx is what the write-side repositories need: statements and reads on the same pool or transaction.
type dbtx interface {
	execer
	querier
}
