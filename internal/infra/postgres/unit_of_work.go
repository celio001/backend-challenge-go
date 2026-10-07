package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/celio001/backend-challenge-go/internal/usecase"
)

type UnitOfWork struct {
	pool *pgxpool.Pool
}

func NewUnitOfWork(pool *pgxpool.Pool) *UnitOfWork {
	return &UnitOfWork{pool: pool}
}

// Do surfaces deferred constraint failures (e.g. wallet/ledger consistency) from Commit, not from the statements.
func (u *UnitOfWork) Do(ctx context.Context, fn func(ctx context.Context, tx usecase.Repos) error) error {
	tx, err := u.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	if err := fn(ctx, repos{tx}); err != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			return fmt.Errorf("%w (rollback: %v)", err, rbErr)
		}
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

type repos struct{ tx pgx.Tx }

func (r repos) Wallets() usecase.WalletRepository           { return NewWalletRepository(r.tx) }
func (r repos) Transactions() usecase.TransactionRepository { return NewTransactionRepository(r.tx) }
func (r repos) Ledger() usecase.LedgerRepository            { return NewLedgerRepository(r.tx) }
func (r repos) Outbox() usecase.OutboxWriter                { return NewOutboxWriter(r.tx) }
