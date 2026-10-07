package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/celio001/backend-challenge-go/internal/usecase"
	"github.com/celio001/backend-challenge-go/pkg/faultinject"
)

// Observer is told about the database's own signals, so they can be counted without the use cases knowing.
type Observer interface {
	// TransientFailure is called each time the database asked to try again; sqlstate is the SQLSTATE, or "none" for timeouts and lost connections.
	TransientFailure(sqlstate string)
	// LockWait is the time one request spent waiting for a wallet row lock.
	LockWait(d time.Duration)
}

type Option func(*UnitOfWork)

func WithObserver(o Observer) Option { return func(u *UnitOfWork) { u.obs = o } }

type UnitOfWork struct {
	pool *pgxpool.Pool
	obs  Observer
}

func NewUnitOfWork(pool *pgxpool.Pool, opts ...Option) *UnitOfWork {
	u := &UnitOfWork{pool: pool}
	for _, o := range opts {
		o(u)
	}
	return u
}

func (u *UnitOfWork) classify(err error) error {
	err = classify(err)
	if u.obs != nil && errors.Is(err, usecase.ErrTransient) {
		u.obs.TransientFailure(sqlState(err))
	}
	return err
}

func sqlState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return "none"
}

// Do surfaces deferred constraint failures (e.g. wallet/ledger consistency) from Commit, not from the statements.
func (u *UnitOfWork) Do(ctx context.Context, fn func(ctx context.Context, tx usecase.Repos) error) error {
	tx, err := u.pool.Begin(ctx)
	if err != nil {
		return u.classify(fmt.Errorf("begin tx: %w", err))
	}
	if err := fn(ctx, repos{tx: tx, obs: u.obs}); err != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			return u.classify(fmt.Errorf("%w (rollback: %v)", err, rbErr))
		}
		return u.classify(err)
	}
	faultinject.Hit("before_commit")
	if err := tx.Commit(ctx); err != nil {
		return u.classify(fmt.Errorf("commit tx: %w", err))
	}
	return nil
}

type repos struct {
	tx  pgx.Tx
	obs Observer
}

func (r repos) Wallets() usecase.WalletRepository {
	w := NewWalletRepository(r.tx)
	if r.obs != nil {
		w.lockWait = r.obs.LockWait
	}
	return w
}
func (r repos) Transactions() usecase.TransactionRepository { return NewTransactionRepository(r.tx) }
func (r repos) Ledger() usecase.LedgerRepository            { return NewLedgerRepository(r.tx) }
func (r repos) Outbox() usecase.OutboxWriter                { return NewOutboxWriter(r.tx) }
func (r repos) Inbox() usecase.InboxRepository              { return NewInboxRepository(r.tx) }
