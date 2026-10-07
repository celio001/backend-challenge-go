package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/internal/usecase/reconcile"
)

type ReconciliationReader struct {
	pool *pgxpool.Pool
}

func NewReconciliationReader(pool *pgxpool.Pool) *ReconciliationReader {
	return &ReconciliationReader{pool: pool}
}

// Snapshot reads the balance and the ledger totals in one REPEATABLE READ, READ ONLY transaction: both come from the same
// instant even while the wallet keeps moving, so a payment in flight never looks like a divergence.
func (r *ReconciliationReader) Snapshot(ctx context.Context, id wallet.WalletID) (reconcile.Snapshot, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return reconcile.Snapshot{}, classify(fmt.Errorf("begin reconciliation snapshot: %w", err))
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var (
		currency        string
		stored, entries int64
		credits, debits string
	)
	err = tx.QueryRow(ctx, `SELECT currency, balance_minor FROM wallets WHERE id = $1`, string(id)).Scan(&currency, &stored)
	if errors.Is(err, pgx.ErrNoRows) {
		return reconcile.Snapshot{}, wallet.ErrNotFound
	}
	if err != nil {
		return reconcile.Snapshot{}, classify(fmt.Errorf("select wallet balance: %w", err))
	}
	// Summed as numeric and read as text: the total may exceed what a bigint can hold, which must surface as an error, not wrap.
	err = tx.QueryRow(ctx,
		`SELECT count(*),
		        coalesce(sum(amount_minor) FILTER (WHERE direction = 'CREDIT'), 0)::text,
		        coalesce(sum(amount_minor) FILTER (WHERE direction = 'DEBIT'), 0)::text
		   FROM wallet_ledger_entries WHERE wallet_id = $1`, string(id)).Scan(&entries, &credits, &debits)
	if err != nil {
		return reconcile.Snapshot{}, classify(fmt.Errorf("sum ledger: %w", err))
	}
	if err := tx.Commit(ctx); err != nil {
		return reconcile.Snapshot{}, classify(fmt.Errorf("end reconciliation snapshot: %w", err))
	}

	c := money.Currency(currency)
	snap := reconcile.Snapshot{Entries: entries}
	if snap.Stored, err = money.FromMinor(stored, c); err != nil {
		return reconcile.Snapshot{}, fmt.Errorf("wallet %s balance: %w", id, err)
	}
	if snap.Credits, err = parseTotal(credits, c); err != nil {
		return reconcile.Snapshot{}, fmt.Errorf("wallet %s credits: %w", id, err)
	}
	if snap.Debits, err = parseTotal(debits, c); err != nil {
		return reconcile.Snapshot{}, fmt.Errorf("wallet %s debits: %w", id, err)
	}
	return snap, nil
}

func parseTotal(s string, c money.Currency) (money.Money, error) {
	var minor int64
	if _, err := fmt.Sscanf(s, "%d", &minor); err != nil {
		return money.Money{}, fmt.Errorf("total %q does not fit in minor units: %w", s, money.ErrOverflow)
	}
	return money.FromMinor(minor, c)
}
