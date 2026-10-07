package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/internal/usecase/queries"
)

type LedgerReader struct {
	db querier
}

func NewLedgerReader(db querier) *LedgerReader {
	return &LedgerReader{db: db}
}

func (r *LedgerReader) After(ctx context.Context, id wallet.WalletID, afterVersion int64, limit int) ([]queries.LedgerItem, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id, transaction_id, direction, amount_minor, currency, balance_before_minor, balance_after_minor, wallet_version, created_at
		   FROM wallet_ledger_entries
		  WHERE wallet_id = $1 AND wallet_version > $2
		  ORDER BY wallet_version
		  LIMIT $3`,
		string(id), afterVersion, limit)
	if err != nil {
		return nil, fmt.Errorf("select ledger: %w", err)
	}
	defer rows.Close()

	var items []queries.LedgerItem
	for rows.Next() {
		var (
			item                  queries.LedgerItem
			direction, currency   string
			amount, before, after int64
			createdAt             time.Time
		)
		if err := rows.Scan(&item.ID, &item.TransactionID, &direction, &amount, &currency, &before, &after, &item.WalletVersion, &createdAt); err != nil {
			return nil, fmt.Errorf("scan ledger entry: %w", err)
		}
		c := money.Currency(currency)
		if item.Money, err = money.FromMinor(amount, c); err != nil {
			return nil, fmt.Errorf("ledger entry %s amount: %w", item.ID, err)
		}
		if item.BalanceBefore, err = money.FromMinor(before, c); err != nil {
			return nil, fmt.Errorf("ledger entry %s balance before: %w", item.ID, err)
		}
		if item.BalanceAfter, err = money.FromMinor(after, c); err != nil {
			return nil, fmt.Errorf("ledger entry %s balance after: %w", item.ID, err)
		}
		item.Direction = wallet.Direction(direction)
		item.CreatedAt = createdAt.UTC()
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate ledger: %w", err)
	}
	return items, nil
}
