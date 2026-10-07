package postgres

import (
	"context"

	"github.com/celio001/backend-challenge-go/internal/domain/wager"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
)

type TransactionReader struct {
	db querier
}

func NewTransactionReader(db querier) *TransactionReader {
	return &TransactionReader{db: db}
}

func (r *TransactionReader) ByID(ctx context.Context, id wallet.TxID) (*wager.Transaction, error) {
	return selectTransaction(ctx, r.db, `SELECT `+selectColumns+` FROM wager_transactions WHERE id = $1`, string(id))
}

func (r *TransactionReader) ByExternalID(ctx context.Context, providerID, externalID string) (*wager.Transaction, error) {
	return selectTransaction(ctx, r.db,
		`SELECT `+selectColumns+` FROM wager_transactions WHERE origin = 'EXTERNAL' AND provider_id = $1 AND external_transaction_id = $2`,
		providerID, externalID)
}
