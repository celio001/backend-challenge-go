package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/celio001/backend-challenge-go/internal/domain/wager"
)

type TransactionRepository struct {
	db execer
}

func NewTransactionRepository(db execer) *TransactionRepository {
	return &TransactionRepository{db: db}
}

func (r *TransactionRepository) Insert(ctx context.Context, t *wager.Transaction) error {
	var result any
	if t.ResultBalance().IsValid() {
		result = t.ResultBalance().Minor()
	}
	_, err := r.db.Exec(ctx,
		`INSERT INTO wager_transactions (
		   id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
		   provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
		   reference_external_transaction_id, reference_transaction_id, failure_code, result_balance_minor,
		   correlation_id, expires_at, created_at, updated_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)`,
		string(t.ID()), string(t.Origin()), string(t.Kind()), string(t.Status()), string(t.WalletID()), string(t.PlayerID()),
		t.Amount().Minor(), string(t.Amount().Currency()),
		nullString(t.ProviderID()), nullString(t.ExternalTransactionID()), nullString(t.IdempotencyKey()), nullBytes(t.PayloadHash()),
		nullString(t.RoundID()), nullString(t.GameID()),
		nullString(t.ReferenceExternalID()), nullString(string(t.ReferenceTxID())), nullString(string(t.FailureCode())), result,
		nullString(t.CorrelationID()), nullTime(t.ExpiresAt()), t.CreatedAt(), t.UpdatedAt())
	if err != nil {
		return fmt.Errorf("insert wager transaction: %w", err)
	}
	return nil
}

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullBytes(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return b
}

func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}
