package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wager"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
)

const txColumns = `id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
	provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
	reference_external_transaction_id, reference_transaction_id, failure_code, result_balance_minor,
	correlation_id, expires_at, created_at, updated_at`

type TransactionRepository struct {
	db dbtx
}

func NewTransactionRepository(db dbtx) *TransactionRepository {
	return &TransactionRepository{db: db}
}

func (r *TransactionRepository) Insert(ctx context.Context, t *wager.Transaction) error {
	if _, err := r.insert(ctx, t, ""); err != nil {
		return err
	}
	return nil
}

func (r *TransactionRepository) InsertIfAbsent(ctx context.Context, t *wager.Transaction) (bool, error) {
	n, err := r.insert(ctx, t, " ON CONFLICT DO NOTHING")
	return n == 1, err
}

func (r *TransactionRepository) insert(ctx context.Context, t *wager.Transaction, suffix string) (int64, error) {
	var result any
	if t.ResultBalance().IsValid() {
		result = t.ResultBalance().Minor()
	}
	tag, err := r.db.Exec(ctx,
		`INSERT INTO wager_transactions (`+txColumns+`)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)`+suffix,
		string(t.ID()), string(t.Origin()), string(t.Kind()), string(t.Status()), string(t.WalletID()), string(t.PlayerID()),
		t.Amount().Minor(), string(t.Amount().Currency()),
		nullString(t.ProviderID()), nullString(t.ExternalTransactionID()), nullString(t.IdempotencyKey()), nullBytes(t.PayloadHash()),
		nullString(t.RoundID()), nullString(t.GameID()),
		nullString(t.ReferenceExternalID()), nullString(string(t.ReferenceTxID())), nullString(string(t.FailureCode())), result,
		nullString(t.CorrelationID()), nullTime(t.ExpiresAt()), t.CreatedAt(), t.UpdatedAt())
	if err != nil {
		return 0, fmt.Errorf("insert wager transaction: %w", err)
	}
	return tag.RowsAffected(), nil
}

func (r *TransactionRepository) FindDuplicate(ctx context.Context, providerID, idempotencyKey, externalID string) (*wager.Transaction, error) {
	return selectTransaction(ctx, r.db,
		`SELECT `+txColumns+` FROM wager_transactions
		  WHERE origin = 'EXTERNAL' AND provider_id = $1 AND (idempotency_key = $2 OR external_transaction_id = $3)
		  ORDER BY (idempotency_key = $2) DESC LIMIT 1`,
		providerID, idempotencyKey, externalID)
}

func (r *TransactionRepository) FindByExternalID(ctx context.Context, providerID, externalID string) (*wager.Transaction, error) {
	return selectTransaction(ctx, r.db,
		`SELECT `+txColumns+` FROM wager_transactions
		  WHERE origin = 'EXTERNAL' AND provider_id = $1 AND external_transaction_id = $2`,
		providerID, externalID)
}

func (r *TransactionRepository) ProcessedReversalOf(ctx context.Context, refID wallet.TxID) (*wager.Transaction, error) {
	return selectTransaction(ctx, r.db,
		`SELECT `+txColumns+` FROM wager_transactions
		  WHERE reference_transaction_id = $1 AND status = 'PROCESSED' AND kind IN ('REFUND','ROLLBACK')`,
		string(refID))
}

func (r *TransactionRepository) Update(ctx context.Context, t *wager.Transaction, nextAttemptAt time.Time) error {
	var result any
	if t.ResultBalance().IsValid() {
		result = t.ResultBalance().Minor()
	}
	tag, err := r.db.Exec(ctx,
		`UPDATE wager_transactions
		    SET status = $2, failure_code = $3, result_balance_minor = $4, reference_transaction_id = $5,
		        expires_at = $6, next_attempt_at = $7, updated_at = $8
		  WHERE id = $1`,
		string(t.ID()), string(t.Status()), nullString(string(t.FailureCode())), result, nullString(string(t.ReferenceTxID())),
		nullTime(t.ExpiresAt()), nullTime(nextAttemptAt), t.UpdatedAt())
	if err != nil {
		return fmt.Errorf("update wager transaction: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("update wager transaction %s: %w", t.ID(), wager.ErrNotFound)
	}
	return nil
}

func selectTransaction(ctx context.Context, db querier, sql string, args ...any) (*wager.Transaction, error) {
	var (
		s                                                      wager.Snapshot
		id, origin, kind, status, walletID, playerID, currency string
		amount                                                 int64
		provider, external, key, round, game, refExt, refID    *string
		failure, correlation                                   *string
		hash                                                   []byte
		result                                                 *int64
		expires                                                *time.Time
	)
	err := db.QueryRow(ctx, sql, args...).Scan(&id, &origin, &kind, &status, &walletID, &playerID, &amount, &currency,
		&provider, &external, &key, &hash, &round, &game, &refExt, &refID, &failure, &result,
		&correlation, &expires, &s.CreatedAt, &s.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, wager.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("select wager transaction: %w", err)
	}

	amountMoney, err := money.FromMinor(amount, money.Currency(currency))
	if err != nil {
		return nil, fmt.Errorf("rehydrate transaction %s amount: %w", id, err)
	}
	s.ID, s.Origin, s.Kind, s.Status = wallet.TxID(id), wager.Origin(origin), wager.Kind(kind), wager.Status(status)
	s.WalletID, s.PlayerID, s.Amount = wallet.WalletID(walletID), wallet.PlayerID(playerID), amountMoney
	s.ProviderID, s.ExternalTransactionID, s.IdempotencyKey = deref(provider), deref(external), deref(key)
	s.PayloadHash, s.RoundID, s.GameID = hash, deref(round), deref(game)
	s.ReferenceExternalID, s.ReferenceTxID = deref(refExt), wallet.TxID(deref(refID))
	s.FailureCode, s.CorrelationID = wager.FailureCode(deref(failure)), deref(correlation)
	if result != nil {
		if s.ResultBalance, err = money.FromMinor(*result, money.Currency(currency)); err != nil {
			return nil, fmt.Errorf("rehydrate transaction %s result: %w", id, err)
		}
	}
	if expires != nil {
		s.ExpiresAt = *expires
	}
	t, err := wager.Rehydrate(s)
	if err != nil {
		return nil, fmt.Errorf("rehydrate transaction %s: %w", id, err)
	}
	return t, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
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
