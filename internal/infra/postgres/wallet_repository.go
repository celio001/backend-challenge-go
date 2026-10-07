package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/internal/usecase"
)

const uniqueViolation = "23505"

type WalletRepository struct {
	db dbtx
}

func NewWalletRepository(db dbtx) *WalletRepository {
	return &WalletRepository{db: db}
}

func (r *WalletRepository) Create(ctx context.Context, w *wallet.Wallet) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO wallets (id, player_id, currency, balance_minor, version, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		string(w.ID()), string(w.PlayerID()), string(w.Currency()), w.Balance().Minor(), w.Version(), w.CreatedAt(), w.UpdatedAt())
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation && pgErr.ConstraintName == "wallets_player_id_currency_key" {
			return wallet.ErrAlreadyExists
		}
		return fmt.Errorf("insert wallet: %w", err)
	}
	return nil
}

func (r *WalletRepository) ByID(ctx context.Context, id wallet.WalletID) (*wallet.Wallet, error) {
	return loadWallet(ctx, r.db, id, "")
}

// Lock uses FOR NO KEY UPDATE rather than FOR UPDATE: writers still exclude each other, but the lock does not conflict
// with the FOR KEY SHARE that foreign keys take when inserting transactions. With FOR UPDATE, two operations that had
// inserted their transaction and then asked for the lock would each wait on the other's key share (deadlock).
func (r *WalletRepository) Lock(ctx context.Context, id wallet.WalletID) (*wallet.Wallet, error) {
	return loadWallet(ctx, r.db, id, " FOR NO KEY UPDATE")
}

func (r *WalletRepository) Update(ctx context.Context, w *wallet.Wallet, expectedVersion int64) error {
	tag, err := r.db.Exec(ctx,
		`UPDATE wallets SET balance_minor = $2, version = $3, updated_at = $4 WHERE id = $1 AND version = $5`,
		string(w.ID()), w.Balance().Minor(), w.Version(), w.UpdatedAt(), expectedVersion)
	if err != nil {
		return fmt.Errorf("update wallet: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w (%w): wallet %s expected version %d", usecase.ErrStaleWallet, usecase.ErrTransient, w.ID(), expectedVersion)
	}
	return nil
}
