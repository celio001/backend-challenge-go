package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
)

const uniqueViolation = "23505"

type WalletRepository struct {
	db execer
}

func NewWalletRepository(db execer) *WalletRepository {
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
