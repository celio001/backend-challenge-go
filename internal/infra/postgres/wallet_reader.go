package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
)

type WalletReader struct {
	db querier
}

func NewWalletReader(db querier) *WalletReader {
	return &WalletReader{db: db}
}

func (r *WalletReader) ByID(ctx context.Context, id wallet.WalletID) (*wallet.Wallet, error) {
	return loadWallet(ctx, r.db, id, "")
}

// loadWallet rehydrates a wallet; suffix is appended to the query (e.g. " FOR UPDATE").
func loadWallet(ctx context.Context, db querier, id wallet.WalletID, suffix string) (*wallet.Wallet, error) {
	var (
		playerID, currency   string
		balance, version     int64
		createdAt, updatedAt time.Time
	)
	err := db.QueryRow(ctx,
		`SELECT player_id, currency, balance_minor, version, created_at, updated_at FROM wallets WHERE id = $1`+suffix,
		string(id)).Scan(&playerID, &currency, &balance, &version, &createdAt, &updatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, wallet.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("select wallet: %w", err)
	}

	m, err := money.FromMinor(balance, money.Currency(currency))
	if err != nil {
		return nil, fmt.Errorf("rehydrate wallet %s balance: %w", id, err)
	}
	w, err := wallet.Rehydrate(id, wallet.PlayerID(playerID), m, version, createdAt, updatedAt)
	if err != nil {
		return nil, fmt.Errorf("rehydrate wallet %s: %w", id, err)
	}
	return w, nil
}
