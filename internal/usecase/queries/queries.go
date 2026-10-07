package queries

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/pkg/uuid"
)

const (
	DefaultLimit = 50
	MaxLimit     = 200
)

var (
	ErrInvalidCursor = errors.New("queries: invalid cursor")
	ErrInvalidLimit  = errors.New("queries: limit must be between 1 and 200")
)

type LedgerItem struct {
	ID            string
	TransactionID string
	Direction     wallet.Direction
	Money         money.Money
	BalanceBefore money.Money
	BalanceAfter  money.Money
	WalletVersion int64
	CreatedAt     time.Time
}

type LedgerPage struct {
	Items      []LedgerItem
	NextCursor string
}

type WalletReader interface {
	// ByID must return wallet.ErrNotFound when the wallet does not exist.
	ByID(ctx context.Context, id wallet.WalletID) (*wallet.Wallet, error)
}

type LedgerReader interface {
	// After returns up to limit entries with walletVersion greater than afterVersion, ordered by walletVersion.
	After(ctx context.Context, id wallet.WalletID, afterVersion int64, limit int) ([]LedgerItem, error)
}

type Queries struct {
	wallets WalletReader
	ledger  LedgerReader
}

func New(wallets WalletReader, ledger LedgerReader) *Queries {
	return &Queries{wallets: wallets, ledger: ledger}
}

func (q *Queries) Wallet(ctx context.Context, id string) (*wallet.Wallet, error) {
	if !uuid.Valid(id) {
		return nil, fmt.Errorf("%w: wallet id must be a UUID", wallet.ErrInvalidID)
	}
	return q.wallets.ByID(ctx, wallet.WalletID(id))
}

// Ledger pages by walletVersion, which is unique per wallet and only grows, so the order is stable. A zero limit means DefaultLimit.
func (q *Queries) Ledger(ctx context.Context, id, cursor string, limit int) (LedgerPage, error) {
	if limit == 0 {
		limit = DefaultLimit
	}
	if limit < 0 || limit > MaxLimit {
		return LedgerPage{}, ErrInvalidLimit
	}
	after, err := decodeCursor(cursor)
	if err != nil {
		return LedgerPage{}, err
	}
	if _, err := q.Wallet(ctx, id); err != nil {
		return LedgerPage{}, err
	}

	items, err := q.ledger.After(ctx, wallet.WalletID(id), after, limit+1)
	if err != nil {
		return LedgerPage{}, fmt.Errorf("read ledger: %w", err)
	}
	if len(items) <= limit {
		return LedgerPage{Items: items}, nil
	}
	items = items[:limit]
	return LedgerPage{Items: items, NextCursor: encodeCursor(items[limit-1].WalletVersion)}, nil
}

type cursorPayload struct {
	V int64 `json:"v"`
}

func encodeCursor(version int64) string {
	b, _ := json.Marshal(cursorPayload{V: version})
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(c string) (int64, error) {
	if c == "" {
		return 0, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil {
		return 0, ErrInvalidCursor
	}
	var p cursorPayload
	if err := json.Unmarshal(b, &p); err != nil || p.V < 1 {
		return 0, ErrInvalidCursor
	}
	return p.V, nil
}
