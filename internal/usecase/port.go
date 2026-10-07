package usecase

import (
	"context"
	"time"

	"github.com/celio001/backend-challenge-go/internal/domain/wager"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
)

type WalletRepository interface {
	// Create must return wallet.ErrAlreadyExists when the (player, currency) pair is taken.
	Create(ctx context.Context, w *wallet.Wallet) error
}

type TransactionRepository interface {
	Insert(ctx context.Context, t *wager.Transaction) error
}

type LedgerRepository interface {
	Append(ctx context.Context, e wallet.LedgerEntry) error
}

type OutboxEvent struct {
	ID            string
	AggregateType string
	AggregateID   string
	PartitionKey  string
	Type          string
	Version       int
	Payload       []byte
	OccurredAt    time.Time
}

type OutboxWriter interface {
	Add(ctx context.Context, e OutboxEvent) error
}

// Repos is only valid inside UnitOfWork.Do: every repository shares the same SQL transaction.
type Repos interface {
	Wallets() WalletRepository
	Transactions() TransactionRepository
	Ledger() LedgerRepository
	Outbox() OutboxWriter
}

// Do commits when fn returns nil and rolls back otherwise.
type UnitOfWork interface {
	Do(ctx context.Context, fn func(ctx context.Context, tx Repos) error) error
}

type Clock interface {
	Now() time.Time
}

type IDGenerator interface {
	NewID() string
}
