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
	// ByID reads without locking and returns wallet.ErrNotFound when absent.
	ByID(ctx context.Context, id wallet.WalletID) (*wallet.Wallet, error)
	// Lock reads the wallet and holds its row until the unit of work ends, serializing writers of that wallet.
	Lock(ctx context.Context, id wallet.WalletID) (*wallet.Wallet, error)
	// Update persists balance, version and timestamp only if the stored version is still expectedVersion, else ErrStaleWallet.
	Update(ctx context.Context, w *wallet.Wallet, expectedVersion int64) error
}

type TransactionRepository interface {
	Insert(ctx context.Context, t *wager.Transaction) error
	// InsertIfAbsent reports false, without failing the unit of work, when a unique key (idempotency key or external id) is already taken.
	InsertIfAbsent(ctx context.Context, t *wager.Transaction) (bool, error)
	// FindDuplicate returns the transaction of this provider that has the idempotency key or the external id, preferring the key; wager.ErrNotFound if none.
	FindDuplicate(ctx context.Context, providerID, idempotencyKey, externalID string) (*wager.Transaction, error)
	// FindByExternalID resolves a reference; wager.ErrNotFound if none.
	FindByExternalID(ctx context.Context, providerID, externalID string) (*wager.Transaction, error)
	// ProcessedReversalOf returns the PROCESSED REFUND/ROLLBACK that reverted refID; wager.ErrNotFound if none.
	ProcessedReversalOf(ctx context.Context, refID wallet.TxID) (*wager.Transaction, error)
	// Update persists the state of a transaction that was inserted earlier; nextAttemptAt is only used while waiting for a reference.
	Update(ctx context.Context, t *wager.Transaction, nextAttemptAt time.Time) error
	// LockPending reads a PENDING_REFERENCE transaction and holds its row; wager.ErrNotFound if it is no longer pending.
	LockPending(ctx context.Context, id wallet.TxID) (*wager.Transaction, error)
	// Reschedule counts one more attempt and sets the next one delay from now, on the database clock.
	Reschedule(ctx context.Context, id wallet.TxID, delay time.Duration) error
	// WakeWaiting makes the pending transactions that wait for this external id due now. It skips rows another replica is working on.
	WakeWaiting(ctx context.Context, providerID, externalID string) error
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

type InboxMessage struct {
	Consumer   string
	MessageID  string
	Hash       []byte
	ReceivedAt time.Time
}

type InboxRepository interface {
	// Register records the message as handled by this unit of work. It reports duplicate when the message was already handled
	// with the same hash, and returns ErrInboxHashMismatch when the id was seen with different content.
	Register(ctx context.Context, m InboxMessage) (duplicate bool, err error)
}

// Repos is only valid inside UnitOfWork.Do: every repository shares the same SQL transaction.
type Repos interface {
	Wallets() WalletRepository
	Transactions() TransactionRepository
	Ledger() LedgerRepository
	Outbox() OutboxWriter
	Inbox() InboxRepository
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
