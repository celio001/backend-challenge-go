package wallet

import (
	"errors"
	"fmt"
	"time"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
)

var (
	ErrInvalidDirection  = errors.New("wallet: invalid ledger direction")
	ErrInconsistentEntry = errors.New("wallet: ledger entry balances do not match amount and direction")
)

type Direction string

const (
	Debit  Direction = "DEBIT"
	Credit Direction = "CREDIT"
)

// LedgerEntry is immutable: it has no setters and can only be built through NewLedgerEntry.
type LedgerEntry struct {
	walletID      WalletID
	txID          TxID
	walletVersion int64
	direction     Direction
	amount        money.Money
	balanceBefore money.Money
	balanceAfter  money.Money
	createdAt     time.Time
}

// NewLedgerEntry also serves to rehydrate from storage, since it only validates and applies nothing.
func NewLedgerEntry(
	walletID WalletID, txID TxID, walletVersion int64, direction Direction,
	amount, balanceBefore, balanceAfter money.Money, createdAt time.Time,
) (LedgerEntry, error) {
	if walletID == "" || txID == "" {
		return LedgerEntry{}, ErrInvalidID
	}
	if walletVersion < 1 {
		return LedgerEntry{}, ErrInvalidVersion
	}
	if createdAt.IsZero() {
		return LedgerEntry{}, ErrInvalidTimestamp
	}
	if !amount.IsPositive() {
		if !amount.IsValid() {
			return LedgerEntry{}, money.ErrUninitialized
		}
		return LedgerEntry{}, money.ErrNotPositive
	}

	var expected money.Money
	var err error
	switch direction {
	case Credit:
		expected, err = balanceBefore.Add(amount)
	case Debit:
		expected, err = balanceBefore.Sub(amount)
	default:
		return LedgerEntry{}, fmt.Errorf("%w: %q", ErrInvalidDirection, string(direction))
	}
	if err != nil {
		return LedgerEntry{}, err
	}
	if cmp, err := expected.Cmp(balanceAfter); err != nil {
		return LedgerEntry{}, err
	} else if cmp != 0 {
		return LedgerEntry{}, ErrInconsistentEntry
	}

	return LedgerEntry{
		walletID:      walletID,
		txID:          txID,
		walletVersion: walletVersion,
		direction:     direction,
		amount:        amount,
		balanceBefore: balanceBefore,
		balanceAfter:  balanceAfter,
		createdAt:     createdAt.UTC(),
	}, nil
}

func (e LedgerEntry) WalletID() WalletID         { return e.walletID }
func (e LedgerEntry) TxID() TxID                 { return e.txID }
func (e LedgerEntry) WalletVersion() int64       { return e.walletVersion }
func (e LedgerEntry) Direction() Direction       { return e.direction }
func (e LedgerEntry) Amount() money.Money        { return e.amount }
func (e LedgerEntry) BalanceBefore() money.Money { return e.balanceBefore }
func (e LedgerEntry) BalanceAfter() money.Money  { return e.balanceAfter }
func (e LedgerEntry) CreatedAt() time.Time       { return e.createdAt }
