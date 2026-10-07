package wallet

import (
	"errors"
	"time"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
)

var (
	ErrInvalidID         = errors.New("wallet: invalid identifier")
	ErrInvalidVersion    = errors.New("wallet: version must be at least 1")
	ErrInvalidTimestamp  = errors.New("wallet: invalid timestamp")
	ErrInsufficientFunds = errors.New("wallet: insufficient funds")
	ErrNegativeBalance   = errors.New("wallet: balance cannot be negative")
)

type (
	WalletID string
	PlayerID string
	TxID     string
)

type Wallet struct {
	id        WalletID
	playerID  PlayerID
	balance   money.Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

// Open creates an empty wallet at version 1.
func Open(id WalletID, playerID PlayerID, currency money.Currency, now time.Time) (*Wallet, error) {
	if err := validateNew(id, playerID, now); err != nil {
		return nil, err
	}
	zero, err := money.Zero(currency)
	if err != nil {
		return nil, err
	}
	return &Wallet{id: id, playerID: playerID, balance: zero, version: 1, createdAt: now.UTC(), updatedAt: now.UTC()}, nil
}

// OpenWithBalance keeps version 1 (Open followed by Credit would end at 2) and returns the opening ledger entry.
func OpenWithBalance(id WalletID, playerID PlayerID, initial money.Money, openingTxID TxID, now time.Time) (*Wallet, LedgerEntry, error) {
	if err := validateNew(id, playerID, now); err != nil {
		return nil, LedgerEntry{}, err
	}
	if !initial.IsPositive() {
		if !initial.IsValid() {
			return nil, LedgerEntry{}, money.ErrUninitialized
		}
		return nil, LedgerEntry{}, money.ErrNotPositive
	}
	zero, err := money.Zero(initial.Currency())
	if err != nil {
		return nil, LedgerEntry{}, err
	}
	entry, err := NewLedgerEntry(id, openingTxID, 1, Credit, initial, zero, initial, now)
	if err != nil {
		return nil, LedgerEntry{}, err
	}
	w := &Wallet{id: id, playerID: playerID, balance: initial, version: 1, createdAt: now.UTC(), updatedAt: now.UTC()}
	return w, entry, nil
}

// Rehydrate restores persisted state without applying any movement.
func Rehydrate(id WalletID, playerID PlayerID, balance money.Money, version int64, createdAt, updatedAt time.Time) (*Wallet, error) {
	if id == "" || playerID == "" {
		return nil, ErrInvalidID
	}
	if !balance.IsValid() {
		return nil, money.ErrUninitialized
	}
	if balance.IsNegative() {
		return nil, ErrNegativeBalance
	}
	if version < 1 {
		return nil, ErrInvalidVersion
	}
	if createdAt.IsZero() || updatedAt.IsZero() || updatedAt.Before(createdAt) {
		return nil, ErrInvalidTimestamp
	}
	return &Wallet{id: id, playerID: playerID, balance: balance, version: version, createdAt: createdAt.UTC(), updatedAt: updatedAt.UTC()}, nil
}

func (w *Wallet) ID() WalletID             { return w.id }
func (w *Wallet) PlayerID() PlayerID       { return w.playerID }
func (w *Wallet) Balance() money.Money     { return w.balance }
func (w *Wallet) Currency() money.Currency { return w.balance.Currency() }
func (w *Wallet) Version() int64           { return w.version }
func (w *Wallet) CreatedAt() time.Time     { return w.createdAt }
func (w *Wallet) UpdatedAt() time.Time     { return w.updatedAt }

// Debit fails with ErrInsufficientFunds if the balance would go below zero.
func (w *Wallet) Debit(m money.Money, txID TxID, now time.Time) (LedgerEntry, error) {
	if err := w.checkMovement(m, txID, now); err != nil {
		return LedgerEntry{}, err
	}
	after, err := w.balance.Sub(m)
	if err != nil {
		return LedgerEntry{}, err
	}
	if after.IsNegative() {
		return LedgerEntry{}, ErrInsufficientFunds
	}
	return w.apply(Debit, m, after, txID, now)
}

func (w *Wallet) Credit(m money.Money, txID TxID, now time.Time) (LedgerEntry, error) {
	if err := w.checkMovement(m, txID, now); err != nil {
		return LedgerEntry{}, err
	}
	after, err := w.balance.Add(m)
	if err != nil {
		return LedgerEntry{}, err
	}
	return w.apply(Credit, m, after, txID, now)
}

func (w *Wallet) checkMovement(m money.Money, txID TxID, now time.Time) error {
	if txID == "" {
		return ErrInvalidID
	}
	if now.IsZero() {
		return ErrInvalidTimestamp
	}
	if !m.IsValid() {
		return money.ErrUninitialized
	}
	if m.Currency() != w.balance.Currency() {
		return money.ErrCurrencyMismatch
	}
	if !m.IsPositive() {
		return money.ErrNotPositive
	}
	return nil
}

// apply builds the entry first so the wallet is left untouched if it is rejected.
func (w *Wallet) apply(dir Direction, m, after money.Money, txID TxID, now time.Time) (LedgerEntry, error) {
	entry, err := NewLedgerEntry(w.id, txID, w.version+1, dir, m, w.balance, after, now)
	if err != nil {
		return LedgerEntry{}, err
	}
	w.balance = after
	w.version++
	w.updatedAt = now.UTC()
	return entry, nil
}

func validateNew(id WalletID, playerID PlayerID, now time.Time) error {
	if id == "" || playerID == "" {
		return ErrInvalidID
	}
	if now.IsZero() {
		return ErrInvalidTimestamp
	}
	return nil
}
