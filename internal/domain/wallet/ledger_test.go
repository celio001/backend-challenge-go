package wallet

import (
	"errors"
	"testing"
	"time"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
)

func TestNewLedgerEntry(t *testing.T) {
	brl := func(minor int64) money.Money { return mm(t, minor, money.BRL) }
	tests := []struct {
		name          string
		walletID      WalletID
		txID          TxID
		version       int64
		dir           Direction
		amount        money.Money
		before, after money.Money
		createdAt     time.Time
		wantErr       error
	}{
		{"credit", "w", "t", 2, Credit, brl(250), brl(1000), brl(1250), t0, nil},
		{"debit", "w", "t", 2, Debit, brl(250), brl(1000), brl(750), t0, nil},
		{"debit to zero", "w", "t", 2, Debit, brl(1000), brl(1000), brl(0), t0, nil},
		{"credit wrong after", "w", "t", 2, Credit, brl(250), brl(1000), brl(1000), t0, ErrInconsistentEntry},
		{"debit wrong after", "w", "t", 2, Debit, brl(250), brl(1000), brl(1250), t0, ErrInconsistentEntry},
		{"credit applied as debit", "w", "t", 2, Debit, brl(250), brl(1000), brl(1250), t0, ErrInconsistentEntry},
		{"invalid direction", "w", "t", 2, "SIDEWAYS", brl(250), brl(1000), brl(1250), t0, ErrInvalidDirection},
		{"empty direction", "w", "t", 2, "", brl(250), brl(1000), brl(1250), t0, ErrInvalidDirection},
		{"zero amount", "w", "t", 2, Credit, brl(0), brl(1000), brl(1000), t0, money.ErrNotPositive},
		{"negative amount", "w", "t", 2, Credit, brl(-5), brl(1000), brl(995), t0, money.ErrNotPositive},
		{"uninitialized amount", "w", "t", 2, Credit, money.Money{}, brl(1000), brl(1000), t0, money.ErrUninitialized},
		{"uninitialized before", "w", "t", 2, Credit, brl(1), money.Money{}, brl(1), t0, money.ErrUninitialized},
		{"uninitialized after", "w", "t", 2, Credit, brl(1), brl(0), money.Money{}, t0, money.ErrUninitialized},
		{"currency mismatch", "w", "t", 2, Credit, mm(t, 1, money.USD), brl(1000), brl(1001), t0, money.ErrCurrencyMismatch},
		{"after other currency", "w", "t", 2, Credit, brl(1), brl(1000), mm(t, 1001, money.USD), t0, money.ErrCurrencyMismatch},
		{"overflow", "w", "t", 2, Credit, brl(1), brl(1<<63 - 1), brl(1), t0, money.ErrOverflow},
		{"empty wallet id", "", "t", 2, Credit, brl(1), brl(0), brl(1), t0, ErrInvalidID},
		{"empty tx id", "w", "", 2, Credit, brl(1), brl(0), brl(1), t0, ErrInvalidID},
		{"version zero", "w", "t", 0, Credit, brl(1), brl(0), brl(1), t0, ErrInvalidVersion},
		{"zero time", "w", "t", 1, Credit, brl(1), brl(0), brl(1), time.Time{}, ErrInvalidTimestamp},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, err := NewLedgerEntry(tt.walletID, tt.txID, tt.version, tt.dir, tt.amount, tt.before, tt.after, tt.createdAt)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if e.WalletID() != tt.walletID || e.TxID() != tt.txID || e.WalletVersion() != tt.version ||
				e.Direction() != tt.dir || e.Amount().Minor() != tt.amount.Minor() ||
				e.BalanceBefore().Minor() != tt.before.Minor() || e.BalanceAfter().Minor() != tt.after.Minor() ||
				!e.CreatedAt().Equal(tt.createdAt) {
				t.Fatalf("fields not preserved: %+v", e)
			}
		})
	}
}
