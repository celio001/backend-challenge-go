package wallet

import (
	"errors"
	"testing"
	"time"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
)

var t0 = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

func mm(t *testing.T, minor int64, c money.Currency) money.Money {
	t.Helper()
	m, err := money.FromMinor(minor, c)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func newWallet(t *testing.T, minor int64) *Wallet {
	t.Helper()
	w, err := Rehydrate("w1", "p1", mm(t, minor, money.BRL), 1, t0, t0)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestOpen(t *testing.T) {
	tests := []struct {
		name     string
		id       WalletID
		player   PlayerID
		currency money.Currency
		now      time.Time
		wantErr  error
	}{
		{"ok", "w1", "p1", money.BRL, t0, nil},
		{"empty id", "", "p1", money.BRL, t0, ErrInvalidID},
		{"empty player", "w1", "", money.BRL, t0, ErrInvalidID},
		{"invalid currency", "w1", "p1", "XXX", t0, money.ErrInvalidCurrency},
		{"zero time", "w1", "p1", money.BRL, time.Time{}, ErrInvalidTimestamp},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, err := Open(tt.id, tt.player, tt.currency, tt.now)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if !w.Balance().IsZero() || w.Currency() != tt.currency || w.Version() != 1 {
				t.Fatalf("unexpected wallet: balance=%s version=%d", w.Balance(), w.Version())
			}
		})
	}
}

func TestOpenWithBalance(t *testing.T) {
	tests := []struct {
		name    string
		id      WalletID
		initial money.Money
		txID    TxID
		wantErr error
	}{
		{"ok", "w1", mm(t, 100000, money.BRL), "tx1", nil},
		{"zero rejected", "w1", mm(t, 0, money.BRL), "tx1", money.ErrNotPositive},
		{"negative rejected", "w1", mm(t, -1, money.BRL), "tx1", money.ErrNotPositive},
		{"uninitialized", "w1", money.Money{}, "tx1", money.ErrUninitialized},
		{"empty wallet id", "", mm(t, 100, money.BRL), "tx1", ErrInvalidID},
		{"empty tx id", "w1", mm(t, 100, money.BRL), "", ErrInvalidID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, e, err := OpenWithBalance(tt.id, "p1", tt.initial, tt.txID, t0)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if w.Version() != 1 || w.Balance().Minor() != tt.initial.Minor() {
				t.Fatalf("wallet version=%d balance=%s", w.Version(), w.Balance())
			}
			if e.Direction() != Credit || e.WalletVersion() != 1 || !e.BalanceBefore().IsZero() ||
				e.BalanceAfter().Minor() != tt.initial.Minor() || e.TxID() != tt.txID {
				t.Fatalf("unexpected opening entry: %+v", e)
			}
		})
	}
}

func TestRehydrate(t *testing.T) {
	later := t0.Add(time.Hour)
	tests := []struct {
		name      string
		id        WalletID
		player    PlayerID
		balance   money.Money
		version   int64
		createdAt time.Time
		updatedAt time.Time
		wantErr   error
	}{
		{"ok", "w1", "p1", mm(t, 500, money.BRL), 3, t0, later, nil},
		{"zero balance", "w1", "p1", mm(t, 0, money.BRL), 1, t0, t0, nil},
		{"empty id", "", "p1", mm(t, 500, money.BRL), 1, t0, t0, ErrInvalidID},
		{"empty player", "w1", "", mm(t, 500, money.BRL), 1, t0, t0, ErrInvalidID},
		{"uninitialized balance", "w1", "p1", money.Money{}, 1, t0, t0, money.ErrUninitialized},
		{"negative balance", "w1", "p1", mm(t, -1, money.BRL), 1, t0, t0, ErrNegativeBalance},
		{"version zero", "w1", "p1", mm(t, 500, money.BRL), 0, t0, t0, ErrInvalidVersion},
		{"zero created", "w1", "p1", mm(t, 500, money.BRL), 1, time.Time{}, t0, ErrInvalidTimestamp},
		{"updated before created", "w1", "p1", mm(t, 500, money.BRL), 1, later, t0, ErrInvalidTimestamp},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, err := Rehydrate(tt.id, tt.player, tt.balance, tt.version, tt.createdAt, tt.updatedAt)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err == nil && (w.Version() != tt.version || w.Balance().Minor() != tt.balance.Minor()) {
				t.Fatalf("state not preserved: version=%d balance=%s", w.Version(), w.Balance())
			}
		})
	}
}

func TestMovements(t *testing.T) {
	tests := []struct {
		name        string
		start       int64
		op          Direction
		amount      money.Money
		txID        TxID
		now         time.Time
		wantErr     error
		wantBalance int64
		wantVersion int64
	}{
		{"credit", 1000, Credit, mm(t, 250, money.BRL), "tx", t0, nil, 1250, 2},
		{"debit", 1000, Debit, mm(t, 250, money.BRL), "tx", t0, nil, 750, 2},
		{"debit to exactly zero", 1000, Debit, mm(t, 1000, money.BRL), "tx", t0, nil, 0, 2},
		{"debit above balance", 1000, Debit, mm(t, 1001, money.BRL), "tx", t0, ErrInsufficientFunds, 1000, 1},
		{"debit empty wallet", 0, Debit, mm(t, 1, money.BRL), "tx", t0, ErrInsufficientFunds, 0, 1},
		{"credit overflow", math64Max, Credit, mm(t, 1, money.BRL), "tx", t0, money.ErrOverflow, math64Max, 1},
		{"credit currency mismatch", 1000, Credit, mm(t, 1, money.USD), "tx", t0, money.ErrCurrencyMismatch, 1000, 1},
		{"debit currency mismatch", 1000, Debit, mm(t, 1, money.EUR), "tx", t0, money.ErrCurrencyMismatch, 1000, 1},
		{"credit zero", 1000, Credit, mm(t, 0, money.BRL), "tx", t0, money.ErrNotPositive, 1000, 1},
		{"debit zero", 1000, Debit, mm(t, 0, money.BRL), "tx", t0, money.ErrNotPositive, 1000, 1},
		{"credit negative", 1000, Credit, mm(t, -5, money.BRL), "tx", t0, money.ErrNotPositive, 1000, 1},
		{"debit negative", 1000, Debit, mm(t, -5, money.BRL), "tx", t0, money.ErrNotPositive, 1000, 1},
		{"credit uninitialized", 1000, Credit, money.Money{}, "tx", t0, money.ErrUninitialized, 1000, 1},
		{"debit uninitialized", 1000, Debit, money.Money{}, "tx", t0, money.ErrUninitialized, 1000, 1},
		{"empty tx id", 1000, Credit, mm(t, 1, money.BRL), "", t0, ErrInvalidID, 1000, 1},
		{"zero time", 1000, Debit, mm(t, 1, money.BRL), "tx", time.Time{}, ErrInvalidTimestamp, 1000, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := newWallet(t, tt.start)
			before := w.Balance()
			now := t0.Add(time.Minute)
			if tt.now.IsZero() {
				now = tt.now
			}

			var e LedgerEntry
			var err error
			if tt.op == Credit {
				e, err = w.Credit(tt.amount, tt.txID, now)
			} else {
				e, err = w.Debit(tt.amount, tt.txID, now)
			}

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if w.Balance().Minor() != tt.wantBalance || w.Version() != tt.wantVersion {
				t.Fatalf("balance=%s version=%d, want %d / %d", w.Balance(), w.Version(), tt.wantBalance, tt.wantVersion)
			}
			if err != nil {
				if !w.UpdatedAt().Equal(t0) {
					t.Fatal("updatedAt must not change on failure")
				}
				return
			}
			if e.Direction() != tt.op || e.WalletVersion() != w.Version() || e.TxID() != tt.txID ||
				e.BalanceBefore().Minor() != before.Minor() || e.BalanceAfter().Minor() != w.Balance().Minor() ||
				e.Amount().Minor() != tt.amount.Minor() || e.WalletID() != w.ID() {
				t.Fatalf("unexpected entry: %+v", e)
			}
			if !w.UpdatedAt().Equal(now) {
				t.Fatalf("updatedAt = %v, want %v", w.UpdatedAt(), now)
			}
		})
	}
}

const math64Max = int64(1<<63 - 1)

func TestSequentialMovementsKeepLedgerChained(t *testing.T) {
	w := newWallet(t, 10000)
	steps := []struct {
		dir    Direction
		amount int64
	}{{Debit, 8000}, {Credit, 500}, {Debit, 2500}}

	prev := w.Balance()
	for i, s := range steps {
		var e LedgerEntry
		var err error
		m := mm(t, s.amount, money.BRL)
		if s.dir == Credit {
			e, err = w.Credit(m, TxID("tx"), t0)
		} else {
			e, err = w.Debit(m, TxID("tx"), t0)
		}
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		if e.BalanceBefore().Minor() != prev.Minor() {
			t.Fatalf("step %d: entry does not chain from previous balance", i)
		}
		if e.WalletVersion() != int64(i+2) {
			t.Fatalf("step %d: version = %d", i, e.WalletVersion())
		}
		prev = e.BalanceAfter()
	}
	if w.Balance().Minor() != 0 || w.Version() != 4 {
		t.Fatalf("final balance=%s version=%d", w.Balance(), w.Version())
	}
}
