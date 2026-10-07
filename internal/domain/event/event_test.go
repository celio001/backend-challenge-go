package event

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wager"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
)

var now = time.Date(2026, 9, 8, 12, 0, 0, 123_000_000, time.UTC)

func mustMoney(t *testing.T, minor int64) money.Money {
	t.Helper()
	m, err := money.FromMinor(minor, money.BRL)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestNewWalletBalanceChanged(t *testing.T) {
	before, after, amount := mustMoney(t, 0), mustMoney(t, 100000), mustMoney(t, 100000)
	credit, err := wallet.NewLedgerEntry("w1", "tx1", 1, wallet.Credit, amount, before, after, now)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		id      string
		entry   wallet.LedgerEntry
		wantErr error
		want    string
	}{
		{
			name: "credit", id: "ev1", entry: credit,
			want: `{"eventId":"ev1","eventType":"WalletBalanceChanged","version":1,"aggregateId":"w1","correlationId":"corr","causationId":"cause","occurredAt":"2026-09-08T12:00:00.123Z","data":{"walletId":"w1","transactionId":"tx1","direction":"CREDIT","money":{"amount":"1000.00","currency":"BRL"},"balanceBefore":{"amount":"0.00","currency":"BRL"},"balanceAfter":{"amount":"1000.00","currency":"BRL"},"walletVersion":1}}`,
		},
		{name: "missing id", entry: credit, wantErr: ErrMissingID},
		{name: "zero entry", id: "ev1", entry: wallet.LedgerEntry{}, wantErr: ErrInvalidMovement},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev, err := NewWalletBalanceChanged(tt.id, "corr", "cause", tt.entry)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			got, err := json.Marshal(ev)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tt.want {
				t.Fatalf("json =\n%s\nwant\n%s", got, tt.want)
			}
			if ev.AggregateType != "wallet" || ev.PartitionKey != "w1" {
				t.Fatalf("routing = %q/%q", ev.AggregateType, ev.PartitionKey)
			}
		})
	}
}

func TestNewWagerTransactionProcessed(t *testing.T) {
	opening, err := wager.NewOpening("tx1", "w1", "p1", mustMoney(t, 100000), "corr", now)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := wager.NewExternal(wager.ExternalInput{
		ID: "tx2", ProviderID: "provider-a", ExternalTransactionID: "ext-1", IdempotencyKey: "k",
		PayloadHash: make([]byte, 32), WalletID: "w1", PlayerID: "p1", RoundID: "r", GameID: "g",
		Kind: wager.KindBet, Amount: mustMoney(t, 2500), CorrelationID: "corr",
	}, now)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		id      string
		tx      *wager.Transaction
		wantErr error
		want    string
	}{
		{
			name: "opening", id: "ev1", tx: opening,
			want: `{"eventId":"ev1","eventType":"WagerTransactionProcessed","version":1,"aggregateId":"tx1","correlationId":"corr","occurredAt":"2026-09-08T12:00:00.123Z","data":{"transactionId":"tx1","walletId":"w1","playerId":"p1","kind":"OPENING","origin":"INTERNAL","money":{"amount":"1000.00","currency":"BRL"},"resultBalance":{"amount":"1000.00","currency":"BRL"}}}`,
		},
		{name: "still pending", id: "ev1", tx: pending, wantErr: ErrNotProcessed},
		{name: "missing id", tx: opening, wantErr: ErrMissingID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev, err := NewWagerTransactionProcessed(tt.id, "", tt.tx)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			got, err := json.Marshal(ev)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tt.want {
				t.Fatalf("json =\n%s\nwant\n%s", got, tt.want)
			}
			if ev.AggregateType != "wager_transaction" || ev.PartitionKey != "w1" {
				t.Fatalf("routing = %q/%q", ev.AggregateType, ev.PartitionKey)
			}
		})
	}
}

func TestNewWagerTransactionRejectedAndPending(t *testing.T) {
	bet := func(kind wager.Kind, ref string) *wager.Transaction {
		tx, err := wager.NewExternal(wager.ExternalInput{
			ID: "tx2", ProviderID: "provider-a", ExternalTransactionID: "ext-1", IdempotencyKey: "k",
			PayloadHash: make([]byte, 32), WalletID: "w1", PlayerID: "p1", RoundID: "r", GameID: "g",
			Kind: kind, Amount: mustMoney(t, 2500), ReferenceExternalID: ref, CorrelationID: "corr",
		}, now)
		if err != nil {
			t.Fatal(err)
		}
		return tx
	}
	rejected := bet(wager.KindBet, "")
	if err := rejected.Reject(wager.CodeInsufficientFunds, now); err != nil {
		t.Fatal(err)
	}
	pending := bet(wager.KindRefund, "ext-0")
	if err := pending.MarkPendingReference(now.Add(10*time.Minute), now); err != nil {
		t.Fatal(err)
	}

	t.Run("rejected", func(t *testing.T) {
		ev, err := NewWagerTransactionRejected("ev1", "msg-1", rejected)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := json.Marshal(ev)
		want := `{"eventId":"ev1","eventType":"WagerTransactionRejected","version":1,"aggregateId":"tx2","correlationId":"corr","causationId":"msg-1","occurredAt":"2026-09-08T12:00:00.123Z","data":{"transactionId":"tx2","walletId":"w1","playerId":"p1","kind":"BET","money":{"amount":"25.00","currency":"BRL"},"failureCode":"INSUFFICIENT_FUNDS","providerId":"provider-a","externalTransactionId":"ext-1"}}`
		if string(got) != want {
			t.Fatalf("json =\n%s\nwant\n%s", got, want)
		}
	})
	t.Run("pending reference", func(t *testing.T) {
		ev, err := NewWagerTransactionPendingReference("ev2", "", pending)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := json.Marshal(ev)
		want := `{"eventId":"ev2","eventType":"WagerTransactionPendingReference","version":1,"aggregateId":"tx2","correlationId":"corr","occurredAt":"2026-09-08T12:00:00.123Z","data":{"transactionId":"tx2","walletId":"w1","playerId":"p1","kind":"REFUND","money":{"amount":"25.00","currency":"BRL"},"referenceExternalTransactionId":"ext-0","expiresAt":"2026-09-08T12:10:00.123Z","providerId":"provider-a","externalTransactionId":"ext-1"}}`
		if string(got) != want {
			t.Fatalf("json =\n%s\nwant\n%s", got, want)
		}
	})

	tests := []struct {
		name    string
		build   func() error
		wantErr error
	}{
		{name: "rejected event from a pending transaction", build: func() error { _, err := NewWagerTransactionRejected("e", "", bet(wager.KindBet, "")); return err }, wantErr: ErrNotRejected},
		{name: "rejected event without id", build: func() error { _, err := NewWagerTransactionRejected("", "", rejected); return err }, wantErr: ErrMissingID},
		{name: "pending event from a rejected transaction", build: func() error { _, err := NewWagerTransactionPendingReference("e", "", rejected); return err }, wantErr: ErrNotPending},
		{name: "pending event without id", build: func() error { _, err := NewWagerTransactionPendingReference("", "", pending); return err }, wantErr: ErrMissingID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.build(); !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}
