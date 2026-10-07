package openwallet

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wager"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/internal/usecase"
)

const player = "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1"

type fakeStore struct {
	walletErr error
	outboxErr error
	wallets   []*wallet.Wallet
	txs       []*wager.Transaction
	entries   []wallet.LedgerEntry
	events    []usecase.OutboxEvent
}

func (s *fakeStore) Wallets() usecase.WalletRepository           { return fakeWallets{s: s} }
func (s *fakeStore) Transactions() usecase.TransactionRepository { return fakeTxs{s: s} }
func (s *fakeStore) Ledger() usecase.LedgerRepository            { return fakeLedger{s} }
func (s *fakeStore) Outbox() usecase.OutboxWriter                { return fakeOutbox{s} }

// Do drops staged writes when fn fails, mimicking a rollback.
func (s *fakeStore) Do(ctx context.Context, fn func(context.Context, usecase.Repos) error) error {
	wallets, txs, entries, events := s.wallets, s.txs, s.entries, s.events
	if err := fn(ctx, s); err != nil {
		s.wallets, s.txs, s.entries, s.events = wallets, txs, entries, events
		return err
	}
	return nil
}

// Embedding the port keeps the fake minimal: only the methods this use case calls are implemented.
type fakeWallets struct {
	usecase.WalletRepository
	s *fakeStore
}

func (f fakeWallets) Create(_ context.Context, w *wallet.Wallet) error {
	if f.s.walletErr != nil {
		return f.s.walletErr
	}
	f.s.wallets = append(f.s.wallets, w)
	return nil
}

type fakeTxs struct {
	usecase.TransactionRepository
	s *fakeStore
}

func (f fakeTxs) Insert(_ context.Context, t *wager.Transaction) error {
	f.s.txs = append(f.s.txs, t)
	return nil
}

type fakeLedger struct{ s *fakeStore }

func (f fakeLedger) Append(_ context.Context, e wallet.LedgerEntry) error {
	f.s.entries = append(f.s.entries, e)
	return nil
}

type fakeOutbox struct{ s *fakeStore }

func (f fakeOutbox) Add(_ context.Context, e usecase.OutboxEvent) error {
	if f.s.outboxErr != nil && len(f.s.events) == 1 {
		return f.s.outboxErr
	}
	f.s.events = append(f.s.events, e)
	return nil
}

type fixedClock time.Time

func (c fixedClock) Now() time.Time { return time.Time(c) }

type seqIDs struct{ n int }

func (s *seqIDs) NewID() string {
	s.n++
	return "id-" + strconv.Itoa(s.n)
}

type emptyIDs struct{}

func (emptyIDs) NewID() string { return "" }

func brl(t *testing.T, minor int64) money.Money {
	t.Helper()
	m, err := money.FromMinor(minor, money.BRL)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestUseCase(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	errDB := errors.New("db down")

	tests := []struct {
		name        string
		in          Input
		ids         usecase.IDGenerator
		store       *fakeStore
		wantErr     error
		wantBalance int64
		wantTxs     int
		wantEvents  []string
	}{
		{name: "zero balance opens only the wallet", in: Input{PlayerID: player, InitialBalance: brl(t, 0)}, ids: &seqIDs{}, store: &fakeStore{}},
		{
			name: "positive balance opens wallet, opening, ledger and both events",
			in:   Input{PlayerID: player, InitialBalance: brl(t, 100000), CorrelationID: "corr"}, ids: &seqIDs{}, store: &fakeStore{},
			wantBalance: 100000, wantTxs: 1, wantEvents: []string{"WagerTransactionProcessed", "WalletBalanceChanged"},
		},
		{name: "uninitialized balance", in: Input{PlayerID: player}, ids: &seqIDs{}, store: &fakeStore{}, wantErr: money.ErrUninitialized},
		{name: "negative balance", in: Input{PlayerID: player, InitialBalance: brl(t, -1)}, ids: &seqIDs{}, store: &fakeStore{}, wantErr: money.ErrNegative},
		{name: "empty player", in: Input{InitialBalance: brl(t, 0)}, ids: &seqIDs{}, store: &fakeStore{}, wantErr: wallet.ErrInvalidID},
		{name: "player is not a uuid", in: Input{PlayerID: "p1", InitialBalance: brl(t, 0)}, ids: &seqIDs{}, store: &fakeStore{}, wantErr: wallet.ErrInvalidID},
		{name: "empty generated id", in: Input{PlayerID: player, InitialBalance: brl(t, 0)}, ids: emptyIDs{}, store: &fakeStore{}, wantErr: wallet.ErrInvalidID},
		{name: "already exists", in: Input{PlayerID: player, InitialBalance: brl(t, 100000)}, ids: &seqIDs{}, store: &fakeStore{walletErr: wallet.ErrAlreadyExists}, wantErr: wallet.ErrAlreadyExists},
		{name: "repository failure", in: Input{PlayerID: player, InitialBalance: brl(t, 0)}, ids: &seqIDs{}, store: &fakeStore{walletErr: errDB}, wantErr: errDB},
		{name: "outbox failure rolls everything back", in: Input{PlayerID: player, InitialBalance: brl(t, 100000)}, ids: &seqIDs{}, store: &fakeStore{outboxErr: errDB}, wantErr: errDB},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc := New(tt.store, fixedClock(now), tt.ids)

			w, err := uc.Execute(context.Background(), tt.in)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			s := tt.store
			if tt.wantErr != nil {
				if w != nil || len(s.wallets)+len(s.txs)+len(s.entries)+len(s.events) != 0 {
					t.Fatalf("nothing should be returned or persisted on error")
				}
				return
			}

			if len(s.wallets) != 1 || s.wallets[0] != w {
				t.Fatalf("wallet was not persisted")
			}
			if w.ID() != "id-1" || w.PlayerID() != player || w.Balance().Minor() != tt.wantBalance || w.Version() != 1 || !w.CreatedAt().Equal(now) {
				t.Fatalf("unexpected wallet state: %+v", w)
			}
			if len(s.txs) != tt.wantTxs || len(s.entries) != tt.wantTxs || len(s.events) != len(tt.wantEvents) {
				t.Fatalf("txs/entries/events = %d/%d/%d", len(s.txs), len(s.entries), len(s.events))
			}
			if tt.wantTxs == 0 {
				return
			}

			opening, entry := s.txs[0], s.entries[0]
			if opening.Kind() != wager.KindOpening || opening.Status() != wager.StatusProcessed || opening.ID() != "id-2" || opening.CorrelationID() != "corr" {
				t.Fatalf("unexpected opening transaction: %+v", opening)
			}
			if entry.TxID() != opening.ID() || entry.WalletID() != w.ID() || entry.Direction() != wallet.Credit || entry.WalletVersion() != 1 || entry.BalanceAfter() != w.Balance() {
				t.Fatalf("unexpected ledger entry: %+v", entry)
			}
			for i, e := range s.events {
				if e.Type != tt.wantEvents[i] || e.PartitionKey != "id-1" || !e.OccurredAt.Equal(now) {
					t.Fatalf("event %d = %+v", i, e)
				}
				var envelope struct {
					CorrelationID string `json:"correlationId"`
				}
				if err := json.Unmarshal(e.Payload, &envelope); err != nil || envelope.CorrelationID != "corr" {
					t.Fatalf("event %d payload = %s (%v)", i, e.Payload, err)
				}
			}
		})
	}
}
