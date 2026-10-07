package processwager

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wager"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/internal/usecase"
)

const (
	player   = "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1"
	walletID = "0192f291-27dd-7d3f-8071-5f8685deef37"
)

type fixedClock time.Time

func (c fixedClock) Now() time.Time { return time.Time(c) }

type seqIDs struct{ n int }

func (s *seqIDs) NewID() string {
	s.n++
	return "id-" + strconv.Itoa(s.n)
}

type harness struct {
	t     *testing.T
	store *fakeStore
	uc    *UseCase
	ids   *seqIDs
	now   time.Time
}

var testOptions = Options{RetryBase: time.Millisecond, ReferenceTTL: 10 * time.Minute}

// at returns a use case over the same store whose clock is d later.
func (h *harness) at(d time.Duration) *UseCase {
	return New(h.store, fixedClock(h.now.Add(d)), h.ids, testOptions)
}

// resume does what the resolver does for one pending transaction: wallet lock, pending row lock, Resume.
func (h *harness) resume(uc *UseCase, id string) (Output, error) {
	h.t.Helper()
	var out Output
	err := h.store.Do(context.Background(), func(ctx context.Context, tx usecase.Repos) error {
		w, err := tx.Wallets().Lock(ctx, walletID)
		if err != nil {
			return err
		}
		t, err := tx.Transactions().LockPending(ctx, wallet.TxID(id))
		if err != nil {
			return err
		}
		out, err = uc.Resume(ctx, tx, t, w)
		return err
	})
	return out, err
}

func newHarness(t *testing.T, balance int64) *harness {
	t.Helper()
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	store := newFakeStore()
	b, err := money.FromMinor(balance, money.BRL)
	if err != nil {
		t.Fatal(err)
	}
	w, err := wallet.Rehydrate(walletID, player, b, 1, now.Add(-time.Hour), now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	store.wallets[walletID] = w
	ids := &seqIDs{}
	return &harness{t: t, store: store, now: now, ids: ids, uc: New(store, fixedClock(now), ids, testOptions)}
}

type spec struct {
	ext, kind, ref, round, key, provider, wallet, player string
	minor                                                int64
	currency                                             money.Currency
}

func (h *harness) input(s spec) Input {
	h.t.Helper()
	if s.provider == "" {
		s.provider = "provider-a"
	}
	if s.key == "" {
		s.key = "k-" + s.ext
	}
	if s.round == "" {
		s.round = "round-1"
	}
	if s.currency == "" {
		s.currency = money.BRL
	}
	if s.wallet == "" {
		s.wallet = walletID
	}
	if s.player == "" {
		s.player = player
	}
	m, err := money.FromMinor(s.minor, s.currency)
	if err != nil {
		h.t.Fatal(err)
	}
	return Input{
		ProviderID: s.provider, ExternalTransactionID: s.ext, IdempotencyKey: s.key, PlayerID: s.player, WalletID: s.wallet,
		RoundID: s.round, GameID: "game-1", Kind: s.kind, Money: m, ReferenceExternalTransactionID: s.ref,
	}
}

func (h *harness) balance() int64 { return h.store.wallets[walletID].Balance().Minor() }

type step struct {
	name        string
	spec        spec
	wantStatus  wager.Status
	wantCode    wager.FailureCode
	wantBalance int64 // checked for PROCESSED only
	wantErr     error
	wantReplay  bool
}

type scenario struct {
	name        string
	initial     int64
	steps       []step
	wantBalance int64
	wantVersion int64
	wantLedger  int
	wantEvents  []string // outbox event types, in order
}

func TestScenarios(t *testing.T) {
	const (
		processed = wager.StatusProcessed
		rejected  = wager.StatusRejected
		pending   = wager.StatusPendingReference
	)
	bet := func(ext string, minor int64) spec { return spec{ext: ext, kind: "BET", minor: minor} }
	reversal := func(kind, ext, ref string, minor int64) spec {
		return spec{ext: ext, kind: kind, ref: ref, minor: minor}
	}

	tests := []scenario{
		{
			name: "bet debits the wallet", initial: 100000,
			steps:       []step{{name: "bet", spec: bet("b1", 2500), wantStatus: processed, wantBalance: 97500}},
			wantBalance: 97500, wantVersion: 2, wantLedger: 1,
			wantEvents: []string{"WagerTransactionProcessed", "WalletBalanceChanged"},
		},
		{
			name: "bet above the balance is rejected and nothing moves", initial: 100000,
			steps:       []step{{name: "bet", spec: bet("b1", 150000), wantStatus: rejected, wantCode: wager.CodeInsufficientFunds}},
			wantBalance: 100000, wantVersion: 1, wantLedger: 0,
			wantEvents: []string{"WagerTransactionRejected"},
		},
		{
			name: "win credits the wallet", initial: 100000,
			steps:       []step{{name: "win", spec: spec{ext: "w1", kind: "WIN", minor: 5000}, wantStatus: processed, wantBalance: 105000}},
			wantBalance: 105000, wantVersion: 2, wantLedger: 1,
			wantEvents: []string{"WagerTransactionProcessed", "WalletBalanceChanged"},
		},
		{
			name: "win may reference a bet of the same round", initial: 100000,
			steps: []step{
				{name: "bet", spec: bet("b1", 2500), wantStatus: processed, wantBalance: 97500},
				{name: "win", spec: spec{ext: "w1", kind: "WIN", minor: 5000, ref: "b1"}, wantStatus: processed, wantBalance: 102500},
			},
			wantBalance: 102500, wantVersion: 3, wantLedger: 2,
		},
		{
			name: "win referencing another round is rejected", initial: 100000,
			steps: []step{
				{name: "bet", spec: bet("b1", 2500), wantStatus: processed, wantBalance: 97500},
				{name: "win", spec: spec{ext: "w1", kind: "WIN", minor: 5000, ref: "b1", round: "round-2"}, wantStatus: rejected, wantCode: wager.CodeReferenceMismatch},
			},
			wantBalance: 97500, wantVersion: 2, wantLedger: 1,
		},
		{
			name: "loss moves nothing and keeps the version", initial: 100000,
			steps:       []step{{name: "loss", spec: spec{ext: "l1", kind: "LOSS"}, wantStatus: processed, wantBalance: 100000}},
			wantBalance: 100000, wantVersion: 1, wantLedger: 0,
			wantEvents: []string{"WagerTransactionProcessed"},
		},
		{
			name: "currency different from the wallet is rejected", initial: 100000,
			steps:       []step{{name: "usd bet", spec: spec{ext: "b1", kind: "BET", minor: 100, currency: money.USD}, wantStatus: rejected, wantCode: wager.CodeCurrencyMismatch}},
			wantBalance: 100000, wantVersion: 1, wantLedger: 0,
		},
		{
			name: "loss in another currency is rejected too", initial: 100000,
			steps:       []step{{name: "usd loss", spec: spec{ext: "l1", kind: "LOSS", currency: money.EUR}, wantStatus: rejected, wantCode: wager.CodeCurrencyMismatch}},
			wantBalance: 100000, wantVersion: 1,
		},

		// Reversal policy, ARCHITECTURE.md §4.5.
		{
			name: "refund returns a bet", initial: 100000,
			steps: []step{
				{name: "bet", spec: bet("b1", 5000), wantStatus: processed, wantBalance: 95000},
				{name: "refund", spec: reversal("REFUND", "r1", "b1", 5000), wantStatus: processed, wantBalance: 100000},
			},
			wantBalance: 100000, wantVersion: 3, wantLedger: 2,
		},
		{
			name: "rollback of a bet returns it", initial: 100000,
			steps: []step{
				{name: "bet", spec: bet("b1", 5000), wantStatus: processed, wantBalance: 95000},
				{name: "rollback", spec: reversal("ROLLBACK", "r1", "b1", 5000), wantStatus: processed, wantBalance: 100000},
			},
			wantBalance: 100000, wantVersion: 3, wantLedger: 2,
		},
		{
			name: "rollback after refund of the same bet is refused: no double return", initial: 100000,
			steps: []step{
				{name: "bet", spec: bet("b1", 5000), wantStatus: processed, wantBalance: 95000},
				{name: "refund", spec: reversal("REFUND", "r1", "b1", 5000), wantStatus: processed, wantBalance: 100000},
				{name: "rollback", spec: reversal("ROLLBACK", "r2", "b1", 5000), wantStatus: rejected, wantCode: wager.CodeReferenceAlreadyReversed},
			},
			wantBalance: 100000, wantVersion: 3, wantLedger: 2,
		},
		{
			name: "rollback of a refund reinstates the bet, and a new refund of that bet is refused", initial: 100000,
			steps: []step{
				{name: "bet", spec: bet("b1", 5000), wantStatus: processed, wantBalance: 95000},
				{name: "refund", spec: reversal("REFUND", "r1", "b1", 5000), wantStatus: processed, wantBalance: 100000},
				{name: "rollback refund", spec: reversal("ROLLBACK", "r2", "r1", 5000), wantStatus: processed, wantBalance: 95000},
				{name: "refund again", spec: reversal("REFUND", "r3", "b1", 5000), wantStatus: rejected, wantCode: wager.CodeReferenceAlreadyReversed},
			},
			wantBalance: 95000, wantVersion: 4, wantLedger: 3,
		},
		{
			name: "second rollback of the same bet is refused", initial: 100000,
			steps: []step{
				{name: "bet", spec: bet("b1", 5000), wantStatus: processed, wantBalance: 95000},
				{name: "rollback", spec: reversal("ROLLBACK", "r1", "b1", 5000), wantStatus: processed, wantBalance: 100000},
				{name: "rollback again", spec: reversal("ROLLBACK", "r2", "b1", 5000), wantStatus: rejected, wantCode: wager.CodeReferenceAlreadyReversed},
			},
			wantBalance: 100000, wantVersion: 3, wantLedger: 2,
		},
		{
			name: "rollback of a win the player already spent has its own failure code", initial: 100000,
			steps: []step{
				{name: "win", spec: spec{ext: "w1", kind: "WIN", minor: 10000}, wantStatus: processed, wantBalance: 110000},
				{name: "spend", spec: bet("b1", 105000), wantStatus: processed, wantBalance: 5000},
				{name: "rollback win", spec: reversal("ROLLBACK", "r1", "w1", 10000), wantStatus: rejected, wantCode: wager.CodeReversalInsufficientFunds},
			},
			wantBalance: 5000, wantVersion: 3, wantLedger: 2,
		},
		{
			name: "rollback of a rollback is not allowed", initial: 100000,
			steps: []step{
				{name: "bet", spec: bet("b1", 5000), wantStatus: processed, wantBalance: 95000},
				{name: "rollback", spec: reversal("ROLLBACK", "r1", "b1", 5000), wantStatus: processed, wantBalance: 100000},
				{name: "rollback the rollback", spec: reversal("ROLLBACK", "r2", "r1", 5000), wantStatus: rejected, wantCode: wager.CodeInvalidReferenceKind},
			},
			wantBalance: 100000, wantVersion: 3, wantLedger: 2,
		},
		{
			name: "refund only applies to bets", initial: 100000,
			steps: []step{
				{name: "win", spec: spec{ext: "w1", kind: "WIN", minor: 5000}, wantStatus: processed, wantBalance: 105000},
				{name: "refund a win", spec: reversal("REFUND", "r1", "w1", 5000), wantStatus: rejected, wantCode: wager.CodeInvalidReferenceKind},
			},
			wantBalance: 105000, wantVersion: 2, wantLedger: 1,
		},
		{
			name: "reversal must match round and amount", initial: 100000,
			steps: []step{
				{name: "bet", spec: bet("b1", 5000), wantStatus: processed, wantBalance: 95000},
				{name: "other round", spec: spec{ext: "r1", kind: "REFUND", ref: "b1", minor: 5000, round: "round-2"}, wantStatus: rejected, wantCode: wager.CodeReferenceMismatch},
				{name: "partial amount", spec: reversal("REFUND", "r2", "b1", 2500), wantStatus: rejected, wantCode: wager.CodeReferenceAmountMismatch},
				{name: "correct one still works", spec: reversal("REFUND", "r3", "b1", 5000), wantStatus: processed, wantBalance: 100000},
			},
			wantBalance: 100000, wantVersion: 3, wantLedger: 2,
		},
		{
			name: "reversal of a rejected bet is refused", initial: 1000,
			steps: []step{
				{name: "bet", spec: bet("b1", 5000), wantStatus: rejected, wantCode: wager.CodeInsufficientFunds},
				{name: "refund", spec: reversal("REFUND", "r1", "b1", 5000), wantStatus: rejected, wantCode: wager.CodeReferenceNotProcessed},
			},
			wantBalance: 1000, wantVersion: 1, wantLedger: 0,
		},

		// References that have not arrived yet.
		{
			name: "refund before its bet waits", initial: 100000,
			steps:       []step{{name: "refund first", spec: reversal("REFUND", "r1", "b1", 5000), wantStatus: pending}},
			wantBalance: 100000, wantVersion: 1, wantLedger: 0,
			wantEvents: []string{"WagerTransactionPendingReference"},
		},
		{
			name: "a reversal that points at a pending one waits as well", initial: 100000,
			steps: []step{
				{name: "refund first", spec: reversal("REFUND", "r1", "b1", 5000), wantStatus: pending},
				{name: "rollback of the pending refund", spec: reversal("ROLLBACK", "r2", "r1", 5000), wantStatus: pending},
			},
			wantBalance: 100000, wantVersion: 1, wantLedger: 0,
		},
		{
			name: "win that names a missing bet waits", initial: 100000,
			steps:       []step{{name: "win", spec: spec{ext: "w1", kind: "WIN", minor: 5000, ref: "b-missing"}, wantStatus: pending}},
			wantBalance: 100000, wantVersion: 1, wantLedger: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, tt.initial)
			for _, s := range tt.steps {
				out, err := h.uc.Execute(context.Background(), h.input(s.spec))
				if !errors.Is(err, s.wantErr) {
					t.Fatalf("%s: err = %v, want %v", s.name, err, s.wantErr)
				}
				if s.wantErr != nil {
					continue
				}
				if out.Status != s.wantStatus || out.FailureCode != s.wantCode || out.Replay != s.wantReplay {
					t.Fatalf("%s: status/code/replay = %s/%s/%v, want %s/%s/%v", s.name, out.Status, out.FailureCode, out.Replay, s.wantStatus, s.wantCode, s.wantReplay)
				}
				if s.wantStatus == wager.StatusProcessed && out.Balance.Minor() != s.wantBalance {
					t.Fatalf("%s: balance = %d, want %d", s.name, out.Balance.Minor(), s.wantBalance)
				}
			}
			w := h.store.wallets[walletID]
			if w.Balance().Minor() != tt.wantBalance || w.Version() != tt.wantVersion {
				t.Fatalf("wallet = balance %d version %d, want %d / %d", w.Balance().Minor(), w.Version(), tt.wantBalance, tt.wantVersion)
			}
			if len(h.store.ledger) != tt.wantLedger {
				t.Fatalf("ledger entries = %d, want %d", len(h.store.ledger), tt.wantLedger)
			}
			if tt.wantEvents != nil {
				if len(h.store.outbox) != len(tt.wantEvents) {
					t.Fatalf("events = %d, want %v", len(h.store.outbox), tt.wantEvents)
				}
				for i, typ := range tt.wantEvents {
					if h.store.outbox[i].Type != typ {
						t.Fatalf("event %d = %s, want %s", i, h.store.outbox[i].Type, typ)
					}
				}
			}
			// The stored ledger must always explain the stored balance.
			var sum int64
			for _, e := range h.store.ledger {
				if e.Direction() == wallet.Credit {
					sum += e.Amount().Minor()
				} else {
					sum -= e.Amount().Minor()
				}
			}
			if tt.initial+sum != w.Balance().Minor() {
				t.Fatalf("ledger sums to %d from %d but the wallet holds %d", sum, tt.initial, w.Balance().Minor())
			}
		})
	}
}

func TestPendingReferenceIsScheduled(t *testing.T) {
	h := newHarness(t, 100000)
	out, err := h.uc.Execute(context.Background(), h.input(spec{ext: "r1", kind: "REFUND", ref: "b1", minor: 5000}))
	if err != nil || out.Status != wager.StatusPendingReference {
		t.Fatalf("out = %+v, err = %v", out, err)
	}
	stored := h.store.txs[wallet.TxID(out.TransactionID)]
	if want := h.now.Add(10 * time.Minute); !stored.ExpiresAt().Equal(want) {
		t.Fatalf("expires at %v, want %v", stored.ExpiresAt(), want)
	}
	if next := h.store.nextAt[stored.ID()]; !next.After(h.now) || next.After(h.now.Add(2*time.Second)) {
		t.Fatalf("next attempt at %v, want about a second after %v", next, h.now)
	}
}

func TestIdempotency(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 100000)
	run := func(s spec) (Output, error) { return h.uc.Execute(ctx, h.input(s)) }

	first, err := run(spec{ext: "b1", kind: "BET", minor: 2500})
	if err != nil || first.Replay || first.Balance.Minor() != 97500 {
		t.Fatalf("first = %+v, %v", first, err)
	}
	if _, err := run(spec{ext: "b2", kind: "BET", minor: 2500}); err != nil {
		t.Fatal(err)
	}
	ledger, events := len(h.store.ledger), len(h.store.outbox)

	tests := []struct {
		name       string
		spec       spec
		wantErr    error
		wantReplay bool
		wantID     bool
		wantMinor  int64
	}{
		{name: "same request replays the original result, not the current balance", spec: spec{ext: "b1", kind: "BET", minor: 2500}, wantReplay: true, wantID: true, wantMinor: 97500},
		{name: "same key with another amount is a conflict", spec: spec{ext: "b1", kind: "BET", minor: 9999}, wantErr: ErrIdempotencyKeyReused},
		{name: "same key with another round is a conflict", spec: spec{ext: "b1", kind: "BET", minor: 2500, round: "round-9"}, wantErr: ErrIdempotencyKeyReused},
		{name: "same key with another kind is a conflict", spec: spec{ext: "b1", kind: "WIN", minor: 2500}, wantErr: ErrIdempotencyKeyReused},
		{name: "same external id under another key is a conflict", spec: spec{ext: "b1", kind: "BET", minor: 2500, key: "another-key"}, wantErr: ErrExternalIDConflict},
		{name: "same key and external id for another provider is a new operation", spec: spec{ext: "b1", kind: "BET", minor: 2500, provider: "provider-b", key: "k-b1"}, wantMinor: 92500},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := h.store.doCalls
			out, err := run(tt.spec)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if out.Replay != tt.wantReplay || out.Balance.Minor() != tt.wantMinor || (tt.wantID && out.TransactionID != first.TransactionID) {
				t.Fatalf("out = %+v", out)
			}
			if tt.wantReplay && (len(h.store.ledger) != ledger || len(h.store.outbox) != events) {
				t.Fatalf("a replay wrote: ledger %d→%d, events %d→%d", ledger, len(h.store.ledger), events, len(h.store.outbox))
			}
			if h.store.doCalls != before+1 {
				t.Fatalf("units of work = %d", h.store.doCalls-before)
			}
		})
	}

	t.Run("a rejection is replayed as a rejection", func(t *testing.T) {
		h := newHarness(t, 1000)
		first, err := h.uc.Execute(ctx, h.input(spec{ext: "b1", kind: "BET", minor: 5000}))
		if err != nil || first.Status != wager.StatusRejected {
			t.Fatalf("first = %+v, %v", first, err)
		}
		again, err := h.uc.Execute(ctx, h.input(spec{ext: "b1", kind: "BET", minor: 5000}))
		if err != nil || !again.Replay || again.Status != wager.StatusRejected || again.FailureCode != wager.CodeInsufficientFunds || again.TransactionID != first.TransactionID {
			t.Fatalf("again = %+v, %v", again, err)
		}
	})

	t.Run("a pending reference is replayed as pending", func(t *testing.T) {
		h := newHarness(t, 1000)
		first, _ := h.uc.Execute(ctx, h.input(spec{ext: "r1", kind: "REFUND", ref: "b1", minor: 500}))
		again, err := h.uc.Execute(ctx, h.input(spec{ext: "r1", kind: "REFUND", ref: "b1", minor: 500}))
		if err != nil || !again.Replay || again.Status != wager.StatusPendingReference || again.TransactionID != first.TransactionID {
			t.Fatalf("again = %+v, %v", again, err)
		}
		if len(h.store.outbox) != 1 {
			t.Fatalf("events = %d, want only the first PendingReference", len(h.store.outbox))
		}
	})
}

func TestValidationStoresNothing(t *testing.T) {
	zero, _ := money.Zero(money.BRL)
	_ = zero
	tests := []struct {
		name    string
		mutate  func(*Input)
		spec    spec
		wantErr error
	}{
		{name: "opening is internal only", spec: spec{ext: "o1", kind: "OPENING", minor: 100}, wantErr: ErrKindNotAllowed},
		{name: "unknown kind", spec: spec{ext: "x1", kind: "TIP", minor: 100}, wantErr: ErrValidation},
		{name: "missing idempotency key", spec: spec{ext: "b1", kind: "BET", minor: 100}, mutate: func(in *Input) { in.IdempotencyKey = "" }, wantErr: ErrMissingIdempotencyKey},
		{name: "missing provider", spec: spec{ext: "b1", kind: "BET", minor: 100}, mutate: func(in *Input) { in.ProviderID = "" }, wantErr: ErrValidation},
		{name: "missing external id", spec: spec{ext: "b1", kind: "BET", minor: 100}, mutate: func(in *Input) { in.ExternalTransactionID = "" }, wantErr: ErrValidation},
		{name: "missing round", spec: spec{ext: "b1", kind: "BET", minor: 100}, mutate: func(in *Input) { in.RoundID = "" }, wantErr: ErrValidation},
		{name: "missing game", spec: spec{ext: "b1", kind: "BET", minor: 100}, mutate: func(in *Input) { in.GameID = "" }, wantErr: ErrValidation},
		{name: "overlong external id", spec: spec{ext: "b1", kind: "BET", minor: 100}, mutate: func(in *Input) { in.ExternalTransactionID = string(make([]byte, 256)) }, wantErr: ErrValidation},
		{name: "wallet id is not a uuid", spec: spec{ext: "b1", kind: "BET", minor: 100}, mutate: func(in *Input) { in.WalletID = "w1" }, wantErr: ErrValidation},
		{name: "player id is not a uuid", spec: spec{ext: "b1", kind: "BET", minor: 100}, mutate: func(in *Input) { in.PlayerID = "p1" }, wantErr: ErrValidation},
		{name: "uninitialized money", spec: spec{ext: "b1", kind: "BET", minor: 100}, mutate: func(in *Input) { in.Money = money.Money{} }, wantErr: money.ErrUninitialized},
		{name: "bet of zero", spec: spec{ext: "b1", kind: "BET", minor: 0}, wantErr: money.ErrNotPositive},
		{name: "win of zero", spec: spec{ext: "w1", kind: "WIN", minor: 0}, wantErr: money.ErrNotPositive},
		{name: "negative bet", spec: spec{ext: "b1", kind: "BET", minor: -100}, wantErr: money.ErrNotPositive},
		{name: "loss with value", spec: spec{ext: "l1", kind: "LOSS", minor: 100}, wantErr: wager.ErrLossMustBeZero},
		{name: "refund without reference", spec: spec{ext: "r1", kind: "REFUND", minor: 100}, wantErr: ErrValidation},
		{name: "rollback without reference", spec: spec{ext: "r1", kind: "ROLLBACK", minor: 100}, wantErr: ErrValidation},
		{name: "bet with reference", spec: spec{ext: "b1", kind: "BET", minor: 100, ref: "x"}, wantErr: ErrValidation},
		{name: "wallet does not exist", spec: spec{ext: "b1", kind: "BET", minor: 100, wallet: "0192f291-27dd-7d3f-8071-000000000000"}, wantErr: wallet.ErrNotFound},
		{name: "wallet belongs to another player", spec: spec{ext: "b1", kind: "BET", minor: 100, player: "0192f28f-5dc0-7d58-bdb2-000000000000"}, wantErr: ErrPlayerWalletMismatch},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, 100000)
			in := h.input(tt.spec)
			if tt.mutate != nil {
				tt.mutate(&in)
			}

			_, err := h.uc.Execute(context.Background(), in)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if len(h.store.txs)+len(h.store.ledger)+len(h.store.outbox) != 0 || h.balance() != 100000 {
				t.Fatalf("a rejected request left state behind: %d txs, %d ledger, %d events", len(h.store.txs), len(h.store.ledger), len(h.store.outbox))
			}
		})
	}
}

func TestRetriesAndFailures(t *testing.T) {
	errTransient := errors.Join(usecase.ErrTransient, errors.New("deadlock"))
	errPermanent := errors.Join(usecase.ErrPermanent, errors.New("corrupt"))

	t.Run("a transient failure is retried and then succeeds", func(t *testing.T) {
		h := newHarness(t, 100000)
		h.store.doErrs = []error{errTransient, errTransient}

		out, err := h.uc.Execute(context.Background(), h.input(spec{ext: "b1", kind: "BET", minor: 2500}))

		if err != nil || out.Status != wager.StatusProcessed || h.store.doCalls != 3 {
			t.Fatalf("out = %+v, err = %v, units of work = %d", out, err, h.store.doCalls)
		}
		if h.balance() != 97500 || len(h.store.ledger) != 1 {
			t.Fatalf("balance %d, ledger %d: the operation must be applied exactly once", h.balance(), len(h.store.ledger))
		}
	})

	t.Run("giving up after the maximum attempts reports a transient error and stores nothing", func(t *testing.T) {
		h := newHarness(t, 100000)
		h.store.doErrs = []error{errTransient, errTransient, errTransient, errTransient}

		_, err := h.uc.Execute(context.Background(), h.input(spec{ext: "b1", kind: "BET", minor: 2500}))

		if !errors.Is(err, usecase.ErrTransient) || h.store.doCalls != 3 {
			t.Fatalf("err = %v, units of work = %d", err, h.store.doCalls)
		}
		if len(h.store.txs) != 0 || h.balance() != 100000 {
			t.Fatal("state changed although every attempt failed")
		}
	})

	t.Run("retries stop when the caller gives up", func(t *testing.T) {
		h := newHarness(t, 100000)
		h.store.doErrs = []error{errTransient, errTransient}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := h.uc.Execute(ctx, h.input(spec{ext: "b1", kind: "BET", minor: 2500}))

		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("a permanent failure is not retried and is recorded as FAILED", func(t *testing.T) {
		h := newHarness(t, 100000)
		h.store.lockErr = errPermanent

		_, err := h.uc.Execute(context.Background(), h.input(spec{ext: "b1", kind: "BET", minor: 2500}))

		if !errors.Is(err, usecase.ErrPermanent) {
			t.Fatalf("err = %v", err)
		}
		if len(h.store.txs) != 1 {
			t.Fatalf("stored transactions = %d, want the FAILED audit row", len(h.store.txs))
		}
		for _, tx := range h.store.txs {
			if tx.Status() != wager.StatusFailed || tx.FailureCode() != wager.CodeInternalInvariantViolation {
				t.Fatalf("stored %s/%s", tx.Status(), tx.FailureCode())
			}
		}
		if h.balance() != 100000 || len(h.store.ledger) != 0 {
			t.Fatal("a failed operation moved money")
		}

		h.store.lockErr = nil
		again, err := h.uc.Execute(context.Background(), h.input(spec{ext: "b1", kind: "BET", minor: 2500}))
		if err != nil || !again.Replay || again.Status != wager.StatusFailed {
			t.Fatalf("replay of a failed operation = %+v, %v", again, err)
		}
	})

	t.Run("a failure while writing events undoes the wallet and the ledger", func(t *testing.T) {
		h := newHarness(t, 100000)
		h.store.outboxErr = errors.New("outbox down")

		_, err := h.uc.Execute(context.Background(), h.input(spec{ext: "b1", kind: "BET", minor: 2500}))

		if err == nil {
			t.Fatal("expected an error")
		}
		if h.balance() != 100000 || len(h.store.ledger) != 0 || len(h.store.txs) != 0 || h.store.wallets[walletID].Version() != 1 {
			t.Fatalf("partial state: balance %d, ledger %d, txs %d", h.balance(), len(h.store.ledger), len(h.store.txs))
		}
	})
}

func TestPayloadHash(t *testing.T) {
	h := newHarness(t, 0)
	base := spec{ext: "b1", kind: "BET", minor: 2500}
	hashOf := func(in Input) string {
		c, err := prepare(in)
		if err != nil {
			t.Fatal(err)
		}
		return string(c.hash)
	}
	ref := hashOf(h.input(base))

	same := []struct {
		name   string
		mutate func(*Input)
	}{
		{name: "idempotency key", mutate: func(in *Input) { in.IdempotencyKey = "another" }},
		{name: "correlation id", mutate: func(in *Input) { in.CorrelationID = "corr" }},
		{name: "causation id", mutate: func(in *Input) { in.CausationID = "msg-1" }},
	}
	for _, tt := range same {
		t.Run("ignores "+tt.name, func(t *testing.T) {
			in := h.input(base)
			tt.mutate(&in)
			if hashOf(in) != ref {
				t.Fatal("transport metadata changed the hash")
			}
		})
	}

	differs := []struct {
		name   string
		mutate func(*Input)
	}{
		{name: "provider", mutate: func(in *Input) { in.ProviderID = "provider-b" }},
		{name: "external id", mutate: func(in *Input) { in.ExternalTransactionID = "b2" }},
		{name: "player", mutate: func(in *Input) { in.PlayerID = "0192f28f-5dc0-7d58-bdb2-000000000000" }},
		{name: "wallet", mutate: func(in *Input) { in.WalletID = "0192f291-27dd-7d3f-8071-000000000000" }},
		{name: "round", mutate: func(in *Input) { in.RoundID = "round-2" }},
		{name: "game", mutate: func(in *Input) { in.GameID = "game-2" }},
		{name: "kind", mutate: func(in *Input) { in.Kind = "WIN" }},
		{name: "amount", mutate: func(in *Input) { in.Money, _ = money.FromMinor(2501, money.BRL) }},
		{name: "currency", mutate: func(in *Input) { in.Money, _ = money.FromMinor(2500, money.USD) }},
		{name: "reference", mutate: func(in *Input) { in.ReferenceExternalTransactionID = "b0" }},
	}
	seen := map[string]string{ref: "base"}
	for _, tt := range differs {
		t.Run("covers "+tt.name, func(t *testing.T) {
			in := h.input(base)
			tt.mutate(&in)
			got := hashOf(in)
			if prev, dup := seen[got]; dup {
				t.Fatalf("hash equals the one of %s", prev)
			}
			seen[got] = tt.name
		})
	}

	t.Run("is deterministic and 32 bytes", func(t *testing.T) {
		for range 20 {
			if hashOf(h.input(base)) != ref {
				t.Fatal("hash changed between calls")
			}
		}
		if len(ref) != 32 {
			t.Fatalf("hash length = %d", len(ref))
		}
	})
}

func TestInbox(t *testing.T) {
	ctx := context.Background()
	inbox := func(id, hash string) *usecase.InboxMessage {
		return &usecase.InboxMessage{Consumer: "wager-transactions", MessageID: id, Hash: []byte(hash)}
	}

	tests := []struct {
		name       string
		first      *usecase.InboxMessage
		second     *usecase.InboxMessage
		secondSpec spec
		wantErr    error
		wantReplay bool
		wantLedger int
		wantInbox  int
	}{
		{
			name: "redelivery of a handled message does not touch the wallet", first: inbox("m1", "h1"), second: inbox("m1", "h1"),
			secondSpec: spec{ext: "b1", kind: "BET", minor: 2500}, wantReplay: true, wantLedger: 1, wantInbox: 1,
		},
		{
			name: "redelivery is absorbed even when the operation would be new", first: inbox("m1", "h1"), second: inbox("m1", "h1"),
			secondSpec: spec{ext: "b2", kind: "BET", minor: 2500}, wantReplay: true, wantLedger: 1, wantInbox: 1,
		},
		{
			name: "same message id with another hash is rejected and stores nothing", first: inbox("m1", "h1"), second: inbox("m1", "h2"),
			secondSpec: spec{ext: "b2", kind: "BET", minor: 2500}, wantErr: usecase.ErrInboxHashMismatch, wantLedger: 1, wantInbox: 1,
		},
		{
			name: "another message with the same operation replays through the idempotency key", first: inbox("m1", "h1"), second: inbox("m2", "h1"),
			secondSpec: spec{ext: "b1", kind: "BET", minor: 2500}, wantReplay: true, wantLedger: 1, wantInbox: 2,
		},
		{
			name: "without inbox the operation is still idempotent", first: inbox("m1", "h1"), second: nil,
			secondSpec: spec{ext: "b1", kind: "BET", minor: 2500}, wantReplay: true, wantLedger: 1, wantInbox: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, 100000)
			in := h.input(spec{ext: "b1", kind: "BET", minor: 2500})
			in.Inbox = tt.first
			if _, err := h.uc.Execute(ctx, in); err != nil {
				t.Fatal(err)
			}

			in = h.input(tt.secondSpec)
			in.Inbox = tt.second
			out, err := h.uc.Execute(ctx, in)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err == nil && out.Replay != tt.wantReplay {
				t.Fatalf("replay = %v, want %v", out.Replay, tt.wantReplay)
			}
			if len(h.store.ledger) != tt.wantLedger || len(h.store.inbox) != tt.wantInbox {
				t.Fatalf("ledger = %d, inbox = %d, want %d and %d", len(h.store.ledger), len(h.store.inbox), tt.wantLedger, tt.wantInbox)
			}
		})
	}

	t.Run("a failed unit of work leaves no inbox row, so the redelivery is processed", func(t *testing.T) {
		h := newHarness(t, 100000)
		h.store.outboxErr = errors.New("outbox down")
		in := h.input(spec{ext: "b1", kind: "BET", minor: 2500})
		in.Inbox = inbox("m1", "h1")
		if _, err := h.uc.Execute(ctx, in); err == nil {
			t.Fatal("want an error")
		}
		if len(h.store.inbox) != 0 || len(h.store.ledger) != 0 {
			t.Fatalf("inbox = %d, ledger = %d after rollback", len(h.store.inbox), len(h.store.ledger))
		}
		h.store.outboxErr = nil
		out, err := h.uc.Execute(ctx, in)
		if err != nil || out.Replay || out.Status != wager.StatusProcessed || len(h.store.ledger) != 1 {
			t.Fatalf("out = %+v, %v, ledger = %d", out, err, len(h.store.ledger))
		}
	})
}

func TestResume(t *testing.T) {
	ctx := context.Background()
	// pendingRefund parks a refund of a bet that does not exist yet.
	pendingRefund := func(t *testing.T, h *harness) string {
		t.Helper()
		out, err := h.uc.Execute(ctx, h.input(spec{ext: "r1", kind: "REFUND", ref: "b1", minor: 2500}))
		if err != nil || out.Status != wager.StatusPendingReference {
			t.Fatalf("parked = %+v, %v", out, err)
		}
		return out.TransactionID
	}
	eventTypes := func(h *harness) (types []string) {
		for _, e := range h.store.outbox {
			types = append(types, e.Type)
		}
		return types
	}

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *harness)
		after      time.Duration
		wantStatus wager.Status
		wantCode   wager.FailureCode
		wantMinor  int64
		wantEvents []string // emitted by the resume itself
	}{
		{
			name:       "the reference arrived and the operation completes",
			setup:      func(t *testing.T, h *harness) { mustExecute(t, h, spec{ext: "b1", kind: "BET", minor: 2500}) },
			wantStatus: wager.StatusProcessed, wantMinor: 100000,
			wantEvents: []string{"WalletBalanceChanged", "WagerTransactionProcessed"},
		},
		{
			name:       "still missing before the deadline: it keeps waiting without a new event",
			wantStatus: wager.StatusPendingReference, wantMinor: 100000,
		},
		{
			name: "still missing after the deadline: rejected as not found", after: 11 * time.Minute,
			wantStatus: wager.StatusRejected, wantCode: wager.CodeReferenceNotFound, wantMinor: 100000,
			wantEvents: []string{"WagerTransactionRejected"},
		},
		{
			name: "the reference is itself waiting, so the deadline rejects as not processed", after: 11 * time.Minute,
			setup: func(t *testing.T, h *harness) {
				// A rollback of the refund: the reference exists but is pending.
				if out, err := h.uc.Execute(ctx, h.input(spec{ext: "b1", kind: "ROLLBACK", ref: "never", minor: 2500})); err != nil || out.Status != wager.StatusPendingReference {
					t.Fatalf("setup = %+v, %v", out, err)
				}
			},
			wantStatus: wager.StatusRejected, wantCode: wager.CodeReferenceNotProcessed, wantMinor: 100000,
			wantEvents: []string{"WagerTransactionRejected"},
		},
		{
			name: "a reference that ended without success rejects at once, before the deadline",
			setup: func(t *testing.T, h *harness) {
				if out, err := h.uc.Execute(ctx, h.input(spec{ext: "b1", kind: "BET", minor: 999999})); err != nil || out.Status != wager.StatusRejected {
					t.Fatalf("setup = %+v, %v", out, err)
				}
			},
			wantStatus: wager.StatusRejected, wantCode: wager.CodeReferenceNotProcessed, wantMinor: 100000,
			wantEvents: []string{"WagerTransactionRejected"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, 100000)
			id := pendingRefund(t, h)
			if tt.setup != nil {
				tt.setup(t, h)
			}
			beforeEvents := eventTypes(h)

			out, err := h.resume(h.at(tt.after), id)
			if err != nil {
				t.Fatal(err)
			}
			if out.Status != tt.wantStatus || out.FailureCode != tt.wantCode {
				t.Fatalf("out = %+v", out)
			}
			if got := h.balance(); got != tt.wantMinor {
				t.Fatalf("balance = %d, want %d", got, tt.wantMinor)
			}
			got := eventTypes(h)[len(beforeEvents):]
			if len(got) != len(tt.wantEvents) {
				t.Fatalf("new events = %v, want %v", got, tt.wantEvents)
			}
			if tt.wantStatus == wager.StatusPendingReference && (len(h.store.delays) != 1 || h.store.txs[wallet.TxID(id)].Attempts() != 1) {
				t.Fatalf("delays = %v, attempts = %d", h.store.delays, h.store.txs[wallet.TxID(id)].Attempts())
			}
		})
	}

	t.Run("the waiting time grows with the attempts and is capped", func(t *testing.T) {
		h := newHarness(t, 100000)
		id := pendingRefund(t, h)
		for range 12 {
			if _, err := h.resume(h.at(0), id); err != nil {
				t.Fatal(err)
			}
		}
		d := h.store.delays
		within := func(i int, want time.Duration) {
			t.Helper()
			if lo, hi := want*8/10, want*12/10; d[i] < lo || d[i] > hi {
				t.Fatalf("delay %d = %v, want %v ±20%%", i, d[i], want)
			}
		}
		within(0, time.Second)
		within(1, 2*time.Second)
		within(2, 4*time.Second)
		within(5, 32*time.Second)
		within(6, time.Minute)
		within(11, time.Minute)
	})

	t.Run("a transaction settled by another replica is not found, so nothing happens twice", func(t *testing.T) {
		h := newHarness(t, 100000)
		id := pendingRefund(t, h)
		mustExecute(t, h, spec{ext: "b1", kind: "BET", minor: 2500})
		if _, err := h.resume(h.at(0), id); err != nil {
			t.Fatal(err)
		}
		ledger := len(h.store.ledger)
		if _, err := h.resume(h.at(0), id); !errors.Is(err, wager.ErrNotFound) {
			t.Fatalf("second resume err = %v, want ErrNotFound", err)
		}
		if len(h.store.ledger) != ledger {
			t.Fatal("the second resume wrote to the ledger")
		}
	})

	t.Run("a new transaction wakes the ones waiting for it", func(t *testing.T) {
		h := newHarness(t, 100000)
		mustExecute(t, h, spec{ext: "b1", kind: "BET", minor: 2500})
		if len(h.store.woken) != 1 || h.store.woken[0] != "provider-a/b1" {
			t.Fatalf("woken = %v", h.store.woken)
		}
		// A replay is not a new transaction: nobody waits on it any more than before.
		mustExecute(t, h, spec{ext: "b1", kind: "BET", minor: 2500})
		if len(h.store.woken) != 1 {
			t.Fatalf("woken after replay = %v", h.store.woken)
		}
	})
}

func mustExecute(t *testing.T, h *harness, s spec) Output {
	t.Helper()
	out, err := h.uc.Execute(context.Background(), h.input(s))
	if err != nil {
		t.Fatal(err)
	}
	return out
}
