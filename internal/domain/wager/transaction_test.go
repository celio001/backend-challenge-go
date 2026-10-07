package wager

import (
	"errors"
	"testing"
	"time"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
)

func TestNewExternal(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*ExternalInput)
		wantErr error
	}{
		{"bet", func(in *ExternalInput) {}, nil},
		{"win without reference", func(in *ExternalInput) { in.Kind = KindWin }, nil},
		{"win with reference", func(in *ExternalInput) { in.Kind = KindWin; in.ReferenceExternalID = "ext-0" }, nil},
		{"loss zero", func(in *ExternalInput) { in.Kind = KindLoss; in.Amount = brl(t, 0) }, nil},
		{"refund", func(in *ExternalInput) { in.Kind = KindRefund; in.ReferenceExternalID = "ext-0" }, nil},
		{"rollback", func(in *ExternalInput) { in.Kind = KindRollback; in.ReferenceExternalID = "ext-0" }, nil},
		{"opening rejected", func(in *ExternalInput) { in.Kind = KindOpening }, ErrKindNotAllowed},
		{"unknown kind", func(in *ExternalInput) { in.Kind = "PAYOUT" }, ErrInvalidKind},
		{"empty kind", func(in *ExternalInput) { in.Kind = "" }, ErrInvalidKind},
		{"bet zero", func(in *ExternalInput) { in.Amount = brl(t, 0) }, money.ErrNotPositive},
		{"bet negative", func(in *ExternalInput) { in.Amount = brl(t, -1) }, money.ErrNotPositive},
		{"win zero", func(in *ExternalInput) { in.Kind = KindWin; in.Amount = brl(t, 0) }, money.ErrNotPositive},
		{"refund zero", func(in *ExternalInput) { in.Kind = KindRefund; in.ReferenceExternalID = "x"; in.Amount = brl(t, 0) }, money.ErrNotPositive},
		{"rollback zero", func(in *ExternalInput) { in.Kind = KindRollback; in.ReferenceExternalID = "x"; in.Amount = brl(t, 0) }, money.ErrNotPositive},
		{"loss positive", func(in *ExternalInput) { in.Kind = KindLoss }, ErrLossMustBeZero},
		{"uninitialized amount", func(in *ExternalInput) { in.Amount = money.Money{} }, money.ErrUninitialized},
		{"refund without reference", func(in *ExternalInput) { in.Kind = KindRefund; in.ReferenceExternalID = "" }, ErrReferenceRequired},
		{"rollback without reference", func(in *ExternalInput) { in.Kind = KindRollback; in.ReferenceExternalID = "" }, ErrReferenceRequired},
		{"bet with reference", func(in *ExternalInput) { in.ReferenceExternalID = "x" }, ErrUnexpectedField},
		{"loss with reference", func(in *ExternalInput) {
			in.Kind = KindLoss
			in.Amount = brl(t, 0)
			in.ReferenceExternalID = "x"
		}, ErrUnexpectedField},
		{"missing id", func(in *ExternalInput) { in.ID = "" }, ErrMissingField},
		{"missing provider", func(in *ExternalInput) { in.ProviderID = "" }, ErrMissingField},
		{"missing external id", func(in *ExternalInput) { in.ExternalTransactionID = "" }, ErrMissingField},
		{"missing idempotency key", func(in *ExternalInput) { in.IdempotencyKey = "" }, ErrMissingField},
		{"missing hash", func(in *ExternalInput) { in.PayloadHash = nil }, ErrMissingField},
		{"short hash", func(in *ExternalInput) { in.PayloadHash = []byte{1} }, ErrMissingField},
		{"missing wallet", func(in *ExternalInput) { in.WalletID = "" }, ErrMissingField},
		{"missing player", func(in *ExternalInput) { in.PlayerID = "" }, ErrMissingField},
		{"missing round", func(in *ExternalInput) { in.RoundID = "" }, ErrMissingField},
		{"missing game", func(in *ExternalInput) { in.GameID = "" }, ErrMissingField},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := input(t, KindBet, 2500)
			tt.mutate(&in)
			tx, err := NewExternal(in, t0)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if tx.Status() != StatusPending || tx.Origin() != OriginExternal || tx.Kind() != in.Kind ||
				tx.ProviderID() != in.ProviderID || tx.IsTerminal() || tx.ResultBalance().IsValid() {
				t.Fatalf("unexpected initial state: %+v", tx)
			}
		})
	}
}

func TestNewExternalCopiesPayloadHash(t *testing.T) {
	in := input(t, KindBet, 100)
	tx, err := NewExternal(in, t0)
	if err != nil {
		t.Fatal(err)
	}
	in.PayloadHash[0] = 9
	tx.PayloadHash()[1] = 9
	if h := tx.PayloadHash(); h[0] != 0 || h[1] != 0 {
		t.Fatal("payload hash must be immutable from the outside")
	}
}

func TestNewOpening(t *testing.T) {
	tests := []struct {
		name     string
		id       wallet.TxID
		walletID wallet.WalletID
		player   wallet.PlayerID
		amount   money.Money
		wantErr  error
	}{
		{"ok", "tx1", "w1", "p1", brl(t, 100000), nil},
		{"zero rejected", "tx1", "w1", "p1", brl(t, 0), money.ErrNotPositive},
		{"negative rejected", "tx1", "w1", "p1", brl(t, -1), money.ErrNotPositive},
		{"uninitialized", "tx1", "w1", "p1", money.Money{}, money.ErrUninitialized},
		{"missing id", "", "w1", "p1", brl(t, 1), ErrMissingField},
		{"missing wallet", "tx1", "", "p1", brl(t, 1), ErrMissingField},
		{"missing player", "tx1", "w1", "", brl(t, 1), ErrMissingField},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx, err := NewOpening(tt.id, tt.walletID, tt.player, tt.amount, "corr", t0)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if tx.Kind() != KindOpening || tx.Origin() != OriginInternal || tx.Status() != StatusProcessed ||
				tx.ResultBalance().Minor() != tt.amount.Minor() || tx.ProviderID() != "" ||
				tx.ExternalTransactionID() != "" || tx.IdempotencyKey() != "" || tx.RoundID() != "" || tx.GameID() != "" {
				t.Fatalf("unexpected opening: %+v", tx)
			}
		})
	}
}

func TestTransitions(t *testing.T) {
	later := t0.Add(time.Second)
	expiry := t0.Add(10 * time.Minute)

	type step func(*Transaction) error
	process := func(tx *Transaction) error { return tx.Process(brl(t, 500), later) }
	reject := func(tx *Transaction) error { return tx.Reject(CodeInsufficientFunds, later) }
	fail := func(tx *Transaction) error { return tx.Fail(later) }
	park := func(tx *Transaction) error { return tx.MarkPendingReference(expiry, t0) }

	tests := []struct {
		name       string
		kind       Kind
		minor      int64
		steps      []step
		wantErr    error
		wantStatus Status
	}{
		{"pending to processed", KindBet, 100, []step{process}, nil, StatusProcessed},
		{"pending to rejected", KindBet, 100, []step{reject}, nil, StatusRejected},
		{"pending to failed", KindBet, 100, []step{fail}, nil, StatusFailed},
		{"pending to pending reference", KindRefund, 100, []step{park}, nil, StatusPendingReference},
		{"pending reference to processed", KindRefund, 100, []step{park, process}, nil, StatusProcessed},
		{"pending reference to rejected", KindRefund, 100, []step{park, reject}, nil, StatusRejected},
		{"pending reference to failed", KindRefund, 100, []step{park, fail}, nil, StatusFailed},
		{"bet cannot wait for reference", KindBet, 100, []step{park}, ErrInvalidTransition, StatusPending},
		{"pending reference twice", KindRefund, 100, []step{park, park}, ErrInvalidTransition, StatusPendingReference},
		{"processed then processed", KindBet, 100, []step{process, process}, ErrTerminalState, StatusProcessed},
		{"processed then rejected", KindBet, 100, []step{process, reject}, ErrTerminalState, StatusProcessed},
		{"processed then failed", KindBet, 100, []step{process, fail}, ErrTerminalState, StatusProcessed},
		{"rejected then processed", KindBet, 100, []step{reject, process}, ErrTerminalState, StatusRejected},
		{"rejected then rejected", KindBet, 100, []step{reject, reject}, ErrTerminalState, StatusRejected},
		{"failed then processed", KindBet, 100, []step{fail, process}, ErrTerminalState, StatusFailed},
		{"terminal cannot wait", KindRefund, 100, []step{process, park}, ErrTerminalState, StatusProcessed},
		{"loss processes", KindLoss, 0, []step{process}, nil, StatusProcessed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx := newTx(t, tt.kind, tt.minor)
			var err error
			for _, s := range tt.steps {
				if err = s(tx); err != nil {
					break
				}
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tx.Status() != tt.wantStatus {
				t.Fatalf("status = %s, want %s", tx.Status(), tt.wantStatus)
			}
		})
	}
}

func TestProcessAndRejectArguments(t *testing.T) {
	usd, err := money.FromMinor(1, money.USD)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		act     func(*Transaction) error
		wantErr error
	}{
		{"process ok", func(tx *Transaction) error { return tx.Process(brl(t, 0), t0) }, nil},
		{"process uninitialized", func(tx *Transaction) error { return tx.Process(money.Money{}, t0) }, ErrInvalidResult},
		{"process negative", func(tx *Transaction) error { return tx.Process(brl(t, -1), t0) }, ErrInvalidResult},
		{"process other currency", func(tx *Transaction) error { return tx.Process(usd, t0) }, ErrInvalidResult},
		{"process zero time", func(tx *Transaction) error { return tx.Process(brl(t, 1), time.Time{}) }, ErrInvalidTimestamp},
		{"process before last update", func(tx *Transaction) error { return tx.Process(brl(t, 1), t0.Add(-time.Second)) }, ErrInvalidTimestamp},
		{"reject empty code", func(tx *Transaction) error { return tx.Reject("", t0) }, ErrInvalidFailureCode},
		{"reject correctable code", func(tx *Transaction) error { return tx.Reject("VALIDATION_ERROR", t0) }, ErrInvalidFailureCode},
		{"reject internal code", func(tx *Transaction) error { return tx.Reject(CodeInternalInvariantViolation, t0) }, ErrInvalidFailureCode},
		{"reject reversal funds", func(tx *Transaction) error { return tx.Reject(CodeReversalInsufficientFunds, t0) }, nil},
		{"link empty", func(tx *Transaction) error { return tx.LinkReference("", t0) }, ErrMissingField},
		{"link on terminal", func(tx *Transaction) error {
			if err := tx.Process(brl(t, 1), t0); err != nil {
				t.Fatal(err)
			}
			return tx.LinkReference("ref1", t0)
		}, ErrTerminalState},
		{"link ok", func(tx *Transaction) error { return tx.LinkReference("ref1", t0) }, nil},
		{"park expiry in the past", func(tx *Transaction) error { return tx.MarkPendingReference(t0, t0) }, ErrInvalidTimestamp},
		{"park zero expiry", func(tx *Transaction) error { return tx.MarkPendingReference(time.Time{}, t0) }, ErrInvalidTimestamp},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx := newTx(t, KindRefund, 100)
			if err := tt.act(tx); !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestTerminalStateKeepsPersistedResult(t *testing.T) {
	tx := newTx(t, KindBet, 100)
	if err := tx.Process(brl(t, 900), t0.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Reject(CodeInsufficientFunds, t0.Add(2*time.Second)); !errors.Is(err, ErrTerminalState) {
		t.Fatalf("err = %v", err)
	}
	if tx.ResultBalance().Minor() != 900 || tx.FailureCode() != "" || !tx.UpdatedAt().Equal(t0.Add(time.Second)) {
		t.Fatal("terminal transaction must not change")
	}
}

func TestRehydrate(t *testing.T) {
	base := func() Snapshot {
		return Snapshot{
			ID: "tx1", Origin: OriginExternal, Kind: KindBet, Status: StatusPending,
			WalletID: "w1", PlayerID: "p1", Amount: brl(t, 100),
			ProviderID: "provider-a", ExternalTransactionID: "e1", IdempotencyKey: "k1",
			PayloadHash: hash(), RoundID: "r1", GameID: "g1",
			CreatedAt: t0, UpdatedAt: t0,
		}
	}
	tests := []struct {
		name    string
		mutate  func(*Snapshot)
		wantErr error
	}{
		{"pending", func(s *Snapshot) {}, nil},
		{"processed", func(s *Snapshot) { s.Status = StatusProcessed; s.ResultBalance = brl(t, 50) }, nil},
		{"rejected", func(s *Snapshot) { s.Status = StatusRejected; s.FailureCode = CodeInsufficientFunds }, nil},
		{"failed", func(s *Snapshot) { s.Status = StatusFailed; s.FailureCode = CodeInternalInvariantViolation }, nil},
		{"pending reference", func(s *Snapshot) {
			s.Kind, s.ReferenceExternalID, s.Status = KindRefund, "e0", StatusPendingReference
			s.ExpiresAt = t0.Add(time.Minute)
		}, nil},
		{"opening", func(s *Snapshot) {
			*s = Snapshot{
				ID: "tx1", Origin: OriginInternal, Kind: KindOpening, Status: StatusProcessed,
				WalletID: "w1", PlayerID: "p1", Amount: brl(t, 100), ResultBalance: brl(t, 100),
				CreatedAt: t0, UpdatedAt: t0,
			}
		}, nil},
		{"opening with other result", func(s *Snapshot) {
			*s = Snapshot{
				ID: "tx1", Origin: OriginInternal, Kind: KindOpening, Status: StatusProcessed,
				WalletID: "w1", PlayerID: "p1", Amount: brl(t, 100), ResultBalance: brl(t, 99),
				CreatedAt: t0, UpdatedAt: t0,
			}
		}, ErrInvalidResult},
		{"invalid status", func(s *Snapshot) { s.Status = "DONE" }, ErrInvalidStatus},
		{"invalid kind", func(s *Snapshot) { s.Kind = "PAYOUT" }, ErrInvalidKind},
		{"updated before created", func(s *Snapshot) { s.UpdatedAt = t0.Add(-time.Second) }, ErrInvalidTimestamp},
		{"processed without result", func(s *Snapshot) { s.Status = StatusProcessed }, ErrInvalidResult},
		{"processed with other currency result", func(s *Snapshot) {
			s.Status = StatusProcessed
			usd, err := money.FromMinor(1, money.USD)
			if err != nil {
				t.Fatal(err)
			}
			s.ResultBalance = usd
		}, ErrInvalidResult},
		{"processed with failure code", func(s *Snapshot) {
			s.Status, s.ResultBalance, s.FailureCode = StatusProcessed, brl(t, 1), CodeInsufficientFunds
		}, ErrInconsistentState},
		{"rejected without code", func(s *Snapshot) { s.Status = StatusRejected }, ErrInvalidFailureCode},
		{"rejected with internal code", func(s *Snapshot) {
			s.Status, s.FailureCode = StatusRejected, CodeInternalInvariantViolation
		}, ErrInvalidFailureCode},
		{"failed with business code", func(s *Snapshot) {
			s.Status, s.FailureCode = StatusFailed, CodeInsufficientFunds
		}, ErrInvalidFailureCode},
		{"pending with failure code", func(s *Snapshot) { s.FailureCode = CodeInsufficientFunds }, ErrInconsistentState},
		{"pending with result", func(s *Snapshot) { s.ResultBalance = brl(t, 1) }, ErrInconsistentState},
		{"pending reference without expiry", func(s *Snapshot) {
			s.Kind, s.ReferenceExternalID, s.Status = KindRefund, "e0", StatusPendingReference
		}, ErrInconsistentState},
		{"unknown origin", func(s *Snapshot) { s.Origin = "OTHER" }, ErrInconsistentState},
		{"internal with provider", func(s *Snapshot) {
			s.Origin, s.Kind, s.Amount = OriginInternal, KindOpening, brl(t, 1)
			s.Status, s.ResultBalance = StatusProcessed, brl(t, 1)
		}, ErrUnexpectedField},
		{"internal non opening", func(s *Snapshot) {
			*s = Snapshot{
				ID: "tx1", Origin: OriginInternal, Kind: KindBet, Status: StatusPending,
				WalletID: "w1", PlayerID: "p1", Amount: brl(t, 1), CreatedAt: t0, UpdatedAt: t0,
			}
		}, ErrKindNotAllowed},
		{"external opening", func(s *Snapshot) { s.Kind = KindOpening }, ErrKindNotAllowed},
		{"external missing provider", func(s *Snapshot) { s.ProviderID = "" }, ErrMissingField},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := base()
			tt.mutate(&s)
			tx, err := Rehydrate(s)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err == nil && (tx.Status() != s.Status || tx.ID() != s.ID || tx.FailureCode() != s.FailureCode) {
				t.Fatalf("state not preserved: %+v", tx)
			}
		})
	}
}
