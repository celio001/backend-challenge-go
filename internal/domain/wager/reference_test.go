package wager

import (
	"errors"
	"testing"
	"time"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
)

func TestCheckReference(t *testing.T) {
	pendingBet := func() *Transaction {
		in := input(t, KindBet, 1000)
		in.ID, in.ExternalTransactionID = "ref1", "ext-0"
		return mustTx(t, in)
	}
	rejectedBet := func() *Transaction {
		tx := pendingBet()
		if err := tx.Reject(CodeInsufficientFunds, t0); err != nil {
			t.Fatal(err)
		}
		return tx
	}
	failedBet := func() *Transaction {
		tx := pendingBet()
		if err := tx.Fail(t0); err != nil {
			t.Fatal(err)
		}
		return tx
	}
	pendingRef := func() *Transaction {
		in := input(t, KindRefund, 1000)
		in.ID, in.ExternalTransactionID = "ref1", "ext-0"
		tx := mustTx(t, in)
		if err := tx.MarkPendingReference(t0.Add(time.Minute), t0); err != nil {
			t.Fatal(err)
		}
		return tx
	}
	mutate := func(kind Kind, minor int64, f func(*ExternalInput)) *Transaction {
		in := input(t, kind, minor)
		in.ID, in.ExternalTransactionID, in.ReferenceExternalID = "ref1", "ext-0", ""
		if kind == KindRefund || kind == KindRollback {
			in.ReferenceExternalID = "ext-x"
		}
		f(&in)
		tx, err := NewExternal(in, t0)
		if err != nil {
			t.Fatal(err)
		}
		result, err := money.FromMinor(5000, in.Amount.Currency())
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Process(result, t0); err != nil {
			t.Fatal(err)
		}
		return tx
	}
	same := func(*ExternalInput) {}
	winWithRef := func(minor int64) *Transaction {
		in := input(t, KindWin, minor)
		in.ReferenceExternalID = "ext-0"
		return mustTx(t, in)
	}

	tests := []struct {
		name    string
		tx      *Transaction
		ref     *Transaction
		wantErr error
	}{
		{"refund of bet", newTx(t, KindRefund, 1000), mutate(KindBet, 1000, same), nil},
		{"rollback of bet", newTx(t, KindRollback, 1000), mutate(KindBet, 1000, same), nil},
		{"rollback of win", newTx(t, KindRollback, 1000), mutate(KindWin, 1000, same), nil},
		{"rollback of refund", newTx(t, KindRollback, 1000), mutate(KindRefund, 1000, same), nil},
		{"win of bet with another amount", winWithRef(5000), mutate(KindBet, 1000, same), nil},
		{"refund of win", newTx(t, KindRefund, 1000), mutate(KindWin, 1000, same), ErrInvalidReferenceKind},
		{"refund of refund", newTx(t, KindRefund, 1000), mutate(KindRefund, 1000, same), ErrInvalidReferenceKind},
		{"rollback of rollback", newTx(t, KindRollback, 1000), mutate(KindRollback, 1000, same), ErrInvalidReferenceKind},
		{"rollback of loss", newTx(t, KindRollback, 1000), mutate(KindLoss, 0, same), ErrInvalidReferenceKind},
		{"win of win", winWithRef(1000), mutate(KindWin, 1000, same), ErrInvalidReferenceKind},
		{"nil reference", newTx(t, KindRefund, 1000), nil, ErrReferenceNotFound},
		{"pending reference", newTx(t, KindRefund, 1000), pendingBet(), ErrReferencePending},
		{"reference waiting for its own reference", newTx(t, KindRollback, 1000), pendingRef(), ErrReferencePending},
		{"rejected reference", newTx(t, KindRefund, 1000), rejectedBet(), ErrReferenceNotProcessed},
		{"failed reference", newTx(t, KindRefund, 1000), failedBet(), ErrReferenceNotProcessed},
		{"other provider", newTx(t, KindRefund, 1000), mutate(KindBet, 1000, func(in *ExternalInput) { in.ProviderID = "provider-b" }), ErrReferenceMismatch},
		{"other player", newTx(t, KindRefund, 1000), mutate(KindBet, 1000, func(in *ExternalInput) { in.PlayerID = "p2" }), ErrReferenceMismatch},
		{"other wallet", newTx(t, KindRefund, 1000), mutate(KindBet, 1000, func(in *ExternalInput) { in.WalletID = "w2" }), ErrReferenceMismatch},
		{"other round", newTx(t, KindRefund, 1000), mutate(KindBet, 1000, func(in *ExternalInput) { in.RoundID = "round-2" }), ErrReferenceMismatch},
		{"other currency", newTx(t, KindRefund, 1000), mutate(KindBet, 1000, func(in *ExternalInput) {
			usd, err := money.FromMinor(1000, money.USD)
			if err != nil {
				t.Fatal(err)
			}
			in.Amount = usd
		}), ErrReferenceMismatch},
		{"refund amount differs", newTx(t, KindRefund, 1000), mutate(KindBet, 1001, same), ErrReferenceAmountMismatch},
		{"rollback amount differs", newTx(t, KindRollback, 999), mutate(KindBet, 1000, same), ErrReferenceAmountMismatch},
		{"no reference id on tx", newTx(t, KindBet, 1000), mutate(KindBet, 1000, same), ErrReferenceRequired},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.tx.CheckReference(tt.ref); !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}
