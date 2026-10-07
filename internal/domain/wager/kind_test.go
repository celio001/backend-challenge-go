package wager

import (
	"testing"

	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
)

func TestKindMovement(t *testing.T) {
	tests := []struct {
		name      string
		kind, ref Kind
		wantDir   wallet.Direction
		wantMoves bool
	}{
		{"bet debits", KindBet, "", wallet.Debit, true},
		{"win credits", KindWin, "", wallet.Credit, true},
		{"refund credits", KindRefund, KindBet, wallet.Credit, true},
		{"opening credits", KindOpening, "", wallet.Credit, true},
		{"loss does not move", KindLoss, "", "", false},
		{"rollback of bet credits", KindRollback, KindBet, wallet.Credit, true},
		{"rollback of win debits", KindRollback, KindWin, wallet.Debit, true},
		{"rollback of refund debits", KindRollback, KindRefund, wallet.Debit, true},
		{"rollback of rollback does not move", KindRollback, KindRollback, "", false},
		{"rollback without reference does not move", KindRollback, "", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, moves := tt.kind.Movement(tt.ref)
			if dir != tt.wantDir || moves != tt.wantMoves {
				t.Fatalf("got (%q, %v), want (%q, %v)", dir, moves, tt.wantDir, tt.wantMoves)
			}
		})
	}
}

func TestStatusIsTerminal(t *testing.T) {
	tests := []struct {
		status Status
		want   bool
	}{
		{StatusPending, false},
		{StatusPendingReference, false},
		{StatusProcessed, true},
		{StatusRejected, true},
		{StatusFailed, true},
	}
	for _, tt := range tests {
		t.Run(string(tt.status), func(t *testing.T) {
			if tt.status.IsTerminal() != tt.want {
				t.Fatalf("IsTerminal(%s) != %v", tt.status, tt.want)
			}
		})
	}
}
