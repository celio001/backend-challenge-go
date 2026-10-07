package wager

import "errors"

var (
	ErrReferenceNotFound       = errors.New("wager: reference not found")
	ErrReferencePending        = errors.New("wager: reference is still pending")
	ErrReferenceNotProcessed   = errors.New("wager: reference did not complete successfully")
	ErrInvalidReferenceKind    = errors.New("wager: referenced kind cannot be used here")
	ErrReferenceMismatch       = errors.New("wager: reference does not match provider, player, wallet, currency or round")
	ErrReferenceAmountMismatch = errors.New("wager: amount differs from the referenced amount")
)

// CheckReference validates t against the transaction it points to, in the order: status, kind, ownership, amount.
// ErrReferencePending means keep waiting; every other error is a definitive rejection.
func (t *Transaction) CheckReference(ref *Transaction) error {
	if t.referenceExternalID == "" {
		return ErrReferenceRequired
	}
	if ref == nil {
		return ErrReferenceNotFound
	}

	switch ref.status {
	case StatusPending, StatusPendingReference:
		return ErrReferencePending
	case StatusRejected, StatusFailed:
		return ErrReferenceNotProcessed
	}

	if !t.allowsReferenceKind(ref.kind) {
		return ErrInvalidReferenceKind
	}
	if ref.providerID != t.providerID || ref.playerID != t.playerID || ref.walletID != t.walletID ||
		ref.amount.Currency() != t.amount.Currency() || ref.roundID != t.roundID {
		return ErrReferenceMismatch
	}
	if t.kind.isReversal() && ref.amount.Minor() != t.amount.Minor() {
		return ErrReferenceAmountMismatch
	}
	return nil
}

func (t *Transaction) allowsReferenceKind(ref Kind) bool {
	switch t.kind {
	case KindWin, KindRefund:
		return ref == KindBet
	case KindRollback:
		return ref == KindBet || ref == KindWin || ref == KindRefund
	}
	return false
}
