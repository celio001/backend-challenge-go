package wager

import (
	"errors"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
)

var (
	ErrKindNotAllowed = errors.New("wager: kind not allowed for this origin")
	ErrInvalidKind    = errors.New("wager: invalid kind")
	ErrLossMustBeZero = errors.New("wager: LOSS amount must be zero")
)

type Kind string
type Status string
type FailureCode string

const (
	KindOpening  Kind = "OPENING"
	KindBet      Kind = "BET"
	KindWin      Kind = "WIN"
	KindLoss     Kind = "LOSS"
	KindRefund   Kind = "REFUND"
	KindRollback Kind = "ROLLBACK"

	StatusPending          Status = "PENDING"
	StatusPendingReference Status = "PENDING_REFERENCE"
	StatusProcessed        Status = "PROCESSED"
	StatusRejected         Status = "REJECTED"
	StatusFailed           Status = "FAILED"

	CodeInsufficientFunds         FailureCode = "INSUFFICIENT_FUNDS"
	CodeReversalInsufficientFunds FailureCode = "REVERSAL_INSUFFICIENT_FUNDS"
	CodeCurrencyMismatch          FailureCode = "CURRENCY_MISMATCH"
	CodeReferenceNotFound         FailureCode = "REFERENCE_NOT_FOUND"
	CodeReferenceNotProcessed     FailureCode = "REFERENCE_NOT_PROCESSED"
	CodeReferenceAlreadyReversed  FailureCode = "REFERENCE_ALREADY_REVERSED"
	CodeReferenceMismatch         FailureCode = "REFERENCE_MISMATCH"
	CodeReferenceAmountMismatch   FailureCode = "REFERENCE_AMOUNT_MISMATCH"
	CodeInvalidReferenceKind      FailureCode = "INVALID_REFERENCE_KIND"

	CodeInternalInvariantViolation FailureCode = "INTERNAL_INVARIANT_VIOLATION"
)

func (k Kind) valid() bool {
	switch k {
	case KindOpening, KindBet, KindWin, KindLoss, KindRefund, KindRollback:
		return true
	}
	return false
}

func (k Kind) isReversal() bool { return k == KindRefund || k == KindRollback }

func (k Kind) checkAmount(m money.Money) error {
	if !m.IsValid() {
		return money.ErrUninitialized
	}
	if k == KindLoss {
		if !m.IsZero() {
			return ErrLossMustBeZero
		}
		return nil
	}
	if !m.IsPositive() {
		return money.ErrNotPositive
	}
	return nil
}

// Movement reports the wallet movement of k; ref is only used by ROLLBACK, which undoes the referenced kind.
func (k Kind) Movement(ref Kind) (wallet.Direction, bool) {
	switch k {
	case KindBet:
		return wallet.Debit, true
	case KindWin, KindRefund, KindOpening:
		return wallet.Credit, true
	case KindRollback:
		switch ref {
		case KindBet:
			return wallet.Credit, true
		case KindWin, KindRefund:
			return wallet.Debit, true
		}
	}
	return "", false
}

func (s Status) valid() bool {
	switch s {
	case StatusPending, StatusPendingReference, StatusProcessed, StatusRejected, StatusFailed:
		return true
	}
	return false
}

func (s Status) IsTerminal() bool {
	return s == StatusProcessed || s == StatusRejected || s == StatusFailed
}

type Origin string

const (
	OriginInternal Origin = "INTERNAL"
	OriginExternal Origin = "EXTERNAL"
)

// isRejection covers the persisted business rejections; correctable input errors are never stored.
func (c FailureCode) isRejection() bool {
	switch c {
	case CodeInsufficientFunds, CodeReversalInsufficientFunds, CodeCurrencyMismatch,
		CodeReferenceNotFound, CodeReferenceNotProcessed, CodeReferenceAlreadyReversed,
		CodeReferenceMismatch, CodeReferenceAmountMismatch, CodeInvalidReferenceKind:
		return true
	}
	return false
}
