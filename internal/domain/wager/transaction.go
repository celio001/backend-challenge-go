package wager

import (
	"errors"
	"fmt"
	"time"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
)

const payloadHashSize = 32

var (
	ErrNotFound           = errors.New("wager: transaction not found")
	ErrMissingField       = errors.New("wager: missing required field")
	ErrUnexpectedField    = errors.New("wager: field not applicable")
	ErrReferenceRequired  = errors.New("wager: reference external transaction id is required")
	ErrTerminalState      = errors.New("wager: transaction is in a terminal state")
	ErrInvalidTransition  = errors.New("wager: invalid state transition")
	ErrInvalidStatus      = errors.New("wager: invalid status")
	ErrInvalidFailureCode = errors.New("wager: invalid failure code")
	ErrInvalidTimestamp   = errors.New("wager: invalid timestamp")
	ErrInvalidResult      = errors.New("wager: invalid result balance")
	ErrInconsistentState  = errors.New("wager: inconsistent persisted state")
)

type Transaction struct {
	id        wallet.TxID
	origin    Origin
	kind      Kind
	status    Status
	walletID  wallet.WalletID
	playerID  wallet.PlayerID
	amount    money.Money
	createdAt time.Time
	updatedAt time.Time

	providerID            string
	externalTransactionID string
	idempotencyKey        string
	payloadHash           []byte
	roundID               string
	gameID                string
	referenceExternalID   string
	correlationID         string

	referenceTxID wallet.TxID
	failureCode   FailureCode
	resultBalance money.Money
	expiresAt     time.Time
}

type ExternalInput struct {
	ID                    wallet.TxID
	ProviderID            string
	ExternalTransactionID string
	IdempotencyKey        string
	PayloadHash           []byte
	WalletID              wallet.WalletID
	PlayerID              wallet.PlayerID
	RoundID               string
	GameID                string
	Kind                  Kind
	Amount                money.Money
	ReferenceExternalID   string
	CorrelationID         string
}

func NewExternal(in ExternalInput, now time.Time) (*Transaction, error) {
	if in.Kind == KindOpening {
		return nil, ErrKindNotAllowed
	}
	if !in.Kind.valid() {
		return nil, fmt.Errorf("%w: %q", ErrInvalidKind, string(in.Kind))
	}
	t := &Transaction{
		id:                    in.ID,
		origin:                OriginExternal,
		kind:                  in.Kind,
		status:                StatusPending,
		walletID:              in.WalletID,
		playerID:              in.PlayerID,
		amount:                in.Amount,
		createdAt:             now.UTC(),
		updatedAt:             now.UTC(),
		providerID:            in.ProviderID,
		externalTransactionID: in.ExternalTransactionID,
		idempotencyKey:        in.IdempotencyKey,
		payloadHash:           append([]byte(nil), in.PayloadHash...),
		roundID:               in.RoundID,
		gameID:                in.GameID,
		referenceExternalID:   in.ReferenceExternalID,
		correlationID:         in.CorrelationID,
	}
	if err := t.validateIdentity(); err != nil {
		return nil, err
	}
	return t, nil
}

// NewOpening creates the internal wallet opening, already PROCESSED with the initial balance as its result.
func NewOpening(id wallet.TxID, walletID wallet.WalletID, playerID wallet.PlayerID, amount money.Money, correlationID string, now time.Time) (*Transaction, error) {
	t := &Transaction{
		id:            id,
		origin:        OriginInternal,
		kind:          KindOpening,
		status:        StatusProcessed,
		walletID:      walletID,
		playerID:      playerID,
		amount:        amount,
		createdAt:     now.UTC(),
		updatedAt:     now.UTC(),
		correlationID: correlationID,
		resultBalance: amount,
	}
	if err := t.validateIdentity(); err != nil {
		return nil, err
	}
	return t, nil
}

type Snapshot struct {
	ID                    wallet.TxID
	Origin                Origin
	Kind                  Kind
	Status                Status
	WalletID              wallet.WalletID
	PlayerID              wallet.PlayerID
	Amount                money.Money
	ProviderID            string
	ExternalTransactionID string
	IdempotencyKey        string
	PayloadHash           []byte
	RoundID               string
	GameID                string
	ReferenceExternalID   string
	ReferenceTxID         wallet.TxID
	FailureCode           FailureCode
	ResultBalance         money.Money
	CorrelationID         string
	ExpiresAt             time.Time
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

// Rehydrate restores persisted state, validating it without replaying any transition.
func Rehydrate(s Snapshot) (*Transaction, error) {
	if !s.Status.valid() {
		return nil, fmt.Errorf("%w: %q", ErrInvalidStatus, string(s.Status))
	}
	if !s.Kind.valid() {
		return nil, fmt.Errorf("%w: %q", ErrInvalidKind, string(s.Kind))
	}
	if s.CreatedAt.IsZero() || s.UpdatedAt.IsZero() || s.UpdatedAt.Before(s.CreatedAt) {
		return nil, ErrInvalidTimestamp
	}
	t := &Transaction{
		id:                    s.ID,
		origin:                s.Origin,
		kind:                  s.Kind,
		status:                s.Status,
		walletID:              s.WalletID,
		playerID:              s.PlayerID,
		amount:                s.Amount,
		createdAt:             s.CreatedAt.UTC(),
		updatedAt:             s.UpdatedAt.UTC(),
		providerID:            s.ProviderID,
		externalTransactionID: s.ExternalTransactionID,
		idempotencyKey:        s.IdempotencyKey,
		payloadHash:           append([]byte(nil), s.PayloadHash...),
		roundID:               s.RoundID,
		gameID:                s.GameID,
		referenceExternalID:   s.ReferenceExternalID,
		correlationID:         s.CorrelationID,
		referenceTxID:         s.ReferenceTxID,
		failureCode:           s.FailureCode,
		resultBalance:         s.ResultBalance,
		expiresAt:             s.ExpiresAt.UTC(),
	}
	if err := t.validateIdentity(); err != nil {
		return nil, err
	}
	if err := t.validateState(); err != nil {
		return nil, err
	}
	return t, nil
}

type textField struct{ name, value string }

func missing(field string) error { return fmt.Errorf("%w: %s", ErrMissingField, field) }

func unexpected(field string) error { return fmt.Errorf("%w: %s", ErrUnexpectedField, field) }

func (t *Transaction) validateIdentity() error {
	if t.id == "" {
		return missing("id")
	}
	if t.walletID == "" {
		return missing("walletId")
	}
	if t.playerID == "" {
		return missing("playerId")
	}
	if t.createdAt.IsZero() {
		return ErrInvalidTimestamp
	}
	if err := t.kind.checkAmount(t.amount); err != nil {
		return err
	}

	switch t.origin {
	case OriginInternal:
		if t.kind != KindOpening {
			return ErrKindNotAllowed
		}
		for _, f := range []textField{
			{"providerId", t.providerID}, {"externalTransactionId", t.externalTransactionID},
			{"idempotencyKey", t.idempotencyKey}, {"roundId", t.roundID}, {"gameId", t.gameID},
			{"referenceExternalTransactionId", t.referenceExternalID},
		} {
			if f.value != "" {
				return unexpected(f.name)
			}
		}
		if len(t.payloadHash) != 0 {
			return unexpected("payloadHash")
		}
	case OriginExternal:
		return t.validateExternal()
	default:
		return fmt.Errorf("%w: origin %q", ErrInconsistentState, string(t.origin))
	}
	return nil
}

func (t *Transaction) validateExternal() error {
	if t.kind == KindOpening {
		return ErrKindNotAllowed
	}
	for _, f := range []textField{
		{"providerId", t.providerID}, {"externalTransactionId", t.externalTransactionID},
		{"idempotencyKey", t.idempotencyKey}, {"roundId", t.roundID}, {"gameId", t.gameID},
	} {
		if f.value == "" {
			return missing(f.name)
		}
	}
	if len(t.payloadHash) != payloadHashSize {
		return missing("payloadHash")
	}
	switch {
	case t.kind.isReversal() && t.referenceExternalID == "":
		return ErrReferenceRequired
	case (t.kind == KindBet || t.kind == KindLoss) && t.referenceExternalID != "":
		return unexpected("referenceExternalTransactionId")
	}
	return nil
}

func (t *Transaction) validateState() error {
	switch t.status {
	case StatusProcessed:
		if !t.resultBalance.IsValid() || t.resultBalance.IsNegative() || t.resultBalance.Currency() != t.amount.Currency() {
			return ErrInvalidResult
		}
		if t.failureCode != "" {
			return ErrInconsistentState
		}
		if t.origin == OriginInternal && t.resultBalance.Minor() != t.amount.Minor() {
			return ErrInvalidResult
		}
	case StatusRejected:
		if !t.failureCode.isRejection() {
			return ErrInvalidFailureCode
		}
	case StatusFailed:
		if t.failureCode != CodeInternalInvariantViolation {
			return ErrInvalidFailureCode
		}
	case StatusPendingReference:
		if t.expiresAt.IsZero() {
			return fmt.Errorf("%w: pending reference without expiry", ErrInconsistentState)
		}
	}
	if t.status != StatusProcessed && t.resultBalance.IsValid() {
		return fmt.Errorf("%w: result balance on %s", ErrInconsistentState, t.status)
	}
	if t.status != StatusRejected && t.status != StatusFailed && t.failureCode != "" {
		return ErrInconsistentState
	}
	return nil
}

func (t *Transaction) ID() wallet.TxID               { return t.id }
func (t *Transaction) Origin() Origin                { return t.origin }
func (t *Transaction) Kind() Kind                    { return t.kind }
func (t *Transaction) Status() Status                { return t.status }
func (t *Transaction) WalletID() wallet.WalletID     { return t.walletID }
func (t *Transaction) PlayerID() wallet.PlayerID     { return t.playerID }
func (t *Transaction) Amount() money.Money           { return t.amount }
func (t *Transaction) ProviderID() string            { return t.providerID }
func (t *Transaction) ExternalTransactionID() string { return t.externalTransactionID }
func (t *Transaction) IdempotencyKey() string        { return t.idempotencyKey }
func (t *Transaction) PayloadHash() []byte           { return append([]byte(nil), t.payloadHash...) }
func (t *Transaction) RoundID() string               { return t.roundID }
func (t *Transaction) GameID() string                { return t.gameID }
func (t *Transaction) ReferenceExternalID() string   { return t.referenceExternalID }
func (t *Transaction) ReferenceTxID() wallet.TxID    { return t.referenceTxID }
func (t *Transaction) FailureCode() FailureCode      { return t.failureCode }
func (t *Transaction) ResultBalance() money.Money    { return t.resultBalance }
func (t *Transaction) CorrelationID() string         { return t.correlationID }
func (t *Transaction) ExpiresAt() time.Time          { return t.expiresAt }
func (t *Transaction) CreatedAt() time.Time          { return t.createdAt }
func (t *Transaction) UpdatedAt() time.Time          { return t.updatedAt }
func (t *Transaction) IsTerminal() bool              { return t.status.IsTerminal() }

func (t *Transaction) MarkPendingReference(expiresAt, now time.Time) error {
	if err := t.guard(StatusPending, now); err != nil {
		return err
	}
	if t.referenceExternalID == "" {
		return ErrInvalidTransition
	}
	if expiresAt.IsZero() || !expiresAt.After(now) {
		return ErrInvalidTimestamp
	}
	t.status = StatusPendingReference
	t.expiresAt = expiresAt.UTC()
	t.updatedAt = now.UTC()
	return nil
}

func (t *Transaction) LinkReference(id wallet.TxID, now time.Time) error {
	if err := t.guard(StatusPending, now, StatusPendingReference); err != nil {
		return err
	}
	if id == "" {
		return missing("referenceTransactionId")
	}
	t.referenceTxID = id
	t.updatedAt = now.UTC()
	return nil
}

// Process completes the operation; resultBalance is the wallet balance returned to the provider on replays.
func (t *Transaction) Process(resultBalance money.Money, now time.Time) error {
	if err := t.guard(StatusPending, now, StatusPendingReference); err != nil {
		return err
	}
	if !resultBalance.IsValid() || resultBalance.IsNegative() || resultBalance.Currency() != t.amount.Currency() {
		return ErrInvalidResult
	}
	t.status = StatusProcessed
	t.resultBalance = resultBalance
	t.updatedAt = now.UTC()
	return nil
}

func (t *Transaction) Reject(code FailureCode, now time.Time) error {
	if err := t.guard(StatusPending, now, StatusPendingReference); err != nil {
		return err
	}
	if !code.isRejection() {
		return fmt.Errorf("%w: %q", ErrInvalidFailureCode, string(code))
	}
	t.status = StatusRejected
	t.failureCode = code
	t.updatedAt = now.UTC()
	return nil
}

// Fail records a permanent infrastructure failure for audit.
func (t *Transaction) Fail(now time.Time) error {
	if err := t.guard(StatusPending, now, StatusPendingReference); err != nil {
		return err
	}
	t.status = StatusFailed
	t.failureCode = CodeInternalInvariantViolation
	t.updatedAt = now.UTC()
	return nil
}

func (t *Transaction) guard(from Status, now time.Time, alsoFrom ...Status) error {
	if t.status.IsTerminal() {
		return ErrTerminalState
	}
	if now.IsZero() || now.Before(t.updatedAt) {
		return ErrInvalidTimestamp
	}
	if t.status == from {
		return nil
	}
	for _, s := range alsoFrom {
		if t.status == s {
			return nil
		}
	}
	return fmt.Errorf("%w: from %s", ErrInvalidTransition, t.status)
}
