// Package sqsmsg translates a WagerTransactionRequested message into the same use case command the HTTP API builds.
package sqsmsg

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/internal/usecase"
	"github.com/celio001/backend-challenge-go/internal/usecase/processwager"
	"github.com/celio001/backend-challenge-go/pkg/canonicaljson"
)

const (
	// ConsumerName is the inbox consumer identity of this handler.
	ConsumerName = "wager-transactions"
	// MessageType is the only envelope type accepted on the queue.
	MessageType = "WagerTransactionRequested"

	maxIDLen = 255
)

type Action int

const (
	// Delete means the outcome is durable (or a harmless duplicate): remove the message.
	Delete Action = iota
	// DeadLetter means retrying cannot help: park the message in the DLQ with the result code.
	DeadLetter
	// Retry means the failure may pass: leave the message for redelivery.
	Retry
)

// Stable codes sent with dead-lettered messages.
const (
	CodeMalformed        = "MALFORMED_MESSAGE"
	CodeIdentityMismatch = "PROVIDER_IDENTITY_MISMATCH"
	CodeKeyReused        = "IDEMPOTENCY_KEY_REUSED"
	CodeExternalConflict = "EXTERNAL_ID_CONFLICT"
	CodeMessageIDReused  = "MESSAGE_ID_REUSED"
	CodeKindNotAllowed   = "KIND_NOT_ALLOWED"
	CodePlayerMismatch   = "PLAYER_WALLET_MISMATCH"
	CodeWalletNotFound   = "WALLET_NOT_FOUND"
	CodePermanentFailure = "PERMANENT_FAILURE"
)

type ProcessWager interface {
	Execute(ctx context.Context, in processwager.Input) (processwager.Output, error)
}

type Delivery struct {
	Body string
	// SenderID is the SQS system attribute naming who sent the message; the body's providerId is never trusted on its own.
	SenderID string
}

type Result struct {
	Action    Action
	Code      string
	MessageID string
	Err       error
}

type Handler struct {
	process ProcessWager
	senders map[string]string // SenderId → providerId
}

func NewHandler(process ProcessWager, senders map[string]string) *Handler {
	return &Handler{process: process, senders: senders}
}

type envelope struct {
	MessageID  string          `json:"messageId"`
	Type       string          `json:"type"`
	OccurredAt string          `json:"occurredAt"`
	Data       json.RawMessage `json:"data"`
}

type data struct {
	ProviderID                     string      `json:"providerId"`
	ExternalTransactionID          string      `json:"externalTransactionId"`
	IdempotencyKey                 string      `json:"idempotencyKey"`
	PlayerID                       string      `json:"playerId"`
	WalletID                       string      `json:"walletId"`
	RoundID                        string      `json:"roundId"`
	GameID                         string      `json:"gameId"`
	Kind                           string      `json:"kind"`
	Money                          money.Money `json:"money"`
	ReferenceExternalTransactionID string      `json:"referenceExternalTransactionId"`
}

var (
	errMalformed = errors.New("sqsmsg: malformed message")
	errIdentity  = errors.New("sqsmsg: sender does not match the provider in the message")
)

// Handle never returns an error: every outcome is an action the consumer applies to the message.
func (h *Handler) Handle(ctx context.Context, d Delivery) Result {
	in, err := h.parse(d)
	if err != nil {
		return classify(in.Inbox, err)
	}

	// PROCESSED, REJECTED, PENDING_REFERENCE and replays are all durable results.
	if _, err := h.process.Execute(ctx, in); err != nil {
		return classify(in.Inbox, err)
	}
	return Result{Action: Delete, MessageID: in.Inbox.MessageID}
}

func (h *Handler) parse(d Delivery) (processwager.Input, error) {
	var env envelope
	if err := decodeStrict(d.Body, &env); err != nil {
		return processwager.Input{}, fmt.Errorf("%w: envelope: %v", errMalformed, err)
	}
	// The id is kept even on failure so the caller can log which message was rejected.
	partial := processwager.Input{Inbox: &usecase.InboxMessage{Consumer: ConsumerName, MessageID: env.MessageID}}

	if env.MessageID == "" || len(env.MessageID) > maxIDLen {
		return partial, fmt.Errorf("%w: messageId must have 1 to %d bytes", errMalformed, maxIDLen)
	}
	if env.Type != MessageType {
		return partial, fmt.Errorf("%w: unsupported type %q", errMalformed, env.Type)
	}
	if _, err := time.Parse(time.RFC3339, env.OccurredAt); err != nil {
		return partial, fmt.Errorf("%w: occurredAt: %v", errMalformed, err)
	}
	var msg data
	if err := decodeStrict(string(env.Data), &msg); err != nil {
		return partial, fmt.Errorf("%w: data: %v", errMalformed, err)
	}
	if !msg.Money.IsValid() {
		return partial, fmt.Errorf("%w: money is required", errMalformed)
	}

	provider, ok := h.senders[d.SenderID]
	if !ok || provider != msg.ProviderID {
		return partial, errIdentity
	}

	hash, err := hashOf(msg)
	if err != nil {
		return partial, fmt.Errorf("%w: %v", errMalformed, err)
	}
	return processwager.Input{
		ProviderID:                     msg.ProviderID,
		ExternalTransactionID:          msg.ExternalTransactionID,
		IdempotencyKey:                 msg.IdempotencyKey,
		PlayerID:                       msg.PlayerID,
		WalletID:                       msg.WalletID,
		RoundID:                        msg.RoundID,
		GameID:                         msg.GameID,
		Kind:                           msg.Kind,
		Money:                          msg.Money,
		ReferenceExternalTransactionID: msg.ReferenceExternalTransactionID,
		CorrelationID:                  env.MessageID,
		CausationID:                    env.MessageID,
		Inbox:                          &usecase.InboxMessage{Consumer: ConsumerName, MessageID: env.MessageID, Hash: hash},
	}, nil
}

// hashOf covers the type and every business field including the idempotency key, so a redelivery that changed anything is detected.
func hashOf(m data) ([]byte, error) {
	fields := map[string]any{
		"type":                  MessageType,
		"providerId":            m.ProviderID,
		"externalTransactionId": m.ExternalTransactionID,
		"idempotencyKey":        m.IdempotencyKey,
		"playerId":              m.PlayerID,
		"walletId":              m.WalletID,
		"roundId":               m.RoundID,
		"gameId":                m.GameID,
		"kind":                  m.Kind,
		"money":                 map[string]any{"amount": m.Money.String(), "currency": string(m.Money.Currency())},
	}
	if m.ReferenceExternalTransactionID != "" {
		fields["referenceExternalTransactionId"] = m.ReferenceExternalTransactionID
	}
	canonical, err := canonicaljson.Marshal(fields)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(canonical)
	return sum[:], nil
}

func decodeStrict(s string, v any) error {
	dec := json.NewDecoder(bytes.NewReader([]byte(s)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("unexpected data after the JSON document")
	}
	return nil
}

func classify(inbox *usecase.InboxMessage, err error) Result {
	r := Result{Action: DeadLetter, Err: err}
	if inbox != nil {
		r.MessageID = inbox.MessageID
	}
	switch {
	case errors.Is(err, errMalformed), errors.Is(err, processwager.ErrValidation),
		errors.Is(err, processwager.ErrMissingIdempotencyKey), errors.Is(err, money.ErrUninitialized):
		r.Code = CodeMalformed
	case errors.Is(err, errIdentity):
		r.Code = CodeIdentityMismatch
	case errors.Is(err, processwager.ErrIdempotencyKeyReused):
		r.Code = CodeKeyReused
	case errors.Is(err, processwager.ErrExternalIDConflict):
		r.Code = CodeExternalConflict
	case errors.Is(err, usecase.ErrInboxHashMismatch):
		r.Code = CodeMessageIDReused
	case errors.Is(err, processwager.ErrKindNotAllowed):
		r.Code = CodeKindNotAllowed
	case errors.Is(err, processwager.ErrPlayerWalletMismatch):
		r.Code = CodePlayerMismatch
	case errors.Is(err, wallet.ErrNotFound):
		r.Code = CodeWalletNotFound
	case errors.Is(err, usecase.ErrPermanent):
		r.Code = CodePermanentFailure
	default:
		// Transient failures and anything unknown: let the queue redeliver, its redrive policy bounds the attempts.
		r.Action = Retry
	}
	return r
}
