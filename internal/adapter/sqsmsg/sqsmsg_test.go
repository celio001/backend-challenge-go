package sqsmsg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wager"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/internal/usecase"
	"github.com/celio001/backend-challenge-go/internal/usecase/processwager"
)

type fakeProcess struct {
	calls []processwager.Input
	out   processwager.Output
	err   error
}

func (f *fakeProcess) Execute(_ context.Context, in processwager.Input) (processwager.Output, error) {
	f.calls = append(f.calls, in)
	return f.out, f.err
}

const validBody = `{
  "messageId": "msg-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-09-08T12:00:00.000Z",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "transaction-123",
    "idempotencyKey": "provider-a:transaction-123",
    "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
    "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
    "roundId": "round-987",
    "gameId": "fortune-chimp",
    "kind": "BET",
    "money": { "amount": "25.00", "currency": "BRL" }
  }
}`

var senders = map[string]string{"sender-a": "provider-a", "sender-b": "provider-b"}

func TestHandleTranslatesToTheSameCommandAsHTTP(t *testing.T) {
	p := &fakeProcess{}
	res := NewHandler(p, senders).Handle(context.Background(), Delivery{Body: validBody, SenderID: "sender-a"})
	if res.Action != Delete || res.MessageID != "msg-123" || len(p.calls) != 1 {
		t.Fatalf("result = %+v, calls = %d", res, len(p.calls))
	}

	in := p.calls[0]
	want := processwager.Input{
		ProviderID: "provider-a", ExternalTransactionID: "transaction-123", IdempotencyKey: "provider-a:transaction-123",
		PlayerID: "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1", WalletID: "0192f291-27dd-7d3f-8071-5f8685deef37",
		RoundID: "round-987", GameID: "fortune-chimp", Kind: "BET", CorrelationID: "msg-123", CausationID: "msg-123",
	}
	got := in
	got.Money, got.Inbox = money.Money{}, nil
	if got != want {
		t.Fatalf("input = %+v, want %+v", got, want)
	}
	if in.Money.Minor() != 2500 || in.Money.Currency() != money.BRL {
		t.Fatalf("money = %v", in.Money)
	}
	if in.Inbox == nil || in.Inbox.Consumer != ConsumerName || in.Inbox.MessageID != "msg-123" || len(in.Inbox.Hash) != 32 {
		t.Fatalf("inbox = %+v", in.Inbox)
	}
}

func TestHandleRejectsBeforeTouchingTheUseCase(t *testing.T) {
	replace := func(old, new string) string { return strings.Replace(validBody, old, new, 1) }
	tests := []struct {
		name   string
		body   string
		sender string
		want   string
	}{
		{name: "not json", body: `nope`, sender: "sender-a", want: CodeMalformed},
		{name: "trailing data", body: validBody + `{}`, sender: "sender-a", want: CodeMalformed},
		{name: "unknown envelope field", body: replace(`"type"`, `"extra": 1, "type"`), sender: "sender-a", want: CodeMalformed},
		{name: "unknown data field", body: replace(`"kind"`, `"extra": 1, "kind"`), sender: "sender-a", want: CodeMalformed},
		{name: "missing message id", body: replace(`"messageId": "msg-123",`, ``), sender: "sender-a", want: CodeMalformed},
		{name: "message id too long", body: replace(`msg-123`, strings.Repeat("m", 256)), sender: "sender-a", want: CodeMalformed},
		{name: "wrong type", body: replace(`WagerTransactionRequested`, `Other`), sender: "sender-a", want: CodeMalformed},
		{name: "bad occurredAt", body: replace(`2026-09-08T12:00:00.000Z`, `yesterday`), sender: "sender-a", want: CodeMalformed},
		{name: "missing money", body: replace(`"money": { "amount": "25.00", "currency": "BRL" }`, `"extra": "x"`), sender: "sender-a", want: CodeMalformed},
		{name: "amount as a number", body: replace(`"25.00"`, `25.00`), sender: "sender-a", want: CodeMalformed},
		{name: "amount in scientific notation", body: replace(`"25.00"`, `"2.5e1"`), sender: "sender-a", want: CodeMalformed},
		{name: "amount with excess scale", body: replace(`"25.00"`, `"25.001"`), sender: "sender-a", want: CodeMalformed},
		{name: "unknown sender", body: validBody, sender: "intruder", want: CodeIdentityMismatch},
		{name: "no sender", body: validBody, sender: "", want: CodeIdentityMismatch},
		{name: "sender of another provider", body: validBody, sender: "sender-b", want: CodeIdentityMismatch},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &fakeProcess{}
			res := NewHandler(p, senders).Handle(context.Background(), Delivery{Body: tt.body, SenderID: tt.sender})
			if res.Action != DeadLetter || res.Code != tt.want || res.Err == nil {
				t.Fatalf("result = %+v", res)
			}
			if len(p.calls) != 0 {
				t.Fatalf("the use case ran for a message that must be rejected")
			}
		})
	}
}

func TestHandleClassifiesUseCaseErrors(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantAction Action
		wantCode   string
	}{
		{name: "validation", err: fmt.Errorf("%w: bad", processwager.ErrValidation), wantAction: DeadLetter, wantCode: CodeMalformed},
		{name: "missing key", err: processwager.ErrMissingIdempotencyKey, wantAction: DeadLetter, wantCode: CodeMalformed},
		{name: "uninitialized money", err: money.ErrUninitialized, wantAction: DeadLetter, wantCode: CodeMalformed},
		{name: "key reused", err: processwager.ErrIdempotencyKeyReused, wantAction: DeadLetter, wantCode: CodeKeyReused},
		{name: "external id conflict", err: processwager.ErrExternalIDConflict, wantAction: DeadLetter, wantCode: CodeExternalConflict},
		{name: "message id reused", err: fmt.Errorf("register: %w", usecase.ErrInboxHashMismatch), wantAction: DeadLetter, wantCode: CodeMessageIDReused},
		{name: "kind not allowed", err: processwager.ErrKindNotAllowed, wantAction: DeadLetter, wantCode: CodeKindNotAllowed},
		{name: "player mismatch", err: processwager.ErrPlayerWalletMismatch, wantAction: DeadLetter, wantCode: CodePlayerMismatch},
		{name: "wallet not found", err: wallet.ErrNotFound, wantAction: DeadLetter, wantCode: CodeWalletNotFound},
		{name: "permanent failure", err: fmt.Errorf("%w: disk", usecase.ErrPermanent), wantAction: DeadLetter, wantCode: CodePermanentFailure},
		{name: "transient failure", err: fmt.Errorf("%w: db down", usecase.ErrTransient), wantAction: Retry},
		{name: "stale wallet", err: usecase.ErrStaleWallet, wantAction: Retry},
		{name: "canceled", err: context.Canceled, wantAction: Retry},
		{name: "unknown", err: errors.New("boom"), wantAction: Retry},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &fakeProcess{err: tt.err}
			res := NewHandler(p, senders).Handle(context.Background(), Delivery{Body: validBody, SenderID: "sender-a"})
			if res.Action != tt.wantAction || res.Code != tt.wantCode || !errors.Is(res.Err, tt.err) || res.MessageID != "msg-123" {
				t.Fatalf("result = %+v", res)
			}
		})
	}
}

// Business outcomes are durable results, so none of them leaves the message on the queue.
func TestHandleDeletesEveryDurableOutcome(t *testing.T) {
	for _, tt := range []struct {
		name string
		out  processwager.Output
	}{
		{name: "processed", out: processwager.Output{Status: wager.StatusProcessed}},
		{name: "rejected", out: processwager.Output{Status: wager.StatusRejected, FailureCode: wager.CodeInsufficientFunds}},
		{name: "pending reference", out: processwager.Output{Status: wager.StatusPendingReference}},
		{name: "replay", out: processwager.Output{Replay: true}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			res := NewHandler(&fakeProcess{out: tt.out}, senders).Handle(context.Background(), Delivery{Body: validBody, SenderID: "sender-a"})
			if res.Action != Delete || res.Err != nil {
				t.Fatalf("result = %+v", res)
			}
		})
	}
}

func TestInboxHash(t *testing.T) {
	hashOfBody := func(body string) []byte {
		t.Helper()
		p := &fakeProcess{}
		NewHandler(p, senders).Handle(context.Background(), Delivery{Body: body, SenderID: "sender-a"})
		if len(p.calls) != 1 {
			t.Fatalf("body was not accepted: %s", body)
		}
		return p.calls[0].Inbox.Hash
	}
	replace := func(old, new string) string { return strings.Replace(validBody, old, new, 1) }
	base := hashOfBody(validBody)

	tests := []struct {
		name string
		body string
		same bool
	}{
		{name: "key order and spacing do not matter", body: `{"data":{"money":{"currency":"BRL","amount":"25.00"},"kind":"BET","gameId":"fortune-chimp","roundId":"round-987","walletId":"0192f291-27dd-7d3f-8071-5f8685deef37","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","idempotencyKey":"provider-a:transaction-123","externalTransactionId":"transaction-123","providerId":"provider-a"},"occurredAt":"2026-09-08T12:00:00.000Z","type":"WagerTransactionRequested","messageId":"msg-123"}`, same: true},
		{name: "occurredAt is transport metadata", body: replace(`2026-09-08T12:00:00.000Z`, `2026-09-09T00:00:00Z`), same: true},
		{name: "another amount", body: replace(`"25.00"`, `"25.01"`)},
		{name: "another idempotency key", body: replace(`provider-a:transaction-123`, `provider-a:other`)},
		{name: "another round", body: replace(`round-987`, `round-1`)},
		{name: "another kind", body: replace(`"BET"`, `"WIN"`)},
		{name: "a reference", body: replace(`"kind": "BET",`, `"kind": "BET", "referenceExternalTransactionId": "x",`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := bytes.Equal(hashOfBody(tt.body), base); got != tt.same {
				t.Fatalf("equal = %v, want %v", got, tt.same)
			}
		})
	}
}
