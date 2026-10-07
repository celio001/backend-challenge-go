package httpapi

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wager"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/internal/usecase"
	"github.com/celio001/backend-challenge-go/internal/usecase/processwager"
)

const betBody = `{"providerId":"provider-a","externalTransactionId":"transaction-123","playerId":"` + playerID + `","walletId":"` + walletID +
	`","roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}`

func postWager(e env, token, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/wagering/transactions", strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if key != "" {
		req.Header.Set(idempotencyHeader, key)
	}
	req.Header.Set(correlationHeader, "corr-7")
	rec := httptest.NewRecorder()
	e.h.ServeHTTP(rec, req)
	return rec
}

func TestPostWagerAuthorization(t *testing.T) {
	tests := []struct {
		name       string
		token      string
		key        string
		body       string
		wantStatus int
		wantCode   string
	}{
		{name: "no token", key: "k", body: betBody, wantStatus: 401, wantCode: "UNAUTHENTICATED"},
		{name: "wallet admin cannot send wagers", token: "admin", key: "k", body: betBody, wantStatus: 403, wantCode: "FORBIDDEN"},
		{name: "read-only provider cannot send wagers", token: "read-only", key: "k", body: betBody, wantStatus: 403, wantCode: "FORBIDDEN"},
		{name: "provider in the body is not the authenticated one", token: "provider-b", key: "k", body: betBody, wantStatus: 403, wantCode: "FORBIDDEN"},
		{name: "token without provider identity", token: "no-provider", key: "k", body: betBody, wantStatus: 403, wantCode: "FORBIDDEN"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			rec := postWager(e, tt.token, tt.key, tt.body)
			if rec.Code != tt.wantStatus || code(t, rec) != tt.wantCode {
				t.Fatalf("status/code = %d/%s, want %d/%s", rec.Code, code(t, rec), tt.wantStatus, tt.wantCode)
			}
			if len(e.process.calls) != 0 {
				t.Fatal("the use case ran for an unauthorized caller")
			}
		})
	}
}

func TestPostWager(t *testing.T) {
	balance := brlMinor(t, 97500)
	tests := []struct {
		name       string
		key        string
		body       string
		out        processwager.Output
		err        error
		wantStatus int
		wantCode   string
		wantBody   string
		wantCalls  int
		wantHeader string
	}{
		{
			name: "processed", key: "provider-a:transaction-123", body: betBody, wantCalls: 1, wantStatus: 200,
			out:      processwager.Output{TransactionID: "tx-1", Status: wager.StatusProcessed, Balance: balance},
			wantBody: `{"transactionId":"tx-1","status":"PROCESSED","balance":{"amount":"975.00","currency":"BRL"},"idempotentReplay":false}`,
		},
		{
			name: "replay", key: "k", body: betBody, wantCalls: 1, wantStatus: 200,
			out:      processwager.Output{TransactionID: "tx-1", Status: wager.StatusProcessed, Balance: balance, Replay: true},
			wantBody: `{"transactionId":"tx-1","status":"PROCESSED","balance":{"amount":"975.00","currency":"BRL"},"idempotentReplay":true}`,
		},
		{
			name: "waiting for the reference", key: "k", body: betBody, wantCalls: 1, wantStatus: 202,
			out:      processwager.Output{TransactionID: "tx-2", Status: wager.StatusPendingReference},
			wantBody: `{"transactionId":"tx-2","status":"PENDING_REFERENCE","idempotentReplay":false}`,
		},
		{
			name: "business rejection", key: "k", body: betBody, wantCalls: 1, wantStatus: 422,
			out:      processwager.Output{TransactionID: "tx-3", Status: wager.StatusRejected, FailureCode: wager.CodeInsufficientFunds},
			wantBody: `{"transactionId":"tx-3","status":"REJECTED","failureCode":"INSUFFICIENT_FUNDS","idempotentReplay":false}`,
		},
		{
			name: "recorded permanent failure", key: "k", body: betBody, wantCalls: 1, wantStatus: 500,
			out:      processwager.Output{TransactionID: "tx-4", Status: wager.StatusFailed, FailureCode: wager.CodeInternalInvariantViolation, Replay: true},
			wantBody: `{"transactionId":"tx-4","status":"FAILED","failureCode":"INTERNAL_INVARIANT_VIOLATION","idempotentReplay":true}`,
		},

		{name: "missing idempotency key", body: betBody, wantStatus: 400, wantCode: "MISSING_IDEMPOTENCY_KEY"},
		{name: "malformed json", key: "k", body: `{`, wantStatus: 400, wantCode: "INVALID_REQUEST"},
		{name: "unknown field", key: "k", body: strings.Replace(betBody, `"kind"`, `"extra":1,"kind"`, 1), wantStatus: 400, wantCode: "INVALID_REQUEST"},
		{name: "amount as a json number", key: "k", body: strings.Replace(betBody, `"25.00"`, `25.00`, 1), wantStatus: 400, wantCode: "INVALID_MONEY"},
		{name: "amount with extra scale", key: "k", body: strings.Replace(betBody, `"25.00"`, `"25.001"`, 1), wantStatus: 400, wantCode: "INVALID_MONEY"},
		{name: "missing money", key: "k", body: strings.Replace(betBody, `,"money":{"amount":"25.00","currency":"BRL"}`, ``, 1), wantCalls: 1, err: money.ErrUninitialized, wantStatus: 400, wantCode: "INVALID_MONEY"},

		{name: "validation error", key: "k", body: betBody, wantCalls: 1, err: errors.Join(processwager.ErrValidation, errors.New("walletId")), wantStatus: 400, wantCode: "VALIDATION_ERROR"},
		{name: "kind not allowed", key: "k", body: betBody, wantCalls: 1, err: processwager.ErrKindNotAllowed, wantStatus: 400, wantCode: "KIND_NOT_ALLOWED"},
		{name: "not positive", key: "k", body: betBody, wantCalls: 1, err: money.ErrNotPositive, wantStatus: 400, wantCode: "INVALID_MONEY"},
		{name: "loss with value", key: "k", body: betBody, wantCalls: 1, err: wager.ErrLossMustBeZero, wantStatus: 400, wantCode: "INVALID_MONEY"},
		{name: "wallet not found is correctable", key: "k", body: betBody, wantCalls: 1, err: wallet.ErrNotFound, wantStatus: 422, wantCode: "WALLET_NOT_FOUND"},
		{name: "wallet of another player", key: "k", body: betBody, wantCalls: 1, err: processwager.ErrPlayerWalletMismatch, wantStatus: 422, wantCode: "PLAYER_WALLET_MISMATCH"},
		{name: "idempotency key reused", key: "k", body: betBody, wantCalls: 1, err: processwager.ErrIdempotencyKeyReused, wantStatus: 409, wantCode: "IDEMPOTENCY_KEY_REUSED"},
		{name: "external id under another key", key: "k", body: betBody, wantCalls: 1, err: processwager.ErrExternalIDConflict, wantStatus: 409, wantCode: "EXTERNAL_TRANSACTION_ID_CONFLICT"},
		{name: "transient infrastructure failure", key: "k", body: betBody, wantCalls: 1, err: errors.Join(usecase.ErrTransient, errors.New("deadlock")), wantStatus: 503, wantCode: "TEMPORARILY_UNAVAILABLE", wantHeader: "1"},
		{name: "unexpected failure", key: "k", body: betBody, wantCalls: 1, err: errors.New("boom"), wantStatus: 500, wantCode: "INTERNAL_ERROR"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			e.process.out, e.process.err = tt.out, tt.err

			rec := postWager(e, "provider", tt.key, tt.body)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (%s)", rec.Code, tt.wantStatus, rec.Body)
			}
			if tt.wantCode != "" && code(t, rec) != tt.wantCode {
				t.Fatalf("code = %s, want %s", code(t, rec), tt.wantCode)
			}
			if tt.wantBody != "" && strings.TrimSpace(rec.Body.String()) != tt.wantBody {
				t.Fatalf("body = %s\nwant %s", rec.Body, tt.wantBody)
			}
			if got := rec.Header().Get("Retry-After"); got != tt.wantHeader {
				t.Fatalf("Retry-After = %q, want %q", got, tt.wantHeader)
			}
			if len(e.process.calls) != tt.wantCalls {
				t.Fatalf("use case calls = %d, want %d", len(e.process.calls), tt.wantCalls)
			}
		})
	}

	t.Run("the provider and key come from the credentials and headers, metadata from the request", func(t *testing.T) {
		e := newEnv(t)
		e.process.out = processwager.Output{TransactionID: "tx-1", Status: wager.StatusProcessed, Balance: balance}

		postWager(e, "provider", "provider-a:transaction-123", betBody)

		in := e.process.calls[0]
		if in.ProviderID != "provider-a" || in.IdempotencyKey != "provider-a:transaction-123" || in.ExternalTransactionID != "transaction-123" ||
			in.PlayerID != playerID || in.WalletID != walletID || in.RoundID != "round-987" || in.GameID != "fortune-chimp" ||
			in.Kind != "BET" || in.Money.Minor() != 2500 || in.CorrelationID != "corr-7" || in.ReferenceExternalTransactionID != "" {
			t.Fatalf("input = %+v", in)
		}
	})

	t.Run("a reversal carries its reference", func(t *testing.T) {
		e := newEnv(t)
		e.process.out = processwager.Output{TransactionID: "tx-1", Status: wager.StatusPendingReference}
		body := strings.Replace(betBody, `"kind":"BET"`, `"kind":"REFUND","referenceExternalTransactionId":"transaction-100"`, 1)

		postWager(e, "provider", "k", body)

		if got := e.process.calls[0]; got.Kind != "REFUND" || got.ReferenceExternalTransactionID != "transaction-100" {
			t.Fatalf("input = %+v", got)
		}
	})
}

func brlMinor(t *testing.T, minor int64) money.Money {
	t.Helper()
	m, err := money.FromMinor(minor, money.BRL)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func testTransaction(t *testing.T, provider string) *wager.Transaction {
	t.Helper()
	at := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	tx, err := wager.NewExternal(wager.ExternalInput{
		ID: "0192f298-345e-7e38-af88-e43f851a819d", ProviderID: provider, ExternalTransactionID: "transaction-123", IdempotencyKey: "k",
		PayloadHash: make([]byte, 32), WalletID: walletID, PlayerID: playerID, RoundID: "round-987", GameID: "fortune-chimp",
		Kind: wager.KindRefund, Amount: brlMinor(t, 2500), ReferenceExternalID: "transaction-100",
	}, at)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.MarkPendingReference(at.Add(10*time.Minute), at); err != nil {
		t.Fatal(err)
	}
	return tx
}

func TestReadTransactions(t *testing.T) {
	const id = "0192f298-345e-7e38-af88-e43f851a819d"
	wantPending := `{"transactionId":"` + id + `","status":"PENDING_REFERENCE","providerId":"provider-a","externalTransactionId":"transaction-123","walletId":"` + walletID +
		`","playerId":"` + playerID + `","roundId":"round-987","gameId":"fortune-chimp","kind":"REFUND","money":{"amount":"25.00","currency":"BRL"},` +
		`"referenceExternalTransactionId":"transaction-100","expiresAt":"2026-09-08T12:10:00.000Z","attempts":0,"createdAt":"2026-09-08T12:00:00.000Z","updatedAt":"2026-09-08T12:00:00.000Z"}`

	tests := []struct {
		name       string
		path       string
		token      string
		owner      string
		err        error
		wantStatus int
		wantCode   string
		wantBody   string
		wantLookup bool
	}{
		{name: "provider reads its own by id", path: "/wagering/transactions/" + id, token: "provider", owner: "provider-a", wantStatus: 200, wantBody: wantPending, wantLookup: true},
		{name: "provider asking for another provider's transaction sees a 404", path: "/wagering/transactions/" + id, token: "provider-b", owner: "provider-a", wantStatus: 404, wantCode: "TRANSACTION_NOT_FOUND", wantLookup: true},
		{name: "wallet admin reads any by id", path: "/wagering/transactions/" + id, token: "admin", owner: "provider-a", wantStatus: 200, wantBody: wantPending, wantLookup: true},
		{name: "unknown id", path: "/wagering/transactions/" + id, token: "provider", err: wager.ErrNotFound, wantStatus: 404, wantCode: "TRANSACTION_NOT_FOUND", wantLookup: true},
		{name: "write-only credentials cannot read", path: "/wagering/transactions/" + id, token: "write-only", owner: "provider-a", wantStatus: 403, wantCode: "FORBIDDEN"},
		{name: "no token", path: "/wagering/transactions/" + id, wantStatus: 401, wantCode: "UNAUTHENTICATED"},

		{name: "provider route, own path", path: "/providers/provider-a/wagering/transactions/transaction-123", token: "provider", owner: "provider-a", wantStatus: 200, wantBody: wantPending, wantLookup: true},
		{name: "provider route, another provider's path", path: "/providers/provider-a/wagering/transactions/transaction-123", token: "provider-b", owner: "provider-a", wantStatus: 403, wantCode: "FORBIDDEN"},
		{name: "provider route, not found", path: "/providers/provider-a/wagering/transactions/missing", token: "provider", err: wager.ErrNotFound, wantStatus: 404, wantCode: "TRANSACTION_NOT_FOUND", wantLookup: true},
		{name: "provider route is not open to the wallet admin", path: "/providers/provider-a/wagering/transactions/transaction-123", token: "admin", owner: "provider-a", wantStatus: 403, wantCode: "FORBIDDEN"},
		{name: "provider route, token without provider identity", path: "/providers/provider-a/wagering/transactions/transaction-123", token: "no-provider", wantStatus: 403, wantCode: "FORBIDDEN"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			if tt.owner != "" {
				e.txq.tx = testTransaction(t, tt.owner)
			}
			e.txq.err = tt.err

			rec := do(e.h, http.MethodGet, tt.path, tt.token, "")

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (%s)", rec.Code, tt.wantStatus, rec.Body)
			}
			if tt.wantCode != "" && code(t, rec) != tt.wantCode {
				t.Fatalf("code = %s, want %s", code(t, rec), tt.wantCode)
			}
			if tt.wantBody != "" && strings.TrimSpace(rec.Body.String()) != tt.wantBody {
				t.Fatalf("body = %s\nwant %s", rec.Body, tt.wantBody)
			}
			if (e.txq.calls > 0) != tt.wantLookup {
				t.Fatalf("lookups = %d, want a lookup: %v", e.txq.calls, tt.wantLookup)
			}
			if tt.wantStatus == 404 && strings.Contains(rec.Body.String(), "provider-a") {
				t.Fatal("a 404 leaked data about the transaction")
			}
		})
	}
}

func TestPendingProgressIsVisibleOnlyWhileWaiting(t *testing.T) {
	at := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	base := wager.Snapshot{
		ID: "0192f298-345e-7e38-af88-e43f851a819d", Origin: wager.OriginExternal, Kind: wager.KindRefund, WalletID: walletID, PlayerID: playerID,
		Amount: brlMinor(t, 2500), ProviderID: "provider-a", ExternalTransactionID: "transaction-123", IdempotencyKey: "k",
		PayloadHash: make([]byte, 32), RoundID: "round-987", GameID: "fortune-chimp", ReferenceExternalID: "transaction-100",
		CreatedAt: at, UpdatedAt: at,
	}
	tests := []struct {
		name         string
		mutate       func(*wager.Snapshot)
		wantContains []string
		wantMissing  []string
	}{
		{
			name: "waiting shows how many tries and when the next one is",
			mutate: func(s *wager.Snapshot) {
				s.Status, s.ExpiresAt, s.Attempts, s.NextAttemptAt = wager.StatusPendingReference, at.Add(10*time.Minute), 3, at.Add(8*time.Second)
			},
			wantContains: []string{`"attempts":3`, `"nextAttemptAt":"2026-09-08T12:00:08.000Z"`},
		},
		{
			name: "rejected after waiting hides the scheduling facts",
			mutate: func(s *wager.Snapshot) {
				s.Status, s.FailureCode, s.Attempts = wager.StatusRejected, wager.CodeReferenceNotFound, 7
			},
			wantContains: []string{`"failureCode":"REFERENCE_NOT_FOUND"`},
			wantMissing:  []string{"attempts", "nextAttemptAt"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snap := base
			tt.mutate(&snap)
			tx, err := wager.Rehydrate(snap)
			if err != nil {
				t.Fatal(err)
			}
			e := newEnv(t)
			e.txq.tx = tx
			rec := do(e.h, http.MethodGet, "/wagering/transactions/"+string(tx.ID()), "admin", "")
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d", rec.Code)
			}
			for _, want := range tt.wantContains {
				if !strings.Contains(rec.Body.String(), want) {
					t.Fatalf("body %s lacks %s", rec.Body.String(), want)
				}
			}
			for _, not := range tt.wantMissing {
				if strings.Contains(rec.Body.String(), not) {
					t.Fatalf("body %s has %s", rec.Body.String(), not)
				}
			}
		})
	}
}
