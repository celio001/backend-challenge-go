package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/celio001/backend-challenge-go/internal/auth"
	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wager"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/internal/usecase/openwallet"
	"github.com/celio001/backend-challenge-go/internal/usecase/processwager"
	"github.com/celio001/backend-challenge-go/internal/usecase/queries"
)

const (
	walletID = "0192f291-27dd-7d3f-8071-5f8685deef37"
	playerID = "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1"
)

type fakeVerifier map[string]auth.Principal

func (f fakeVerifier) Verify(_ context.Context, token string) (auth.Principal, error) {
	if p, ok := f[token]; ok {
		return p, nil
	}
	return auth.Principal{}, auth.ErrUnauthenticated
}

type fakeOpen struct {
	calls []openwallet.Input
	w     *wallet.Wallet
	err   error
}

func (f *fakeOpen) Execute(_ context.Context, in openwallet.Input) (*wallet.Wallet, error) {
	f.calls = append(f.calls, in)
	return f.w, f.err
}

type fakeQueries struct {
	calls     int
	w         *wallet.Wallet
	page      queries.LedgerPage
	err       error
	gotCursor string
	gotLimit  int
}

func (f *fakeQueries) Wallet(context.Context, string) (*wallet.Wallet, error) {
	f.calls++
	return f.w, f.err
}

func (f *fakeQueries) Ledger(_ context.Context, _ string, cursor string, limit int) (queries.LedgerPage, error) {
	f.calls++
	f.gotCursor, f.gotLimit = cursor, limit
	return f.page, f.err
}

type seqIDs struct{}

func (seqIDs) NewID() string { return "generated-id" }

func brl(t *testing.T, minor int64) money.Money {
	t.Helper()
	m, err := money.FromMinor(minor, money.BRL)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func testWallet(t *testing.T, balance int64, version int64) *wallet.Wallet {
	t.Helper()
	at := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	w, err := wallet.Rehydrate(walletID, playerID, brl(t, balance), version, at, at)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

type fakeProcess struct {
	calls []processwager.Input
	out   processwager.Output
	err   error
}

func (f *fakeProcess) Execute(_ context.Context, in processwager.Input) (processwager.Output, error) {
	f.calls = append(f.calls, in)
	return f.out, f.err
}

type fakeTxQueries struct {
	calls int
	tx    *wager.Transaction
	err   error
}

func (f *fakeTxQueries) Transaction(context.Context, string) (*wager.Transaction, error) {
	f.calls++
	return f.tx, f.err
}

func (f *fakeTxQueries) ProviderTransaction(context.Context, string, string) (*wager.Transaction, error) {
	f.calls++
	return f.tx, f.err
}

type env struct {
	h       http.Handler
	open    *fakeOpen
	queries *fakeQueries
	process *fakeProcess
	txq     *fakeTxQueries
}

func newEnv(t *testing.T) env {
	t.Helper()
	e := env{
		open: &fakeOpen{w: testWallet(t, 100000, 1)}, queries: &fakeQueries{w: testWallet(t, 97500, 2)},
		process: &fakeProcess{}, txq: &fakeTxQueries{},
	}
	e.h = New(Deps{
		OpenWallet:   e.open,
		ProcessWager: e.process,
		Queries:      e.queries,
		TxQueries:    e.txq,
		Verifier: fakeVerifier{
			"admin":       {Subject: "svc", Roles: []string{auth.RoleWalletAdmin}},
			"provider":    {Subject: "prov", ProviderID: "provider-a", Roles: []string{auth.RoleWagerWrite, auth.RoleWagerRead}},
			"provider-b":  {Subject: "prov-b", ProviderID: "provider-b", Roles: []string{auth.RoleWagerWrite, auth.RoleWagerRead}},
			"write-only":  {Subject: "w", ProviderID: "provider-a", Roles: []string{auth.RoleWagerWrite}},
			"read-only":   {Subject: "r", ProviderID: "provider-a", Roles: []string{auth.RoleWagerRead}},
			"no-provider": {Subject: "np", Roles: []string{auth.RoleWagerWrite, auth.RoleWagerRead}},
		},
		IDs:    seqIDs{},
		Health: NewHealth(),
		Log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	return e
}

func do(h http.Handler, method, path, token, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func code(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var p problem
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("body is not a problem: %q", rec.Body.String())
	}
	return p.Code
}

func TestAuthentication(t *testing.T) {
	routes := []struct{ method, path, body string }{
		{http.MethodPost, "/wallets", `{"playerId":"` + playerID + `","initialBalance":{"amount":"1.00","currency":"BRL"}}`},
		{http.MethodGet, "/wallets/" + walletID, ""},
		{http.MethodGet, "/wallets/" + walletID + "/ledger", ""},
	}
	tests := []struct {
		name       string
		header     string
		wantStatus int
		wantCode   string
	}{
		{name: "no header", wantStatus: 401, wantCode: "UNAUTHENTICATED"},
		{name: "wrong scheme", header: "Basic admin", wantStatus: 401, wantCode: "UNAUTHENTICATED"},
		{name: "empty bearer", header: "Bearer ", wantStatus: 401, wantCode: "UNAUTHENTICATED"},
		{name: "unknown token", header: "Bearer nope", wantStatus: 401, wantCode: "UNAUTHENTICATED"},
		{name: "provider token has no wallet admin", header: "Bearer provider", wantStatus: 403, wantCode: "FORBIDDEN"},
	}
	for _, rt := range routes {
		for _, tt := range tests {
			t.Run(rt.method+" "+rt.path+" "+tt.name, func(t *testing.T) {
				e := newEnv(t)
				req := httptest.NewRequest(rt.method, rt.path, strings.NewReader(rt.body))
				if tt.header != "" {
					req.Header.Set("Authorization", tt.header)
				}
				rec := httptest.NewRecorder()
				e.h.ServeHTTP(rec, req)

				if rec.Code != tt.wantStatus || code(t, rec) != tt.wantCode {
					t.Fatalf("status/code = %d/%s, want %d/%s", rec.Code, code(t, rec), tt.wantStatus, tt.wantCode)
				}
				if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
					t.Fatalf("content-type = %q", ct)
				}
				if tt.wantStatus == 401 && rec.Header().Get("WWW-Authenticate") == "" {
					t.Fatal("missing WWW-Authenticate")
				}
				if len(e.open.calls) != 0 || e.queries.calls != 0 {
					t.Fatal("use case reached without authorization")
				}
			})
		}
	}
}

func TestOpenWallet(t *testing.T) {
	valid := `{"playerId":"` + playerID + `","initialBalance":{"amount":"1000.00","currency":"BRL"}}`
	tests := []struct {
		name       string
		body       string
		useCaseErr error
		wantStatus int
		wantCode   string
		wantCalls  int
	}{
		{name: "created", body: valid, wantStatus: 201, wantCalls: 1},
		{name: "wallet already exists", body: valid, useCaseErr: wallet.ErrAlreadyExists, wantStatus: 409, wantCode: "WALLET_ALREADY_EXISTS", wantCalls: 1},
		{name: "invalid player id", body: valid, useCaseErr: wallet.ErrInvalidID, wantStatus: 400, wantCode: "INVALID_ID", wantCalls: 1},
		{name: "use case rejects balance", body: valid, useCaseErr: money.ErrNegative, wantStatus: 400, wantCode: "INVALID_MONEY", wantCalls: 1},
		{name: "unexpected failure", body: valid, useCaseErr: errors.New("db down"), wantStatus: 500, wantCode: "INTERNAL_ERROR", wantCalls: 1},
		{name: "malformed json", body: `{"playerId":`, wantStatus: 400, wantCode: "INVALID_REQUEST"},
		{name: "empty body", body: ``, wantStatus: 400, wantCode: "INVALID_REQUEST"},
		{name: "unknown field", body: `{"playerId":"` + playerID + `","initialBalance":{"amount":"1.00","currency":"BRL"},"x":1}`, wantStatus: 400, wantCode: "INVALID_REQUEST"},
		{name: "trailing data", body: valid + `{}`, wantStatus: 400, wantCode: "INVALID_REQUEST"},
		{name: "amount as number", body: `{"playerId":"` + playerID + `","initialBalance":{"amount":10.00,"currency":"BRL"}}`, wantStatus: 400, wantCode: "INVALID_MONEY"},
		{name: "amount with extra scale", body: `{"playerId":"` + playerID + `","initialBalance":{"amount":"1.001","currency":"BRL"}}`, wantStatus: 400, wantCode: "INVALID_MONEY"},
		{name: "scientific notation", body: `{"playerId":"` + playerID + `","initialBalance":{"amount":"1e3","currency":"BRL"}}`, wantStatus: 400, wantCode: "INVALID_MONEY"},
		{name: "unknown currency", body: `{"playerId":"` + playerID + `","initialBalance":{"amount":"1.00","currency":"XXX"}}`, wantStatus: 400, wantCode: "INVALID_MONEY"},
		{name: "null balance", body: `{"playerId":"` + playerID + `","initialBalance":null}`, wantStatus: 400, wantCode: "INVALID_MONEY"},
		{name: "oversized body", body: `{"playerId":"` + strings.Repeat("a", maxBodyBytes) + `"}`, wantStatus: 400, wantCode: "INVALID_REQUEST"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			e.open.err = tt.useCaseErr
			if tt.useCaseErr != nil {
				e.open.w = nil
			}
			req := httptest.NewRequest(http.MethodPost, "/wallets", strings.NewReader(tt.body))
			req.Header.Set("Authorization", "Bearer admin")
			req.Header.Set(correlationHeader, "corr-1")
			rec := httptest.NewRecorder()

			e.h.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (%s)", rec.Code, tt.wantStatus, rec.Body)
			}
			if tt.wantCode != "" && code(t, rec) != tt.wantCode {
				t.Fatalf("code = %s, want %s", code(t, rec), tt.wantCode)
			}
			if len(e.open.calls) != tt.wantCalls {
				t.Fatalf("use case calls = %d, want %d", len(e.open.calls), tt.wantCalls)
			}
			if tt.wantStatus != 201 {
				return
			}

			in := e.open.calls[0]
			if in.PlayerID != playerID || in.InitialBalance.Minor() != 100000 || in.InitialBalance.Currency() != money.BRL || in.CorrelationID != "corr-1" {
				t.Fatalf("use case input = %+v", in)
			}
			if rec.Header().Get("Location") != "/wallets/"+walletID {
				t.Fatalf("location = %q", rec.Header().Get("Location"))
			}
			want := `{"id":"` + walletID + `","playerId":"` + playerID + `","balance":{"amount":"1000.00","currency":"BRL"},"version":1}`
			if strings.TrimSpace(rec.Body.String()) != want {
				t.Fatalf("body = %s\nwant %s", rec.Body, want)
			}
		})
	}
}

func TestGetWallet(t *testing.T) {
	tests := []struct {
		name       string
		useCaseErr error
		wantStatus int
		wantCode   string
	}{
		{name: "found", wantStatus: 200},
		{name: "not found", useCaseErr: wallet.ErrNotFound, wantStatus: 404, wantCode: "WALLET_NOT_FOUND"},
		{name: "invalid id", useCaseErr: wallet.ErrInvalidID, wantStatus: 400, wantCode: "INVALID_ID"},
		{name: "unexpected failure", useCaseErr: errors.New("db down"), wantStatus: 500, wantCode: "INTERNAL_ERROR"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			e.queries.err = tt.useCaseErr
			rec := do(e.h, http.MethodGet, "/wallets/"+walletID, "admin", "")

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if tt.wantCode != "" {
				if code(t, rec) != tt.wantCode {
					t.Fatalf("code = %s, want %s", code(t, rec), tt.wantCode)
				}
				return
			}
			want := `{"id":"` + walletID + `","playerId":"` + playerID + `","balance":{"amount":"975.00","currency":"BRL"},"version":2,"createdAt":"2026-09-08T12:00:00.000Z","updatedAt":"2026-09-08T12:00:00.000Z"}`
			if strings.TrimSpace(rec.Body.String()) != want {
				t.Fatalf("body = %s\nwant %s", rec.Body, want)
			}
		})
	}
}

func TestLedger(t *testing.T) {
	at := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	item := queries.LedgerItem{
		ID: "entry-1", TransactionID: "tx-1", Direction: wallet.Credit,
		Money: brl(t, 100000), BalanceBefore: brl(t, 0), BalanceAfter: brl(t, 100000), WalletVersion: 1, CreatedAt: at,
	}
	wantItem := `{"id":"entry-1","transactionId":"tx-1","direction":"CREDIT","money":{"amount":"1000.00","currency":"BRL"},"balanceBefore":{"amount":"0.00","currency":"BRL"},"balanceAfter":{"amount":"1000.00","currency":"BRL"},"walletVersion":1,"createdAt":"2026-09-08T12:00:00.000Z"}`

	tests := []struct {
		name       string
		query      string
		page       queries.LedgerPage
		useCaseErr error
		wantStatus int
		wantCode   string
		wantBody   string
		wantCursor string
		wantLimit  int
	}{
		{name: "one page", page: queries.LedgerPage{Items: []queries.LedgerItem{item}}, wantStatus: 200, wantBody: `{"items":[` + wantItem + `],"nextCursor":null}`},
		{name: "empty ledger is an empty array", wantStatus: 200, wantBody: `{"items":[],"nextCursor":null}`},
		{name: "next cursor and params are forwarded", query: "?cursor=abc&limit=10", page: queries.LedgerPage{Items: []queries.LedgerItem{item}, NextCursor: "eyJ2IjoxfQ"}, wantStatus: 200, wantBody: `{"items":[` + wantItem + `],"nextCursor":"eyJ2IjoxfQ"}`, wantCursor: "abc", wantLimit: 10},
		{name: "limit is not a number", query: "?limit=ten", wantStatus: 400, wantCode: "INVALID_LIMIT"},
		{name: "limit out of range", query: "?limit=999", useCaseErr: queries.ErrInvalidLimit, wantStatus: 400, wantCode: "INVALID_LIMIT", wantLimit: 999},
		{name: "invalid cursor", query: "?cursor=***", useCaseErr: queries.ErrInvalidCursor, wantStatus: 400, wantCode: "INVALID_CURSOR", wantCursor: "***"},
		{name: "wallet not found", useCaseErr: wallet.ErrNotFound, wantStatus: 404, wantCode: "WALLET_NOT_FOUND"},
		{name: "unexpected failure", useCaseErr: errors.New("db down"), wantStatus: 500, wantCode: "INTERNAL_ERROR"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			e.queries.page, e.queries.err = tt.page, tt.useCaseErr

			rec := do(e.h, http.MethodGet, "/wallets/"+walletID+"/ledger"+tt.query, "admin", "")

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (%s)", rec.Code, tt.wantStatus, rec.Body)
			}
			if tt.wantCode != "" && code(t, rec) != tt.wantCode {
				t.Fatalf("code = %s, want %s", code(t, rec), tt.wantCode)
			}
			if tt.wantBody != "" && strings.TrimSpace(rec.Body.String()) != tt.wantBody {
				t.Fatalf("body = %s\nwant %s", rec.Body, tt.wantBody)
			}
			if e.queries.calls > 0 && (e.queries.gotCursor != tt.wantCursor || e.queries.gotLimit != tt.wantLimit) {
				t.Fatalf("forwarded cursor/limit = %q/%d, want %q/%d", e.queries.gotCursor, e.queries.gotLimit, tt.wantCursor, tt.wantLimit)
			}
		})
	}
}

func TestCorrelationID(t *testing.T) {
	tests := []struct {
		name string
		sent string
		want string
	}{
		{name: "echoes a valid id", sent: "req-123.abc:9", want: "req-123.abc:9"},
		{name: "generates when absent", want: "generated-id"},
		{name: "replaces an id with forbidden characters", sent: "bad id\twith space", want: "generated-id"},
		{name: "replaces an overlong id", sent: strings.Repeat("a", 129), want: "generated-id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			req := httptest.NewRequest(http.MethodGet, "/wallets/"+walletID, nil)
			req.Header.Set("Authorization", "Bearer admin")
			if tt.sent != "" {
				req.Header.Set(correlationHeader, tt.sent)
			}
			rec := httptest.NewRecorder()

			e.h.ServeHTTP(rec, req)

			if got := rec.Header().Get(correlationHeader); got != tt.want {
				t.Fatalf("%s = %q, want %q", correlationHeader, got, tt.want)
			}
		})
	}

	t.Run("also present on authentication failures", func(t *testing.T) {
		rec := do(newEnv(t).h, http.MethodGet, "/wallets/"+walletID, "", "")
		if rec.Header().Get(correlationHeader) == "" {
			t.Fatal("missing correlation id on 401")
		}
	})
}

func TestHealth(t *testing.T) {
	errDown := errors.New("down")
	tests := []struct {
		name       string
		fns        map[string]func(context.Context) error
		drain      bool
		path       string
		wantStatus int
		wantBody   string
	}{
		{name: "live is always up", path: "/health/live", drain: true, wantStatus: 200, wantBody: `{"status":"UP"}`},
		{name: "ready with no checks", path: "/health/ready", wantStatus: 200, wantBody: `{"checks":{},"status":"UP"}`},
		{name: "ready with healthy dependency", path: "/health/ready", fns: map[string]func(context.Context) error{"postgres": func(context.Context) error { return nil }}, wantStatus: 200, wantBody: `{"checks":{"postgres":"up"},"status":"UP"}`},
		{name: "ready hides the failure cause", path: "/health/ready", fns: map[string]func(context.Context) error{"postgres": func(context.Context) error { return errDown }}, wantStatus: 503, wantBody: `{"checks":{"postgres":"down"},"status":"DOWN"}`},
		{name: "draining fails readiness", path: "/health/ready", drain: true, wantStatus: 503, wantBody: `{"reason":"shutting down","status":"DOWN"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var checks []Check
			for name, fn := range tt.fns {
				checks = append(checks, Check{Name: name, Fn: fn})
			}
			health := NewHealth(checks...)
			if tt.drain {
				health.Drain()
			}
			h := New(Deps{Verifier: fakeVerifier{}, IDs: seqIDs{}, Health: health, Log: slog.New(slog.NewTextHandler(io.Discard, nil))})

			rec := do(h, http.MethodGet, tt.path, "", "")

			if rec.Code != tt.wantStatus || strings.TrimSpace(rec.Body.String()) != tt.wantBody {
				t.Fatalf("status/body = %d %s, want %d %s", rec.Code, rec.Body, tt.wantStatus, tt.wantBody)
			}
		})
	}
}

func TestHealthCachesResults(t *testing.T) {
	calls := 0
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	health := NewHealth(Check{Name: "db", Fn: func(context.Context) error { calls++; return nil }})
	health.now = func() time.Time { return now }

	ask := func() {
		health.ready(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	}
	ask()
	ask()
	if calls != 1 {
		t.Fatalf("checks ran %d times within the cache window, want 1", calls)
	}
	now = now.Add(checkCacheTTL + time.Millisecond)
	ask()
	if calls != 2 {
		t.Fatalf("checks ran %d times after the cache expired, want 2", calls)
	}
}
