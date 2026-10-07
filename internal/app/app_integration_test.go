//go:build integration

package app

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/celio001/backend-challenge-go/internal/testutil/pgtest"
)

func keycloakURL(t *testing.T) string {
	t.Helper()
	u := os.Getenv("TEST_KEYCLOAK_URL")
	if u == "" {
		t.Skip("TEST_KEYCLOAK_URL not set")
	}
	return strings.TrimRight(u, "/")
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func clientToken(t *testing.T, kc, client string) string {
	t.Helper()
	resp, err := http.PostForm(kc+"/realms/wallet/protocol/openid-connect/token", url.Values{
		"grant_type": {"client_credentials"}, "client_id": {client}, "client_secret": {client + "-secret"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || body.AccessToken == "" {
		t.Fatalf("token for %s: status %d, err %v", client, resp.StatusCode, err)
	}
	return body.AccessToken
}

func newUUID(t *testing.T) string {
	t.Helper()
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

type reply struct {
	status int
	header http.Header
	body   map[string]any
}

func call(t *testing.T, method, url, token, correlation, body string) reply {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if correlation != "" {
		req.Header.Set("X-Correlation-Id", correlation)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	r := reply{status: resp.StatusCode, header: resp.Header}
	if err := json.Unmarshal(raw, &r.body); err != nil {
		t.Fatalf("%s %s: body is not a JSON object: %q", method, url, raw)
	}
	return r
}

func TestEndToEnd(t *testing.T) {
	kc := keycloakURL(t)
	pool, dbURL := pgtest.New(t)
	addr := freeAddr(t)
	t.Setenv("HTTP_ADDR", addr)
	t.Setenv("DATABASE_URL", dbURL)
	t.Setenv("OIDC_ISSUER", kc+"/realms/wallet")
	t.Setenv("OIDC_DISCOVERY_URL", "")
	base := "http://" + addr

	application := fxtest.New(t, Module, fx.StartTimeout(30*time.Second), fx.StopTimeout(10*time.Second))
	application.RequireStart()
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			application.RequireStop()
		}
	})

	admin := clientToken(t, kc, "wallet-internal")
	provider := clientToken(t, kc, "provider-a")
	player := newUUID(t)
	open := fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"1000.00","currency":"BRL"}}`, player)

	count := func(q string, args ...any) (n int) {
		t.Helper()
		if err := pool.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	var walletID string

	t.Run("unauthenticated and unauthorized callers change nothing", func(t *testing.T) {
		tampered := admin[:len(admin)-4] + "AAAA"
		for name, token := range map[string]string{"no token": "", "tampered token": tampered} {
			if r := call(t, http.MethodPost, base+"/wallets", token, "", open); r.status != http.StatusUnauthorized {
				t.Fatalf("%s: status = %d", name, r.status)
			}
		}
		if r := call(t, http.MethodPost, base+"/wallets", provider, "", open); r.status != http.StatusForbidden || r.body["code"] != "FORBIDDEN" {
			t.Fatalf("provider token: status = %d, body = %v", r.status, r.body)
		}
		if n := count(`SELECT count(*) FROM wallets`); n != 0 {
			t.Fatalf("wallets = %d after refused requests", n)
		}
	})

	t.Run("open wallet with balance", func(t *testing.T) {
		r := call(t, http.MethodPost, base+"/wallets", admin, "e2e-corr-1", open)
		if r.status != http.StatusCreated {
			t.Fatalf("status = %d, body = %v", r.status, r.body)
		}
		walletID, _ = r.body["id"].(string)
		balance, _ := r.body["balance"].(map[string]any)
		if walletID == "" || r.body["playerId"] != player || r.body["version"] != float64(1) || balance["amount"] != "1000.00" || balance["currency"] != "BRL" {
			t.Fatalf("body = %v", r.body)
		}
		if r.header.Get("Location") != "/wallets/"+walletID || r.header.Get("X-Correlation-Id") != "e2e-corr-1" {
			t.Fatalf("headers = %v", r.header)
		}
		for _, typ := range []string{"WagerTransactionProcessed", "WalletBalanceChanged"} {
			if n := count(`SELECT count(*) FROM outbox_events WHERE partition_key = $1 AND event_type = $2 AND payload->>'correlationId' = 'e2e-corr-1'`, walletID, typ); n != 1 {
				t.Fatalf("%s events with the request correlation id = %d", typ, n)
			}
		}
	})

	t.Run("read it back", func(t *testing.T) {
		r := call(t, http.MethodGet, base+"/wallets/"+walletID, admin, "", "")
		balance, _ := r.body["balance"].(map[string]any)
		if r.status != http.StatusOK || balance["amount"] != "1000.00" || r.body["createdAt"] == nil {
			t.Fatalf("status = %d, body = %v", r.status, r.body)
		}

		l := call(t, http.MethodGet, base+"/wallets/"+walletID+"/ledger?limit=10", admin, "", "")
		items, _ := l.body["items"].([]any)
		if l.status != http.StatusOK || len(items) != 1 || l.body["nextCursor"] != nil {
			t.Fatalf("ledger status = %d, body = %v", l.status, l.body)
		}
		entry, _ := items[0].(map[string]any)
		if entry["direction"] != "CREDIT" || entry["walletVersion"] != float64(1) {
			t.Fatalf("entry = %v", entry)
		}
	})

	t.Run("same player and currency conflicts", func(t *testing.T) {
		r := call(t, http.MethodPost, base+"/wallets", admin, "", open)
		if r.status != http.StatusConflict || r.body["code"] != "WALLET_ALREADY_EXISTS" {
			t.Fatalf("status = %d, body = %v", r.status, r.body)
		}
		if n := count(`SELECT count(*) FROM wager_transactions WHERE player_id = $1`, player); n != 1 {
			t.Fatalf("opening transactions = %d", n)
		}
	})

	t.Run("zero balance wallet has an empty ledger", func(t *testing.T) {
		body := fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"0.00","currency":"USD"}}`, newUUID(t))
		r := call(t, http.MethodPost, base+"/wallets", admin, "", body)
		id, _ := r.body["id"].(string)
		if r.status != http.StatusCreated || id == "" {
			t.Fatalf("status = %d, body = %v", r.status, r.body)
		}
		l := call(t, http.MethodGet, base+"/wallets/"+id+"/ledger", admin, "", "")
		if items, ok := l.body["items"].([]any); !ok || len(items) != 0 {
			t.Fatalf("ledger = %v", l.body)
		}
	})

	t.Run("invalid input and unknown wallet", func(t *testing.T) {
		bad := fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"-5.00","currency":"BRL"}}`, newUUID(t))
		if r := call(t, http.MethodPost, base+"/wallets", admin, "", bad); r.status != http.StatusBadRequest || r.body["code"] != "INVALID_MONEY" {
			t.Fatalf("negative: status = %d, body = %v", r.status, r.body)
		}
		if r := call(t, http.MethodGet, base+"/wallets/"+newUUID(t), admin, "", ""); r.status != http.StatusNotFound {
			t.Fatalf("unknown wallet: status = %d", r.status)
		}
	})

	t.Run("wagering over http", func(t *testing.T) {
		providerB := clientToken(t, kc, "provider-b")
		wagerPlayer := newUUID(t)
		w := call(t, http.MethodPost, base+"/wallets", admin, "", fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"1000.00","currency":"BRL"}}`, wagerPlayer))
		wid, _ := w.body["id"].(string)
		wager := func(ext, kind, amount, ref string) string {
			extra := ""
			if ref != "" {
				extra = fmt.Sprintf(`,"referenceExternalTransactionId":%q`, ref)
			}
			return fmt.Sprintf(`{"providerId":"provider-a","externalTransactionId":%q,"playerId":%q,"walletId":%q,"roundId":"round-1","gameId":"fortune-chimp","kind":%q,"money":{"amount":%q,"currency":"BRL"}%s}`,
				ext, wagerPlayer, wid, kind, amount, extra)
		}
		send := func(token, key, body string) reply {
			req, err := http.NewRequest(http.MethodPost, base+"/wagering/transactions", bytes.NewBufferString(body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer "+token)
			if key != "" {
				req.Header.Set("Idempotency-Key", key)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			raw, _ := io.ReadAll(resp.Body)
			r := reply{status: resp.StatusCode, header: resp.Header}
			if err := json.Unmarshal(raw, &r.body); err != nil {
				t.Fatalf("body is not a JSON object: %q", raw)
			}
			return r
		}
		balanceOf := func(r reply) any { b, _ := r.body["balance"].(map[string]any); return b["amount"] }

		bet := send(provider, "provider-a:tx-1", wager("tx-1", "BET", "25.00", ""))
		if bet.status != http.StatusOK || bet.body["status"] != "PROCESSED" || balanceOf(bet) != "975.00" || bet.body["idempotentReplay"] != false {
			t.Fatalf("bet = %d %v", bet.status, bet.body)
		}
		txID, _ := bet.body["transactionId"].(string)

		if r := send(provider, "provider-a:tx-1", wager("tx-1", "BET", "25.00", "")); r.status != http.StatusOK || r.body["idempotentReplay"] != true || r.body["transactionId"] != txID || balanceOf(r) != "975.00" {
			t.Fatalf("replay = %d %v", r.status, r.body)
		}
		if r := send(provider, "provider-a:tx-1", wager("tx-1", "BET", "99.00", "")); r.status != http.StatusConflict || r.body["code"] != "IDEMPOTENCY_KEY_REUSED" {
			t.Fatalf("key reuse = %d %v", r.status, r.body)
		}
		if r := send(provider, "another-key", wager("tx-1", "BET", "25.00", "")); r.status != http.StatusConflict || r.body["code"] != "EXTERNAL_TRANSACTION_ID_CONFLICT" {
			t.Fatalf("external id reuse = %d %v", r.status, r.body)
		}
		if r := send(provider, "", wager("tx-x", "BET", "1.00", "")); r.status != http.StatusBadRequest || r.body["code"] != "MISSING_IDEMPOTENCY_KEY" {
			t.Fatalf("missing key = %d %v", r.status, r.body)
		}
		if r := send(provider, "k-open", wager("tx-open", "OPENING", "1.00", "")); r.status != http.StatusBadRequest || r.body["code"] != "KIND_NOT_ALLOWED" {
			t.Fatalf("opening = %d %v", r.status, r.body)
		}

		t.Run("providers are isolated", func(t *testing.T) {
			if r := send(providerB, "provider-a:tx-b", wager("tx-b", "BET", "1.00", "")); r.status != http.StatusForbidden {
				t.Fatalf("provider-b sending as provider-a = %d %v", r.status, r.body)
			}
			if r := send(admin, "provider-a:tx-b", wager("tx-b", "BET", "1.00", "")); r.status != http.StatusForbidden {
				t.Fatalf("wallet admin sending a wager = %d %v", r.status, r.body)
			}
			if r := call(t, http.MethodGet, base+"/wagering/transactions/"+txID, providerB, "", ""); r.status != http.StatusNotFound {
				t.Fatalf("provider-b reading provider-a's transaction by id = %d %v", r.status, r.body)
			}
			if r := call(t, http.MethodGet, base+"/providers/provider-a/wagering/transactions/tx-1", providerB, "", ""); r.status != http.StatusForbidden {
				t.Fatalf("provider-b reading provider-a's path = %d %v", r.status, r.body)
			}
			if r := call(t, http.MethodGet, base+"/wagering/transactions/"+txID, provider, "", ""); r.status != http.StatusOK || r.body["status"] != "PROCESSED" || r.body["providerId"] != "provider-a" {
				t.Fatalf("owner reading by id = %d %v", r.status, r.body)
			}
			if r := call(t, http.MethodGet, base+"/providers/provider-a/wagering/transactions/tx-1", provider, "", ""); r.status != http.StatusOK || r.body["transactionId"] != txID {
				t.Fatalf("owner reading by external id = %d %v", r.status, r.body)
			}
			if r := call(t, http.MethodGet, base+"/wagering/transactions/"+txID, admin, "", ""); r.status != http.StatusOK {
				t.Fatalf("wallet admin reading by id = %d", r.status)
			}
			if n := count(`SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND kind = 'BET'`, wid); n != 1 {
				t.Fatalf("stored bets = %d, refused requests must store nothing", n)
			}
		})

		t.Run("reversal before its bet waits, one after it applies", func(t *testing.T) {
			pend := send(provider, "provider-a:r-1", wager("r-1", "REFUND", "10.00", "tx-later"))
			if pend.status != http.StatusAccepted || pend.body["status"] != "PENDING_REFERENCE" {
				t.Fatalf("early refund = %d %v", pend.status, pend.body)
			}
			if r := call(t, http.MethodGet, base+"/providers/provider-a/wagering/transactions/r-1", provider, "", ""); r.status != http.StatusOK || r.body["status"] != "PENDING_REFERENCE" || r.body["expiresAt"] == nil {
				t.Fatalf("pending visible = %d %v", r.status, r.body)
			}
			if send(provider, "provider-a:tx-2", wager("tx-2", "BET", "50.00", "")).status != http.StatusOK {
				t.Fatal("bet tx-2")
			}
			refund := send(provider, "provider-a:r-2", wager("r-2", "REFUND", "50.00", "tx-2"))
			if refund.status != http.StatusOK || balanceOf(refund) != "975.00" {
				t.Fatalf("refund = %d %v", refund.status, refund.body)
			}
			again := send(provider, "provider-a:r-3", wager("r-3", "ROLLBACK", "50.00", "tx-2"))
			if again.status != http.StatusUnprocessableEntity || again.body["failureCode"] != "REFERENCE_ALREADY_REVERSED" || again.body["status"] != "REJECTED" {
				t.Fatalf("second reversal = %d %v", again.status, again.body)
			}
		})

		t.Run("two bets of 80 on a balance of 100, at the same time", func(t *testing.T) {
			pl := newUUID(t)
			created := call(t, http.MethodPost, base+"/wallets", admin, "", fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"100.00","currency":"BRL"}}`, pl))
			id, _ := created.body["id"].(string)
			body := func(ext string) string {
				return fmt.Sprintf(`{"providerId":"provider-a","externalTransactionId":%q,"playerId":%q,"walletId":%q,"roundId":"r","gameId":"g","kind":"BET","money":{"amount":"80.00","currency":"BRL"}}`, ext, pl, id)
			}
			results := make([]reply, 2)
			gate := make(chan struct{})
			done := make(chan int, 2)
			for i, ext := range []string{"eighty-a", "eighty-b"} {
				go func() {
					<-gate
					results[i] = send(provider, "provider-a:"+ext, body(ext))
					done <- i
				}()
			}
			close(gate)
			<-done
			<-done
			statuses := []int{results[0].status, results[1].status}
			if !(statuses[0] == 200 && statuses[1] == 422) && !(statuses[0] == 422 && statuses[1] == 200) {
				t.Fatalf("statuses = %v, want one 200 and one 422 (%v / %v)", statuses, results[0].body, results[1].body)
			}
			for _, r := range results {
				if r.status == 422 && r.body["failureCode"] != "INSUFFICIENT_FUNDS" {
					t.Fatalf("rejection = %v", r.body)
				}
			}
			final := call(t, http.MethodGet, base+"/wallets/"+id, admin, "", "")
			if b, _ := final.body["balance"].(map[string]any); b["amount"] != "20.00" {
				t.Fatalf("final balance = %v", final.body)
			}
			if n := count(`SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, id); n != 1 {
				t.Fatalf("debits = %d, want 1", n)
			}
		})
	})

	t.Run("health endpoints are public", func(t *testing.T) {
		if r := call(t, http.MethodGet, base+"/health/live", "", "", ""); r.status != http.StatusOK || r.body["status"] != "UP" {
			t.Fatalf("live: status = %d, body = %v", r.status, r.body)
		}
		checks, _ := call(t, http.MethodGet, base+"/health/ready", "", "", "").body["checks"].(map[string]any)
		if r := call(t, http.MethodGet, base+"/health/ready", "", "", ""); r.status != http.StatusOK || checks["postgres"] != "up" {
			t.Fatalf("ready: status = %d, body = %v", r.status, r.body)
		}
	})

	t.Run("shutdown stops listening", func(t *testing.T) {
		application.RequireStop()
		stopped = true
		if conn, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
			conn.Close()
			t.Fatal("server still accepts connections after stop")
		}
	})
}

func TestStartFailsWhenPostgresIsDown(t *testing.T) {
	addr := freeAddr(t)
	t.Setenv("HTTP_ADDR", addr)
	t.Setenv("DATABASE_URL", "postgres://postgres:postgres@127.0.0.1:1/none?sslmode=disable&connect_timeout=1")
	t.Setenv("OIDC_ISSUER", "http://127.0.0.1:1/realms/wallet")
	t.Setenv("OIDC_DISCOVERY_URL", "")

	application := fxtest.New(t, Module)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := application.Start(ctx)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the start deadline to expire while waiting for postgres", err)
	}
	if conn, derr := net.DialTimeout("tcp", addr, 500*time.Millisecond); derr == nil {
		conn.Close()
		t.Fatal("http server is listening although boot failed")
	}
}
