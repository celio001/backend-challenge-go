//go:build integration

package app

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/celio001/backend-challenge-go/internal/testutil/pgtest"
	"github.com/celio001/backend-challenge-go/internal/testutil/sqstest"
)

type runningApp struct {
	base string
	stop func()
}

// startApp boots the whole application against an existing database, with its own queues.
func startApp(t *testing.T, kc, dbURL string, extraEnv map[string]string) runningApp {
	t.Helper()
	events := sqstest.New(t)
	inbound, dlq := sqstest.NewInbound(t, 30)
	addr := freeAddr(t)
	env := map[string]string{
		"HTTP_ADDR": addr, "SQS_ENDPOINT": os.Getenv("TEST_SQS_ENDPOINT"), "EVENTS_QUEUE_NAME": events.Name,
		"WAGER_QUEUE_NAME": inbound.Name, "WAGER_DLQ_NAME": dlq.Name, "SQS_SENDER_PROVIDER_MAP": localstackSender + "=provider-a",
		"DATABASE_URL": dbURL, "OIDC_ISSUER": kc + "/realms/wallet", "OIDC_DISCOVERY_URL": "",
	}
	for k, v := range extraEnv {
		env[k] = v
	}
	for k, v := range env {
		t.Setenv(k, v)
	}
	application := fxtest.New(t, Module, fx.StartTimeout(30*time.Second), fx.StopTimeout(15*time.Second))
	application.RequireStart()
	stopped := false
	stop := func() {
		if !stopped {
			stopped = true
			application.RequireStop()
		}
	}
	t.Cleanup(stop)
	return runningApp{base: "http://" + addr, stop: stop}
}

func TestPendingReferences(t *testing.T) {
	kc := keycloakURL(t)

	wagerBody := func(player, wallet, ext, kind, amount, ref string) string {
		refField := ""
		if ref != "" {
			refField = fmt.Sprintf(`,"referenceExternalTransactionId":%q`, ref)
		}
		return fmt.Sprintf(`{"providerId":"provider-a","externalTransactionId":%q,"playerId":%q,"walletId":%q,"roundId":"round-1","gameId":"game-1","kind":%q,"money":{"amount":%q,"currency":"BRL"}%s}`,
			ext, player, wallet, kind, amount, refField)
	}
	waitFor := func(t *testing.T, what string, timeout time.Duration, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(timeout)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("%s: not reached in %s", what, timeout)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	t.Run("a reversal that arrives first is applied by the resolver once its bet exists, even across a restart", func(t *testing.T) {
		pool, dbURL := pgtest.New(t)
		app1 := startApp(t, kc, dbURL, nil)
		admin, provider := clientToken(t, kc, "wallet-internal"), clientToken(t, kc, "provider-a")

		player := newUUID(t)
		w := call(t, http.MethodPost, app1.base+"/wallets", admin, "", fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"100.00","currency":"BRL"}}`, player))
		walletID := w.body["id"].(string)

		early := postWager(t, app1.base, provider, "provider-a:r-1", wagerBody(player, walletID, "r-1", "REFUND", "40.00", "b-1"))
		if early.status != http.StatusAccepted || early.body["status"] != "PENDING_REFERENCE" {
			t.Fatalf("early refund = %d %v", early.status, early.body)
		}
		pending := call(t, http.MethodGet, app1.base+"/providers/provider-a/wagering/transactions/r-1", provider, "", "")
		if pending.status != http.StatusOK || pending.body["status"] != "PENDING_REFERENCE" || pending.body["attempts"] == nil || pending.body["expiresAt"] == nil {
			t.Fatalf("pending view = %d %v", pending.status, pending.body)
		}

		// The process dies with the refund still pending; another one starts on the same database.
		app1.stop()
		if got := scalar(t, pool, `SELECT status FROM wager_transactions WHERE external_transaction_id = 'r-1'`); got != "PENDING_REFERENCE" {
			t.Fatalf("status after the stop = %s", got)
		}
		app2 := startApp(t, kc, dbURL, nil)
		provider = clientToken(t, kc, "provider-a")

		bet := postWager(t, app2.base, provider, "provider-a:b-1", wagerBody(player, walletID, "b-1", "BET", "40.00", ""))
		if bet.status != http.StatusOK {
			t.Fatalf("bet = %d %v", bet.status, bet.body)
		}
		waitFor(t, "the refund to be resolved by the restarted service", 20*time.Second, func() bool {
			return scalar(t, pool, `SELECT status FROM wager_transactions WHERE external_transaction_id = 'r-1'`) == "PROCESSED"
		})

		if got := scalar(t, pool, `SELECT balance_minor::text FROM wallets WHERE id = $1`, walletID); got != "10000" {
			t.Fatalf("balance_minor = %s, want the bet refunded", got)
		}
		done := call(t, http.MethodGet, app2.base+"/providers/provider-a/wagering/transactions/r-1", provider, "", "")
		if done.body["status"] != "PROCESSED" || done.body["attempts"] != nil || done.body["nextAttemptAt"] != nil {
			t.Fatalf("settled view = %v", done.body)
		}
		// Same request again: the stored result is returned, nothing is applied twice.
		replay := postWager(t, app2.base, provider, "provider-a:r-1", wagerBody(player, walletID, "r-1", "REFUND", "40.00", "b-1"))
		if replay.status != http.StatusOK || replay.body["idempotentReplay"] != true {
			t.Fatalf("replay = %d %v", replay.status, replay.body)
		}
		if got := scalar(t, pool, `SELECT count(*)::text FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'CREDIT'`, walletID); got != "2" {
			t.Fatalf("credits = %s, want the opening and one refund", got)
		}
		if got := scalar(t, pool, `SELECT count(*)::text FROM outbox_events WHERE partition_key = $1 AND event_type = 'WagerTransactionPendingReference'`, walletID); got != "1" {
			t.Fatalf("pending events = %s", got)
		}
	})

	t.Run("a reference that never arrives is rejected at the deadline with its event", func(t *testing.T) {
		pool, dbURL := pgtest.New(t)
		app := startApp(t, kc, dbURL, map[string]string{"REFERENCE_TTL": "3s"})
		admin, provider := clientToken(t, kc, "wallet-internal"), clientToken(t, kc, "provider-a")

		player := newUUID(t)
		w := call(t, http.MethodPost, app.base+"/wallets", admin, "", fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"100.00","currency":"BRL"}}`, player))
		walletID := w.body["id"].(string)
		if r := postWager(t, app.base, provider, "provider-a:rb-1", wagerBody(player, walletID, "rb-1", "ROLLBACK", "10.00", "ghost")); r.status != http.StatusAccepted {
			t.Fatalf("early rollback = %d %v", r.status, r.body)
		}

		waitFor(t, "a first retry to be counted", 15*time.Second, func() bool {
			v := call(t, http.MethodGet, app.base+"/providers/provider-a/wagering/transactions/rb-1", provider, "", "")
			attempts, _ := v.body["attempts"].(float64)
			return v.body["status"] == "PENDING_REFERENCE" && attempts >= 1 && v.body["nextAttemptAt"] != nil
		})
		waitFor(t, "the deadline rejection", 30*time.Second, func() bool {
			return scalar(t, pool, `SELECT status FROM wager_transactions WHERE external_transaction_id = 'rb-1'`) == "REJECTED"
		})

		view := call(t, http.MethodGet, app.base+"/providers/provider-a/wagering/transactions/rb-1", provider, "", "")
		if view.body["failureCode"] != "REFERENCE_NOT_FOUND" || view.body["attempts"] != nil {
			t.Fatalf("rejected view = %v", view.body)
		}
		if got := scalar(t, pool, `SELECT count(*)::text FROM outbox_events WHERE partition_key = $1 AND event_type = 'WagerTransactionRejected'`, walletID); got != "1" {
			t.Fatalf("rejection events = %s", got)
		}
		if got := scalar(t, pool, `SELECT balance_minor::text FROM wallets WHERE id = $1`, walletID); got != "10000" {
			t.Fatalf("balance_minor = %s, a rejection must not move money", got)
		}
		// The rejection is final: the late reference does not revive it.
		late := postWager(t, app.base, provider, "provider-a:ghost", wagerBody(player, walletID, "ghost", "BET", "10.00", ""))
		if late.status != http.StatusOK {
			t.Fatalf("late bet = %d %v", late.status, late.body)
		}
		time.Sleep(2500 * time.Millisecond)
		if got := scalar(t, pool, `SELECT status FROM wager_transactions WHERE external_transaction_id = 'rb-1'`); got != "REJECTED" {
			t.Fatalf("status after the late reference = %s", got)
		}
	})

	t.Run("a reversal that comes by SQS before its bet leaves the queue and is resolved later", func(t *testing.T) {
		pool, dbURL := pgtest.New(t)
		inbound, dlq := sqstest.NewInbound(t, 30)
		app := startApp(t, kc, dbURL, map[string]string{"WAGER_QUEUE_NAME": inbound.Name, "WAGER_DLQ_NAME": dlq.Name})
		admin := clientToken(t, kc, "wallet-internal")

		player := newUUID(t)
		w := call(t, http.MethodPost, app.base+"/wallets", admin, "", fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"100.00","currency":"BRL"}}`, player))
		walletID := w.body["id"].(string)

		sendRaw(t, inbound, walletID, wagerMsg{messageID: "m-early", ext: "sr-1", kind: "REFUND", amount: "25.00", ref: "sb-1", player: player, wallet: walletID}.body())
		waitFor(t, "the early refund to be stored", 20*time.Second, func() bool {
			return scalar(t, pool, `SELECT coalesce(max(status), '') FROM wager_transactions WHERE external_transaction_id = 'sr-1'`) == "PENDING_REFERENCE"
		})
		idle(t, inbound) // the message is gone from the queue although the operation is not finished

		sendRaw(t, inbound, walletID, wagerMsg{messageID: "m-bet", ext: "sb-1", kind: "BET", amount: "25.00", player: player, wallet: walletID}.body())
		waitFor(t, "the resolver to settle the refund", 20*time.Second, func() bool {
			return scalar(t, pool, `SELECT status FROM wager_transactions WHERE external_transaction_id = 'sr-1'`) == "PROCESSED"
		})
		if got := scalar(t, pool, `SELECT balance_minor::text FROM wallets WHERE id = $1`, walletID); got != "10000" {
			t.Fatalf("balance_minor = %s", got)
		}
		if got := dlq.Drain(t); len(got) != 0 {
			t.Fatalf("dead letters: %+v", got)
		}
	})
}
