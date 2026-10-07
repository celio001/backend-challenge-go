//go:build integration

package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/celio001/backend-challenge-go/internal/adapter/sqsmsg"
	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/infra/postgres"
	infrasqs "github.com/celio001/backend-challenge-go/internal/infra/sqs"
	"github.com/celio001/backend-challenge-go/internal/infra/system"
	"github.com/celio001/backend-challenge-go/internal/testutil/pgtest"
	"github.com/celio001/backend-challenge-go/internal/testutil/sqstest"
	"github.com/celio001/backend-challenge-go/internal/usecase/openwallet"
	"github.com/celio001/backend-challenge-go/internal/usecase/processwager"
)

// LocalStack reports its account id as the SenderId of every message, whoever sent it.
const localstackSender = "000000000000"

type wagerMsg struct {
	messageID, provider, ext, key, kind, amount, ref, player, wallet string
}

func (m wagerMsg) body() string {
	if m.provider == "" {
		m.provider = "provider-a"
	}
	if m.key == "" {
		m.key = m.provider + ":" + m.ext
	}
	ref := ""
	if m.ref != "" {
		ref = fmt.Sprintf(`,"referenceExternalTransactionId":%q`, m.ref)
	}
	return fmt.Sprintf(`{"messageId":%q,"type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00.000Z","data":{"providerId":%q,"externalTransactionId":%q,"idempotencyKey":%q,"playerId":%q,"walletId":%q,"roundId":"round-1","gameId":"game-1","kind":%q,"money":{"amount":%q,"currency":"BRL"}%s}}`,
		m.messageID, m.provider, m.ext, m.key, m.player, m.wallet, m.kind, m.amount, ref)
}

var dedupSeq atomic.Int64

// sendRaw uses a fresh deduplication id, so the broker's own filter never hides a repeated message from the application.
func sendRaw(t *testing.T, q sqstest.Queue, group, body string) {
	t.Helper()
	if _, err := q.Client.SendMessage(context.Background(), &awssqs.SendMessageInput{
		QueueUrl: aws.String(q.URL), MessageBody: aws.String(body), MessageGroupId: aws.String(group),
		MessageDeduplicationId: aws.String(fmt.Sprintf("dedup-%d", dedupSeq.Add(1))),
	}); err != nil {
		t.Fatal(err)
	}
}

// idle waits until the queue holds nothing, visible or in flight.
func idle(t *testing.T, q sqstest.Queue) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	quiet := 0
	for quiet < 3 {
		if time.Now().After(deadline) {
			t.Fatal("the queue did not drain")
		}
		out, err := q.Client.GetQueueAttributes(context.Background(), &awssqs.GetQueueAttributesInput{
			QueueUrl: aws.String(q.URL),
			AttributeNames: []types.QueueAttributeName{
				types.QueueAttributeNameApproximateNumberOfMessages, types.QueueAttributeNameApproximateNumberOfMessagesNotVisible, types.QueueAttributeNameApproximateNumberOfMessagesDelayed,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if out.Attributes["ApproximateNumberOfMessages"] == "0" && out.Attributes["ApproximateNumberOfMessagesNotVisible"] == "0" && out.Attributes["ApproximateNumberOfMessagesDelayed"] == "0" {
			quiet++
		} else {
			quiet = 0
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func scalar(t *testing.T, pool *pgxpool.Pool, q string, args ...any) (v string) {
	t.Helper()
	if err := pool.QueryRow(context.Background(), q, args...).Scan(&v); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return v
}

func postWager(t *testing.T, base, token, key, body string) reply {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, base+"/wagering/transactions", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Idempotency-Key", key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	r := reply{status: resp.StatusCode, header: resp.Header}
	r.body = map[string]any{}
	_ = json.Unmarshal(raw, &r.body)
	return r
}

func TestSQSEntry(t *testing.T) {
	kc := keycloakURL(t)
	pool, dbURL := pgtest.New(t)
	events := sqstest.New(t)
	inbound, dlq := sqstest.NewInbound(t, 30)
	addr := freeAddr(t)
	t.Setenv("HTTP_ADDR", addr)
	t.Setenv("SQS_ENDPOINT", os.Getenv("TEST_SQS_ENDPOINT"))
	t.Setenv("EVENTS_QUEUE_NAME", events.Name)
	t.Setenv("WAGER_QUEUE_NAME", inbound.Name)
	t.Setenv("WAGER_DLQ_NAME", dlq.Name)
	t.Setenv("SQS_SENDER_PROVIDER_MAP", localstackSender+"=provider-a")
	t.Setenv("DATABASE_URL", dbURL)
	t.Setenv("OIDC_ISSUER", kc+"/realms/wallet")
	t.Setenv("OIDC_DISCOVERY_URL", "")
	base := "http://" + addr

	application := fxtest.New(t, Module, fx.StartTimeout(30*time.Second), fx.StopTimeout(15*time.Second))
	application.RequireStart()
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			application.RequireStop()
		}
	})

	admin := clientToken(t, kc, "wallet-internal")
	provider := clientToken(t, kc, "provider-a")

	openWallet := func(balance string) (player, walletID string) {
		t.Helper()
		player = newUUID(t)
		r := call(t, http.MethodPost, base+"/wallets", admin, "", fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":%q,"currency":"BRL"}}`, player, balance))
		if r.status != http.StatusCreated {
			t.Fatalf("open wallet: %d %v", r.status, r.body)
		}
		return player, r.body["id"].(string)
	}
	balance := func(walletID string) string {
		return scalar(t, pool, `SELECT (balance_minor / 100) || '.' || lpad((balance_minor % 100)::text, 2, '0') FROM wallets WHERE id = $1`, walletID)
	}
	debits := func(walletID string) string {
		return scalar(t, pool, `SELECT count(*)::text FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, walletID)
	}
	inboxRows := func(id string) string {
		return scalar(t, pool, `SELECT count(*)::text FROM inbox_messages WHERE message_id = $1`, id)
	}
	status := func(provider, ext string) string {
		var s string
		if err := pool.QueryRow(context.Background(), `SELECT status FROM wager_transactions WHERE provider_id = $1 AND external_transaction_id = $2`, provider, ext).Scan(&s); err != nil {
			return ""
		}
		return s
	}
	waitStatus := func(provider, ext, want string) {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for status(provider, ext) != want {
			if time.Now().After(deadline) {
				t.Fatalf("%s/%s is %q, want %q", provider, ext, status(provider, ext), want)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	noDeadLetters := func(t *testing.T) {
		t.Helper()
		if got := dlq.Drain(t); len(got) != 0 {
			t.Fatalf("dead-lettered messages: %+v", got)
		}
	}

	player, walletID := openWallet("1000.00")
	bet := func(msgID, ext string, amount string) wagerMsg {
		return wagerMsg{messageID: msgID, ext: ext, kind: "BET", amount: amount, player: player, wallet: walletID}
	}

	t.Run("a bet by SQS debits once and the same bet by HTTP is a replay", func(t *testing.T) {
		m := bet("msg-1", "sqs-1", "25.00")
		sendRaw(t, inbound, walletID, m.body())
		waitStatus("provider-a", "sqs-1", "PROCESSED")
		idle(t, inbound)

		httpBody := fmt.Sprintf(`{"providerId":"provider-a","externalTransactionId":"sqs-1","playerId":%q,"walletId":%q,"roundId":"round-1","gameId":"game-1","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}`, player, walletID)
		r := postWager(t, base, provider, "provider-a:sqs-1", httpBody)
		bal, _ := r.body["balance"].(map[string]any)
		if r.status != http.StatusOK || r.body["idempotentReplay"] != true || bal["amount"] != "975.00" {
			t.Fatalf("HTTP after SQS = %d %v", r.status, r.body)
		}
		if debits(walletID) != "1" || balance(walletID) != "975.00" || inboxRows("msg-1") != "1" {
			t.Fatalf("debits = %s, balance = %s, inbox = %s", debits(walletID), balance(walletID), inboxRows("msg-1"))
		}
		noDeadLetters(t)
	})

	t.Run("a bet by HTTP delivered again by SQS is a replay", func(t *testing.T) {
		httpBody := fmt.Sprintf(`{"providerId":"provider-a","externalTransactionId":"http-1","playerId":%q,"walletId":%q,"roundId":"round-1","gameId":"game-1","kind":"BET","money":{"amount":"10.00","currency":"BRL"}}`, player, walletID)
		if r := postWager(t, base, provider, "provider-a:http-1", httpBody); r.status != http.StatusOK || r.body["idempotentReplay"] != false {
			t.Fatalf("HTTP = %d %v", r.status, r.body)
		}
		sendRaw(t, inbound, walletID, bet("msg-2", "http-1", "10.00").body())
		idle(t, inbound)

		if debits(walletID) != "2" || balance(walletID) != "965.00" || inboxRows("msg-2") != "1" {
			t.Fatalf("debits = %s, balance = %s, inbox = %s", debits(walletID), balance(walletID), inboxRows("msg-2"))
		}
		noDeadLetters(t)
	})

	t.Run("the same message delivered again is absorbed by the inbox", func(t *testing.T) {
		for range 3 {
			sendRaw(t, inbound, walletID, bet("msg-1", "sqs-1", "25.00").body())
		}
		idle(t, inbound)
		if debits(walletID) != "2" || balance(walletID) != "965.00" || inboxRows("msg-1") != "1" {
			t.Fatalf("debits = %s, balance = %s, inbox = %s", debits(walletID), balance(walletID), inboxRows("msg-1"))
		}
		noDeadLetters(t)
	})

	t.Run("messages that cannot succeed are dead-lettered with a code and change nothing", func(t *testing.T) {
		reusedID := bet("msg-1", "sqs-1", "99.00")
		otherProvider := bet("msg-x1", "intruder-1", "5.00")
		otherProvider.provider = "provider-b"
		opening := bet("msg-x2", "opening-1", "5.00")
		opening.kind = "OPENING"
		unknownWallet := bet("msg-x3", "ghost-1", "5.00")
		unknownWallet.wallet = newUUID(t)
		wrongPlayer := bet("msg-x4", "wrong-player-1", "5.00")
		wrongPlayer.player = newUUID(t)
		conflict := bet("msg-x5", "sqs-1", "25.00")
		conflict.key = "provider-a:another-key"

		tests := []struct {
			name string
			body string
			code string
		}{
			{name: "message id reused with other content", body: reusedID.body(), code: "MESSAGE_ID_REUSED"},
			{name: "not json", body: "{nope", code: "MALFORMED_MESSAGE"},
			{name: "sender is not provider-b", body: otherProvider.body(), code: "PROVIDER_IDENTITY_MISMATCH"},
			{name: "OPENING is internal", body: opening.body(), code: "KIND_NOT_ALLOWED"},
			{name: "wallet does not exist", body: unknownWallet.body(), code: "WALLET_NOT_FOUND"},
			{name: "wallet belongs to another player", body: wrongPlayer.body(), code: "PLAYER_WALLET_MISMATCH"},
			{name: "external id under another key", body: conflict.body(), code: "EXTERNAL_ID_CONFLICT"},
		}
		for _, tt := range tests {
			sendRaw(t, inbound, walletID, tt.body)
		}
		got := dlq.WaitFor(t, len(tests), 30*time.Second)
		codes := map[string]int{}
		for _, m := range got {
			codes[m.FailureCode]++
			if m.GroupID != walletID {
				t.Fatalf("dead letter lost its group: %+v", m)
			}
		}
		for _, tt := range tests {
			if codes[tt.code] != 1 {
				t.Fatalf("%s: failure codes seen = %v", tt.name, codes)
			}
		}
		idle(t, inbound)
		if debits(walletID) != "2" || balance(walletID) != "965.00" {
			t.Fatalf("debits = %s, balance = %s after bad messages", debits(walletID), balance(walletID))
		}
		if n := scalar(t, pool, `SELECT count(*)::text FROM wager_transactions WHERE external_transaction_id IN ('intruder-1','opening-1','ghost-1','wrong-player-1')`); n != "0" {
			t.Fatalf("stored operations of refused messages = %s", n)
		}
	})

	t.Run("business outcomes are final: rejected and pending messages leave the queue", func(t *testing.T) {
		sendRaw(t, inbound, walletID, bet("msg-r1", "too-big-1", "5000.00").body())
		early := wagerMsg{messageID: "msg-p1", ext: "refund-early-1", kind: "REFUND", amount: "10.00", ref: "bet-not-here-yet", player: player, wallet: walletID}
		sendRaw(t, inbound, walletID, early.body())
		waitStatus("provider-a", "too-big-1", "REJECTED")
		waitStatus("provider-a", "refund-early-1", "PENDING_REFERENCE")
		idle(t, inbound)

		if code := scalar(t, pool, `SELECT failure_code FROM wager_transactions WHERE external_transaction_id = 'too-big-1'`); code != "INSUFFICIENT_FUNDS" {
			t.Fatalf("failure code = %s", code)
		}
		if n := scalar(t, pool, `SELECT count(*)::text FROM outbox_events WHERE partition_key = $1 AND event_type IN ('WagerTransactionRejected','WagerTransactionPendingReference')`, walletID); n != "2" {
			t.Fatalf("rejection and pending events = %s", n)
		}
		if inboxRows("msg-r1") != "1" || inboxRows("msg-p1") != "1" {
			t.Fatal("rejected and pending messages must be recorded in the inbox")
		}
		noDeadLetters(t)
	})

	t.Run("the same operation from HTTP and SQS at once is applied once", func(t *testing.T) {
		pl, wid := openWallet("100.00")
		const each = 25
		m := wagerMsg{ext: "race-1", kind: "BET", amount: "30.00", player: pl, wallet: wid}
		httpBody := fmt.Sprintf(`{"providerId":"provider-a","externalTransactionId":"race-1","playerId":%q,"walletId":%q,"roundId":"round-1","gameId":"game-1","kind":"BET","money":{"amount":"30.00","currency":"BRL"}}`, pl, wid)

		var wg sync.WaitGroup
		var processed, replays atomic.Int32
		gate := make(chan struct{})
		for i := range each {
			wg.Add(2)
			go func() {
				defer wg.Done()
				<-gate
				mm := m
				mm.messageID = fmt.Sprintf("race-msg-%d", i)
				sendRaw(t, inbound, wid, mm.body())
			}()
			go func() {
				defer wg.Done()
				<-gate
				r := postWager(t, base, provider, "provider-a:race-1", httpBody)
				switch {
				case r.status == http.StatusOK && r.body["idempotentReplay"] == false:
					processed.Add(1)
				case r.status == http.StatusOK && r.body["idempotentReplay"] == true:
					replays.Add(1)
				default:
					t.Errorf("HTTP = %d %v", r.status, r.body)
				}
			}()
		}
		close(gate)
		wg.Wait()
		idle(t, inbound)

		if debits(wid) != "1" || balance(wid) != "70.00" {
			t.Fatalf("debits = %s, balance = %s, want one debit and 70.00", debits(wid), balance(wid))
		}
		if scalar(t, pool, `SELECT count(*)::text FROM wager_transactions WHERE external_transaction_id = 'race-1'`) != "1" {
			t.Fatal("the operation was stored more than once")
		}
		noDeadLetters(t)
	})

	t.Run("health reports the inbound queue and shutdown drains the consumer", func(t *testing.T) {
		r := call(t, http.MethodGet, base+"/health/ready", "", "", "")
		checks, _ := r.body["checks"].(map[string]any)
		if r.status != http.StatusOK || checks["sqs-inbound"] != "up" {
			t.Fatalf("ready = %d %v", r.status, r.body)
		}
		application.RequireStop()
		stopped = true
	})
}

// failingDelete simulates a replica that dies after the commit and before DeleteMessage.
type failingDelete struct {
	infrasqs.ConsumerAPI
	deletes atomic.Int32
}

func (f *failingDelete) DeleteMessage(context.Context, *awssqs.DeleteMessageInput, ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
	f.deletes.Add(1)
	return nil, fmt.Errorf("connection lost")
}

func TestSQSRedeliveryAfterCrashBeforeDelete(t *testing.T) {
	pool, _ := pgtest.New(t)
	inbound, dlq := sqstest.NewInbound(t, 2)
	ctx := context.Background()

	uow := postgres.NewUnitOfWork(pool)
	clock, ids := system.Clock{}, system.IDs{}
	player := newUUID(t)
	initial, _ := money.Parse("100.00", "BRL")
	w, err := openwallet.New(uow, clock, ids).Execute(ctx, openwallet.Input{PlayerID: player, InitialBalance: initial})
	if err != nil {
		t.Fatal(err)
	}
	walletID := string(w.ID())

	process := processwager.New(uow, clock, ids, processwager.Options{})
	handler := verdictHandler{sqsmsg.NewHandler(process, map[string]string{localstackSender: "provider-a"})}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	run := func(api infrasqs.ConsumerAPI) (stop func()) {
		c := infrasqs.NewConsumer(api, handler, infrasqs.ConsumerConfig{QueueURL: inbound.URL, DLQURL: dlq.URL, Visibility: 2 * time.Second, WaitTime: time.Second}, log)
		cctx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() { c.Run(cctx); close(done) }()
		return func() { cancel(); <-done }
	}

	m := wagerMsg{messageID: "crash-msg-1", ext: "crash-1", kind: "BET", amount: "40.00", player: player, wallet: walletID}
	sendRaw(t, inbound, walletID, m.body())

	// First delivery: the operation commits, the delete fails, so the message stays on the queue.
	crashing := &failingDelete{ConsumerAPI: inbound.Client}
	stop := run(crashing)
	deadline := time.Now().Add(20 * time.Second)
	for crashing.deletes.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the message was never handled")
		}
		time.Sleep(20 * time.Millisecond)
	}
	stop()
	if got := scalar(t, pool, `SELECT status FROM wager_transactions WHERE external_transaction_id = 'crash-1'`); got != "PROCESSED" {
		t.Fatalf("status = %s, want the operation committed before the crash", got)
	}

	// Redelivery to a healthy replica: the inbox recognizes the message, so only the delete happens.
	stop = run(inbound.Client)
	defer stop()
	idle(t, inbound)

	if n := scalar(t, pool, `SELECT count(*)::text FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, walletID); n != "1" {
		t.Fatalf("debits = %s after redelivery, want 1", n)
	}
	if b := scalar(t, pool, `SELECT balance_minor::text FROM wallets WHERE id = $1`, walletID); b != "6000" {
		t.Fatalf("balance_minor = %s, want 6000", b)
	}
	if n := scalar(t, pool, `SELECT count(*)::text FROM inbox_messages WHERE message_id = 'crash-msg-1'`); n != "1" {
		t.Fatalf("inbox rows = %s", n)
	}
	if got := dlq.Drain(t); len(got) != 0 {
		t.Fatalf("dead letters: %+v", got)
	}
}
