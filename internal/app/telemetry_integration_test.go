//go:build integration

package app

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/celio001/backend-challenge-go/internal/testutil/pgtest"
	"github.com/celio001/backend-challenge-go/internal/testutil/sqstest"
)

// collector stands in for an OTLP/HTTP trace backend and keeps what it is sent.
type collector struct {
	srv  *httptest.Server
	mu   sync.Mutex
	body []byte
	hits int
}

func newCollector(t *testing.T) *collector {
	t.Helper()
	c := &collector{}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		c.mu.Lock()
		defer c.mu.Unlock()
		if r.URL.Path == "/v1/traces" {
			c.body, c.hits = append(c.body, raw...), c.hits+1
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(c.srv.Close)
	return c
}

func (c *collector) received() ([]byte, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.body...), c.hits
}

// In OTLP/protobuf a trace id is carried as its 16 raw bytes, so finding those bytes proves the span belongs to that trace.
func hasTrace(payload []byte, traceID string) bool {
	raw, _ := hex.DecodeString(traceID)
	return bytes.Contains(payload, raw)
}

func TestTracingExportsTheOperationsSpansAndContinuesTheCallersTrace(t *testing.T) {
	kc := keycloakURL(t)
	pool, dbURL := pgtest.New(t)
	col := newCollector(t)
	inbound, dlq := sqstest.NewInbound(t, 30)
	app := startApp(t, kc, dbURL, map[string]string{
		"OTEL_EXPORTER_OTLP_ENDPOINT": col.srv.URL, "OTEL_SERVICE_NAME": "wallet-service-test",
		"WAGER_QUEUE_NAME": inbound.Name, "WAGER_DLQ_NAME": dlq.Name,
	})
	admin, provider := clientToken(t, kc, "wallet-internal"), clientToken(t, kc, "provider-a")

	player := newUUID(t)
	w := call(t, http.MethodPost, app.base+"/wallets", admin, "", fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"100.00","currency":"BRL"}}`, player))
	walletID := w.body["id"].(string)

	const httpTrace, sqsTrace = "4bf92f3577b34da6a3ce929d0e0e4736", "0af7651916cd43dd8448eb211c80319c"
	body := fmt.Sprintf(`{"providerId":"provider-a","externalTransactionId":"trace-http","playerId":%q,"walletId":%q,"roundId":"r","gameId":"g","kind":"BET","money":{"amount":"10.00","currency":"BRL"}}`, player, walletID)
	req, _ := http.NewRequest(http.MethodPost, app.base+"/wagering/transactions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+provider)
	req.Header.Set("Idempotency-Key", "provider-a:trace-http")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("traceparent", "00-"+httpTrace+"-00f067aa0ba902b7-01")
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("bet: %v %v", resp, err)
	}
	resp.Body.Close()

	// The same operation arrives through the queue, from a sender that put its trace in a message attribute.
	msg := wagerMsg{messageID: "trace-msg", ext: "trace-sqs", kind: "BET", amount: "5.00", player: player, wallet: walletID}
	if _, err := inbound.Client.SendMessage(t.Context(), &awssqs.SendMessageInput{
		QueueUrl: aws.String(inbound.URL), MessageBody: aws.String(msg.body()), MessageGroupId: aws.String(walletID), MessageDeduplicationId: aws.String("trace-dedup"),
		MessageAttributes: map[string]types.MessageAttributeValue{"traceparent": {DataType: aws.String("String"), StringValue: aws.String("00-" + sqsTrace + "-00f067aa0ba902b7-01")}},
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for scalar(t, pool, `SELECT coalesce(max(status), '') FROM wager_transactions WHERE external_transaction_id = 'trace-sqs'`) != "PROCESSED" {
		if time.Now().After(deadline) {
			t.Fatal("the queued bet was not processed")
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Events are published by the outbox while the app runs; give the relay a moment so its spans exist before the flush.
	for scalar(t, pool, `SELECT count(*)::text FROM outbox_events WHERE published_at IS NULL`) != "0" {
		if time.Now().After(deadline) {
			t.Fatal("the outbox was not drained")
		}
		time.Sleep(100 * time.Millisecond)
	}

	app.stop() // stopping flushes the spans still buffered

	payload, hits := col.received()
	if hits == 0 {
		t.Fatal("the collector received nothing")
	}
	for _, want := range []string{
		"wallet-service-test", "POST /wagering/transactions", "POST /wallets", "wager.execute", "db.transaction", "sqs.process", "outbox.publish",
	} {
		if !bytes.Contains(payload, []byte(want)) {
			t.Errorf("no exported span or attribute %q", want)
		}
	}
	if !hasTrace(payload, httpTrace) {
		t.Error("the HTTP caller's trace id was not continued")
	}
	if !hasTrace(payload, sqsTrace) {
		t.Error("the queue sender's trace id was not continued")
	}
	// What must never be exported: credentials and the amount.
	for _, secret := range []string{provider, admin, "Bearer", "10.00"} {
		if bytes.Contains(payload, []byte(secret)) {
			t.Errorf("the exported spans contain %q", secret)
		}
	}
}

func TestTracingOffExportsNothingAndKeepsServing(t *testing.T) {
	kc := keycloakURL(t)
	_, dbURL := pgtest.New(t)
	col := newCollector(t)
	// No endpoint is given to the service, so the collector must never hear from it.
	app := startApp(t, kc, dbURL, nil)
	admin := clientToken(t, kc, "wallet-internal")
	if r := call(t, http.MethodPost, app.base+"/wallets", admin, "", fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"1.00","currency":"BRL"}}`, newUUID(t))); r.status != http.StatusCreated {
		t.Fatalf("open wallet: %d %v", r.status, r.body)
	}
	app.stop()
	if _, hits := col.received(); hits != 0 {
		t.Fatalf("the collector received %d requests although tracing is off", hits)
	}
}
