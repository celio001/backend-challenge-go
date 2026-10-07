package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wager"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/internal/usecase"
	"github.com/celio001/backend-challenge-go/internal/usecase/processwager"
	"github.com/celio001/backend-challenge-go/internal/usecase/reconcile"
)

type fakeStats struct {
	pending    int64
	oldest     time.Duration
	refs       int64
	outboxErr  error
	refsErr    error
	outboxHits int
}

func (f *fakeStats) OutboxBacklog(context.Context) (int64, time.Duration, error) {
	f.outboxHits++
	return f.pending, f.oldest, f.outboxErr
}

func (f *fakeStats) PendingReferences(context.Context) (int64, error) { return f.refs, f.refsErr }

type fakeProcess struct {
	out processwager.Output
	err error
}

func (f fakeProcess) Execute(context.Context, processwager.Input) (processwager.Output, error) {
	return f.out, f.err
}

func newMetrics(t *testing.T) (*Metrics, *fakeStats) {
	t.Helper()
	stats := &fakeStats{}
	return New(stats), stats
}

func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("scrape status = %d", rec.Code)
	}
	return rec.Body.String()
}

func TestInstrumentCountsEveryResultByLabel(t *testing.T) {
	brl, _ := money.FromMinor(100, money.BRL)
	in := processwager.Input{ProviderID: "provider-a", WalletID: "w-1", Kind: "BET", CorrelationID: "corr-1", Money: brl}

	tests := []struct {
		name         string
		out          processwager.Output
		err          error
		wantLabels   []string // channel, kind, status, failure_code
		wantReplays  float64
		wantConflict map[string]float64
	}{
		{name: "processed", out: processwager.Output{TransactionID: "t1", Status: wager.StatusProcessed}, wantLabels: []string{"http", "BET", "PROCESSED", ""}},
		{name: "rejected carries its failure code", out: processwager.Output{TransactionID: "t1", Status: wager.StatusRejected, FailureCode: wager.CodeInsufficientFunds}, wantLabels: []string{"http", "BET", "REJECTED", "INSUFFICIENT_FUNDS"}},
		{name: "pending reference", out: processwager.Output{TransactionID: "t1", Status: wager.StatusPendingReference}, wantLabels: []string{"http", "BET", "PENDING_REFERENCE", ""}},
		{name: "a replay is also counted as one", out: processwager.Output{TransactionID: "t1", Status: wager.StatusProcessed, Replay: true}, wantLabels: []string{"http", "BET", "PROCESSED", ""}, wantReplays: 1},
		{name: "key reused is a conflict", err: processwager.ErrIdempotencyKeyReused, wantLabels: []string{"http", "BET", "ERROR", "CONFLICT"}, wantConflict: map[string]float64{"key_reused": 1}},
		{name: "external id conflict", err: fmt.Errorf("x: %w", processwager.ErrExternalIDConflict), wantLabels: []string{"http", "BET", "ERROR", "CONFLICT"}, wantConflict: map[string]float64{"external_id_conflict": 1}},
		{name: "message id reused", err: usecase.ErrInboxHashMismatch, wantLabels: []string{"http", "BET", "ERROR", "CONFLICT"}, wantConflict: map[string]float64{"message_id_reused": 1}},
		{name: "validation", err: fmt.Errorf("%w: bad", processwager.ErrValidation), wantLabels: []string{"http", "BET", "ERROR", "VALIDATION"}},
		{name: "unknown wallet is a refusal", err: wallet.ErrNotFound, wantLabels: []string{"http", "BET", "ERROR", "VALIDATION"}},
		{name: "transient", err: fmt.Errorf("%w: db", usecase.ErrTransient), wantLabels: []string{"http", "BET", "ERROR", "TRANSIENT"}},
		{name: "permanent and unknown are internal", err: errors.New("boom"), wantLabels: []string{"http", "BET", "ERROR", "INTERNAL"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, _ := newMetrics(t)
			p := m.Instrument("http", fakeProcess{out: tt.out, err: tt.err}, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))

			out, err := p.Execute(context.Background(), in)

			if !errors.Is(err, tt.err) || out != tt.out {
				t.Fatalf("the wrapper changed the answer: %+v, %v", out, err)
			}
			if got := testutil.ToFloat64(m.transactions.WithLabelValues(tt.wantLabels...)); got != 1 {
				t.Fatalf("wager_transactions_total%v = %v, want 1\n%s", tt.wantLabels, got, scrape(t, m))
			}
			if got := testutil.ToFloat64(m.replays.WithLabelValues("http")); got != tt.wantReplays {
				t.Fatalf("replays = %v, want %v", got, tt.wantReplays)
			}
			for reason, want := range tt.wantConflict {
				if got := testutil.ToFloat64(m.conflicts.WithLabelValues(reason)); got != want {
					t.Fatalf("conflicts{%s} = %v, want %v", reason, got, want)
				}
			}
			if testutil.CollectAndCount(m.duration) != 1 {
				t.Fatal("the duration was not observed exactly once")
			}
		})
	}
}

func TestInstrumentLogsIdentifiersAndNeverTheAmount(t *testing.T) {
	brl, _ := money.FromMinor(123456, money.BRL)
	base := processwager.Input{
		ProviderID: "provider-a", WalletID: "w-1", Kind: "BET", CorrelationID: "corr-1", Money: brl,
		Inbox: &usecase.InboxMessage{Consumer: "wager-transactions", MessageID: "msg-9"},
	}

	tests := []struct {
		name      string
		out       processwager.Output
		err       error
		wantLevel string
		wantMsg   string
		want      map[string]any
		silent    bool
	}{
		{
			name: "an outcome is logged at info with every identifier", out: processwager.Output{TransactionID: "t-1", Status: wager.StatusRejected, FailureCode: wager.CodeInsufficientFunds},
			wantLevel: "INFO", wantMsg: "wager transaction handled",
			want: map[string]any{"channel": "sqs", "kind": "BET", "correlationId": "corr-1", "messageId": "msg-9", "transactionId": "t-1", "walletId": "w-1", "providerId": "provider-a", "status": "REJECTED", "failureCode": "INSUFFICIENT_FUNDS", "replay": false},
		},
		{
			name: "a refusal is a warning", err: fmt.Errorf("%w: bad", processwager.ErrValidation),
			wantLevel: "WARN", wantMsg: "wager request refused",
			want: map[string]any{"correlationId": "corr-1", "messageId": "msg-9", "walletId": "w-1", "providerId": "provider-a", "status": "ERROR", "failureCode": "VALIDATION"},
		},
		{name: "transient failures are logged by the transport, not twice", err: fmt.Errorf("%w: db", usecase.ErrTransient), silent: true},
		{name: "unexpected failures are logged by the transport, not twice", err: errors.New("boom"), silent: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			m, _ := newMetrics(t)
			p := m.Instrument("sqs", fakeProcess{out: tt.out, err: tt.err}, slog.New(slog.NewJSONHandler(&buf, nil)))
			_, _ = p.Execute(context.Background(), base)

			if tt.silent {
				if buf.Len() != 0 {
					t.Fatalf("logged: %s", buf.String())
				}
				return
			}
			var line map[string]any
			if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
				t.Fatalf("not a JSON line: %q", buf.String())
			}
			if line["level"] != tt.wantLevel || line["msg"] != tt.wantMsg {
				t.Fatalf("line = %s", buf.String())
			}
			for k, v := range tt.want {
				if line[k] != v {
					t.Fatalf("field %s = %v, want %v\n%s", k, line[k], v, buf.String())
				}
			}
			for _, secret := range []string{"1234.56", "123456", "amount", "money"} {
				if strings.Contains(buf.String(), secret) {
					t.Fatalf("the log line carries the amount (%q): %s", secret, buf.String())
				}
			}
		})
	}
}

func TestObserversFeedTheirMetrics(t *testing.T) {
	m, _ := newMetrics(t)

	m.TransientFailure("40P01")
	m.TransientFailure("40P01")
	m.TransientFailure("none")
	m.LockWait(30 * time.Millisecond)
	m.Message("deleted")
	m.Message("duplicate")
	m.Message("duplicate")
	m.ReceiveError()
	m.Attempt("published")
	m.Attempt("failed")
	m.References().Attempt("PROCESSED")
	m.Divergence(context.Background(), reconcile.Report{})

	checks := map[string]float64{
		`db_tx_retries_total{sqlstate="40P01"}`:                2,
		`db_tx_retries_total{sqlstate="none"}`:                 1,
		`sqs_messages_total{outcome="deleted"}`:                1,
		`sqs_messages_total{outcome="duplicate"}`:              2,
		`sqs_receive_errors_total`:                             1,
		`outbox_publish_attempts_total{result="published"}`:    1,
		`outbox_publish_attempts_total{result="failed"}`:       1,
		`pending_reference_attempts_total{result="PROCESSED"}`: 1,
		`wallet_reconciliation_divergence_total`:               1,
		`wallet_lock_wait_seconds_count`:                       1,
	}
	body := scrape(t, m)
	for series, want := range checks {
		line := fmt.Sprintf("%s %v", series, want)
		if !strings.Contains(body, line+"\n") {
			t.Errorf("missing %q in the scrape", line)
		}
	}
}

func TestBacklogGaugesAreReadAtScrapeTimeAndCached(t *testing.T) {
	m, stats := newMetrics(t)
	stats.pending, stats.oldest, stats.refs = 7, 90*time.Second, 3

	body := scrape(t, m)
	for _, want := range []string{"outbox_pending_events 7\n", "outbox_oldest_pending_age_seconds 90\n", "pending_reference_open 3\n"} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in\n%s", want, body)
		}
	}

	stats.pending = 99
	scrape(t, m)
	scrape(t, m)
	if stats.outboxHits != 1 {
		t.Fatalf("the backlog query ran %d times for 3 scrapes, want 1 (cached)", stats.outboxHits)
	}
}

func TestAFailingBacklogQueryDropsTheSeriesInsteadOfLyingWithZero(t *testing.T) {
	m, stats := newMetrics(t)
	stats.outboxErr = errors.New("db down")
	stats.refs = 2

	body := scrape(t, m)
	if strings.Contains(body, "outbox_pending_events") || strings.Contains(body, "outbox_oldest_pending_age_seconds") {
		t.Fatalf("outbox gauges were exposed although the query failed:\n%s", body)
	}
	if !strings.Contains(body, "pending_reference_open 2\n") {
		t.Fatalf("a healthy gauge must survive another one's failure:\n%s", body)
	}
}

func TestEveryDocumentedMetricIsRegistered(t *testing.T) {
	m, stats := newMetrics(t)
	stats.pending = 1
	// Counters with labels show up only after their first use; touch each one.
	m.transactions.WithLabelValues("http", "BET", "PROCESSED", "").Inc()
	m.replays.WithLabelValues("http").Inc()
	m.conflicts.WithLabelValues("key_reused").Inc()
	m.duration.WithLabelValues("http", "BET").Observe(0.01)
	m.TransientFailure("40001")
	m.LockWait(time.Millisecond)
	m.Message("deleted")
	m.ReceiveError()
	m.Attempt("published")
	m.References().Attempt("PROCESSED")
	m.Divergence(context.Background(), reconcile.Report{})

	body := scrape(t, m)
	for _, name := range []string{
		"wager_transactions_total", "wager_idempotent_replays_total", "wager_idempotency_conflicts_total", "wager_processing_duration_seconds",
		"db_tx_retries_total", "wallet_lock_wait_seconds", "sqs_messages_total", "sqs_receive_errors_total",
		"outbox_pending_events", "outbox_oldest_pending_age_seconds", "outbox_publish_attempts_total", "pending_reference_open",
		"wallet_reconciliation_divergence_total", "go_goroutines",
	} {
		if !strings.Contains(body, "\n"+name) && !strings.HasPrefix(body, name) {
			t.Errorf("metric %s is not exposed", name)
		}
	}
}
