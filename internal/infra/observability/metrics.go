// Package observability exposes what the service does as Prometheus metrics and as structured log lines.
package observability

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/celio001/backend-challenge-go/internal/usecase/reconcile"
)

// Stats are the figures only the database knows. They are read when Prometheus scrapes, so they are never stale.
type Stats interface {
	// OutboxBacklog returns how many events are waiting to be published and the age of the oldest.
	OutboxBacklog(ctx context.Context) (pending int64, oldest time.Duration, err error)
	PendingReferences(ctx context.Context) (int64, error)
}

type Metrics struct {
	registry *prometheus.Registry

	transactions   *prometheus.CounterVec
	replays        *prometheus.CounterVec
	conflicts      *prometheus.CounterVec
	duration       *prometheus.HistogramVec
	dbRetries      *prometheus.CounterVec
	lockWait       prometheus.Histogram
	sqsMessages    *prometheus.CounterVec
	sqsReceiveErrs prometheus.Counter
	outboxAttempts *prometheus.CounterVec
	refAttempts    *prometheus.CounterVec
	divergence     prometheus.Counter
}

// New registers every metric on a registry of its own, so nothing leaks in from or out to the process-wide default.
func New(stats Stats) *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		registry: reg,
		transactions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wager_transactions_total", Help: "Operations answered, by channel, kind, resulting status and failure code.",
		}, []string{"channel", "kind", "status", "failure_code"}),
		replays: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wager_idempotent_replays_total", Help: "Operations received again and answered from the stored result.",
		}, []string{"channel"}),
		conflicts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wager_idempotency_conflicts_total", Help: "Requests refused because an idempotency key or id was reused with other content.",
		}, []string{"reason"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "wager_processing_duration_seconds", Help: "Time to process one operation, from receiving it to its answer.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		}, []string{"channel", "kind"}),
		dbRetries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "db_tx_retries_total", Help: "Times the database asked a transaction to be tried again, by SQLSTATE.",
		}, []string{"sqlstate"}),
		lockWait: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "wallet_lock_wait_seconds", Help: "Time spent waiting for a wallet row lock.",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2},
		}),
		sqsMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sqs_messages_total", Help: "Inbound messages by what happened to them: deleted, duplicate, retried, dead_lettered.",
		}, []string{"outcome"}),
		sqsReceiveErrs: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "sqs_receive_errors_total", Help: "Failed attempts to read the inbound queue.",
		}),
		outboxAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "outbox_publish_attempts_total", Help: "Outbox publish attempts by result: published, failed, lease_lost.",
		}, []string{"result"}),
		refAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pending_reference_attempts_total", Help: "Attempts to resolve a pending reference, by what the transaction became.",
		}, []string{"result"}),
		divergence: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wallet_reconciliation_divergence_total", Help: "Reconciliations that found a wallet balance different from its ledger. Should stay 0.",
		}),
	}
	reg.MustRegister(
		m.transactions, m.replays, m.conflicts, m.duration, m.dbRetries, m.lockWait,
		m.sqsMessages, m.sqsReceiveErrs, m.outboxAttempts, m.refAttempts, m.divergence,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		newStatsCollector(stats),
	)
	return m
}

// Handler serves the metrics in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// postgres.Observer
func (m *Metrics) TransientFailure(sqlstate string) { m.dbRetries.WithLabelValues(sqlstate).Inc() }
func (m *Metrics) LockWait(d time.Duration)         { m.lockWait.Observe(d.Seconds()) }

// sqs.Observer
func (m *Metrics) Message(outcome string) { m.sqsMessages.WithLabelValues(outcome).Inc() }
func (m *Metrics) ReceiveError()          { m.sqsReceiveErrs.Inc() }

// outbox.Observer and refworker.Observer share this shape.
func (m *Metrics) Attempt(result string) { m.outboxAttempts.WithLabelValues(result).Inc() }

// ReferenceObserver adapts the same registry to the reference resolver, whose results are a different set.
type ReferenceObserver struct{ m *Metrics }

func (m *Metrics) References() ReferenceObserver { return ReferenceObserver{m} }

func (r ReferenceObserver) Attempt(result string) { r.m.refAttempts.WithLabelValues(result).Inc() }

// reconcile.Observer
func (m *Metrics) Divergence(context.Context, reconcile.Report) { m.divergence.Inc() }
