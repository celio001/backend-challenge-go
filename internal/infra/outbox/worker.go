// Package outbox relays committed outbox rows to the message broker, safely shared by many replicas.
package outbox

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/celio001/backend-challenge-go/pkg/backoff"
	"github.com/celio001/backend-challenge-go/pkg/faultinject"
)

type Message struct {
	ID           string
	PartitionKey string
	Type         string
	Payload      []byte
	// Attempts counts the failed deliveries so far.
	Attempts int
}

type Store interface {
	// Claim leases up to limit due, unpublished events to owner; rows leased by others, until their lease ends, are skipped.
	Claim(ctx context.Context, owner string, limit int, lease time.Duration) ([]Message, error)
	// MarkPublished reports false when owner no longer holds the lease (another replica took the event over).
	MarkPublished(ctx context.Context, id, owner string) (bool, error)
	MarkFailed(ctx context.Context, id, owner string, retryIn time.Duration, cause string) (bool, error)
	// Release gives back leases that were claimed but not used, so another replica can take them right away.
	Release(ctx context.Context, owner string, ids []string) error
}

type Publisher interface {
	// Publish must be safe to repeat for the same Message.ID: delivery is at least once.
	Publish(ctx context.Context, m Message) error
}

type Config struct {
	Owner          string
	BatchSize      int
	Lease          time.Duration
	PublishTimeout time.Duration
	PollInterval   time.Duration
	BackoffBase    time.Duration
	BackoffMax     time.Duration
	// StuckAfter is the attempt count from which an event is logged as an error, so it can raise an alert.
	StuckAfter int
	// Observer is optional.
	Observer Observer
}

// Results reported to the Observer.
const (
	ResultPublished = "published"
	ResultFailed    = "failed"
	ResultLeaseLost = "lease_lost"
)

type Observer interface {
	Attempt(result string)
}

func (c Config) withDefaults() Config {
	if c.BatchSize <= 0 {
		c.BatchSize = 50
	}
	if c.Lease <= 0 {
		c.Lease = 30 * time.Second
	}
	if c.PublishTimeout <= 0 {
		c.PublishTimeout = 10 * time.Second
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 500 * time.Millisecond
	}
	if c.BackoffBase <= 0 {
		c.BackoffBase = time.Second
	}
	if c.BackoffMax <= 0 {
		c.BackoffMax = 5 * time.Minute
	}
	if c.StuckAfter <= 0 {
		c.StuckAfter = 10
	}
	return c
}

const tracerName = "github.com/celio001/backend-challenge-go/outbox"

type Worker struct {
	store Store
	pub   Publisher
	cfg   Config
	log   *slog.Logger
}

func New(store Store, pub Publisher, cfg Config, log *slog.Logger) *Worker {
	return &Worker{store: store, pub: pub, cfg: cfg.withDefaults(), log: log}
}

// Run relays events until ctx ends. The event being published when ctx ends is finished; unused leases are released.
func (w *Worker) Run(ctx context.Context) {
	for ctx.Err() == nil {
		claimed, err := w.RunOnce(ctx)
		if err != nil {
			w.log.Warn("outbox relay failed", "owner", w.cfg.Owner, "error", err)
		}
		// A full batch means more may be waiting; otherwise (or after an error) idle for a moment.
		if err == nil && claimed >= w.cfg.BatchSize {
			continue
		}
		select {
		case <-ctx.Done():
		case <-time.After(w.cfg.PollInterval):
		}
	}
}

// RunOnce claims one batch and relays it, returning how many events were claimed.
func (w *Worker) RunOnce(ctx context.Context) (int, error) {
	msgs, err := w.store.Claim(ctx, w.cfg.Owner, w.cfg.BatchSize, w.cfg.Lease)
	if err != nil {
		return 0, err
	}
	for i, m := range msgs {
		if ctx.Err() != nil {
			w.release(msgs[i:])
			return len(msgs), nil
		}
		w.relay(ctx, m)
	}
	return len(msgs), nil
}

// relay detaches from ctx so a shutdown never leaves an event half done between the broker and the database.
func (w *Worker) relay(ctx context.Context, m Message) {
	bg := context.WithoutCancel(ctx)

	bg, span := otel.Tracer(tracerName).Start(bg, "outbox.publish", trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(
		attribute.String("event.id", m.ID), attribute.String("event.type", m.Type), attribute.String("wallet.id", m.PartitionKey), attribute.Int("outbox.attempt", m.Attempts+1),
	))
	defer span.End()

	pctx, cancel := context.WithTimeout(bg, w.cfg.PublishTimeout)
	err := w.pub.Publish(pctx, m)
	cancel()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "publish failed")
	}
	if err == nil {
		// The event is at the broker but the row still says unpublished: another replica must send it again.
		faultinject.Hit("after_publish_before_mark")
	}

	mctx, cancelMark := context.WithTimeout(bg, 5*time.Second)
	defer cancelMark()

	if err != nil {
		w.observe(ResultFailed)
		retryIn := backoff.Delay(m.Attempts, w.cfg.BackoffBase, w.cfg.BackoffMax, 0.2)
		level := slog.LevelWarn
		if m.Attempts+1 >= w.cfg.StuckAfter {
			level = slog.LevelError
		}
		w.log.Log(ctx, level, "outbox publish failed", "eventId", m.ID, "eventType", m.Type, "attempt", m.Attempts+1, "retryIn", retryIn.String(), "error", err)
		if _, markErr := w.store.MarkFailed(mctx, m.ID, w.cfg.Owner, retryIn, err.Error()); markErr != nil {
			w.log.Warn("could not record outbox failure; the lease will expire", "eventId", m.ID, "error", markErr)
		}
		return
	}

	held, err := w.store.MarkPublished(mctx, m.ID, w.cfg.Owner)
	switch {
	case err != nil:
		// The event is out but still unmarked: it will be relayed again with the same id, which consumers discard.
		w.log.Warn("published but could not mark; it will be sent again", "eventId", m.ID, "error", err)
	case !held:
		w.observe(ResultLeaseLost)
		w.log.Warn("lease lost before marking; another replica relays this event too", "eventId", m.ID)
	default:
		w.observe(ResultPublished)
	}
}

func (w *Worker) observe(result string) {
	if w.cfg.Observer != nil {
		w.cfg.Observer.Attempt(result)
	}
}

func (w *Worker) release(msgs []Message) {
	ids := make([]string, len(msgs))
	for i, m := range msgs {
		ids[i] = m.ID
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.store.Release(ctx, w.cfg.Owner, ids); err != nil {
		w.log.Warn("could not release outbox leases; they will expire", "count", len(ids), "error", err)
	}
}
