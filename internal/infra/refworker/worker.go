// Package refworker retries operations parked as PENDING_REFERENCE, safely shared by many replicas.
package refworker

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/celio001/backend-challenge-go/internal/domain/wager"
	"github.com/celio001/backend-challenge-go/internal/usecase/resolvereference"
	"github.com/celio001/backend-challenge-go/pkg/faultinject"
)

type Store interface {
	// Claim leases up to limit due pending transactions; rows leased by others, until their lease ends, are skipped.
	Claim(ctx context.Context, limit int, lease time.Duration) ([]resolvereference.Pending, error)
	// Release gives back leases that were claimed but not used, so another replica can take them right away.
	Release(ctx context.Context, ids []string) error
}

type Resolver interface {
	// Resolve returns "" when another replica already settled the transaction.
	Resolve(ctx context.Context, p resolvereference.Pending) (wager.Status, error)
}

type Config struct {
	Owner          string
	BatchSize      int
	Lease          time.Duration
	ResolveTimeout time.Duration
	PollInterval   time.Duration
	// Observer is optional.
	Observer Observer
}

// Results reported to the Observer, besides the status the transaction ended in.
const (
	ResultSettledElsewhere = "settled_elsewhere"
	ResultError            = "error"
)

type Observer interface {
	// Attempt reports what one resolution attempt led to: the status the transaction has afterwards, or a Result constant.
	Attempt(result string)
}

func (c Config) withDefaults() Config {
	if c.BatchSize <= 0 {
		c.BatchSize = 20
	}
	if c.Lease <= 0 {
		c.Lease = 30 * time.Second
	}
	if c.ResolveTimeout <= 0 {
		c.ResolveTimeout = 30 * time.Second
	}
	if c.PollInterval <= 0 {
		c.PollInterval = time.Second
	}
	return c
}

const tracerName = "github.com/celio001/backend-challenge-go/refworker"

type Worker struct {
	store    Store
	resolver Resolver
	cfg      Config
	log      *slog.Logger
}

func New(store Store, resolver Resolver, cfg Config, log *slog.Logger) *Worker {
	return &Worker{store: store, resolver: resolver, cfg: cfg.withDefaults(), log: log}
}

// Run resolves pending transactions until ctx ends. The one being resolved when ctx ends is finished; unused leases are released.
func (w *Worker) Run(ctx context.Context) {
	for ctx.Err() == nil {
		claimed, err := w.RunOnce(ctx)
		if err != nil {
			w.log.Warn("reference resolver failed", "owner", w.cfg.Owner, "error", err)
		}
		if err == nil && claimed >= w.cfg.BatchSize {
			continue
		}
		select {
		case <-ctx.Done():
		case <-time.After(w.cfg.PollInterval):
		}
	}
}

// RunOnce claims one batch and resolves it, returning how many transactions were claimed.
func (w *Worker) RunOnce(ctx context.Context) (int, error) {
	pending, err := w.store.Claim(ctx, w.cfg.BatchSize, w.cfg.Lease)
	if err != nil {
		return 0, err
	}
	for i, p := range pending {
		if ctx.Err() != nil {
			w.release(pending[i:])
			return len(pending), nil
		}
		w.resolve(ctx, p)
	}
	return len(pending), nil
}

// resolve detaches from ctx: a shutdown rolls back or finishes the transaction in the database, never leaves it half done.
// A failed attempt keeps its lease, which doubles as the wait before the next try.
func (w *Worker) resolve(ctx context.Context, p resolvereference.Pending) {
	faultinject.Hit("after_claim_before_resolve")
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.cfg.ResolveTimeout)
	defer cancel()
	rctx, span := otel.Tracer(tracerName).Start(rctx, "pending_reference.resolve", trace.WithAttributes(
		attribute.String("transaction.id", string(p.ID)), attribute.String("wallet.id", string(p.WalletID)),
	))
	defer span.End()

	status, err := w.resolver.Resolve(rctx, p)
	span.SetAttributes(attribute.String("wager.status", string(status)))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "resolution failed")
	}
	log := w.log.With("transactionId", string(p.ID), "walletId", string(p.WalletID))
	switch {
	case err != nil:
		w.observe(ResultError)
		log.Warn("pending reference not resolved; it will be tried again when its lease ends", "status", string(status), "error", err)
	case status == "":
		w.observe(ResultSettledElsewhere)
		log.Debug("pending reference already settled by another replica")
	default:
		w.observe(string(status))
		log.Info("pending reference attempt finished", "status", string(status))
	}
}

func (w *Worker) observe(result string) {
	if w.cfg.Observer != nil {
		w.cfg.Observer.Attempt(result)
	}
}

func (w *Worker) release(pending []resolvereference.Pending) {
	ids := make([]string, len(pending))
	for i, p := range pending {
		ids[i] = string(p.ID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.store.Release(ctx, ids); err != nil {
		w.log.Warn("could not release pending leases; they will expire", "count", len(ids), "error", err)
	}
}
