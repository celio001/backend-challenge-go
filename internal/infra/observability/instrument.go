package observability

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/internal/usecase"
	"github.com/celio001/backend-challenge-go/internal/usecase/processwager"
)

// Process is the use case both transports call, so one wrapper observes HTTP and SQS alike.
type Process interface {
	Execute(ctx context.Context, in processwager.Input) (processwager.Output, error)
}

// Instrument counts, times and logs every call to next under the given channel ("http" or "sqs").
// Amounts never reach the log line; the identifiers are what lets an operator follow one operation through the system.
func (m *Metrics) Instrument(channel string, next Process, log *slog.Logger) Process {
	return &instrumented{m: m, channel: channel, next: next, log: log}
}

type instrumented struct {
	m       *Metrics
	channel string
	next    Process
	log     *slog.Logger
}

func (i *instrumented) Execute(ctx context.Context, in processwager.Input) (processwager.Output, error) {
	start := time.Now()
	out, err := i.next.Execute(ctx, in)
	elapsed := time.Since(start)

	i.m.duration.WithLabelValues(i.channel, in.Kind).Observe(elapsed.Seconds())

	attrs := []any{
		"channel", i.channel, "kind", in.Kind, "correlationId", in.CorrelationID, "walletId", in.WalletID,
		"providerId", in.ProviderID, "durationMs", elapsed.Milliseconds(),
	}
	if in.Inbox != nil {
		attrs = append(attrs, "messageId", in.Inbox.MessageID)
	}

	if err != nil {
		class := errorClass(err)
		i.m.transactions.WithLabelValues(i.channel, in.Kind, "ERROR", class).Inc()
		if reason := conflictReason(err); reason != "" {
			i.m.conflicts.WithLabelValues(reason).Inc()
		}
		// Transient and unexpected failures are logged where they are handled (HTTP problem mapping, SQS consumer); refusals only here.
		if class != classTransient && class != classInternal {
			i.log.WarnContext(ctx, "wager request refused", append(attrs, "status", "ERROR", "failureCode", class, "error", err.Error())...)
		}
		return out, err
	}

	i.m.transactions.WithLabelValues(i.channel, in.Kind, string(out.Status), string(out.FailureCode)).Inc()
	if out.Replay {
		i.m.replays.WithLabelValues(i.channel).Inc()
	}
	attrs = append(attrs, "transactionId", out.TransactionID, "status", string(out.Status), "replay", out.Replay)
	if out.FailureCode != "" {
		attrs = append(attrs, "failureCode", string(out.FailureCode))
	}
	i.log.InfoContext(ctx, "wager transaction handled", attrs...)
	return out, nil
}

const (
	classValidation = "VALIDATION"
	classConflict   = "CONFLICT"
	classTransient  = "TRANSIENT"
	classInternal   = "INTERNAL"
)

func errorClass(err error) string {
	switch {
	case conflictReason(err) != "":
		return classConflict
	case errors.Is(err, usecase.ErrTransient):
		return classTransient
	case errors.Is(err, usecase.ErrPermanent):
		return classInternal
	case isRefusal(err):
		return classValidation
	}
	return classInternal
}

func conflictReason(err error) string {
	switch {
	case errors.Is(err, processwager.ErrIdempotencyKeyReused):
		return "key_reused"
	case errors.Is(err, processwager.ErrExternalIDConflict):
		return "external_id_conflict"
	case errors.Is(err, usecase.ErrInboxHashMismatch):
		return "message_id_reused"
	}
	return ""
}

// isRefusal recognizes the correctable errors: nothing was stored and the sender may fix the request.
func isRefusal(err error) bool {
	for _, target := range []error{
		processwager.ErrValidation, processwager.ErrMissingIdempotencyKey, processwager.ErrKindNotAllowed,
		processwager.ErrPlayerWalletMismatch, wallet.ErrNotFound, money.ErrUninitialized,
	} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}
