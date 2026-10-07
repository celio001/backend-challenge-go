package processwager

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/celio001/backend-challenge-go/internal/domain/event"
	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wager"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/internal/usecase"
	"github.com/celio001/backend-challenge-go/pkg/backoff"
	"github.com/celio001/backend-challenge-go/pkg/canonicaljson"
	"github.com/celio001/backend-challenge-go/pkg/uuid"
)

const (
	maxTextLen          = 255
	pendingFirstAttempt = time.Second
	pendingMaxDelay     = time.Minute
)

type Input struct {
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PlayerID                       string
	WalletID                       string
	RoundID                        string
	GameID                         string
	Kind                           string
	Money                          money.Money
	ReferenceExternalTransactionID string
	// CorrelationID and CausationID are transport metadata: they never enter the idempotency hash.
	CorrelationID string
	CausationID   string
	// Inbox is set by the SQS entry point: the message is registered in the same unit of work as the operation.
	Inbox *usecase.InboxMessage
}

type Output struct {
	TransactionID string
	Status        wager.Status
	// Balance is the balance observed when the transaction was processed; it is only valid for PROCESSED.
	Balance     money.Money
	FailureCode wager.FailureCode
	Replay      bool
}

type Options struct {
	ReferenceTTL time.Duration
	MaxAttempts  int
	RetryBase    time.Duration
}

type UseCase struct {
	uow   usecase.UnitOfWork
	clock usecase.Clock
	ids   usecase.IDGenerator
	opts  Options
}

func New(uow usecase.UnitOfWork, clock usecase.Clock, ids usecase.IDGenerator, opts Options) *UseCase {
	if opts.ReferenceTTL <= 0 {
		opts.ReferenceTTL = 10 * time.Minute
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 3
	}
	if opts.RetryBase <= 0 {
		opts.RetryBase = 50 * time.Millisecond
	}
	return &UseCase{uow: uow, clock: clock, ids: ids, opts: opts}
}

// meta is what the events of one execution need besides the transaction itself. attempts is how many times a resumed
// pending transaction was already tried.
type meta struct {
	correlationID string
	causationID   string
	attempts      int
}

type command struct {
	in   Input
	kind wager.Kind
	hash []byte
}

// Execute is safe to call concurrently from many replicas and to repeat: the database decides, see ARCHITECTURE.md §4.2.
func (uc *UseCase) Execute(ctx context.Context, in Input) (Output, error) {
	cmd, err := prepare(in)
	if err != nil {
		return Output{}, err
	}

	// Reject malformed requests before opening a unit of work; the domain object is rebuilt on every attempt.
	if _, err := uc.newTransaction(cmd); err != nil {
		return Output{}, err
	}

	var out Output
	for attempt := 0; ; attempt++ {
		out, err = uc.once(ctx, cmd)
		if !errors.Is(err, usecase.ErrTransient) || attempt+1 >= uc.opts.MaxAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return Output{}, ctx.Err()
		case <-time.After(backoff.Delay(attempt, uc.opts.RetryBase, time.Second, 0.2)):
		}
	}
	if errors.Is(err, usecase.ErrPermanent) {
		uc.recordFailure(ctx, cmd)
	}
	return out, err
}

func (uc *UseCase) once(ctx context.Context, c command) (Output, error) {
	var out Output
	err := uc.uow.Do(ctx, func(ctx context.Context, tx usecase.Repos) error {
		if c.in.Inbox != nil {
			m := *c.in.Inbox
			m.ReceivedAt = uc.clock.Now()
			duplicate, err := tx.Inbox().Register(ctx, m)
			if err != nil {
				return err
			}
			if duplicate {
				out = Output{Replay: true}
				return nil
			}
		}
		t, err := uc.newTransaction(c)
		if err != nil {
			return err
		}
		out, err = uc.run(ctx, tx, c, t)
		return err
	})
	if err != nil {
		return Output{}, err
	}
	return out, nil
}

func (uc *UseCase) newTransaction(c command) (*wager.Transaction, error) {
	t, err := wager.NewExternal(wager.ExternalInput{
		ID:                    wallet.TxID(uc.ids.NewID()),
		ProviderID:            c.in.ProviderID,
		ExternalTransactionID: c.in.ExternalTransactionID,
		IdempotencyKey:        c.in.IdempotencyKey,
		PayloadHash:           c.hash,
		WalletID:              wallet.WalletID(c.in.WalletID),
		PlayerID:              wallet.PlayerID(c.in.PlayerID),
		RoundID:               c.in.RoundID,
		GameID:                c.in.GameID,
		Kind:                  c.kind,
		Amount:                c.in.Money,
		ReferenceExternalID:   c.in.ReferenceExternalTransactionID,
		CorrelationID:         c.in.CorrelationID,
	}, uc.clock.Now())
	if err != nil {
		return nil, classifyDomainError(err)
	}
	return t, nil
}

func (uc *UseCase) run(ctx context.Context, tx usecase.Repos, c command, t *wager.Transaction) (Output, error) {
	// The unlocked read comes first: inserting against a missing wallet would abort the SQL transaction on the foreign key.
	known, err := tx.Wallets().ByID(ctx, t.WalletID())
	if err != nil {
		return Output{}, err
	}
	if known.PlayerID() != t.PlayerID() {
		return Output{}, ErrPlayerWalletMismatch
	}

	// Inserting before locking makes N concurrent duplicates queue on the unique index instead of on the wallet.
	inserted, err := tx.Transactions().InsertIfAbsent(ctx, t)
	if err != nil {
		return Output{}, err
	}
	if !inserted {
		return replay(ctx, tx, c)
	}

	w, err := tx.Wallets().Lock(ctx, t.WalletID())
	if err != nil {
		return Output{}, err
	}
	out, err := uc.apply(ctx, tx, meta{correlationID: c.in.CorrelationID, causationID: c.in.CausationID}, t, w)
	if err != nil {
		return Output{}, err
	}
	return out, uc.wakeDependents(ctx, tx, t)
}

// Resume applies a PENDING_REFERENCE transaction again with the same rules as a new one. tx must hold the wallet lock and
// the pending row, taken in that order. While the reference is still missing it reschedules, and once the deadline passes it rejects.
func (uc *UseCase) Resume(ctx context.Context, tx usecase.Repos, t *wager.Transaction, w *wallet.Wallet) (Output, error) {
	out, err := uc.apply(ctx, tx, meta{correlationID: t.CorrelationID(), attempts: t.Attempts()}, t, w)
	if err != nil {
		return Output{}, err
	}
	return out, uc.wakeDependents(ctx, tx, t)
}

// wakeDependents is the shortcut of ARCHITECTURE.md §4.4: whoever waits for this transaction is tried now, not at its next backoff.
func (uc *UseCase) wakeDependents(ctx context.Context, tx usecase.Repos, t *wager.Transaction) error {
	return tx.Transactions().WakeWaiting(ctx, t.ProviderID(), t.ExternalTransactionID())
}

func replay(ctx context.Context, tx usecase.Repos, c command) (Output, error) {
	existing, err := tx.Transactions().FindDuplicate(ctx, c.in.ProviderID, c.in.IdempotencyKey, c.in.ExternalTransactionID)
	if errors.Is(err, wager.ErrNotFound) {
		return Output{}, fmt.Errorf("%w: conflicting transaction is not visible yet", usecase.ErrTransient)
	}
	if err != nil {
		return Output{}, err
	}
	if existing.IdempotencyKey() != c.in.IdempotencyKey {
		return Output{}, ErrExternalIDConflict
	}
	if !bytes.Equal(existing.PayloadHash(), c.hash) {
		return Output{}, ErrIdempotencyKeyReused
	}
	out := outputOf(existing)
	out.Replay = true
	return out, nil
}

func (uc *UseCase) apply(ctx context.Context, tx usecase.Repos, m meta, t *wager.Transaction, w *wallet.Wallet) (Output, error) {
	now := uc.clock.Now()

	var ref *wager.Transaction
	if t.ReferenceExternalID() != "" {
		found, err := tx.Transactions().FindByExternalID(ctx, t.ProviderID(), t.ReferenceExternalID())
		if err != nil && !errors.Is(err, wager.ErrNotFound) {
			return Output{}, err
		}
		if err == nil {
			ref = found
		}

		switch err := t.CheckReference(ref); {
		case err == nil:
		case errors.Is(err, wager.ErrReferenceNotFound):
			return uc.waitForReference(ctx, tx, m, t, wager.CodeReferenceNotFound, now)
		case errors.Is(err, wager.ErrReferencePending):
			return uc.waitForReference(ctx, tx, m, t, wager.CodeReferenceNotProcessed, now)
		default:
			return uc.reject(ctx, tx, m, t, referenceFailureCode(err), now)
		}

		if t.Kind() == wager.KindRefund || t.Kind() == wager.KindRollback {
			_, err := tx.Transactions().ProcessedReversalOf(ctx, ref.ID())
			if err == nil {
				return uc.reject(ctx, tx, m, t, wager.CodeReferenceAlreadyReversed, now)
			}
			if !errors.Is(err, wager.ErrNotFound) {
				return Output{}, err
			}
		}
		if err := t.LinkReference(ref.ID(), now); err != nil {
			return Output{}, err
		}
	}

	if t.Amount().Currency() != w.Currency() {
		return uc.reject(ctx, tx, m, t, wager.CodeCurrencyMismatch, now)
	}

	if t.Kind() == wager.KindLoss {
		return uc.complete(ctx, tx, m, t, w, nil, now)
	}

	var refKind wager.Kind
	if ref != nil {
		refKind = ref.Kind()
	}
	direction, moves := t.Kind().Movement(refKind)
	if !moves {
		return Output{}, fmt.Errorf("%w: %s has no movement for referenced kind %q", usecase.ErrPermanent, t.Kind(), refKind)
	}

	versionRead := w.Version()
	var entry wallet.LedgerEntry
	var err error
	if direction == wallet.Debit {
		entry, err = w.Debit(t.Amount(), wallet.TxID(t.ID()), now)
	} else {
		entry, err = w.Credit(t.Amount(), wallet.TxID(t.ID()), now)
	}
	switch {
	case errors.Is(err, wallet.ErrInsufficientFunds):
		code := wager.CodeInsufficientFunds
		if t.Kind() == wager.KindRollback {
			code = wager.CodeReversalInsufficientFunds
		}
		return uc.reject(ctx, tx, m, t, code, now)
	case err != nil:
		return Output{}, err
	}

	if err := tx.Wallets().Update(ctx, w, versionRead); err != nil {
		return Output{}, err
	}
	if err := tx.Ledger().Append(ctx, entry); err != nil {
		return Output{}, fmt.Errorf("append ledger entry: %w", err)
	}
	return uc.complete(ctx, tx, m, t, w, &entry, now)
}

// complete marks t processed with the wallet balance observed now and emits its events.
func (uc *UseCase) complete(ctx context.Context, tx usecase.Repos, m meta, t *wager.Transaction, w *wallet.Wallet, entry *wallet.LedgerEntry, now time.Time) (Output, error) {
	if err := t.Process(w.Balance(), now); err != nil {
		return Output{}, err
	}
	if err := tx.Transactions().Update(ctx, t, time.Time{}); err != nil {
		return Output{}, err
	}
	processed, err := event.NewWagerTransactionProcessed(uc.ids.NewID(), m.causationID, t)
	if err != nil {
		return Output{}, err
	}
	events := []event.Event{processed}
	if entry != nil {
		changed, err := event.NewWalletBalanceChanged(uc.ids.NewID(), m.correlationID, m.causationID, *entry)
		if err != nil {
			return Output{}, err
		}
		events = append(events, changed)
	}
	if err := usecase.AddEvents(ctx, tx, events...); err != nil {
		return Output{}, err
	}
	return outputOf(t), nil
}

func (uc *UseCase) reject(ctx context.Context, tx usecase.Repos, m meta, t *wager.Transaction, code wager.FailureCode, now time.Time) (Output, error) {
	if err := t.Reject(code, now); err != nil {
		return Output{}, err
	}
	if err := tx.Transactions().Update(ctx, t, time.Time{}); err != nil {
		return Output{}, err
	}
	rejected, err := event.NewWagerTransactionRejected(uc.ids.NewID(), m.causationID, t)
	if err != nil {
		return Output{}, err
	}
	if err := usecase.AddEvents(ctx, tx, rejected); err != nil {
		return Output{}, err
	}
	return outputOf(t), nil
}

// waitForReference first parks a new transaction as PENDING_REFERENCE. A transaction that is already parked is rescheduled with
// a growing delay, or rejected once its deadline has passed: with the code that says why the reference never became usable.
func (uc *UseCase) waitForReference(ctx context.Context, tx usecase.Repos, m meta, t *wager.Transaction, expiredCode wager.FailureCode, now time.Time) (Output, error) {
	if t.Status() == wager.StatusPendingReference {
		if !now.Before(t.ExpiresAt()) {
			return uc.reject(ctx, tx, m, t, expiredCode, now)
		}
		delay := backoff.Delay(m.attempts, pendingFirstAttempt, pendingMaxDelay, 0.2)
		if err := tx.Transactions().Reschedule(ctx, t.ID(), delay); err != nil {
			return Output{}, err
		}
		return outputOf(t), nil
	}

	if err := t.MarkPendingReference(now.Add(uc.opts.ReferenceTTL), now); err != nil {
		return Output{}, err
	}
	if err := tx.Transactions().Update(ctx, t, now.Add(pendingFirstAttempt)); err != nil {
		return Output{}, err
	}
	pending, err := event.NewWagerTransactionPendingReference(uc.ids.NewID(), m.causationID, t)
	if err != nil {
		return Output{}, err
	}
	if err := usecase.AddEvents(ctx, tx, pending); err != nil {
		return Output{}, err
	}
	return outputOf(t), nil
}

// recordFailure audits a permanent infrastructure failure in its own unit of work, since the failed one was rolled back.
func (uc *UseCase) recordFailure(ctx context.Context, c command) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = uc.uow.Do(ctx, func(ctx context.Context, tx usecase.Repos) error {
		t, err := uc.newTransaction(c)
		if err != nil {
			return err
		}
		if _, err := tx.Wallets().ByID(ctx, t.WalletID()); err != nil {
			return err
		}
		inserted, err := tx.Transactions().InsertIfAbsent(ctx, t)
		if err != nil || !inserted {
			return err
		}
		if err := t.Fail(uc.clock.Now()); err != nil {
			return err
		}
		return tx.Transactions().Update(ctx, t, time.Time{})
	})
}

func outputOf(t *wager.Transaction) Output {
	out := Output{TransactionID: string(t.ID()), Status: t.Status(), FailureCode: t.FailureCode()}
	if t.Status() == wager.StatusProcessed {
		out.Balance = t.ResultBalance()
	}
	return out
}

func referenceFailureCode(err error) wager.FailureCode {
	switch {
	case errors.Is(err, wager.ErrReferenceNotProcessed):
		return wager.CodeReferenceNotProcessed
	case errors.Is(err, wager.ErrInvalidReferenceKind):
		return wager.CodeInvalidReferenceKind
	case errors.Is(err, wager.ErrReferenceMismatch):
		return wager.CodeReferenceMismatch
	case errors.Is(err, wager.ErrReferenceAmountMismatch):
		return wager.CodeReferenceAmountMismatch
	}
	return wager.CodeReferenceNotFound
}

func prepare(in Input) (command, error) {
	if in.IdempotencyKey == "" {
		return command{}, ErrMissingIdempotencyKey
	}
	for name, v := range map[string]string{
		"providerId": in.ProviderID, "externalTransactionId": in.ExternalTransactionID, "idempotencyKey": in.IdempotencyKey,
		"roundId": in.RoundID, "gameId": in.GameID,
	} {
		if v == "" || len(v) > maxTextLen {
			return command{}, fmt.Errorf("%w: %s must have 1 to %d bytes", ErrValidation, name, maxTextLen)
		}
	}
	if len(in.ReferenceExternalTransactionID) > maxTextLen {
		return command{}, fmt.Errorf("%w: referenceExternalTransactionId is too long", ErrValidation)
	}
	if !uuid.Valid(in.PlayerID) || !uuid.Valid(in.WalletID) {
		return command{}, fmt.Errorf("%w: playerId and walletId must be UUIDs", ErrValidation)
	}
	if !in.Money.IsValid() {
		return command{}, money.ErrUninitialized
	}
	kind := wager.Kind(in.Kind)
	if kind == wager.KindOpening {
		return command{}, ErrKindNotAllowed
	}

	fields := map[string]any{
		"providerId":            in.ProviderID,
		"externalTransactionId": in.ExternalTransactionID,
		"playerId":              in.PlayerID,
		"walletId":              in.WalletID,
		"roundId":               in.RoundID,
		"gameId":                in.GameID,
		"kind":                  in.Kind,
		"money":                 map[string]any{"amount": in.Money.String(), "currency": string(in.Money.Currency())},
	}
	if in.ReferenceExternalTransactionID != "" {
		fields["referenceExternalTransactionId"] = in.ReferenceExternalTransactionID
	}
	canonical, err := canonicaljson.Marshal(fields)
	if err != nil {
		return command{}, fmt.Errorf("canonical payload: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return command{in: in, kind: kind, hash: sum[:]}, nil
}

func classifyDomainError(err error) error {
	switch {
	case errors.Is(err, wager.ErrKindNotAllowed):
		return ErrKindNotAllowed
	case errors.Is(err, wager.ErrInvalidKind), errors.Is(err, wager.ErrMissingField), errors.Is(err, wager.ErrReferenceRequired),
		errors.Is(err, wager.ErrUnexpectedField):
		return fmt.Errorf("%w: %v", ErrValidation, err)
	}
	return err
}
