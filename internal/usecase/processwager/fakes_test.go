package processwager

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/celio001/backend-challenge-go/internal/domain/wager"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/internal/usecase"
)

// fakeStore is an in-memory unit of work with real rollback: Do restores a snapshot when fn fails.
// Entities are cloned on every read and write, so mutating a domain object never changes stored state by accident.
type fakeStore struct {
	wallets map[wallet.WalletID]*wallet.Wallet
	txs     map[wallet.TxID]*wager.Transaction
	ledger  []wallet.LedgerEntry
	outbox  []usecase.OutboxEvent
	nextAt  map[wallet.TxID]time.Time
	inbox   map[string][]byte // consumer/messageId → hash
	delays  []time.Duration   // Reschedule calls, in order
	woken   []string          // provider/externalId of WakeWaiting calls

	doErrs    []error // consumed one per Do call, before fn runs
	doCalls   int
	lockErr   error
	outboxErr error
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		wallets: map[wallet.WalletID]*wallet.Wallet{},
		txs:     map[wallet.TxID]*wager.Transaction{},
		nextAt:  map[wallet.TxID]time.Time{},
		inbox:   map[string][]byte{},
	}
}

func (s *fakeStore) Wallets() usecase.WalletRepository           { return fakeWallets{s} }
func (s *fakeStore) Transactions() usecase.TransactionRepository { return fakeTxs{s} }
func (s *fakeStore) Ledger() usecase.LedgerRepository            { return fakeLedger{s} }
func (s *fakeStore) Outbox() usecase.OutboxWriter                { return fakeOutbox{s} }
func (s *fakeStore) Inbox() usecase.InboxRepository              { return fakeInbox{s} }

func (s *fakeStore) Do(ctx context.Context, fn func(context.Context, usecase.Repos) error) error {
	s.doCalls++
	if len(s.doErrs) > 0 {
		err := s.doErrs[0]
		s.doErrs = s.doErrs[1:]
		if err != nil {
			return err
		}
	}
	wallets, txs, ledger, outbox, nextAt := cloneWallets(s.wallets), cloneTxs(s.txs), append([]wallet.LedgerEntry(nil), s.ledger...), append([]usecase.OutboxEvent(nil), s.outbox...), map[wallet.TxID]time.Time{}
	for k, v := range s.nextAt {
		nextAt[k] = v
	}
	inbox := make(map[string][]byte, len(s.inbox))
	for k, v := range s.inbox {
		inbox[k] = v
	}
	if err := fn(ctx, s); err != nil {
		s.wallets, s.txs, s.ledger, s.outbox, s.nextAt, s.inbox = wallets, txs, ledger, outbox, nextAt, inbox
		return err
	}
	return nil
}

func cloneWallet(w *wallet.Wallet) *wallet.Wallet {
	c, err := wallet.Rehydrate(w.ID(), w.PlayerID(), w.Balance(), w.Version(), w.CreatedAt(), w.UpdatedAt())
	if err != nil {
		panic(err)
	}
	return c
}

func cloneWallets(in map[wallet.WalletID]*wallet.Wallet) map[wallet.WalletID]*wallet.Wallet {
	out := make(map[wallet.WalletID]*wallet.Wallet, len(in))
	for k, v := range in {
		out[k] = cloneWallet(v)
	}
	return out
}

func cloneTx(t *wager.Transaction) *wager.Transaction {
	c, err := wager.Rehydrate(wager.Snapshot{
		ID: t.ID(), Origin: t.Origin(), Kind: t.Kind(), Status: t.Status(), WalletID: t.WalletID(), PlayerID: t.PlayerID(),
		Amount: t.Amount(), ProviderID: t.ProviderID(), ExternalTransactionID: t.ExternalTransactionID(),
		IdempotencyKey: t.IdempotencyKey(), PayloadHash: t.PayloadHash(), RoundID: t.RoundID(), GameID: t.GameID(),
		ReferenceExternalID: t.ReferenceExternalID(), ReferenceTxID: t.ReferenceTxID(), FailureCode: t.FailureCode(),
		ResultBalance: t.ResultBalance(), CorrelationID: t.CorrelationID(), ExpiresAt: t.ExpiresAt(), Attempts: t.Attempts(),
		CreatedAt: t.CreatedAt(), UpdatedAt: t.UpdatedAt(),
	})
	if err != nil {
		panic(err)
	}
	return c
}

func cloneTxs(in map[wallet.TxID]*wager.Transaction) map[wallet.TxID]*wager.Transaction {
	out := make(map[wallet.TxID]*wager.Transaction, len(in))
	for k, v := range in {
		out[k] = cloneTx(v)
	}
	return out
}

type fakeWallets struct{ s *fakeStore }

func (f fakeWallets) Create(context.Context, *wallet.Wallet) error {
	panic("not used by this use case")
}

func (f fakeWallets) ByID(_ context.Context, id wallet.WalletID) (*wallet.Wallet, error) {
	w, ok := f.s.wallets[id]
	if !ok {
		return nil, wallet.ErrNotFound
	}
	return cloneWallet(w), nil
}

func (f fakeWallets) Lock(ctx context.Context, id wallet.WalletID) (*wallet.Wallet, error) {
	if f.s.lockErr != nil {
		return nil, f.s.lockErr
	}
	return f.ByID(ctx, id)
}

func (f fakeWallets) Update(_ context.Context, w *wallet.Wallet, expected int64) error {
	if stored := f.s.wallets[w.ID()]; stored.Version() != expected {
		return errors.Join(usecase.ErrStaleWallet, usecase.ErrTransient)
	}
	f.s.wallets[w.ID()] = cloneWallet(w)
	return nil
}

type fakeTxs struct{ s *fakeStore }

func (f fakeTxs) Insert(context.Context, *wager.Transaction) error {
	panic("not used by this use case")
}

func (f fakeTxs) InsertIfAbsent(_ context.Context, t *wager.Transaction) (bool, error) {
	for _, o := range f.s.txs {
		if o.ProviderID() == t.ProviderID() && (o.IdempotencyKey() == t.IdempotencyKey() || o.ExternalTransactionID() == t.ExternalTransactionID()) {
			return false, nil
		}
	}
	f.s.txs[t.ID()] = cloneTx(t)
	return true, nil
}

func (f fakeTxs) FindDuplicate(_ context.Context, provider, key, ext string) (*wager.Transaction, error) {
	var byExt *wager.Transaction
	for _, o := range f.s.txs {
		if o.ProviderID() != provider {
			continue
		}
		if o.IdempotencyKey() == key {
			return cloneTx(o), nil
		}
		if o.ExternalTransactionID() == ext {
			byExt = o
		}
	}
	if byExt != nil {
		return cloneTx(byExt), nil
	}
	return nil, wager.ErrNotFound
}

func (f fakeTxs) FindByExternalID(_ context.Context, provider, ext string) (*wager.Transaction, error) {
	for _, o := range f.s.txs {
		if o.ProviderID() == provider && o.ExternalTransactionID() == ext {
			return cloneTx(o), nil
		}
	}
	return nil, wager.ErrNotFound
}

func (f fakeTxs) ProcessedReversalOf(_ context.Context, ref wallet.TxID) (*wager.Transaction, error) {
	for _, o := range f.s.txs {
		if o.ReferenceTxID() == ref && o.Status() == wager.StatusProcessed && (o.Kind() == wager.KindRefund || o.Kind() == wager.KindRollback) {
			return cloneTx(o), nil
		}
	}
	return nil, wager.ErrNotFound
}

func (f fakeTxs) Update(_ context.Context, t *wager.Transaction, next time.Time) error {
	stored, ok := f.s.txs[t.ID()]
	if !ok {
		return wager.ErrNotFound
	}
	if stored.IsTerminal() {
		return errors.New("fake: terminal transaction cannot change")
	}
	f.s.txs[t.ID()] = cloneTx(t)
	f.s.nextAt[t.ID()] = next
	return nil
}

type fakeLedger struct{ s *fakeStore }

func (f fakeLedger) Append(_ context.Context, e wallet.LedgerEntry) error {
	f.s.ledger = append(f.s.ledger, e)
	return nil
}

type fakeOutbox struct{ s *fakeStore }

func (f fakeOutbox) Add(_ context.Context, e usecase.OutboxEvent) error {
	if f.s.outboxErr != nil {
		return f.s.outboxErr
	}
	f.s.outbox = append(f.s.outbox, e)
	return nil
}

type fakeInbox struct{ s *fakeStore }

func (f fakeInbox) Register(_ context.Context, m usecase.InboxMessage) (bool, error) {
	key := m.Consumer + "/" + m.MessageID
	stored, seen := f.s.inbox[key]
	if !seen {
		f.s.inbox[key] = m.Hash
		return false, nil
	}
	if !bytes.Equal(stored, m.Hash) {
		return false, usecase.ErrInboxHashMismatch
	}
	return true, nil
}

func (f fakeTxs) LockPending(_ context.Context, id wallet.TxID) (*wager.Transaction, error) {
	t, ok := f.s.txs[id]
	if !ok || t.Status() != wager.StatusPendingReference {
		return nil, wager.ErrNotFound
	}
	return cloneTx(t), nil
}

func (f fakeTxs) Reschedule(_ context.Context, id wallet.TxID, delay time.Duration) error {
	t, ok := f.s.txs[id]
	if !ok || t.Status() != wager.StatusPendingReference {
		return wager.ErrNotFound
	}
	snap := wager.Snapshot{
		ID: t.ID(), Origin: t.Origin(), Kind: t.Kind(), Status: t.Status(), WalletID: t.WalletID(), PlayerID: t.PlayerID(),
		Amount: t.Amount(), ProviderID: t.ProviderID(), ExternalTransactionID: t.ExternalTransactionID(),
		IdempotencyKey: t.IdempotencyKey(), PayloadHash: t.PayloadHash(), RoundID: t.RoundID(), GameID: t.GameID(),
		ReferenceExternalID: t.ReferenceExternalID(), ReferenceTxID: t.ReferenceTxID(), CorrelationID: t.CorrelationID(),
		ExpiresAt: t.ExpiresAt(), Attempts: t.Attempts() + 1, CreatedAt: t.CreatedAt(), UpdatedAt: t.UpdatedAt(),
	}
	bumped, err := wager.Rehydrate(snap)
	if err != nil {
		return err
	}
	f.s.txs[id] = bumped
	f.s.delays = append(f.s.delays, delay)
	return nil
}

func (f fakeTxs) WakeWaiting(_ context.Context, provider, ext string) error {
	f.s.woken = append(f.s.woken, provider+"/"+ext)
	return nil
}
