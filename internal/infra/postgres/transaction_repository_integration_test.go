//go:build integration

package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/celio001/backend-challenge-go/internal/domain/wager"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/internal/usecase"
	"github.com/celio001/backend-challenge-go/internal/usecase/openwallet"
)

type extSpec struct {
	provider, key, ext, round, ref string
	kind                           wager.Kind
	minor                          int64
}

func newWalletWithBalance(t *testing.T, pool *pgxpool.Pool, minor int64) *wallet.Wallet {
	t.Helper()
	w, err := openwallet.New(NewUnitOfWork(pool), realClock{}, uuidIDs{t}).Execute(context.Background(),
		openwallet.Input{PlayerID: newUUID(t), InitialBalance: brl(t, minor)})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func extTx(t *testing.T, w *wallet.Wallet, s extSpec) *wager.Transaction {
	t.Helper()
	if s.provider == "" {
		s.provider = "provider-a"
	}
	if s.round == "" {
		s.round = "round-1"
	}
	if s.kind == "" {
		s.kind = wager.KindBet
	}
	if s.minor == 0 {
		s.minor = 2500
	}
	hash := sha256.Sum256([]byte(s.provider + s.key + s.ext))
	tx, err := wager.NewExternal(wager.ExternalInput{
		ID: wallet.TxID(newUUID(t)), ProviderID: s.provider, ExternalTransactionID: s.ext, IdempotencyKey: s.key,
		PayloadHash: hash[:], WalletID: w.ID(), PlayerID: w.PlayerID(), RoundID: s.round, GameID: "game",
		Kind: s.kind, Amount: brl(t, s.minor), ReferenceExternalID: s.ref, CorrelationID: "corr",
	}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

func TestTransactionRepositoryIdempotencyKeys(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	repo := NewTransactionRepository(pool)
	w := newWalletWithBalance(t, pool, 100000)

	first := extTx(t, w, extSpec{key: "k1", ext: "e1"})
	if ok, err := repo.InsertIfAbsent(ctx, first); err != nil || !ok {
		t.Fatalf("first insert = %v, %v", ok, err)
	}

	tests := []struct {
		name string
		spec extSpec
		want bool
	}{
		{name: "same key, same external id", spec: extSpec{key: "k1", ext: "e1"}},
		{name: "same key, other external id", spec: extSpec{key: "k1", ext: "e2"}},
		{name: "other key, same external id", spec: extSpec{key: "k2", ext: "e1"}},
		{name: "other key, other external id", spec: extSpec{key: "k2", ext: "e2"}, want: true},
		{name: "other provider reuses key and external id", spec: extSpec{provider: "provider-b", key: "k1", ext: "e1"}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ok, err := repo.InsertIfAbsent(ctx, extTx(t, w, tt.spec))
			if err != nil || ok != tt.want {
				t.Fatalf("InsertIfAbsent = %v, %v; want %v", ok, err, tt.want)
			}
		})
	}

	finds := []struct {
		name         string
		provider     string
		key, ext     string
		wantErr      error
		wantSameAsID bool
	}{
		{name: "by key and external id", provider: "provider-a", key: "k1", ext: "e1", wantSameAsID: true},
		{name: "by key only", provider: "provider-a", key: "k1", ext: "unknown", wantSameAsID: true},
		{name: "by external id only", provider: "provider-a", key: "unknown", ext: "e1", wantSameAsID: true},
		{name: "unknown", provider: "provider-a", key: "x", ext: "y", wantErr: wager.ErrNotFound},
		{name: "scoped by provider", provider: "provider-c", key: "k1", ext: "e1", wantErr: wager.ErrNotFound},
	}
	for _, tt := range finds {
		t.Run("find "+tt.name, func(t *testing.T) {
			got, err := repo.FindDuplicate(ctx, tt.provider, tt.key, tt.ext)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantSameAsID && (got.ID() != first.ID() || string(got.PayloadHash()) != string(first.PayloadHash()) || got.Status() != wager.StatusPending) {
				t.Fatalf("found %+v, want %s", got, first.ID())
			}
		})
	}

	t.Run("key match wins over external id match", func(t *testing.T) {
		// k2/e2 belongs to one row; k1 belongs to the first row: asking for k2 with e1 must return the k2 row.
		got, err := repo.FindDuplicate(ctx, "provider-a", "k2", "e1")
		if err != nil || got.IdempotencyKey() != "k2" {
			t.Fatalf("got %v, %v", got, err)
		}
	})
}

func TestTransactionRepositoryUpdateAndReferences(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	repo := NewTransactionRepository(pool)
	w := newWalletWithBalance(t, pool, 100000)

	loss, err := wager.NewExternal(wager.ExternalInput{
		ID: wallet.TxID(newUUID(t)), ProviderID: "provider-a", ExternalTransactionID: "e-loss", IdempotencyKey: "k-loss",
		PayloadHash: make([]byte, 32), WalletID: w.ID(), PlayerID: w.PlayerID(), RoundID: "r", GameID: "g",
		Kind: wager.KindLoss, Amount: brl(t, 0),
	}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := repo.InsertIfAbsent(ctx, loss); err != nil || !ok {
		t.Fatalf("insert = %v, %v", ok, err)
	}
	if err := loss.Process(w.Balance(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := repo.Update(ctx, loss, time.Time{}); err != nil {
		t.Fatal(err)
	}
	got, err := repo.FindByExternalID(ctx, "provider-a", "e-loss")
	if err != nil || got.Status() != wager.StatusProcessed || got.ResultBalance().Minor() != 100000 {
		t.Fatalf("after update: %+v, %v", got, err)
	}
	if err := repo.Update(ctx, loss, time.Time{}); err == nil {
		t.Fatal("a terminal transaction was updated again")
	}

	t.Run("pending reference keeps expiry and schedule", func(t *testing.T) {
		refund := extTx(t, w, extSpec{key: "k-refund", ext: "e-refund", kind: wager.KindRefund, ref: "e-missing"})
		if _, err := repo.InsertIfAbsent(ctx, refund); err != nil {
			t.Fatal(err)
		}
		now := time.Now()
		if err := refund.MarkPendingReference(now.Add(10*time.Minute), now); err != nil {
			t.Fatal(err)
		}
		if err := repo.Update(ctx, refund, now.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		got, err := repo.FindByExternalID(ctx, "provider-a", "e-refund")
		if err != nil || got.Status() != wager.StatusPendingReference || got.ExpiresAt().Sub(now.Add(10*time.Minute)) > time.Millisecond {
			t.Fatalf("got %+v, %v", got, err)
		}
		var next *time.Time
		if err := pool.QueryRow(ctx, `SELECT next_attempt_at FROM wager_transactions WHERE id = $1`, string(refund.ID())).Scan(&next); err != nil || next == nil {
			t.Fatalf("next_attempt_at = %v, %v", next, err)
		}
	})

	t.Run("updating an unknown transaction fails", func(t *testing.T) {
		ghost := extTx(t, w, extSpec{key: "ghost", ext: "ghost"})
		if err := repo.Update(ctx, ghost, time.Time{}); !errors.Is(err, wager.ErrNotFound) {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestSecondProcessedReversalIsRefusedAsTransient(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	repo := NewTransactionRepository(pool)
	w := newWalletWithBalance(t, pool, 100000)

	bet := extTx(t, w, extSpec{key: "k-bet", ext: "e-bet"})
	if _, err := repo.InsertIfAbsent(ctx, bet); err != nil {
		t.Fatal(err)
	}
	if err := bet.Process(brl(t, 97500), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := repo.Update(ctx, bet, time.Time{}); err != nil {
		t.Fatal(err)
	}

	reverse := func(key, ext string, kind wager.Kind) error {
		tx := extTx(t, w, extSpec{key: key, ext: ext, kind: kind, ref: "e-bet"})
		if err := tx.LinkReference(bet.ID(), time.Now()); err != nil {
			t.Fatal(err)
		}
		if err := tx.Process(brl(t, 100000), time.Now()); err != nil {
			t.Fatal(err)
		}
		return NewUnitOfWork(pool).Do(ctx, func(ctx context.Context, r usecase.Repos) error {
			return r.Transactions().Insert(ctx, tx)
		})
	}
	if err := reverse("k-r1", "e-r1", wager.KindRefund); err != nil {
		t.Fatalf("first reversal: %v", err)
	}
	err := reverse("k-r2", "e-r2", wager.KindRollback)
	if !errors.Is(err, usecase.ErrTransient) {
		t.Fatalf("second reversal err = %v, want transient", err)
	}

	got, err := repo.ProcessedReversalOf(ctx, bet.ID())
	if err != nil || got.ExternalTransactionID() != "e-r1" {
		t.Fatalf("ProcessedReversalOf = %v, %v", got, err)
	}
	if _, err := repo.ProcessedReversalOf(ctx, wallet.TxID(newUUID(t))); !errors.Is(err, wager.ErrNotFound) {
		t.Fatalf("unknown reference: %v", err)
	}
}

func TestWalletLockingAndVersionedUpdate(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	uow := NewUnitOfWork(pool)
	seed := newWalletWithBalance(t, pool, 100000)
	id := seed.ID()

	t.Run("update needs the version it read", func(t *testing.T) {
		err := uow.Do(ctx, func(ctx context.Context, r usecase.Repos) error {
			w, err := r.Wallets().Lock(ctx, id)
			if err != nil {
				return err
			}
			if _, err := w.Credit(brl(t, 2500), wallet.TxID(newUUID(t)), time.Now()); err != nil {
				return err
			}
			// Credit already advanced the version, so passing it as "expected" is stale on purpose.
			return r.Wallets().Update(ctx, w, w.Version())
		})
		if !errors.Is(err, usecase.ErrStaleWallet) || !errors.Is(err, usecase.ErrTransient) {
			t.Fatalf("err = %v, want stale + transient", err)
		}
	})

	t.Run("a second locker waits for the first to finish", func(t *testing.T) {
		held := make(chan struct{})
		release := make(chan struct{})
		var mu sync.Mutex
		var order []string
		mark := func(s string) { mu.Lock(); order = append(order, s); mu.Unlock() }

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = uow.Do(ctx, func(ctx context.Context, r usecase.Repos) error {
				if _, err := r.Wallets().Lock(ctx, id); err != nil {
					return err
				}
				close(held)
				<-release
				mark("first done")
				return nil
			})
		}()
		go func() {
			defer wg.Done()
			<-held
			_ = uow.Do(ctx, func(ctx context.Context, r usecase.Repos) error {
				if _, err := r.Wallets().Lock(ctx, id); err != nil {
					return err
				}
				mark("second locked")
				return nil
			})
		}()

		<-held
		time.Sleep(300 * time.Millisecond)
		mu.Lock()
		early := len(order)
		mu.Unlock()
		close(release)
		wg.Wait()
		if early != 0 || len(order) != 2 || order[0] != "first done" || order[1] != "second locked" {
			t.Fatalf("order = %v (events before release: %d)", order, early)
		}
	})

	t.Run("unknown wallet", func(t *testing.T) {
		err := uow.Do(ctx, func(ctx context.Context, r usecase.Repos) error {
			_, err := r.Wallets().Lock(ctx, wallet.WalletID(newUUID(t)))
			return err
		})
		if !errors.Is(err, wallet.ErrNotFound) {
			t.Fatalf("err = %v", err)
		}
	})
}
