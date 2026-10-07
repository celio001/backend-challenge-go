//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/internal/infra/system"
	"github.com/celio001/backend-challenge-go/internal/usecase/openwallet"
	"github.com/celio001/backend-challenge-go/internal/usecase/processwager"
	"github.com/celio001/backend-challenge-go/internal/usecase/reconcile"
)

type recordingObserver struct {
	mu  sync.Mutex
	got []reconcile.Report
}

func (o *recordingObserver) Divergence(_ context.Context, r reconcile.Report) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.got = append(o.got, r)
}

func TestReconciliation(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	uow := NewUnitOfWork(pool)
	clock, ids := system.Clock{}, system.IDs{}
	process := processwager.New(uow, clock, ids, processwager.Options{})
	obs := &recordingObserver{}
	rec := reconcile.New(NewReconciliationReader(pool), obs)

	open := func(balance string) (player, id string) {
		t.Helper()
		player = newUUID(t)
		initial, _ := money.Parse(balance, "BRL")
		w, err := openwallet.New(uow, clock, ids).Execute(ctx, openwallet.Input{PlayerID: player, InitialBalance: initial})
		if err != nil {
			t.Fatal(err)
		}
		return player, string(w.ID())
	}
	bet := func(player, walletID, ext, amount string) {
		t.Helper()
		m, _ := money.Parse(amount, "BRL")
		ext = walletID[len(walletID)-8:] + "-" + ext // idempotency is per provider, so each wallet needs its own ids; the tail of a UUIDv7 is the random part
		if _, err := process.Execute(ctx, processwager.Input{
			ProviderID: "provider-a", ExternalTransactionID: ext, IdempotencyKey: "k-" + ext, PlayerID: player, WalletID: walletID,
			RoundID: "r", GameID: "g", Kind: "BET", Money: m,
		}); err != nil {
			t.Fatalf("bet %s: %v", ext, err)
		}
	}

	t.Run("a wallet moved only through the application always reconciles", func(t *testing.T) {
		player, id := open("1000.00")
		bet(player, id, "b1", "25.00")
		bet(player, id, "b2", "100.00")

		got, err := rec.Execute(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if !got.Consistent || got.Stored.String() != "875.00" || got.Calculated.String() != "875.00" || got.Difference.String() != "0.00" || got.CheckedEntries != 3 {
			t.Fatalf("report = %+v", got)
		}
	})

	t.Run("an empty wallet has nothing to check and is consistent", func(t *testing.T) {
		_, id := open("0.00")
		got, err := rec.Execute(ctx, id)
		if err != nil || !got.Consistent || got.CheckedEntries != 0 || got.Calculated.String() != "0.00" {
			t.Fatalf("report = %+v, %v", got, err)
		}
	})

	t.Run("a balance changed behind the application's back is reported, and nothing is repaired", func(t *testing.T) {
		player, id := open("1000.00")
		bet(player, id, "b1", "25.00")
		before := len(obs.got)

		// Only the table owner can do this: triggers are switched off for the session so neither the ledger guard nor the
		// deferred consistency check stops the write. It stands for a bug, a bad manual fix or corruption.
		tamper, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tamper.Exec(ctx, `SET LOCAL session_replication_role = replica`); err != nil {
			t.Skipf("this role cannot disable triggers (%v); run the tests as the database owner", err)
		}
		if _, err := tamper.Exec(ctx, `UPDATE wallets SET balance_minor = balance_minor + 500 WHERE id = $1`, id); err != nil {
			t.Fatal(err)
		}
		if err := tamper.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		got, err := rec.Execute(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if got.Consistent || got.Stored.String() != "980.00" || got.Calculated.String() != "975.00" || got.Difference.String() != "5.00" || got.CheckedEntries != 2 {
			t.Fatalf("report = %+v", got)
		}
		if len(obs.got) != before+1 || obs.got[before].WalletID != wallet.WalletID(id) {
			t.Fatalf("divergence alerts = %d, want one more for %s", len(obs.got)-before, id)
		}

		// Reconciling must leave the wallet exactly as it found it.
		var stored int64
		if err := pool.QueryRow(ctx, `SELECT balance_minor FROM wallets WHERE id = $1`, id).Scan(&stored); err != nil || stored != 98000 {
			t.Fatalf("balance_minor after reconciling = %d, %v", stored, err)
		}
		again, err := rec.Execute(ctx, id)
		if err != nil || again.Consistent || again.Difference.String() != "5.00" {
			t.Fatalf("second report = %+v, %v", again, err)
		}
	})

	t.Run("a balance below the ledger gives a negative difference", func(t *testing.T) {
		_, id := open("100.00")
		tamper, _ := pool.Begin(ctx)
		if _, err := tamper.Exec(ctx, `SET LOCAL session_replication_role = replica`); err != nil {
			t.Skip("this role cannot disable triggers")
		}
		if _, err := tamper.Exec(ctx, `UPDATE wallets SET balance_minor = 7000 WHERE id = $1`, id); err != nil {
			t.Fatal(err)
		}
		if err := tamper.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		got, err := rec.Execute(ctx, id)
		if err != nil || got.Consistent || got.Difference.String() != "-30.00" {
			t.Fatalf("report = %+v, %v", got, err)
		}
	})

	t.Run("unknown wallet", func(t *testing.T) {
		if _, err := rec.Execute(ctx, newUUID(t)); !errors.Is(err, wallet.ErrNotFound) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("reconciling while the wallet is busy never reports a false divergence", func(t *testing.T) {
		player, id := open("100000.00")
		const bets, checks = 60, 120
		var wg sync.WaitGroup
		stop := make(chan struct{})
		for i := range bets {
			wg.Add(1)
			go func() {
				defer wg.Done()
				bet(player, id, fmt.Sprintf("busy-%d", i), "1.00")
			}()
		}
		var falseAlarms, runs int
		var mu sync.Mutex
		var checkers sync.WaitGroup
		for range 4 {
			checkers.Add(1)
			go func() {
				defer checkers.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					got, err := rec.Execute(ctx, id)
					if err != nil {
						t.Error(err)
						return
					}
					mu.Lock()
					runs++
					if !got.Consistent {
						falseAlarms++
					}
					mu.Unlock()
					time.Sleep(time.Millisecond)
				}
			}()
		}
		wg.Wait()
		close(stop)
		checkers.Wait()
		if runs < checks/10 || falseAlarms != 0 {
			t.Fatalf("runs = %d, false alarms = %d", runs, falseAlarms)
		}
		final, err := rec.Execute(ctx, id)
		if err != nil || !final.Consistent || final.CheckedEntries != bets+1 {
			t.Fatalf("final = %+v, %v", final, err)
		}
	})
}
