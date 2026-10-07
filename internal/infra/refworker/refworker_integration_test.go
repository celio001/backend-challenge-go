//go:build integration

package refworker_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/infra/postgres"
	"github.com/celio001/backend-challenge-go/internal/infra/refworker"
	"github.com/celio001/backend-challenge-go/internal/infra/system"
	"github.com/celio001/backend-challenge-go/internal/testutil/pgtest"
	"github.com/celio001/backend-challenge-go/internal/usecase/openwallet"
	"github.com/celio001/backend-challenge-go/internal/usecase/processwager"
	"github.com/celio001/backend-challenge-go/internal/usecase/resolvereference"
)

func newUUID(t *testing.T) string {
	t.Helper()
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

type env struct {
	t        *testing.T
	pool     *pgxpool.Pool
	process  *processwager.UseCase
	resolver *resolvereference.UseCase
	store    *postgres.PendingStore
	player   string
	wallet   string
}

func newEnv(t *testing.T, balance string) *env {
	t.Helper()
	pool, _ := pgtest.New(t)
	uow := postgres.NewUnitOfWork(pool)
	clock, ids := system.Clock{}, system.IDs{}
	process := processwager.New(uow, clock, ids, processwager.Options{ReferenceTTL: 10 * time.Minute})
	e := &env{t: t, pool: pool, process: process, resolver: resolvereference.New(uow, clock, process), store: postgres.NewPendingStore(pool), player: newUUID(t)}
	initial, _ := money.Parse(balance, "BRL")
	w, err := openwallet.New(uow, clock, ids).Execute(context.Background(), openwallet.Input{PlayerID: e.player, InitialBalance: initial})
	if err != nil {
		t.Fatal(err)
	}
	e.wallet = string(w.ID())
	return e
}

func (e *env) send(ext, kind, amount, ref string) processwager.Output {
	e.t.Helper()
	m, _ := money.Parse(amount, "BRL")
	out, err := e.process.Execute(context.Background(), processwager.Input{
		ProviderID: "provider-a", ExternalTransactionID: ext, IdempotencyKey: "k-" + ext, PlayerID: e.player, WalletID: e.wallet,
		RoundID: "r", GameID: "g", Kind: kind, Money: m, ReferenceExternalTransactionID: ref,
	})
	if err != nil {
		e.t.Fatalf("%s %s: %v", kind, ext, err)
	}
	return out
}

func (e *env) scalar(q string, args ...any) (v string) {
	e.t.Helper()
	if err := e.pool.QueryRow(context.Background(), q, args...).Scan(&v); err != nil {
		e.t.Fatalf("%s: %v", q, err)
	}
	return v
}

// makeDue brings every pending transaction to now, as if the first backoff had elapsed.
func (e *env) makeDue() {
	e.t.Helper()
	if _, err := e.pool.Exec(context.Background(), `UPDATE wager_transactions SET next_attempt_at = now() WHERE status = 'PENDING_REFERENCE'`); err != nil {
		e.t.Fatal(err)
	}
}

// cancelAfterClaim delivers the shutdown signal right after the claim, as SIGTERM can.
type cancelAfterClaim struct {
	refworker.Store
	cancel context.CancelFunc
}

func (c cancelAfterClaim) Claim(ctx context.Context, limit int, lease time.Duration) ([]resolvereference.Pending, error) {
	got, err := c.Store.Claim(ctx, limit, lease)
	c.cancel()
	return got, err
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestPendingStore(t *testing.T) {
	ctx := context.Background()

	t.Run("concurrent claims lease each due transaction exactly once", func(t *testing.T) {
		e := newEnv(t, "1000.00")
		const n = 60
		for i := range n {
			e.send(fmt.Sprintf("r-%d", i), "REFUND", "1.00", fmt.Sprintf("b-%d", i))
		}
		e.makeDue()

		const claimers = 6
		var wg sync.WaitGroup
		var mu sync.Mutex
		seen := map[string]int{}
		for range claimers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					got, err := e.store.Claim(ctx, 7, time.Minute)
					if err != nil {
						t.Error(err)
						return
					}
					if len(got) == 0 {
						return
					}
					mu.Lock()
					for _, p := range got {
						seen[string(p.ID)]++
					}
					mu.Unlock()
				}
			}()
		}
		wg.Wait()

		if len(seen) != n {
			t.Fatalf("claimed %d distinct transactions, want %d", len(seen), n)
		}
		for id, times := range seen {
			if times != 1 {
				t.Fatalf("%s was claimed %d times", id, times)
			}
		}
	})

	t.Run("only due transactions are claimed, oldest first", func(t *testing.T) {
		e := newEnv(t, "1000.00")
		for i := range 3 {
			e.send(fmt.Sprintf("r-%d", i), "REFUND", "1.00", fmt.Sprintf("b-%d", i))
		}
		if got, err := e.store.Claim(ctx, 10, time.Minute); err != nil || len(got) != 0 {
			t.Fatalf("claimed %d before the first attempt was due (%v)", len(got), err)
		}
		if _, err := e.pool.Exec(ctx, `UPDATE wager_transactions SET next_attempt_at = now() - make_interval(secs => 10) WHERE external_transaction_id = 'r-2'`); err != nil {
			t.Fatal(err)
		}
		if _, err := e.pool.Exec(ctx, `UPDATE wager_transactions SET next_attempt_at = now() - make_interval(secs => 20) WHERE external_transaction_id = 'r-1'`); err != nil {
			t.Fatal(err)
		}
		got, err := e.store.Claim(ctx, 10, time.Minute)
		if err != nil || len(got) != 2 {
			t.Fatalf("claimed = %d, %v", len(got), err)
		}
		first := e.scalar(`SELECT external_transaction_id FROM wager_transactions WHERE id = $1`, string(got[0].ID))
		if first != "r-1" {
			t.Fatalf("first claimed = %s, want the one overdue the longest", first)
		}
	})

	t.Run("a leased transaction comes back after its lease ends, or at once when released", func(t *testing.T) {
		e := newEnv(t, "1000.00")
		e.send("r-1", "REFUND", "1.00", "b-1")
		e.send("r-2", "REFUND", "1.00", "b-2")
		e.makeDue()

		crashed, err := e.store.Claim(ctx, 10, time.Second) // a replica that dies holding both
		if err != nil || len(crashed) != 2 {
			t.Fatalf("claimed = %d, %v", len(crashed), err)
		}
		if got, _ := e.store.Claim(ctx, 10, time.Minute); len(got) != 0 {
			t.Fatalf("a leased transaction was claimed again (%d)", len(got))
		}
		time.Sleep(1200 * time.Millisecond)
		retaken, err := e.store.Claim(ctx, 10, time.Minute)
		if err != nil || len(retaken) != 2 {
			t.Fatalf("after the lease ended claimed = %d, %v", len(retaken), err)
		}

		if err := e.store.Release(ctx, []string{string(retaken[0].ID)}); err != nil {
			t.Fatal(err)
		}
		again, err := e.store.Claim(ctx, 10, time.Minute)
		if err != nil || len(again) != 1 || again[0].ID != retaken[0].ID {
			t.Fatalf("after release claimed = %v, %v", again, err)
		}
	})
}

func TestWorkersAgainstPostgres(t *testing.T) {
	t.Run("replicas race for late references and every operation is applied once", func(t *testing.T) {
		e := newEnv(t, "1000.00")
		const n = 30
		for i := range n {
			e.send(fmt.Sprintf("r-%d", i), "REFUND", "10.00", fmt.Sprintf("b-%d", i))
		}
		// The bets arrive late; each wakes the refund waiting for it.
		for i := range n {
			e.send(fmt.Sprintf("b-%d", i), "BET", "10.00", "")
		}
		if got := e.scalar(`SELECT balance_minor::text FROM wallets WHERE id = $1`, e.wallet); got != fmt.Sprint(100000-n*1000) {
			t.Fatalf("balance_minor after the bets = %s", got)
		}

		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		for r := range 3 {
			w := refworker.New(e.store, e.resolver, refworker.Config{Owner: fmt.Sprintf("replica-%d", r), PollInterval: 20 * time.Millisecond, BatchSize: 4}, quiet())
			wg.Add(1)
			go func() { defer wg.Done(); w.Run(ctx) }()
		}
		deadline := time.Now().Add(30 * time.Second)
		for e.scalar(`SELECT count(*)::text FROM wager_transactions WHERE status = 'PENDING_REFERENCE'`) != "0" {
			if time.Now().After(deadline) {
				cancel()
				wg.Wait()
				t.Fatal("pending references were not resolved")
			}
			time.Sleep(50 * time.Millisecond)
		}
		cancel()
		wg.Wait()

		if got := e.scalar(`SELECT count(*)::text FROM wager_transactions WHERE kind = 'REFUND' AND status = 'PROCESSED'`); got != fmt.Sprint(n) {
			t.Fatalf("processed refunds = %s, want %d", got, n)
		}
		if got := e.scalar(`SELECT balance_minor::text FROM wallets WHERE id = $1`, e.wallet); got != "100000" {
			t.Fatalf("balance_minor = %s, want the wallet back at its start", got)
		}
		if got := e.scalar(`SELECT count(*)::text FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'CREDIT'`, e.wallet); got != fmt.Sprint(n+1) {
			t.Fatalf("credits = %s, want the opening plus %d refunds, none twice", got, n)
		}
		var ids []string
		rows, err := e.pool.Query(context.Background(), `SELECT transaction_id::text FROM wallet_ledger_entries WHERE wallet_id = $1`, e.wallet)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			ids = append(ids, id)
		}
		rows.Close()
		sort.Strings(ids)
		for i := 1; i < len(ids); i++ {
			if ids[i] == ids[i-1] {
				t.Fatalf("transaction %s has two ledger entries", ids[i])
			}
		}
	})

	t.Run("a worker that stops mid-batch hands the rest to another", func(t *testing.T) {
		e := newEnv(t, "1000.00")
		for i := range 6 {
			e.send(fmt.Sprintf("r-%d", i), "REFUND", "1.00", fmt.Sprintf("b-%d", i))
			e.send(fmt.Sprintf("b-%d", i), "BET", "1.00", "")
		}
		e.makeDue()

		ctx, cancel := context.WithCancel(context.Background())
		stopped := refworker.New(cancelAfterClaim{Store: e.store, cancel: cancel}, e.resolver, refworker.Config{Owner: "stopped"}, quiet())
		claimed, err := stopped.RunOnce(ctx)
		if err != nil || claimed != 6 {
			t.Fatalf("claimed = %d, %v", claimed, err)
		}
		if got := e.scalar(`SELECT count(*)::text FROM wager_transactions WHERE status = 'PENDING_REFERENCE' AND locked_until IS NOT NULL`); got != "0" {
			t.Fatalf("%s leases were left behind by the stopped worker", got)
		}

		other := refworker.New(e.store, e.resolver, refworker.Config{Owner: "other"}, quiet())
		if got, err := other.RunOnce(context.Background()); err != nil || got != 6 {
			t.Fatalf("the other worker claimed %d, %v", got, err)
		}
		if got := e.scalar(`SELECT count(*)::text FROM wager_transactions WHERE kind = 'REFUND' AND status = 'PROCESSED'`); got != "6" {
			t.Fatalf("processed refunds = %s", got)
		}
	})
}
