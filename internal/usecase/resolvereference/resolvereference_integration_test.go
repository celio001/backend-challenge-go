//go:build integration

package resolvereference_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wager"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/internal/infra/postgres"
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
	t       *testing.T
	pool    *pgxpool.Pool
	process *processwager.UseCase
	resolve *resolvereference.UseCase
}

func newEnv(t *testing.T, ttl time.Duration) *env {
	t.Helper()
	pool, _ := pgtest.New(t)
	uow := postgres.NewUnitOfWork(pool)
	clock, ids := system.Clock{}, system.IDs{}
	process := processwager.New(uow, clock, ids, processwager.Options{ReferenceTTL: ttl})
	return &env{t: t, pool: pool, process: process, resolve: resolvereference.New(uow, clock, process)}
}

func (e *env) wallet(balance string) (player, id string) {
	e.t.Helper()
	player = newUUID(e.t)
	initial, _ := money.Parse(balance, "BRL")
	w, err := openwallet.New(postgres.NewUnitOfWork(e.pool), system.Clock{}, system.IDs{}).Execute(context.Background(), openwallet.Input{PlayerID: player, InitialBalance: initial})
	if err != nil {
		e.t.Fatal(err)
	}
	return player, string(w.ID())
}

func (e *env) send(player, walletID, ext, kind, amount, ref string) processwager.Output {
	e.t.Helper()
	m, _ := money.Parse(amount, "BRL")
	out, err := e.process.Execute(context.Background(), processwager.Input{
		ProviderID: "provider-a", ExternalTransactionID: ext, IdempotencyKey: "k-" + ext, PlayerID: player, WalletID: walletID,
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

func TestResolveAgainstPostgres(t *testing.T) {
	ctx := context.Background()

	t.Run("a refund that waited for its bet completes once the bet exists", func(t *testing.T) {
		e := newEnv(t, 10*time.Minute)
		player, w := e.wallet("100.00")
		early := e.send(player, w, "r1", "REFUND", "25.00", "b1")
		if early.Status != wager.StatusPendingReference {
			t.Fatalf("early refund = %+v", early)
		}
		if got := e.scalar(`SELECT attempts::text FROM wager_transactions WHERE id = $1`, early.TransactionID); got != "0" {
			t.Fatalf("attempts = %s", got)
		}

		e.send(player, w, "b1", "BET", "25.00", "")
		// The bet arriving made the refund due now, long before its 1s first attempt.
		if due := e.scalar(`SELECT (next_attempt_at <= now())::text FROM wager_transactions WHERE id = $1`, early.TransactionID); due != "true" {
			t.Fatalf("the waiting refund was not woken by its bet")
		}

		status, err := e.resolve.Resolve(ctx, resolvereference.Pending{ID: wallet.TxID(early.TransactionID), WalletID: wallet.WalletID(w)})
		if err != nil || status != wager.StatusProcessed {
			t.Fatalf("resolve = %q, %v", status, err)
		}
		if got := e.scalar(`SELECT balance_minor::text FROM wallets WHERE id = $1`, w); got != "10000" {
			t.Fatalf("balance_minor = %s, want the bet refunded", got)
		}
		if got := e.scalar(`SELECT count(*)::text FROM wallet_ledger_entries WHERE transaction_id = $1`, early.TransactionID); got != "1" {
			t.Fatalf("ledger entries of the refund = %s", got)
		}
		if got := e.scalar(`SELECT string_agg(event_type, ',' ORDER BY occurred_at, event_type) FROM outbox_events WHERE aggregate_id = $1`, early.TransactionID); got == "" {
			t.Fatal("no events for the resolved refund")
		}
		// The row is settled: asking again changes nothing.
		again, err := e.resolve.Resolve(ctx, resolvereference.Pending{ID: wallet.TxID(early.TransactionID), WalletID: wallet.WalletID(w)})
		if err != nil || again != "" {
			t.Fatalf("second resolve = %q, %v", again, err)
		}
	})

	t.Run("a missing reference reschedules, counts the attempt, and rejects at the deadline", func(t *testing.T) {
		e := newEnv(t, 2*time.Second)
		player, w := e.wallet("100.00")
		early := e.send(player, w, "r1", "REFUND", "25.00", "never")
		p := resolvereference.Pending{ID: wallet.TxID(early.TransactionID), WalletID: wallet.WalletID(w)}

		status, err := e.resolve.Resolve(ctx, p)
		if err != nil || status != wager.StatusPendingReference {
			t.Fatalf("first resolve = %q, %v", status, err)
		}
		if got := e.scalar(`SELECT attempts::text FROM wager_transactions WHERE id = $1`, early.TransactionID); got != "1" {
			t.Fatalf("attempts = %s", got)
		}
		if got := e.scalar(`SELECT (next_attempt_at > now())::text FROM wager_transactions WHERE id = $1`, early.TransactionID); got != "true" {
			t.Fatal("the next attempt was not pushed into the future")
		}

		time.Sleep(2200 * time.Millisecond)
		status, err = e.resolve.Resolve(ctx, p)
		if err != nil || status != wager.StatusRejected {
			t.Fatalf("resolve after the deadline = %q, %v", status, err)
		}
		if got := e.scalar(`SELECT failure_code FROM wager_transactions WHERE id = $1`, early.TransactionID); got != "REFERENCE_NOT_FOUND" {
			t.Fatalf("failure code = %s", got)
		}
		if got := e.scalar(`SELECT count(*)::text FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'WagerTransactionRejected'`, early.TransactionID); got != "1" {
			t.Fatalf("rejection events = %s", got)
		}
		if got := e.scalar(`SELECT balance_minor::text FROM wallets WHERE id = $1`, w); got != "10000" {
			t.Fatalf("balance_minor = %s, a rejection must not move money", got)
		}
	})

	t.Run("two replicas resolving the same pending apply it once", func(t *testing.T) {
		e := newEnv(t, 10*time.Minute)
		player, w := e.wallet("100.00")
		early := e.send(player, w, "r1", "REFUND", "25.00", "b1")
		e.send(player, w, "b1", "BET", "25.00", "")
		p := resolvereference.Pending{ID: wallet.TxID(early.TransactionID), WalletID: wallet.WalletID(w)}

		const replicas = 8
		var wg sync.WaitGroup
		var applied, skipped atomic.Int32
		gate := make(chan struct{})
		for range replicas {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-gate
				status, err := e.resolve.Resolve(ctx, p)
				switch {
				case err != nil:
					t.Errorf("resolve: %v", err)
				case status == wager.StatusProcessed:
					applied.Add(1)
				case status == "":
					skipped.Add(1)
				default:
					t.Errorf("unexpected status %q", status)
				}
			}()
		}
		close(gate)
		wg.Wait()

		if applied.Load() != 1 || skipped.Load() != replicas-1 {
			t.Fatalf("applied = %d, skipped = %d", applied.Load(), skipped.Load())
		}
		if got := e.scalar(`SELECT count(*)::text FROM wallet_ledger_entries WHERE transaction_id = $1`, early.TransactionID); got != "1" {
			t.Fatalf("ledger entries of the refund = %s", got)
		}
	})

	t.Run("resolvers and arriving transactions on one wallet do not deadlock", func(t *testing.T) {
		e := newEnv(t, 10*time.Minute)
		player, w := e.wallet("1000.00")
		const n = 30
		pendings := make([]resolvereference.Pending, n)
		for i := range n {
			out := e.send(player, w, fmt.Sprintf("r-%d", i), "REFUND", "10.00", fmt.Sprintf("b-%d", i))
			pendings[i] = resolvereference.Pending{ID: wallet.TxID(out.TransactionID), WalletID: wallet.WalletID(w)}
		}

		// Bets (which wake their refunds) and resolvers (which lock the wallet, then the row) race on the same wallet.
		var wg sync.WaitGroup
		for i := range n {
			wg.Add(2)
			go func() {
				defer wg.Done()
				e.send(player, w, fmt.Sprintf("b-%d", i), "BET", "10.00", "")
			}()
			go func() {
				defer wg.Done()
				for range 5 {
					if _, err := e.resolve.Resolve(ctx, pendings[i]); err != nil {
						t.Errorf("resolve %d: %v", i, err)
						return
					}
					time.Sleep(10 * time.Millisecond)
				}
			}()
		}
		wg.Wait()
		// A resolver that ran before its bet committed only rescheduled; one more pass settles the stragglers.
		for _, p := range pendings {
			if _, err := e.resolve.Resolve(ctx, p); err != nil {
				t.Fatal(err)
			}
		}

		if got := e.scalar(`SELECT count(*)::text FROM wager_transactions WHERE status = 'PROCESSED' AND kind = 'REFUND'`); got != fmt.Sprint(n) {
			t.Fatalf("processed refunds = %s, want %d", got, n)
		}
		if got := e.scalar(`SELECT balance_minor::text FROM wallets WHERE id = $1`, w); got != "100000" {
			t.Fatalf("balance_minor = %s, every bet was refunded so the wallet must be back to its start", got)
		}
	})
}
