//go:build integration

package postgres

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/celio001/backend-challenge-go/internal/domain/wager"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/internal/testutil/pgtest"
	"github.com/celio001/backend-challenge-go/internal/usecase"
	"github.com/celio001/backend-challenge-go/internal/usecase/openwallet"
	"github.com/celio001/backend-challenge-go/internal/usecase/processwager"
)

// cluster stands for several replicas: each has its own connection pool and use case, and shares nothing but the database.
type cluster struct {
	t         *testing.T
	admin     *pgxpool.Pool
	pools     []*pgxpool.Pool
	instances []*processwager.UseCase
	open      *openwallet.UseCase
}

func newCluster(t *testing.T, replicas int) *cluster {
	t.Helper()
	admin, url := pgtest.New(t)
	c := &cluster{t: t, admin: admin}
	for range replicas {
		pool, err := pgxpool.New(context.Background(), url)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		c.pools = append(c.pools, pool)
		c.instances = append(c.instances, processwager.New(NewUnitOfWork(pool), realClock{}, uuidIDs{t}, processwager.Options{}))
	}
	c.open = openwallet.New(NewUnitOfWork(c.pools[0]), realClock{}, uuidIDs{t})
	return c
}

func (c *cluster) newWallet(minor int64) *wallet.Wallet {
	c.t.Helper()
	w, err := c.open.Execute(context.Background(), openwallet.Input{PlayerID: newUUID(c.t), InitialBalance: brl(c.t, minor)})
	if err != nil {
		c.t.Fatal(err)
	}
	return w
}

type op struct {
	ext, kind, ref string
	minor          int64
}

// input scopes the external ids to the wallet: they are unique per provider, so tests that reuse names across wallets would collide.
func (c *cluster) input(w *wallet.Wallet, o op) processwager.Input {
	c.t.Helper()
	scope := string(w.ID())[:8] + "-"
	ref := o.ref
	if ref != "" {
		ref = scope + ref
	}
	return processwager.Input{
		ProviderID: "provider-a", ExternalTransactionID: scope + o.ext, IdempotencyKey: "provider-a:" + scope + o.ext, PlayerID: string(w.PlayerID()),
		WalletID: string(w.ID()), RoundID: "round-1", GameID: "game-1", Kind: o.kind, Money: brl(c.t, o.minor),
		ReferenceExternalTransactionID: ref,
	}
}

type outcome struct {
	out processwager.Output
	err error
}

// race starts every function at the same moment, spread over the replicas, and returns their outcomes in order.
func (c *cluster) race(w *wallet.Wallet, ops []op) []outcome {
	c.t.Helper()
	gate := make(chan struct{})
	res := make([]outcome, len(ops))
	var wg sync.WaitGroup
	for i, o := range ops {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			out, err := c.instances[i%len(c.instances)].Execute(context.Background(), c.input(w, o))
			res[i] = outcome{out, err}
		}()
	}
	close(gate)
	wg.Wait()
	return res
}

func (c *cluster) scalar(q string, args ...any) (n int64) {
	c.t.Helper()
	if err := c.admin.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		c.t.Fatal(err)
	}
	return n
}

// assertConsistent checks the invariants the whole design exists to protect, for one wallet.
func (c *cluster) assertConsistent(w *wallet.Wallet) {
	c.t.Helper()
	id := string(w.ID())
	balance := c.scalar(`SELECT balance_minor FROM wallets WHERE id = $1`, id)
	version := c.scalar(`SELECT version FROM wallets WHERE id = $1`, id)
	net := c.scalar(`SELECT COALESCE(SUM(CASE direction WHEN 'CREDIT' THEN amount_minor ELSE -amount_minor END), 0) FROM wallet_ledger_entries WHERE wallet_id = $1`, id)
	entries := c.scalar(`SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, id)
	lowest := c.scalar(`SELECT COALESCE(MIN(balance_after_minor), 0) FROM wallet_ledger_entries WHERE wallet_id = $1`, id)
	gaps := c.scalar(`SELECT count(*) FROM wallet_ledger_entries e WHERE wallet_id = $1 AND balance_before_minor <>
		COALESCE((SELECT p.balance_after_minor FROM wallet_ledger_entries p WHERE p.wallet_id = e.wallet_id AND p.wallet_version = e.wallet_version - 1), 0)`, id)
	// A wallet opened with zero balance starts at version 1 without a ledger entry, so its first movement is version 2.
	expectedVersion := entries
	if c.scalar(`SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND wallet_version = 1`, id) == 0 {
		expectedVersion = entries + 1
	}
	if balance != net || balance < 0 || lowest < 0 || version != expectedVersion || gaps != 0 {
		c.t.Fatalf("wallet %s: balance %d, ledger net %d, version %d (want %d), entries %d, lowest balance %d, chain gaps %d", id, balance, net, version, expectedVersion, entries, lowest, gaps)
	}
	if pending := c.scalar(`SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND status = 'PENDING'`, id); pending != 0 {
		c.t.Fatalf("wallet %s has %d transactions stuck in PENDING", id, pending)
	}
}

func TestFiftyIdenticalBetsDebitOnce(t *testing.T) {
	c := newCluster(t, 3)
	w := c.newWallet(100000)
	ops := make([]op, 50)
	for i := range ops {
		ops[i] = op{ext: "transaction-123", kind: "BET", minor: 2500}
	}

	results := c.race(w, ops)

	fresh := 0
	for i, r := range results {
		if r.err != nil {
			t.Fatalf("request %d failed: %v", i, r.err)
		}
		if r.out.Status != wager.StatusProcessed || r.out.Balance.Minor() != 97500 || r.out.TransactionID != results[0].out.TransactionID {
			t.Fatalf("request %d = %+v, want the one original result", i, r.out)
		}
		if !r.out.Replay {
			fresh++
		}
	}
	if fresh != 1 {
		t.Fatalf("%d requests were applied, want exactly 1 (the other 49 must be replays)", fresh)
	}
	id := string(w.ID())
	if n := c.scalar(`SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, id); n != 1 {
		t.Fatalf("debits = %d, want 1", n)
	}
	if n := c.scalar(`SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND kind = 'BET'`, id); n != 1 {
		t.Fatalf("stored bets = %d, want 1", n)
	}
	for typ, want := range map[string]int64{"WagerTransactionProcessed": 2, "WalletBalanceChanged": 2} { // the opening plus the bet
		if n := c.scalar(`SELECT count(*) FROM outbox_events WHERE partition_key = $1 AND event_type = $2`, id, typ); n != want {
			t.Fatalf("%s events = %d, want %d", typ, n, want)
		}
	}
	c.assertConsistent(w)
}

func TestTwoBetsOfEightyOnAHundred(t *testing.T) {
	c := newCluster(t, 3)

	for round := range 30 {
		w := c.newWallet(10000)
		ops := []op{{ext: "bet-a", kind: "BET", minor: 8000}, {ext: "bet-b", kind: "BET", minor: 8000}}

		results := c.race(w, ops)

		var processed, rejected int
		for _, r := range results {
			if r.err != nil {
				t.Fatalf("round %d: %v", round, r.err)
			}
			switch {
			case r.out.Status == wager.StatusProcessed && r.out.Balance.Minor() == 2000:
				processed++
			case r.out.Status == wager.StatusRejected && r.out.FailureCode == wager.CodeInsufficientFunds:
				rejected++
			default:
				t.Fatalf("round %d: unexpected outcome %+v", round, r.out)
			}
		}
		if processed != 1 || rejected != 1 {
			t.Fatalf("round %d: processed %d, rejected %d, want 1 and 1", round, processed, rejected)
		}
		id := string(w.ID())
		if bal := c.scalar(`SELECT balance_minor FROM wallets WHERE id = $1`, id); bal != 2000 {
			t.Fatalf("round %d: final balance %d, want 2000", round, bal)
		}
		if n := c.scalar(`SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, id); n != 1 {
			t.Fatalf("round %d: debits = %d, want 1", round, n)
		}

		// Resending both, on other replicas, must not change anything.
		for i, o := range ops {
			again, err := c.instances[(i+1)%3].Execute(context.Background(), c.input(w, o))
			if err != nil || !again.Replay || again.Status != results[i].out.Status || again.FailureCode != results[i].out.FailureCode {
				t.Fatalf("round %d: resend %d = %+v, %v; first answer was %+v", round, i, again, err, results[i].out)
			}
		}
		if bal := c.scalar(`SELECT balance_minor FROM wallets WHERE id = $1`, id); bal != 2000 {
			t.Fatalf("round %d: a resend changed the balance to %d", round, bal)
		}
		c.assertConsistent(w)
	}
}

func TestRefundAndRollbackRaceReturnTheBetOnce(t *testing.T) {
	c := newCluster(t, 3)

	for round := range 20 {
		w := c.newWallet(100000)
		if res := c.race(w, []op{{ext: "bet", kind: "BET", minor: 5000}}); res[0].err != nil || res[0].out.Status != wager.StatusProcessed {
			t.Fatalf("round %d: bet = %+v", round, res[0])
		}

		results := c.race(w, []op{
			{ext: "refund", kind: "REFUND", ref: "bet", minor: 5000},
			{ext: "rollback", kind: "ROLLBACK", ref: "bet", minor: 5000},
			{ext: "rollback-2", kind: "ROLLBACK", ref: "bet", minor: 5000},
		})

		var processed, alreadyReversed int
		for _, r := range results {
			if r.err != nil {
				t.Fatalf("round %d: %v", round, r.err)
			}
			switch {
			case r.out.Status == wager.StatusProcessed:
				processed++
			case r.out.Status == wager.StatusRejected && r.out.FailureCode == wager.CodeReferenceAlreadyReversed:
				alreadyReversed++
			default:
				t.Fatalf("round %d: unexpected outcome %+v", round, r.out)
			}
		}
		if processed != 1 || alreadyReversed != 2 {
			t.Fatalf("round %d: processed %d, already reversed %d, want 1 and 2", round, processed, alreadyReversed)
		}
		if bal := c.scalar(`SELECT balance_minor FROM wallets WHERE id = $1`, string(w.ID())); bal != 100000 {
			t.Fatalf("round %d: balance %d, the bet must have been returned exactly once", round, bal)
		}
		c.assertConsistent(w)
	}
}

func TestIndependentWalletsDoNotWaitForEachOther(t *testing.T) {
	c := newCluster(t, 3)
	busy, free := c.newWallet(100000), c.newWallet(100000)

	locked, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- NewUnitOfWork(c.pools[0]).Do(context.Background(), func(ctx context.Context, tx usecase.Repos) error {
			if _, err := tx.Wallets().Lock(ctx, busy.ID()); err != nil {
				return err
			}
			close(locked)
			<-release
			return nil
		})
	}()
	<-locked

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	out, err := c.instances[1].Execute(ctx, c.input(free, op{ext: "free-bet", kind: "BET", minor: 1000}))
	elapsed := time.Since(start)
	if err != nil || out.Status != wager.StatusProcessed {
		t.Fatalf("a bet on another wallet was blocked by the lock on %s: %+v, %v after %v", busy.ID(), out, err, elapsed)
	}

	blocked := make(chan outcome, 1)
	go func() {
		o, err := c.instances[2].Execute(context.Background(), c.input(busy, op{ext: "busy-bet", kind: "BET", minor: 1000}))
		blocked <- outcome{o, err}
	}()
	select {
	case r := <-blocked:
		t.Fatalf("a bet on the locked wallet finished while the lock was held: %+v", r)
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if r := <-blocked; r.err != nil || r.out.Status != wager.StatusProcessed {
		t.Fatalf("the waiting bet = %+v", r)
	}
	c.assertConsistent(busy)
	c.assertConsistent(free)
}

func TestManyWalletsInParallel(t *testing.T) {
	c := newCluster(t, 3)
	const wallets, betsEach = 24, 5
	ws := make([]*wallet.Wallet, wallets)
	for i := range ws {
		ws[i] = c.newWallet(10000)
	}

	var wg sync.WaitGroup
	errs := make(chan error, wallets*betsEach)
	gate := make(chan struct{})
	for i, w := range ws {
		for j := range betsEach {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-gate
				out, err := c.instances[(i+j)%3].Execute(context.Background(), c.input(w, op{ext: fmt.Sprintf("bet-%d", j), kind: "BET", minor: 1000}))
				if err != nil || out.Status != wager.StatusProcessed {
					errs <- fmt.Errorf("wallet %d bet %d: %+v, %v", i, j, out, err)
				}
			}()
		}
	}
	close(gate)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	for _, w := range ws {
		if bal := c.scalar(`SELECT balance_minor FROM wallets WHERE id = $1`, string(w.ID())); bal != 5000 {
			t.Fatalf("wallet %s balance %d, want 5000", w.ID(), bal)
		}
		c.assertConsistent(w)
	}
}

// A random storm of every operation kind, including references that arrive late, must leave every ledger consistent.
func TestRandomOperationsKeepEveryInvariant(t *testing.T) {
	c := newCluster(t, 3)
	ws := []*wallet.Wallet{c.newWallet(50000), c.newWallet(20000), c.newWallet(0), c.newWallet(100000)}

	rng := rand.New(rand.NewPCG(7, 11))
	type plan struct {
		w *wallet.Wallet
		o op
	}
	var plans []plan
	for wi, w := range ws {
		var bets, wins []string
		for i := range 40 {
			ext := fmt.Sprintf("w%d-op%d", wi, i)
			switch k := rng.IntN(10); {
			case k < 4:
				plans = append(plans, plan{w, op{ext: ext, kind: "BET", minor: int64(500 + rng.IntN(5000))}})
				bets = append(bets, ext)
			case k < 5:
				plans = append(plans, plan{w, op{ext: ext, kind: "WIN", minor: int64(500 + rng.IntN(5000))}})
				wins = append(wins, ext)
			case k < 6:
				plans = append(plans, plan{w, op{ext: ext, kind: "LOSS"}})
			case k < 8 && len(bets) > 0:
				ref := bets[rng.IntN(len(bets))]
				plans = append(plans, plan{w, op{ext: ext, kind: []string{"REFUND", "ROLLBACK"}[rng.IntN(2)], ref: ref, minor: 0}})
			case len(wins) > 0:
				plans = append(plans, plan{w, op{ext: ext, kind: "ROLLBACK", ref: wins[rng.IntN(len(wins))], minor: 0}})
			}
		}
		_ = wi
	}
	// Reversals must repeat the amount of what they revert, so fill it in from the plan.
	amount := map[string]int64{}
	for _, p := range plans {
		if p.o.ref == "" {
			amount[p.o.ext] = p.o.minor
		}
	}
	for i := range plans {
		if plans[i].o.ref != "" {
			plans[i].o.minor = amount[plans[i].o.ref]
		}
	}
	// Some operations are sent twice to exercise idempotency under the same storm.
	for i := range 30 {
		plans = append(plans, plans[rng.IntN(len(plans))])
		_ = i
	}
	rng.Shuffle(len(plans), func(i, j int) { plans[i], plans[j] = plans[j], plans[i] })

	var wg sync.WaitGroup
	gate := make(chan struct{})
	errs := make(chan error, len(plans))
	for i, p := range plans {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			if _, err := c.instances[i%3].Execute(context.Background(), c.input(p.w, p.o)); err != nil {
				errs <- fmt.Errorf("%s %s: %w", p.o.kind, p.o.ext, err)
			}
		}()
	}
	close(gate)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	for _, w := range ws {
		c.assertConsistent(w)
	}
	if t.Failed() {
		return
	}
	// Whatever finished must be in a state the domain allows: nothing PENDING, every rejection has its code.
	if n := c.scalar(`SELECT count(*) FROM wager_transactions WHERE status = 'REJECTED' AND failure_code IS NULL`); n != 0 {
		t.Fatalf("%d rejections without a failure code", n)
	}
	if n := c.scalar(`SELECT count(*) FROM (SELECT reference_transaction_id FROM wager_transactions WHERE status = 'PROCESSED' AND kind IN ('REFUND','ROLLBACK') GROUP BY 1 HAVING count(*) > 1) d`); n != 0 {
		t.Fatalf("%d transactions were reversed more than once", n)
	}
}
