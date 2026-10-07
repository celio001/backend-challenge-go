//go:build integration

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wager"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/internal/usecase"
	"github.com/celio001/backend-challenge-go/internal/usecase/openwallet"
	"github.com/celio001/backend-challenge-go/internal/usecase/queries"
)

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

type uuidIDs struct{ t *testing.T }

func (g uuidIDs) NewID() string { return newUUID(g.t) }

func brl(t *testing.T, minor int64) money.Money {
	t.Helper()
	m, err := money.FromMinor(minor, money.BRL)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestWalletRepositoryCreate(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	repo := NewWalletRepository(pool)

	player := newUUID(t)
	open := func(id, player string, c money.Currency) *wallet.Wallet {
		w, err := wallet.Open(wallet.WalletID(id), wallet.PlayerID(player), c, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		return w
	}
	first := open(newUUID(t), player, money.BRL)

	tests := []struct {
		name    string
		w       *wallet.Wallet
		wantErr error
	}{
		{name: "creates", w: first},
		{name: "same player and currency", w: open(newUUID(t), player, money.BRL), wantErr: wallet.ErrAlreadyExists},
		{name: "same player other currency", w: open(newUUID(t), player, money.USD)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := repo.Create(ctx, tt.w); !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}

	t.Run("duplicate id is an infra error", func(t *testing.T) {
		err := repo.Create(ctx, open(string(first.ID()), newUUID(t), money.BRL))
		if err == nil || errors.Is(err, wallet.ErrAlreadyExists) {
			t.Fatalf("err = %v, want generic infra error", err)
		}
	})
}

func TestOpenWalletAgainstPostgres(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	uc := openwallet.New(NewUnitOfWork(pool), realClock{}, uuidIDs{t})

	count := func(q string, args ...any) (n int) {
		t.Helper()
		if err := pool.QueryRow(ctx, q, args...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	t.Run("positive balance writes wallet, opening, ledger and two events atomically", func(t *testing.T) {
		player := newUUID(t)
		w, err := uc.Execute(ctx, openwallet.Input{PlayerID: player, InitialBalance: brl(t, 100000), CorrelationID: "corr"})
		if err != nil {
			t.Fatal(err)
		}
		id := string(w.ID())

		var balance, version int64
		if err := pool.QueryRow(ctx, `SELECT balance_minor, version FROM wallets WHERE id = $1`, id).Scan(&balance, &version); err != nil {
			t.Fatal(err)
		}
		if balance != 100000 || version != 1 {
			t.Fatalf("balance/version = %d/%d", balance, version)
		}
		if n := count(`SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND kind = 'OPENING' AND status = 'PROCESSED' AND origin = 'INTERNAL' AND result_balance_minor = 100000`, id); n != 1 {
			t.Fatalf("opening transactions = %d", n)
		}
		if n := count(`SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'CREDIT' AND wallet_version = 1 AND balance_before_minor = 0 AND balance_after_minor = 100000`, id); n != 1 {
			t.Fatalf("ledger entries = %d", n)
		}
		for _, typ := range []string{"WagerTransactionProcessed", "WalletBalanceChanged"} {
			if n := count(`SELECT count(*) FROM outbox_events WHERE partition_key = $1 AND event_type = $2 AND payload->>'correlationId' = 'corr'`, id, typ); n != 1 {
				t.Fatalf("%s events = %d", typ, n)
			}
		}
	})

	t.Run("zero balance writes only the wallet", func(t *testing.T) {
		w, err := uc.Execute(ctx, openwallet.Input{PlayerID: newUUID(t), InitialBalance: brl(t, 0)})
		if err != nil {
			t.Fatal(err)
		}
		id := string(w.ID())
		if n := count(`SELECT count(*) FROM wallets WHERE id = $1`, id); n != 1 {
			t.Fatalf("wallets = %d", n)
		}
		if n := count(`SELECT (SELECT count(*) FROM wager_transactions WHERE wallet_id = $1) + (SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1) + (SELECT count(*) FROM outbox_events WHERE partition_key = $1)`, id); n != 0 {
			t.Fatalf("unexpected rows for zero balance wallet: %d", n)
		}
	})

	t.Run("duplicate player and currency conflicts and leaves nothing behind", func(t *testing.T) {
		player := newUUID(t)
		if _, err := uc.Execute(ctx, openwallet.Input{PlayerID: player, InitialBalance: brl(t, 5000)}); err != nil {
			t.Fatal(err)
		}
		before := count(`SELECT count(*) FROM outbox_events`)

		_, err := uc.Execute(ctx, openwallet.Input{PlayerID: player, InitialBalance: brl(t, 7000)})

		if !errors.Is(err, wallet.ErrAlreadyExists) {
			t.Fatalf("err = %v", err)
		}
		if after := count(`SELECT count(*) FROM outbox_events`); after != before {
			t.Fatalf("outbox grew from %d to %d on a failed open", before, after)
		}
		if n := count(`SELECT count(*) FROM wager_transactions WHERE player_id = $1`, player); n != 1 {
			t.Fatalf("transactions = %d, want only the first opening", n)
		}
	})
}

func TestUnitOfWorkRollbackAndDeferredConsistency(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	uow := NewUnitOfWork(pool)
	errBoom := errors.New("boom")

	newWallet := func(balance int64) (*wallet.Wallet, wallet.LedgerEntry) {
		w, entry, err := wallet.OpenWithBalance(wallet.WalletID(newUUID(t)), wallet.PlayerID(newUUID(t)), brl(t, balance), wallet.TxID(newUUID(t)), time.Now())
		if err != nil {
			t.Fatal(err)
		}
		return w, entry
	}
	walletCount := func(id wallet.WalletID) (n int) {
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM wallets WHERE id = $1`, string(id)).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	t.Run("fn error rolls back", func(t *testing.T) {
		w, _ := newWallet(1000)
		err := uow.Do(ctx, func(ctx context.Context, tx usecase.Repos) error {
			if err := tx.Wallets().Create(ctx, w); err != nil {
				return err
			}
			return errBoom
		})
		if !errors.Is(err, errBoom) || walletCount(w.ID()) != 0 {
			t.Fatalf("err = %v, wallets = %d", err, walletCount(w.ID()))
		}
	})

	t.Run("balance without ledger entry is refused at commit", func(t *testing.T) {
		w, _ := newWallet(1000)
		err := uow.Do(ctx, func(ctx context.Context, tx usecase.Repos) error {
			return tx.Wallets().Create(ctx, w)
		})
		if err == nil || walletCount(w.ID()) != 0 {
			t.Fatalf("err = %v, wallets = %d", err, walletCount(w.ID()))
		}
	})

	t.Run("wallet, opening and ledger entry commit together", func(t *testing.T) {
		w, entry := newWallet(1000)
		opening, err := wager.NewOpening(entry.TxID(), w.ID(), w.PlayerID(), brl(t, 1000), "", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		err = uow.Do(ctx, func(ctx context.Context, tx usecase.Repos) error {
			if err := tx.Wallets().Create(ctx, w); err != nil {
				return err
			}
			if err := tx.Transactions().Insert(ctx, opening); err != nil {
				return err
			}
			return tx.Ledger().Append(ctx, entry)
		})
		if err != nil || walletCount(w.ID()) != 1 {
			t.Fatalf("err = %v, wallets = %d", err, walletCount(w.ID()))
		}
	})
}

// seedDebit applies a 25.00 debit the way ProcessWager will: wallet update, wager transaction and ledger entry in one transaction.
func seedDebit(t *testing.T, pool *pgxpool.Pool, walletID, playerID string, version, balanceBefore int64) {
	t.Helper()
	ctx := context.Background()
	txID := newUUID(t)
	dbtx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer dbtx.Rollback(ctx)
	for _, q := range []struct {
		sql  string
		args []any
	}{
		{`UPDATE wallets SET balance_minor = $2, version = $3, updated_at = now() WHERE id = $1`, []any{walletID, balanceBefore - 2500, version}},
		{`INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount_minor, currency, provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id, result_balance_minor, created_at, updated_at)
		  VALUES ($1::uuid, 'EXTERNAL', 'BET', 'PROCESSED', $2, $3, 2500, 'BRL', 'provider-a', $1::text, $1::text, decode(repeat('00', 32), 'hex'), 'r', 'g', $4, now(), now())`, []any{txID, walletID, playerID, balanceBefore - 2500}},
		{`INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, wallet_version, direction, amount_minor, currency, balance_before_minor, balance_after_minor)
		  VALUES (gen_random_uuid(), $1, $2, $3, 'DEBIT', 2500, 'BRL', $4, $5)`, []any{walletID, txID, version, balanceBefore, balanceBefore - 2500}},
	} {
		if _, err := dbtx.Exec(ctx, q.sql, q.args...); err != nil {
			t.Fatal(err)
		}
	}
	if err := dbtx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestQueriesAgainstPostgres(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	q := queries.New(NewWalletReader(pool), NewLedgerReader(pool))

	w, err := openwallet.New(NewUnitOfWork(pool), realClock{}, uuidIDs{t}).Execute(ctx, openwallet.Input{PlayerID: newUUID(t), InitialBalance: brl(t, 100000)})
	if err != nil {
		t.Fatal(err)
	}
	id, player := string(w.ID()), string(w.PlayerID())
	seedDebit(t, pool, id, player, 2, 100000)
	seedDebit(t, pool, id, player, 3, 97500)

	t.Run("wallet reflects the latest committed state", func(t *testing.T) {
		got, err := q.Wallet(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if got.Balance().Minor() != 95000 || got.Version() != 3 || got.PlayerID() != w.PlayerID() || got.Currency() != money.BRL {
			t.Fatalf("wallet = %+v", got)
		}
	})

	t.Run("unknown wallet is not found", func(t *testing.T) {
		if _, err := q.Wallet(ctx, newUUID(t)); !errors.Is(err, wallet.ErrNotFound) {
			t.Fatalf("err = %v", err)
		}
		if _, err := q.Ledger(ctx, newUUID(t), "", 10); !errors.Is(err, wallet.ErrNotFound) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("ledger pages by wallet version", func(t *testing.T) {
		first, err := q.Ledger(ctx, id, "", 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(first.Items) != 2 || first.Items[0].WalletVersion != 1 || first.Items[1].WalletVersion != 2 || first.NextCursor == "" {
			t.Fatalf("first page = %+v", first)
		}
		open := first.Items[0]
		if open.Direction != wallet.Credit || open.Money.Minor() != 100000 || open.BalanceBefore.Minor() != 0 || open.BalanceAfter.Minor() != 100000 {
			t.Fatalf("opening entry = %+v", open)
		}
		if d := first.Items[1]; d.Direction != wallet.Debit || d.BalanceBefore.Minor() != 100000 || d.BalanceAfter.Minor() != 97500 {
			t.Fatalf("debit entry = %+v", d)
		}

		second, err := q.Ledger(ctx, id, first.NextCursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(second.Items) != 1 || second.Items[0].WalletVersion != 3 || second.NextCursor != "" {
			t.Fatalf("second page = %+v", second)
		}
	})
}
