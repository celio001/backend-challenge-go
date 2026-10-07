//go:build integration

package postgres

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/celio001/backend-challenge-go/internal/usecase"
)

func TestInboxRepository(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	uow := NewUnitOfWork(pool)
	msg := func(id, hash string) usecase.InboxMessage {
		return usecase.InboxMessage{Consumer: "wager-transactions", MessageID: id, Hash: []byte(hash), ReceivedAt: time.Now().UTC()}
	}
	register := func(m usecase.InboxMessage) (bool, error) {
		var dup bool
		err := uow.Do(ctx, func(ctx context.Context, tx usecase.Repos) error {
			var err error
			dup, err = tx.Inbox().Register(ctx, m)
			return err
		})
		return dup, err
	}
	count := func(id string) (n int) {
		t.Helper()
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM inbox_messages WHERE message_id = $1`, id).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	tests := []struct {
		name    string
		first   usecase.InboxMessage
		second  usecase.InboxMessage
		wantDup bool
		wantErr error
	}{
		{name: "same id and hash is a duplicate", first: msg("dup", "hash-1"), second: msg("dup", "hash-1"), wantDup: true},
		{name: "same id with another hash is a mismatch", first: msg("mismatch", "hash-1"), second: msg("mismatch", "hash-2"), wantErr: usecase.ErrInboxHashMismatch},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if dup, err := register(tt.first); dup || err != nil {
				t.Fatalf("first = %v, %v", dup, err)
			}
			dup, err := register(tt.second)
			if dup != tt.wantDup || !errors.Is(err, tt.wantErr) {
				t.Fatalf("second = %v, %v", dup, err)
			}
			if n := count(tt.first.MessageID); n != 1 {
				t.Fatalf("rows = %d", n)
			}
		})
	}

	t.Run("the row is completed on insert", func(t *testing.T) {
		if _, err := register(msg("completed", "h")); err != nil {
			t.Fatal(err)
		}
		var done bool
		if err := pool.QueryRow(ctx, `SELECT processed_at IS NOT NULL FROM inbox_messages WHERE message_id = 'completed'`).Scan(&done); err != nil || !done {
			t.Fatalf("processed_at set = %v, %v", done, err)
		}
	})

	t.Run("a rolled back unit of work leaves no row", func(t *testing.T) {
		boom := errors.New("boom")
		err := uow.Do(ctx, func(ctx context.Context, tx usecase.Repos) error {
			if _, err := tx.Inbox().Register(ctx, msg("rolled-back", "h")); err != nil {
				return err
			}
			return boom
		})
		if !errors.Is(err, boom) || count("rolled-back") != 0 {
			t.Fatalf("err = %v, rows = %d", err, count("rolled-back"))
		}
	})

	t.Run("concurrent deliveries of one message register it once", func(t *testing.T) {
		const n = 20
		var wg sync.WaitGroup
		var mu sync.Mutex
		fresh, dups := 0, 0
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				dup, err := register(msg("race", "h"))
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				defer mu.Unlock()
				if dup {
					dups++
				} else {
					fresh++
				}
			}()
		}
		wg.Wait()
		if fresh != 1 || dups != n-1 || count("race") != 1 {
			t.Fatalf("fresh = %d, duplicates = %d, rows = %d", fresh, dups, count("race"))
		}
	})
}
