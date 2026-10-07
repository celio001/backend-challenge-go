//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/celio001/backend-challenge-go/internal/infra/outbox"
)

// seedOutbox inserts n unpublished events for one wallet, due immediately, oldest first.
func seedOutbox(t *testing.T, pool *pgxpool.Pool, n int) []string {
	t.Helper()
	walletID := newUUID(t)
	ids := make([]string, n)
	for i := range ids {
		ids[i] = newUUID(t)
		payload, _ := json.Marshal(map[string]any{"eventId": ids[i], "n": i})
		if _, err := pool.Exec(context.Background(),
			`INSERT INTO outbox_events (id, aggregate_type, aggregate_id, partition_key, event_type, event_version, payload, occurred_at)
			 VALUES ($1, 'wallet', $2, $2, 'WalletBalanceChanged', 1, $3, now() + make_interval(secs => $4))`,
			ids[i], walletID, payload, float64(i)/1000); err != nil {
			t.Fatal(err)
		}
	}
	return ids
}

func TestOutboxStore(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	store := NewOutboxStore(pool)
	row := func(id string) (published bool, owner *string, attempts int, lastErr *string) {
		t.Helper()
		var at *time.Time
		if err := pool.QueryRow(ctx, `SELECT published_at, locked_by, attempts, last_error FROM outbox_events WHERE id = $1`, id).Scan(&at, &owner, &attempts, &lastErr); err != nil {
			t.Fatal(err)
		}
		return at != nil, owner, attempts, lastErr
	}

	t.Run("claim leases the oldest events first and hides them from other replicas", func(t *testing.T) {
		ids := seedOutbox(t, pool, 5)

		first, err := store.Claim(ctx, "r1", 3, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if len(first) != 3 || first[0].ID != ids[0] || first[1].ID != ids[1] || first[2].ID != ids[2] {
			t.Fatalf("first claim = %v, want the 3 oldest in order", idsOf(first))
		}
		if first[0].PartitionKey == "" || first[0].Type != "WalletBalanceChanged" || len(first[0].Payload) == 0 {
			t.Fatalf("message not fully loaded: %+v", first[0])
		}
		second, err := store.Claim(ctx, "r2", 10, time.Minute)
		if err != nil || len(second) != 2 || second[0].ID != ids[3] {
			t.Fatalf("second claim = %v, %v; want only the 2 unleased events", idsOf(second), err)
		}
		if third, _ := store.Claim(ctx, "r3", 10, time.Minute); len(third) != 0 {
			t.Fatalf("a third replica got %v although everything is leased", idsOf(third))
		}
	})

	t.Run("mark published needs the lease and is final", func(t *testing.T) {
		ids := seedOutbox(t, pool, 1)
		if _, err := store.Claim(ctx, "r1", 10, time.Minute); err != nil {
			t.Fatal(err)
		}

		if ok, err := store.MarkPublished(ctx, ids[0], "someone-else"); err != nil || ok {
			t.Fatalf("a replica without the lease marked the event: %v, %v", ok, err)
		}
		if ok, err := store.MarkPublished(ctx, ids[0], "r1"); err != nil || !ok {
			t.Fatalf("holder could not mark: %v, %v", ok, err)
		}
		if published, owner, _, _ := row(ids[0]); !published || owner != nil {
			t.Fatalf("published=%v owner=%v", published, owner)
		}
		if got, _ := store.Claim(ctx, "r2", 10, time.Minute); len(got) != 0 {
			t.Fatalf("a published event was claimed again: %v", idsOf(got))
		}
		if ok, _ := store.MarkPublished(ctx, ids[0], "r1"); ok {
			t.Fatal("an already published event was marked twice")
		}
	})

	t.Run("an expired lease is taken over, and the old holder can no longer mark", func(t *testing.T) {
		ids := seedOutbox(t, pool, 1)
		if got, _ := store.Claim(ctx, "crashed", 10, time.Minute); len(got) != 1 {
			t.Fatalf("claim = %v", idsOf(got))
		}
		if got, _ := store.Claim(ctx, "r2", 10, time.Minute); len(got) != 0 {
			t.Fatal("a live lease was stolen")
		}
		if _, err := pool.Exec(ctx, `UPDATE outbox_events SET locked_until = now() - interval '1 second' WHERE id = $1`, ids[0]); err != nil {
			t.Fatal(err)
		}

		got, err := store.Claim(ctx, "r2", 10, time.Minute)
		if err != nil || len(got) != 1 || got[0].ID != ids[0] {
			t.Fatalf("takeover = %v, %v", idsOf(got), err)
		}
		if ok, _ := store.MarkPublished(ctx, ids[0], "crashed"); ok {
			t.Fatal("the crashed replica marked an event it no longer owns")
		}
		if ok, _ := store.MarkPublished(ctx, ids[0], "r2"); !ok {
			t.Fatal("the new owner could not mark")
		}
	})

	t.Run("a failure counts the attempt, hides the event until its retry time, then returns it", func(t *testing.T) {
		ids := seedOutbox(t, pool, 1)
		if _, err := store.Claim(ctx, "r1", 10, time.Minute); err != nil {
			t.Fatal(err)
		}

		if ok, err := store.MarkFailed(ctx, ids[0], "r1", time.Hour, "broker down"); err != nil || !ok {
			t.Fatalf("MarkFailed = %v, %v", ok, err)
		}
		published, owner, attempts, lastErr := row(ids[0])
		if published || owner != nil || attempts != 1 || lastErr == nil || *lastErr != "broker down" {
			t.Fatalf("after failure: published=%v owner=%v attempts=%d err=%v", published, owner, attempts, lastErr)
		}
		if got, _ := store.Claim(ctx, "r2", 10, time.Minute); len(got) != 0 {
			t.Fatal("an event in backoff was claimed early")
		}
		if _, err := pool.Exec(ctx, `UPDATE outbox_events SET next_attempt_at = now() - interval '1 second' WHERE id = $1`, ids[0]); err != nil {
			t.Fatal(err)
		}
		got, _ := store.Claim(ctx, "r2", 10, time.Minute)
		if len(got) != 1 || got[0].Attempts != 1 {
			t.Fatalf("retry claim = %+v", got)
		}
		if ok, _ := store.MarkFailed(ctx, ids[0], "intruder", time.Second, "x"); ok {
			t.Fatal("a replica without the lease recorded a failure")
		}
		if _, err := store.MarkPublished(ctx, ids[0], "r2"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("release returns unused leases immediately", func(t *testing.T) {
		ids := seedOutbox(t, pool, 3)
		got, _ := store.Claim(ctx, "r1", 10, time.Minute)
		if len(got) != 3 {
			t.Fatalf("claim = %v", idsOf(got))
		}

		if err := store.Release(ctx, "r1", ids[1:]); err != nil {
			t.Fatal(err)
		}
		if err := store.Release(ctx, "r1", nil); err != nil {
			t.Fatal(err)
		}
		if err := store.Release(ctx, "not-the-owner", ids[:1]); err != nil {
			t.Fatal(err)
		}

		again, _ := store.Claim(ctx, "r2", 10, time.Minute)
		if len(again) != 2 || again[0].ID != ids[1] || again[1].ID != ids[2] {
			t.Fatalf("after release r2 got %v, want the two released events and not the one still held", idsOf(again))
		}
	})

	t.Run("a long error message is cut to fit", func(t *testing.T) {
		ids := seedOutbox(t, pool, 1)
		if _, err := store.Claim(ctx, "r1", 10, time.Minute); err != nil {
			t.Fatal(err)
		}
		long := make([]byte, 5000)
		for i := range long {
			long[i] = 'x'
		}
		if ok, err := store.MarkFailed(ctx, ids[0], "r1", time.Second, string(long)); err != nil || !ok {
			t.Fatalf("MarkFailed = %v, %v", ok, err)
		}
		if _, _, _, lastErr := row(ids[0]); lastErr == nil || len(*lastErr) != 1000 {
			t.Fatalf("stored error length = %v", lastErr)
		}
	})
}

func TestOutboxStoreSplitsWorkBetweenConcurrentClaimers(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	store := NewOutboxStore(pool)
	ids := seedOutbox(t, pool, 200)

	var mu sync.Mutex
	var all []string
	var wg sync.WaitGroup
	gate := make(chan struct{})
	for r := range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			for {
				got, err := store.Claim(ctx, fmt.Sprintf("r%d", r), 7, time.Minute)
				if err != nil {
					t.Error(err)
					return
				}
				if len(got) == 0 {
					return
				}
				mu.Lock()
				all = append(all, idsOf(got)...)
				mu.Unlock()
			}
		}()
	}
	close(gate)
	wg.Wait()

	sort.Strings(all)
	want := append([]string(nil), ids...)
	sort.Strings(want)
	if len(all) != len(want) {
		t.Fatalf("claimed %d events in total, want %d each exactly once", len(all), len(want))
	}
	for i := range all {
		if all[i] != want[i] {
			t.Fatalf("event %s was claimed twice or never", want[i])
		}
	}
}

func idsOf(msgs []outbox.Message) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = m.ID
	}
	return out
}
