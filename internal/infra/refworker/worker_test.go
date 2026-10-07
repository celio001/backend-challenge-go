package refworker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"

	"github.com/celio001/backend-challenge-go/internal/domain/wager"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/internal/testutil/spantest"
	"github.com/celio001/backend-challenge-go/internal/usecase/resolvereference"
)

type fakeStore struct {
	mu       sync.Mutex
	queue    []resolvereference.Pending
	claimErr error
	released []string
}

func (s *fakeStore) Claim(_ context.Context, limit int, _ time.Duration) ([]resolvereference.Pending, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claimErr != nil {
		return nil, s.claimErr
	}
	n := min(limit, len(s.queue))
	got := s.queue[:n]
	s.queue = s.queue[n:]
	return got, nil
}

func (s *fakeStore) Release(_ context.Context, ids []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.released = append(s.released, ids...)
	return nil
}

type fakeResolver struct {
	mu       sync.Mutex
	resolved []string
	errFor   map[string]error
	onCall   func(id string)
}

func (r *fakeResolver) Resolve(_ context.Context, p resolvereference.Pending) (wager.Status, error) {
	if r.onCall != nil {
		r.onCall(string(p.ID))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resolved = append(r.resolved, string(p.ID))
	if err := r.errFor[string(p.ID)]; err != nil {
		return "", err
	}
	return wager.StatusProcessed, nil
}

func pendings(ids ...string) []resolvereference.Pending {
	out := make([]resolvereference.Pending, len(ids))
	for i, id := range ids {
		out[i] = resolvereference.Pending{ID: wallet.TxID("tx-" + id), WalletID: "w"}
	}
	return out
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestRunOnce(t *testing.T) {
	tests := []struct {
		name         string
		queue        []string
		errFor       map[string]error
		claimErr     error
		batch        int
		wantClaimed  int
		wantResolved []string
		wantErr      bool
	}{
		{name: "every claimed transaction is resolved in order", queue: []string{"a", "b", "c"}, batch: 10, wantClaimed: 3, wantResolved: []string{"tx-a", "tx-b", "tx-c"}},
		{name: "a failing one does not stop the others", queue: []string{"a", "b", "c"}, errFor: map[string]error{"tx-b": errors.New("db down")}, batch: 10, wantClaimed: 3, wantResolved: []string{"tx-a", "tx-b", "tx-c"}},
		{name: "the batch size bounds a round", queue: []string{"a", "b", "c"}, batch: 2, wantClaimed: 2, wantResolved: []string{"tx-a", "tx-b"}},
		{name: "nothing due", batch: 10},
		{name: "a claim failure is reported", claimErr: errors.New("db down"), batch: 10, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeStore{queue: pendings(tt.queue...), claimErr: tt.claimErr}
			res := &fakeResolver{errFor: tt.errFor}
			w := New(store, res, Config{BatchSize: tt.batch}, quiet())

			claimed, err := w.RunOnce(context.Background())
			if (err != nil) != tt.wantErr || claimed != tt.wantClaimed {
				t.Fatalf("claimed = %d, err = %v", claimed, err)
			}
			if len(res.resolved) != len(tt.wantResolved) {
				t.Fatalf("resolved = %v, want %v", res.resolved, tt.wantResolved)
			}
			for i := range tt.wantResolved {
				if res.resolved[i] != tt.wantResolved[i] {
					t.Fatalf("resolved = %v, want %v", res.resolved, tt.wantResolved)
				}
			}
			if len(store.released) != 0 {
				t.Fatalf("released = %v", store.released)
			}
		})
	}
}

func TestShutdownFinishesTheCurrentOneAndReleasesTheRest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &fakeStore{queue: pendings("a", "b", "c")}
	res := &fakeResolver{onCall: func(id string) {
		if id == "tx-a" {
			cancel() // shutdown arrives while a is being resolved
		}
	}}
	w := New(store, res, Config{}, quiet())

	claimed, err := w.RunOnce(ctx)
	if err != nil || claimed != 3 {
		t.Fatalf("claimed = %d, err = %v", claimed, err)
	}
	if len(res.resolved) != 1 || res.resolved[0] != "tx-a" {
		t.Fatalf("resolved = %v, want only the one in flight", res.resolved)
	}
	if len(store.released) != 2 || store.released[0] != "tx-b" || store.released[1] != "tx-c" {
		t.Fatalf("released = %v, want the unused leases handed back", store.released)
	}
}

func TestRunKeepsPollingAndStopsWithTheContext(t *testing.T) {
	store := &fakeStore{queue: pendings("a")}
	res := &fakeResolver{}
	w := New(store, res, Config{PollInterval: 5 * time.Millisecond}, quiet())

	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() { w.Run(ctx); close(finished) }()

	deadline := time.Now().Add(2 * time.Second)
	for {
		res.mu.Lock()
		n := len(res.resolved)
		res.mu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the pending transaction was never resolved")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Work that shows up later is picked up by a later round.
	store.mu.Lock()
	store.queue = append(store.queue, pendings("b")...)
	store.mu.Unlock()
	deadline = time.Now().Add(2 * time.Second)
	for {
		res.mu.Lock()
		n := len(res.resolved)
		res.mu.Unlock()
		if n == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the later transaction was never resolved")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestEachResolutionAttemptHasASpan(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantError bool
		wantState string
	}{
		{name: "an attempt that settles the transaction", wantState: "PROCESSED"},
		{name: "an attempt that fails is a span error", err: errors.New("db down"), wantError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := spantest.Install(t)
			store := &fakeStore{queue: pendings("a")}
			res := &fakeResolver{errFor: map[string]error{"tx-a": tt.err}}
			if _, err := New(store, res, Config{}, quiet()).RunOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			spans := rec.Named("pending_reference.resolve")
			if len(spans) != 1 {
				t.Fatalf("spans = %d", len(spans))
			}
			attrs := spantest.Attrs(spans[0])
			if attrs["transaction.id"] != "tx-a" || attrs["wallet.id"] != "w" || attrs["wager.status"] != tt.wantState {
				t.Fatalf("attributes = %v", attrs)
			}
			if (spans[0].Status().Code == codes.Error) != tt.wantError {
				t.Fatalf("status = %v", spans[0].Status())
			}
		})
	}
}
