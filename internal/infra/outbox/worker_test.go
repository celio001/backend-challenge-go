package outbox

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/celio001/backend-challenge-go/internal/testutil/spantest"
)

type fakeStore struct {
	mu        sync.Mutex
	pending   []Message
	published map[string]bool
	failedFor map[string]time.Duration
	causes    map[string]string
	released  []string
	owners    map[string]string // id -> owner holding the lease

	claimErr       error
	markErr        error // first MarkPublished call only
	markErrUsed    bool
	loseLease      map[string]bool
	claimCalls     int
	onFirstPublish func()
}

func newFakeStore(msgs ...Message) *fakeStore {
	return &fakeStore{pending: msgs, published: map[string]bool{}, failedFor: map[string]time.Duration{}, causes: map[string]string{}, owners: map[string]string{}}
}

func (s *fakeStore) Claim(_ context.Context, owner string, limit int, _ time.Duration) ([]Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claimCalls++
	if s.claimErr != nil {
		return nil, s.claimErr
	}
	var out, rest []Message
	for _, m := range s.pending {
		if len(out) < limit {
			out = append(out, m)
			s.owners[m.ID] = owner
		} else {
			rest = append(rest, m)
		}
	}
	s.pending = rest
	return out, nil
}

func (s *fakeStore) MarkPublished(_ context.Context, id, owner string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.markErr != nil && !s.markErrUsed {
		s.markErrUsed = true
		return false, s.markErr
	}
	if s.loseLease[id] || s.owners[id] != owner {
		return false, nil
	}
	s.published[id] = true
	return true, nil
}

func (s *fakeStore) MarkFailed(_ context.Context, id, owner string, retryIn time.Duration, cause string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failedFor[id], s.causes[id] = retryIn, cause
	return s.owners[id] == owner, nil
}

func (s *fakeStore) Release(_ context.Context, _ string, ids []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.released = append(s.released, ids...)
	return nil
}

type fakePublisher struct {
	mu     sync.Mutex
	sent   []string
	fail   map[string]error
	before func(Message)
}

func (p *fakePublisher) Publish(_ context.Context, m Message) error {
	if p.before != nil {
		p.before(m)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.fail[m.ID]; err != nil {
		return err
	}
	p.sent = append(p.sent, m.ID)
	return nil
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func msg(id string, attempts int) Message {
	return Message{ID: id, PartitionKey: "wallet-1", Type: "WalletBalanceChanged", Payload: []byte(`{}`), Attempts: attempts}
}

func TestRunOnce(t *testing.T) {
	errBroker := errors.New("broker unavailable")
	errDB := errors.New("db down")

	tests := []struct {
		name          string
		msgs          []Message
		fail          map[string]error
		markErr       error
		loseLease     map[string]bool
		wantSent      []string
		wantPublished []string
		wantFailed    []string
	}{
		{name: "publishes in order and marks each one", msgs: []Message{msg("a", 0), msg("b", 0), msg("c", 0)}, wantSent: []string{"a", "b", "c"}, wantPublished: []string{"a", "b", "c"}},
		{name: "nothing due", wantSent: nil},
		{
			name: "a failed publish is rescheduled and does not stop the others",
			msgs: []Message{msg("a", 0), msg("b", 2), msg("c", 0)}, fail: map[string]error{"b": errBroker},
			wantSent: []string{"a", "c"}, wantPublished: []string{"a", "c"}, wantFailed: []string{"b"},
		},
		{
			name: "published but not marked is not reported as published",
			msgs: []Message{msg("a", 0), msg("b", 0)}, markErr: errDB,
			wantSent: []string{"a", "b"}, wantPublished: []string{"b"},
		},
		{
			name: "a lost lease is tolerated",
			msgs: []Message{msg("a", 0), msg("b", 0)}, loseLease: map[string]bool{"a": true},
			wantSent: []string{"a", "b"}, wantPublished: []string{"b"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newFakeStore(tt.msgs...)
			store.markErr, store.loseLease = tt.markErr, tt.loseLease
			pub := &fakePublisher{fail: tt.fail}
			w := New(store, pub, Config{Owner: "r1", BackoffBase: time.Second, BackoffMax: time.Minute}, quiet())

			n, err := w.RunOnce(context.Background())

			if err != nil || n != len(tt.msgs) {
				t.Fatalf("claimed %d, err %v", n, err)
			}
			if len(pub.sent) != len(tt.wantSent) {
				t.Fatalf("sent %v, want %v", pub.sent, tt.wantSent)
			}
			for i := range pub.sent {
				if pub.sent[i] != tt.wantSent[i] {
					t.Fatalf("sent %v, want %v", pub.sent, tt.wantSent)
				}
			}
			for _, id := range tt.wantPublished {
				if !store.published[id] {
					t.Fatalf("%s should be marked published (got %v)", id, store.published)
				}
			}
			if len(store.published) != len(tt.wantPublished) {
				t.Fatalf("published = %v, want %v", store.published, tt.wantPublished)
			}
			for _, id := range tt.wantFailed {
				if store.causes[id] != errBroker.Error() {
					t.Fatalf("failure cause of %s = %q", id, store.causes[id])
				}
			}
			if len(store.failedFor) != len(tt.wantFailed) {
				t.Fatalf("failed = %v, want %v", store.failedFor, tt.wantFailed)
			}
		})
	}
}

func TestFailedEventsBackOffWithTheirAttempts(t *testing.T) {
	store := newFakeStore(msg("first", 0), msg("fourth", 3), msg("capped", 40))
	pub := &fakePublisher{fail: map[string]error{"first": errors.New("x"), "fourth": errors.New("x"), "capped": errors.New("x")}}
	w := New(store, pub, Config{Owner: "r1", BackoffBase: time.Second, BackoffMax: time.Minute}, quiet())

	if _, err := w.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	for id, centre := range map[string]time.Duration{"first": time.Second, "fourth": 8 * time.Second, "capped": time.Minute} {
		got := store.failedFor[id]
		if got < centre*8/10 || got > centre*12/10 {
			t.Fatalf("%s retries in %v, want about %v (±20%%)", id, got, centre)
		}
	}
}

func TestRepublishKeepsTheEventID(t *testing.T) {
	// The first replica publishes but cannot mark; the event comes back and is published again under the same id.
	store := newFakeStore(msg("evt-1", 0))
	store.markErr = errors.New("db down")
	pub := &fakePublisher{}
	w := New(store, pub, Config{Owner: "r1"}, quiet())

	if _, err := w.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.published["evt-1"] {
		t.Fatal("event marked despite the store failure")
	}
	store.pending = []Message{msg("evt-1", 0)} // the lease expired and the row is due again
	if _, err := w.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(pub.sent) != 2 || pub.sent[0] != "evt-1" || pub.sent[1] != "evt-1" || !store.published["evt-1"] {
		t.Fatalf("sent %v, published %v", pub.sent, store.published)
	}
}

func TestShutdownFinishesTheCurrentEventAndReleasesTheRest(t *testing.T) {
	store := newFakeStore(msg("a", 0), msg("b", 0), msg("c", 0))
	ctx, cancel := context.WithCancel(context.Background())
	pub := &fakePublisher{before: func(m Message) {
		if m.ID == "a" {
			cancel() // shutdown arrives while "a" is being published
		}
	}}
	w := New(store, pub, Config{Owner: "r1"}, quiet())

	if _, err := w.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}

	if len(pub.sent) != 1 || pub.sent[0] != "a" || !store.published["a"] {
		t.Fatalf("the in-flight event must complete: sent %v, published %v", pub.sent, store.published)
	}
	if len(store.released) != 2 || store.released[0] != "b" || store.released[1] != "c" {
		t.Fatalf("released = %v, want the two unused leases", store.released)
	}
}

func TestRunKeepsGoingAfterErrorsAndStopsOnCancel(t *testing.T) {
	store := newFakeStore(msg("a", 0))
	store.claimErr = errors.New("db down")
	pub := &fakePublisher{}
	w := New(store, pub, Config{Owner: "r1", PollInterval: time.Millisecond}, quiet())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()

	time.Sleep(30 * time.Millisecond)
	store.mu.Lock()
	failing := store.claimCalls
	store.claimErr = nil
	store.mu.Unlock()
	if failing < 2 {
		t.Fatalf("worker gave up after a claim error (%d calls)", failing)
	}

	deadline := time.After(time.Second)
	for {
		store.mu.Lock()
		ok := store.published["a"]
		store.mu.Unlock()
		if ok {
			break
		}
		select {
		case <-deadline:
			t.Fatal("event not relayed after the store recovered")
		case <-time.After(time.Millisecond):
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after cancellation")
	}
}

func TestFullBatchesAreDrainedWithoutWaiting(t *testing.T) {
	var msgs []Message
	for i := range 7 {
		msgs = append(msgs, msg(string(rune('a'+i)), 0))
	}
	store := newFakeStore(msgs...)
	w := New(store, &fakePublisher{}, Config{Owner: "r1", BatchSize: 3, PollInterval: time.Hour}, quiet())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	deadline := time.After(time.Second)
	for {
		store.mu.Lock()
		n := len(store.published)
		store.mu.Unlock()
		if n == 7 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("only %d of 7 relayed: full batches must be followed immediately by the next claim", n)
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	<-done
}

func TestEachPublicationHasASpanThatSaysWhetherItWorked(t *testing.T) {
	rec := spantest.Install(t)
	store := newFakeStore(msg("evt-ok", 0), msg("evt-bad", 2))
	pub := &fakePublisher{fail: map[string]error{"evt-bad": errors.New("throttled")}}

	if _, err := New(store, pub, Config{Owner: "r1"}, quiet()).RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	spans := rec.Named("outbox.publish")
	if len(spans) != 2 {
		t.Fatalf("spans = %d", len(spans))
	}
	for _, s := range spans {
		attrs := spantest.Attrs(s)
		if attrs["event.type"] != "WalletBalanceChanged" || attrs["wallet.id"] != "wallet-1" {
			t.Fatalf("attributes = %v", attrs)
		}
		switch attrs["event.id"] {
		case "evt-ok":
			if s.Status().Code == codes.Error || attrs["outbox.attempt"] != "1" {
				t.Fatalf("the successful publication: status %v, attributes %v", s.Status(), attrs)
			}
		case "evt-bad":
			if s.Status().Code != codes.Error || attrs["outbox.attempt"] != "3" {
				t.Fatalf("the failed publication: status %v, attributes %v", s.Status(), attrs)
			}
		default:
			t.Fatalf("unexpected event %v", attrs)
		}
	}
}

// The publisher receives the span's context, which is what lets the broker message carry the trace on.
func TestThePublisherIsCalledWithTheSpanContext(t *testing.T) {
	spantest.Install(t)
	var inside bool
	pub := &fakePublisher{}
	store := newFakeStore(msg("evt-1", 0))
	w := New(store, ctxPublisher{pub, &inside}, Config{Owner: "r1"}, quiet())
	if _, err := w.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !inside {
		t.Fatal("Publish ran outside the publication span")
	}
}

type ctxPublisher struct {
	*fakePublisher
	inside *bool
}

func (p ctxPublisher) Publish(ctx context.Context, m Message) error {
	*p.inside = trace.SpanContextFromContext(ctx).IsValid()
	return p.fakePublisher.Publish(ctx, m)
}
