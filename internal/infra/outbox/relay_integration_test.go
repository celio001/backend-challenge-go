//go:build integration

package outbox_test

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/infra/outbox"
	"github.com/celio001/backend-challenge-go/internal/infra/postgres"
	infrasqs "github.com/celio001/backend-challenge-go/internal/infra/sqs"
	"github.com/celio001/backend-challenge-go/internal/infra/system"
	"github.com/celio001/backend-challenge-go/internal/testutil/pgtest"
	"github.com/celio001/backend-challenge-go/internal/testutil/sqstest"
	"github.com/celio001/backend-challenge-go/internal/usecase/openwallet"
)

type fixture struct {
	t     *testing.T
	admin *pgxpool.Pool
	url   string
	queue sqstest.Queue
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	admin, url := pgtest.New(t)
	return &fixture{t: t, admin: admin, url: url, queue: sqstest.New(t)}
}

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

// produce opens wallets with a balance; each one commits two outbox events, like in production.
func (f *fixture) produce(wallets int) {
	f.t.Helper()
	uc := openwallet.New(postgres.NewUnitOfWork(f.admin), system.Clock{}, system.IDs{})
	m, _ := money.FromMinor(10000, money.BRL)
	for range wallets {
		if _, err := uc.Execute(context.Background(), openwallet.Input{PlayerID: newUUID(f.t), InitialBalance: m}); err != nil {
			f.t.Fatal(err)
		}
	}
}

func (f *fixture) count(q string, args ...any) (n int) {
	f.t.Helper()
	if err := f.admin.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		f.t.Fatal(err)
	}
	return n
}

func (f *fixture) eventIDs() []string {
	f.t.Helper()
	rows, err := f.admin.Query(context.Background(), `SELECT id FROM outbox_events ORDER BY id`)
	if err != nil {
		f.t.Fatal(err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			f.t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return ids
}

func (f *fixture) waitPublished(timeout time.Duration) bool {
	f.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f.count(`SELECT count(*) FROM outbox_events WHERE published_at IS NULL`) == 0 {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// assertQueueHoldsEachEventOnce checks nothing was lost and nothing arrived twice, whatever happened on the way.
func (f *fixture) assertQueueHoldsEachEventOnce() {
	f.t.Helper()
	want := f.eventIDs()
	got := f.queue.WaitFor(f.t, len(want), 30*time.Second)
	ids := make([]string, len(got))
	for i, r := range got {
		ids[i] = r.EventID
	}
	sort.Strings(ids)
	if len(ids) != len(want) {
		f.t.Fatalf("queue holds %d events, outbox has %d", len(ids), len(want))
	}
	for i := range ids {
		if ids[i] != want[i] {
			f.t.Fatalf("event %s is missing or duplicated on the queue", want[i])
		}
	}
}

// replica is one publisher process: its own connection pool, store and worker.
type replica struct {
	pool   *pgxpool.Pool
	worker *outbox.Worker
	sends  *atomic.Int64
}

type countingPublisher struct {
	inner outbox.Publisher
	sends *atomic.Int64
	fail  func(m outbox.Message) error
}

func (p countingPublisher) Publish(ctx context.Context, m outbox.Message) error {
	if p.fail != nil {
		if err := p.fail(m); err != nil {
			return err
		}
	}
	if err := p.inner.Publish(ctx, m); err != nil {
		return err
	}
	p.sends.Add(1)
	return nil
}

// loseMarks drops the acknowledgement after publishing, as if the replica crashed right after the broker accepted the event.
type loseMarks struct {
	outbox.Store
	lose func(id string) bool
}

func (s loseMarks) MarkPublished(ctx context.Context, id, owner string) (bool, error) {
	if s.lose(id) {
		return false, errors.New("simulated crash before the mark")
	}
	return s.Store.MarkPublished(ctx, id, owner)
}

func (f *fixture) newReplica(name string, cfg outbox.Config, wrap func(outbox.Store) outbox.Store, fail func(outbox.Message) error) replica {
	f.t.Helper()
	pool, err := pgxpool.New(context.Background(), f.url)
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(pool.Close)
	var store outbox.Store = postgres.NewOutboxStore(pool)
	if wrap != nil {
		store = wrap(store)
	}
	sends := &atomic.Int64{}
	cfg.Owner = name
	pub := countingPublisher{inner: infrasqs.NewPublisher(f.queue.Client, f.queue.URL), sends: sends, fail: fail}
	return replica{pool: pool, sends: sends, worker: outbox.New(store, pub, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))}
}

func (r replica) start(ctx context.Context) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { r.worker.Run(ctx); close(done) }()
	return func() { cancel(); <-done }
}

var fast = outbox.Config{PollInterval: 10 * time.Millisecond, Lease: 30 * time.Second, BackoffBase: 20 * time.Millisecond, BackoffMax: 100 * time.Millisecond, BatchSize: 10}

func TestTwoPublishersShareTheOutboxWithoutSendingTwice(t *testing.T) {
	f := newFixture(t)
	f.produce(40) // 80 events
	a, b := f.newReplica("replica-a", fast, nil, nil), f.newReplica("replica-b", fast, nil, nil)
	stopA, stopB := a.start(context.Background()), b.start(context.Background())

	if !f.waitPublished(30 * time.Second) {
		t.Fatal("events were not all published")
	}
	stopA()
	stopB()

	total := a.sends.Load() + b.sends.Load()
	if total != 80 {
		t.Fatalf("%d sends for 80 events: the leases must give every event to exactly one publisher", total)
	}
	if a.sends.Load() == 0 || b.sends.Load() == 0 {
		t.Logf("one publisher did all the work (a=%d b=%d); legal but the race was not exercised", a.sends.Load(), b.sends.Load())
	}
	f.assertQueueHoldsEachEventOnce()
	if n := f.count(`SELECT count(*) FROM outbox_events WHERE locked_by IS NOT NULL`); n != 0 {
		t.Fatalf("%d events keep a lease after being published", n)
	}
}

func TestEventsOfACrashedPublisherAreTakenOverAfterItsLeaseExpires(t *testing.T) {
	f := newFixture(t)
	f.produce(10) // 20 events

	// "crashed" claims everything and then dies: committed events are neither published nor lost.
	crashedPool, err := pgxpool.New(context.Background(), f.url)
	if err != nil {
		t.Fatal(err)
	}
	defer crashedPool.Close()
	claimed, err := postgres.NewOutboxStore(crashedPool).Claim(context.Background(), "crashed", 100, 1500*time.Millisecond)
	if err != nil || len(claimed) != 20 {
		t.Fatalf("claim = %d events, %v", len(claimed), err)
	}

	survivor := f.newReplica("survivor", fast, nil, nil)
	stop := survivor.start(context.Background())
	defer stop()

	time.Sleep(600 * time.Millisecond)
	if n := f.count(`SELECT count(*) FROM outbox_events WHERE published_at IS NOT NULL`); n != 0 {
		t.Fatalf("%d events were published while the crashed replica's lease was still valid", n)
	}

	if !f.waitPublished(15 * time.Second) {
		t.Fatal("the survivor never took the abandoned events over")
	}
	if got := survivor.sends.Load(); got != 20 {
		t.Fatalf("survivor sent %d, want 20", got)
	}
	f.assertQueueHoldsEachEventOnce()
}

func TestAnEventPublishedBeforeACrashIsNotDuplicatedOnTheQueue(t *testing.T) {
	f := newFixture(t)
	f.produce(8) // 16 events

	// The first replica reaches the broker but "crashes" before recording it, for half of the events.
	var lost sync.Map
	n := 0
	lose := func(id string) bool {
		if _, again := lost.Load(id); again {
			return false // a given event only suffers the crash once
		}
		n++
		if n%2 == 0 {
			lost.Store(id, true)
			return true
		}
		return false
	}
	cfg := fast
	cfg.Lease = 700 * time.Millisecond
	flaky := f.newReplica("flaky", cfg, func(s outbox.Store) outbox.Store { return loseMarks{Store: s, lose: lose} }, nil)
	healthy := f.newReplica("healthy", cfg, nil, nil)

	stopFlaky := flaky.start(context.Background())
	if !waitFor(10*time.Second, func() bool { return flaky.sends.Load() >= 16 }) {
		t.Fatalf("flaky replica sent only %d", flaky.sends.Load())
	}
	stopFlaky()
	unmarked := f.count(`SELECT count(*) FROM outbox_events WHERE published_at IS NULL`)
	if unmarked == 0 {
		t.Fatal("the simulated crash left nothing unmarked, so the scenario did not happen")
	}

	stopHealthy := healthy.start(context.Background())
	defer stopHealthy()
	if !f.waitPublished(15 * time.Second) {
		t.Fatal("unmarked events were never recovered")
	}
	if got := healthy.sends.Load(); got != int64(unmarked) {
		t.Fatalf("healthy replica republished %d events, want the %d that were left unmarked", got, unmarked)
	}
	// The broker saw the events again, yet the queue holds each id exactly once.
	f.assertQueueHoldsEachEventOnce()
}

func TestBrokerOutageDelaysButNeverLosesEvents(t *testing.T) {
	f := newFixture(t)
	f.produce(6) // 12 events

	var calls atomic.Int64
	down := func(outbox.Message) error {
		if calls.Add(1) <= 20 { // the first 20 sends fail
			return errors.New("broker unavailable")
		}
		return nil
	}
	r := f.newReplica("r1", fast, nil, down)
	stop := r.start(context.Background())
	defer stop()

	if !f.waitPublished(30 * time.Second) {
		t.Fatal("events were not published after the broker came back")
	}
	if n := f.count(`SELECT count(*) FROM outbox_events WHERE attempts > 0`); n == 0 {
		t.Fatal("failed attempts were not counted")
	}
	if n := f.count(`SELECT count(*) FROM outbox_events WHERE last_error IS NOT NULL`); n != 0 {
		t.Fatalf("%d published events still show an error", n)
	}
	f.assertQueueHoldsEachEventOnce()
}

func TestGracefulStopHandsRemainingEventsToAnotherPublisher(t *testing.T) {
	f := newFixture(t)
	f.produce(30) // 60 events

	cfg := fast
	cfg.BatchSize = 60 // everything is claimed in one go, so a stop mid-batch has events to release
	cfg.Lease = time.Minute

	first := f.newReplica("first", cfg, nil, nil)
	stopFirst := first.start(context.Background())
	waitFor(5*time.Second, func() bool { return first.sends.Load() >= 5 })
	stopFirst() // the lease would last a minute: only an explicit release lets the other replica finish quickly

	second := f.newReplica("second", cfg, nil, nil)
	stopSecond := second.start(context.Background())
	defer stopSecond()
	if !f.waitPublished(10 * time.Second) {
		t.Fatal("events released by the stopped publisher were not picked up")
	}
	if total := first.sends.Load() + second.sends.Load(); total != 60 {
		t.Fatalf("%d sends for 60 events across a graceful handover", total)
	}
	f.assertQueueHoldsEachEventOnce()
}

func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}
