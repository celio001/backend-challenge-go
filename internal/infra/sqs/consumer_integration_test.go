//go:build integration

package sqs_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/celio001/backend-challenge-go/internal/infra/sqs"
	"github.com/celio001/backend-challenge-go/internal/testutil/sqstest"
)

type handlerFunc func(ctx context.Context, m sqs.Message) sqs.Verdict

func (h handlerFunc) Handle(ctx context.Context, m sqs.Message) sqs.Verdict { return h(ctx, m) }

func send(t *testing.T, q sqstest.Queue, group, id, body string) {
	t.Helper()
	if _, err := q.Client.SendMessage(context.Background(), &awssqs.SendMessageInput{
		QueueUrl: aws.String(q.URL), MessageBody: aws.String(body), MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(id),
	}); err != nil {
		t.Fatal(err)
	}
}

func startConsumer(t *testing.T, q, dlq sqstest.Queue, h sqs.Handler, mutate func(*sqs.ConsumerConfig)) (stop func()) {
	t.Helper()
	cfg := sqs.ConsumerConfig{QueueURL: q.URL, DLQURL: dlq.URL, WaitTime: time.Second, ShutdownGrace: 500 * time.Millisecond}
	if mutate != nil {
		mutate(&cfg)
	}
	c := sqs.NewConsumer(q.Client, h, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("consumer did not stop")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func TestConsumerAgainstLocalStack(t *testing.T) {
	t.Run("consumes every message once, keeping each group in order", func(t *testing.T) {
		q, dlq := sqstest.New(t), sqstest.New(t)
		const groups, perGroup = 4, 5
		for i := range perGroup {
			for g := range groups {
				send(t, q, fmt.Sprintf("wallet-%d", g), fmt.Sprintf("m-%d-%d", g, i), fmt.Sprintf("%d-%d", g, i))
			}
		}

		var mu sync.Mutex
		seen := map[string][]string{}
		total := 0
		startConsumer(t, q, dlq, handlerFunc(func(_ context.Context, m sqs.Message) sqs.Verdict {
			var g, i int
			fmt.Sscanf(m.Body, "%d-%d", &g, &i)
			mu.Lock()
			seen[fmt.Sprint(g)] = append(seen[fmt.Sprint(g)], m.Body)
			total++
			mu.Unlock()
			return sqs.Verdict{Action: sqs.Delete}
		}), nil)

		waitFor(t, 20*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return total >= groups*perGroup })
		time.Sleep(500 * time.Millisecond) // a duplicate delivery would show up here
		mu.Lock()
		defer mu.Unlock()
		if total != groups*perGroup {
			t.Fatalf("handled %d messages, want %d", total, groups*perGroup)
		}
		for g, bodies := range seen {
			want := make([]string, perGroup)
			for i := range want {
				want[i] = fmt.Sprintf("%s-%d", g, i)
			}
			if !sort.StringsAreSorted(bodies) || fmt.Sprint(bodies) != fmt.Sprint(want) {
				t.Fatalf("group %s = %v, want %v", g, bodies, want)
			}
		}
		if left := q.Drain(t); len(left) != 0 {
			t.Fatalf("%d messages left on the queue", len(left))
		}
	})

	t.Run("a retried message comes back with a higher receive count", func(t *testing.T) {
		q, dlq := sqstest.New(t), sqstest.New(t)
		send(t, q, "wallet-1", "retry-1", "payload")

		var mu sync.Mutex
		var counts []int
		startConsumer(t, q, dlq, handlerFunc(func(_ context.Context, m sqs.Message) sqs.Verdict {
			mu.Lock()
			defer mu.Unlock()
			counts = append(counts, m.ReceiveCount)
			if len(counts) < 2 {
				return sqs.Verdict{Action: sqs.Retry}
			}
			return sqs.Verdict{Action: sqs.Delete}
		}), nil)

		waitFor(t, 20*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(counts) >= 2 })
		mu.Lock()
		defer mu.Unlock()
		if counts[0] != 1 || counts[1] != 2 {
			t.Fatalf("receive counts = %v, want [1 2]", counts)
		}
	})

	t.Run("a dead-lettered message lands in the dlq with its code and leaves the queue", func(t *testing.T) {
		q, dlq := sqstest.New(t), sqstest.New(t)
		send(t, q, "wallet-1", "bad-1", `{"broken`)
		startConsumer(t, q, dlq, handlerFunc(func(context.Context, sqs.Message) sqs.Verdict {
			return sqs.Verdict{Action: sqs.DeadLetter, Code: "MALFORMED_MESSAGE"}
		}), nil)

		got := dlq.WaitFor(t, 1, 20*time.Second)
		if len(got) != 1 || got[0].Body != `{"broken` || got[0].GroupID != "wallet-1" {
			t.Fatalf("dlq = %+v", got)
		}
		if left := q.Drain(t); len(left) != 0 {
			t.Fatalf("%d messages left on the queue", len(left))
		}
	})

	t.Run("shutdown releases a message whose work outlives the grace period", func(t *testing.T) {
		q, dlq := sqstest.New(t), sqstest.New(t)
		send(t, q, "wallet-1", "slow-1", "payload")

		entered := make(chan struct{})
		stop := startConsumer(t, q, dlq, handlerFunc(func(ctx context.Context, m sqs.Message) sqs.Verdict {
			close(entered)
			<-ctx.Done()
			return sqs.Verdict{Action: sqs.Retry, Err: ctx.Err()}
		}), nil)
		<-entered
		stop()

		// The queue's visibility timeout is 30s; seeing the message now proves it was released, not left to expire.
		got := q.WaitFor(t, 1, 5*time.Second)
		if len(got) != 1 || got[0].Body != "payload" {
			t.Fatalf("message after shutdown = %+v", got)
		}
	})
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached in time")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
