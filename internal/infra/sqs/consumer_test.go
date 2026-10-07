package sqs

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/celio001/backend-challenge-go/internal/testutil/spantest"
)

type fakeQueue struct {
	mu         sync.Mutex
	batches    [][]types.Message
	receiveErr []error
	sendErr    error

	deleted    []string
	visibility map[string][]int32
	sent       []*awssqs.SendMessageInput
}

func (f *fakeQueue) ReceiveMessage(ctx context.Context, _ *awssqs.ReceiveMessageInput, _ ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
	f.mu.Lock()
	if len(f.receiveErr) > 0 {
		err := f.receiveErr[0]
		f.receiveErr = f.receiveErr[1:]
		f.mu.Unlock()
		return nil, err
	}
	if len(f.batches) > 0 {
		b := f.batches[0]
		f.batches = f.batches[1:]
		f.mu.Unlock()
		return &awssqs.ReceiveMessageOutput{Messages: b}, nil
	}
	f.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (f *fakeQueue) DeleteMessage(_ context.Context, in *awssqs.DeleteMessageInput, _ ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, aws.ToString(in.ReceiptHandle))
	return &awssqs.DeleteMessageOutput{}, nil
}

func (f *fakeQueue) ChangeMessageVisibility(_ context.Context, in *awssqs.ChangeMessageVisibilityInput, _ ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.visibility == nil {
		f.visibility = map[string][]int32{}
	}
	h := aws.ToString(in.ReceiptHandle)
	f.visibility[h] = append(f.visibility[h], in.VisibilityTimeout)
	return &awssqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *fakeQueue) SendMessage(_ context.Context, in *awssqs.SendMessageInput, _ ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sendErr != nil {
		return nil, f.sendErr
	}
	f.sent = append(f.sent, in)
	return &awssqs.SendMessageOutput{}, nil
}

func (f *fakeQueue) snapshot() (deleted []string, visibility map[string][]int32, sent []*awssqs.SendMessageInput) {
	f.mu.Lock()
	defer f.mu.Unlock()
	visibility = map[string][]int32{}
	for k, v := range f.visibility {
		visibility[k] = append([]int32(nil), v...)
	}
	return append([]string(nil), f.deleted...), visibility, append([]*awssqs.SendMessageInput(nil), f.sent...)
}

func msg(id, group string, receives string) types.Message {
	return types.Message{
		MessageId:     aws.String("sqs-" + id),
		ReceiptHandle: aws.String("rh-" + id),
		Body:          aws.String("body-" + id),
		Attributes: map[string]string{
			string(types.MessageSystemAttributeNameMessageGroupId):          group,
			string(types.MessageSystemAttributeNameSenderId):                "sender",
			string(types.MessageSystemAttributeNameApproximateReceiveCount): receives,
		},
	}
}

type handlerFunc func(ctx context.Context, m Message) Verdict

func (h handlerFunc) Handle(ctx context.Context, m Message) Verdict { return h(ctx, m) }

func testConfig() ConsumerConfig {
	return ConsumerConfig{QueueURL: "queue", DLQURL: "dlq", WaitTime: time.Second, Visibility: 3 * time.Second, ShutdownGrace: 300 * time.Millisecond, ReceiveBackoffMax: 20 * time.Millisecond}
}

// runUntil starts the consumer, waits for cond, stops it and waits for Run to return.
func runUntil(t *testing.T, c *Consumer, cond func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() { c.Run(ctx); close(finished) }()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	ok := cond()
	cancel()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
	if !ok {
		t.Fatal("condition not reached in time")
	}
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestVerdictsAreApplied(t *testing.T) {
	tests := []struct {
		name         string
		verdict      Verdict
		sendErr      error
		receives     string
		wantDeleted  bool
		wantSentCode string
		wantVis      []int32
	}{
		{name: "delete removes the message", verdict: Verdict{Action: Delete}, receives: "1", wantDeleted: true},
		{name: "dead letter copies to the dlq and removes", verdict: Verdict{Action: DeadLetter, Code: "MALFORMED_MESSAGE"}, receives: "1", wantDeleted: true, wantSentCode: "MALFORMED_MESSAGE"},
		{name: "a dlq failure keeps the message", verdict: Verdict{Action: DeadLetter, Code: "X"}, sendErr: errors.New("dlq down"), receives: "1"},
		{name: "retry backs off 2^receives seconds", verdict: Verdict{Action: Retry}, receives: "3", wantVis: []int32{8}},
		{name: "retry backoff is capped", verdict: Verdict{Action: Retry}, receives: "9", wantVis: []int32{60}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &fakeQueue{batches: [][]types.Message{{msg("a", "wallet-1", tt.receives)}}, sendErr: tt.sendErr}
			var handled atomic.Bool
			c := NewConsumer(q, handlerFunc(func(context.Context, Message) Verdict { handled.Store(true); return tt.verdict }), testConfig(), quiet())
			// The verdict is applied right after Handle returns, so wait for its visible effect or for a settled quiet period.
			runUntil(t, c, func() bool {
				if !handled.Load() {
					return false
				}
				time.Sleep(50 * time.Millisecond)
				return true
			})

			deleted, vis, sent := q.snapshot()
			if got := len(deleted) == 1; got != tt.wantDeleted {
				t.Fatalf("deleted = %v", deleted)
			}
			if !equalInt32(vis["rh-a"], tt.wantVis) {
				t.Fatalf("visibility = %v, want %v", vis["rh-a"], tt.wantVis)
			}
			if tt.wantSentCode == "" {
				if len(sent) != 0 {
					t.Fatalf("sent = %d", len(sent))
				}
				return
			}
			if len(sent) != 1 {
				t.Fatalf("sent = %d", len(sent))
			}
			in := sent[0]
			if aws.ToString(in.QueueUrl) != "dlq" || aws.ToString(in.MessageBody) != "body-a" || aws.ToString(in.MessageGroupId) != "wallet-1" ||
				aws.ToString(in.MessageDeduplicationId) != "sqs-a" || aws.ToString(in.MessageAttributes["failureCode"].StringValue) != tt.wantSentCode ||
				aws.ToString(in.MessageAttributes["senderId"].StringValue) != "sender" {
				t.Fatalf("dlq message = %+v", in)
			}
		})
	}
}

func equalInt32(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestHandlerReceivesTheTransportFields(t *testing.T) {
	q := &fakeQueue{batches: [][]types.Message{{msg("a", "g", "4")}}}
	got := make(chan Message, 1)
	c := NewConsumer(q, handlerFunc(func(_ context.Context, m Message) Verdict { got <- m; return Verdict{Action: Delete} }), testConfig(), quiet())
	runUntil(t, c, func() bool { return len(got) == 1 })
	if m := <-got; m != (Message{ID: "sqs-a", Body: "body-a", SenderID: "sender", ReceiveCount: 4}) {
		t.Fatalf("message = %+v", m)
	}
}

func TestAFailedMessageHoldsBackItsGroup(t *testing.T) {
	q := &fakeQueue{batches: [][]types.Message{{msg("a", "g1", "1"), msg("b", "g1", "1"), msg("c", "g2", "1")}}}
	var mu sync.Mutex
	var handled []string
	c := NewConsumer(q, handlerFunc(func(_ context.Context, m Message) Verdict {
		mu.Lock()
		handled = append(handled, m.Body)
		mu.Unlock()
		if m.Body == "body-a" {
			return Verdict{Action: Retry}
		}
		return Verdict{Action: Delete}
	}), testConfig(), quiet())
	runUntil(t, c, func() bool {
		d, v, _ := q.snapshot()
		return len(d) == 1 && len(v["rh-a"]) == 1 && len(v["rh-b"]) == 1
	})

	mu.Lock()
	defer mu.Unlock()
	if len(handled) != 2 {
		t.Fatalf("handled = %v: b must not run after a failed", handled)
	}
	deleted, vis, _ := q.snapshot()
	if deleted[0] != "rh-c" || !equalInt32(vis["rh-b"], []int32{0}) {
		t.Fatalf("deleted = %v, visibility of b = %v (want released)", deleted, vis["rh-b"])
	}
}

func TestGroupsRunInParallelAndAGroupRunsInOrder(t *testing.T) {
	q := &fakeQueue{batches: [][]types.Message{{msg("a1", "g1", "1"), msg("a2", "g1", "1"), msg("b1", "g2", "1")}}}
	var mu sync.Mutex
	running := map[string]int{}
	var order []string
	bothStarted := make(chan struct{})
	var started atomic.Int32
	c := NewConsumer(q, handlerFunc(func(_ context.Context, m Message) Verdict {
		group := m.Body[5:6]
		mu.Lock()
		running[group]++
		if running[group] > 1 {
			t.Errorf("group %s handled concurrently", group)
		}
		order = append(order, m.Body)
		mu.Unlock()

		// The first message of each group waits for the other group, which only works if they run in parallel.
		if m.Body == "body-a1" || m.Body == "body-b1" {
			if started.Add(1) == 2 {
				close(bothStarted)
			}
			select {
			case <-bothStarted:
			case <-time.After(3 * time.Second):
				t.Error("groups did not run in parallel")
			}
		}
		mu.Lock()
		running[group]--
		mu.Unlock()
		return Verdict{Action: Delete}
	}), testConfig(), quiet())
	runUntil(t, c, func() bool { d, _, _ := q.snapshot(); return len(d) == 3 })

	mu.Lock()
	defer mu.Unlock()
	var a []string
	for _, o := range order {
		if o[5] == 'a' {
			a = append(a, o)
		}
	}
	if len(a) != 2 || a[0] != "body-a1" || a[1] != "body-a2" {
		t.Fatalf("group order = %v", a)
	}
}

func TestShutdown(t *testing.T) {
	t.Run("an in-flight message is finished and the rest of its group is released", func(t *testing.T) {
		q := &fakeQueue{batches: [][]types.Message{{msg("a", "g", "1"), msg("b", "g", "1")}}}
		entered, release := make(chan struct{}), make(chan struct{})
		var handled atomic.Int32
		c := NewConsumer(q, handlerFunc(func(_ context.Context, m Message) Verdict {
			handled.Add(1)
			close(entered)
			<-release
			return Verdict{Action: Delete}
		}), testConfig(), quiet())

		ctx, cancel := context.WithCancel(context.Background())
		finished := make(chan struct{})
		go func() { c.Run(ctx); close(finished) }()
		<-entered
		cancel()
		select {
		case <-finished:
			t.Fatal("Run returned while a message was in flight")
		case <-time.After(100 * time.Millisecond):
		}
		close(release)
		<-finished

		deleted, vis, _ := q.snapshot()
		if handled.Load() != 1 || len(deleted) != 1 || deleted[0] != "rh-a" || !equalInt32(vis["rh-b"], []int32{0}) {
			t.Fatalf("handled = %d, deleted = %v, visibility of b = %v", handled.Load(), deleted, vis["rh-b"])
		}
	})

	t.Run("work that outlives the grace period is canceled and the message released", func(t *testing.T) {
		q := &fakeQueue{batches: [][]types.Message{{msg("a", "g", "1")}}}
		entered := make(chan struct{})
		c := NewConsumer(q, handlerFunc(func(ctx context.Context, m Message) Verdict {
			close(entered)
			<-ctx.Done()
			return Verdict{Action: Retry, Err: ctx.Err()}
		}), testConfig(), quiet())

		ctx, cancel := context.WithCancel(context.Background())
		finished := make(chan struct{})
		go func() { c.Run(ctx); close(finished) }()
		<-entered
		cancel()
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Fatal("Run did not return after the grace period")
		}

		deleted, vis, _ := q.snapshot()
		if len(deleted) != 0 || !equalInt32(vis["rh-a"], []int32{0}) {
			t.Fatalf("deleted = %v, visibility = %v (want released with 0)", deleted, vis["rh-a"])
		}
	})
}

func TestHeartbeatRenewsTheVisibilityWhileHandling(t *testing.T) {
	q := &fakeQueue{batches: [][]types.Message{{msg("a", "g", "1")}}}
	cfg := testConfig()
	cfg.Visibility = 3 * time.Second
	c := NewConsumer(q, handlerFunc(func(context.Context, Message) Verdict {
		time.Sleep(1300 * time.Millisecond)
		return Verdict{Action: Delete}
	}), cfg, quiet())
	runUntil(t, c, func() bool { d, _, _ := q.snapshot(); return len(d) == 1 })

	_, vis, _ := q.snapshot()
	if len(vis["rh-a"]) == 0 || vis["rh-a"][0] != 3 {
		t.Fatalf("visibility calls = %v, want renewals to 3s", vis["rh-a"])
	}
}

func TestReceiveFailuresAreReportedAndRecovered(t *testing.T) {
	q := &fakeQueue{receiveErr: []error{errors.New("down"), errors.New("down")}, batches: [][]types.Message{{msg("a", "g", "1")}}}
	var sawUnhealthy atomic.Bool
	var c *Consumer
	c = NewConsumer(q, handlerFunc(func(context.Context, Message) Verdict { return Verdict{Action: Delete} }), testConfig(), quiet())
	go func() {
		for i := 0; i < 400 && !sawUnhealthy.Load(); i++ {
			if !c.Healthy() {
				sawUnhealthy.Store(true)
			}
			time.Sleep(time.Millisecond)
		}
	}()
	runUntil(t, c, func() bool { d, _, _ := q.snapshot(); return len(d) == 1 })

	if !sawUnhealthy.Load() || !c.Healthy() {
		t.Fatalf("unhealthy seen = %v, healthy now = %v", sawUnhealthy.Load(), c.Healthy())
	}
}

func msgWithTrace(id, group, traceparent string) types.Message {
	m := msg(id, group, "2")
	if traceparent != "" {
		m.MessageAttributes = map[string]types.MessageAttributeValue{"traceparent": {DataType: aws.String("String"), StringValue: aws.String(traceparent)}}
	}
	return m
}

func TestEachMessageHasASpanThatContinuesTheSendersTrace(t *testing.T) {
	const traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	tests := []struct {
		name        string
		message     types.Message
		verdict     Verdict
		wantParent  bool
		wantOutcome string
		wantError   bool
		wantCode    string
	}{
		{name: "a handled message continues the senders trace", message: msgWithTrace("a", "g", traceparent), verdict: Verdict{Action: Delete}, wantParent: true, wantOutcome: "delete"},
		{name: "without a traceparent it starts a trace of its own", message: msgWithTrace("a", "g", ""), verdict: Verdict{Action: Delete}, wantOutcome: "delete"},
		{name: "a dead letter is a decision, not a service failure", message: msgWithTrace("a", "g", traceparent), verdict: Verdict{Action: DeadLetter, Code: "MALFORMED_MESSAGE"}, wantParent: true, wantOutcome: "dead_letter", wantCode: "MALFORMED_MESSAGE"},
		{name: "a message to retry is a span error with its cause", message: msgWithTrace("a", "g", traceparent), verdict: Verdict{Action: Retry, Err: errors.New("db down")}, wantParent: true, wantOutcome: "retry", wantError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := spantest.Install(t)
			q := &fakeQueue{batches: [][]types.Message{{tt.message}}}
			var handlerTrace string
			c := NewConsumer(q, handlerFunc(func(ctx context.Context, m Message) Verdict {
				handlerTrace = trace.SpanContextFromContext(ctx).TraceID().String()
				return tt.verdict
			}), testConfig(), quiet())
			runUntil(t, c, func() bool {
				d, v, s := q.snapshot()
				return len(d)+len(v)+len(s) > 0
			})

			spans := rec.Named("sqs.process")
			if len(spans) != 1 {
				t.Fatalf("spans = %d", len(spans))
			}
			s, attrs := spans[0], spantest.Attrs(spans[0])
			if s.SpanKind() != trace.SpanKindConsumer || attrs["messaging.system"] != "aws_sqs" || attrs["messaging.message.id"] != "sqs-a" ||
				attrs["messaging.destination.name"] != "queue" || attrs["messaging.sqs.receive_count"] != "2" || attrs["messaging.outcome"] != tt.wantOutcome ||
				attrs["messaging.failure_code"] != tt.wantCode {
				t.Fatalf("span = %v / %v", s.SpanKind(), attrs)
			}
			if tt.wantParent {
				if s.SpanContext().TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" || s.Parent().SpanID().String() != "00f067aa0ba902b7" || !s.Parent().IsRemote() {
					t.Fatalf("the span did not continue the senders trace: trace %s, parent %s", s.SpanContext().TraceID(), s.Parent().SpanID())
				}
			} else if s.Parent().IsValid() {
				t.Fatalf("an unexpected parent: %s", s.Parent().SpanID())
			}
			if handlerTrace != s.SpanContext().TraceID().String() {
				t.Fatalf("the handler ran in trace %s, not the message span's %s", handlerTrace, s.SpanContext().TraceID())
			}
			if (s.Status().Code == codes.Error) != tt.wantError {
				t.Fatalf("status = %v, want error = %v", s.Status(), tt.wantError)
			}
		})
	}
}
