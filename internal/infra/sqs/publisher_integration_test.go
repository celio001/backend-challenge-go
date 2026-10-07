//go:build integration

package sqs_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/celio001/backend-challenge-go/internal/infra/outbox"
	"github.com/celio001/backend-challenge-go/internal/infra/sqs"
	"github.com/celio001/backend-challenge-go/internal/testutil/sqstest"
)

func TestPublisherAgainstLocalStack(t *testing.T) {
	q := sqstest.New(t)
	ctx := context.Background()
	pub := sqs.NewPublisher(q.Client, q.URL)

	t.Run("resolving and pinging a real queue", func(t *testing.T) {
		url, err := sqs.QueueURL(ctx, q.Client, q.Name)
		if err != nil || url == "" {
			t.Fatalf("QueueURL = %q, %v", url, err)
		}
		if err := sqs.Ping(ctx, q.Client, q.URL); err != nil {
			t.Fatal(err)
		}
		if _, err := sqs.QueueURL(ctx, q.Client, "does-not-exist.fifo"); err == nil {
			t.Fatal("an unknown queue was resolved")
		}
	})

	t.Run("keeps each wallet's order and drops a repeated event id", func(t *testing.T) {
		const wallets, perWallet = 3, 4
		sent := map[string][]string{}
		for i := range perWallet {
			for w := range wallets {
				m := outbox.Message{
					ID: fmt.Sprintf("evt-%d-%d", w, i), PartitionKey: fmt.Sprintf("wallet-%d", w), Type: "WalletBalanceChanged",
					Payload: []byte(fmt.Sprintf(`{"eventId":"evt-%d-%d"}`, w, i)),
				}
				if err := pub.Publish(ctx, m); err != nil {
					t.Fatal(err)
				}
				sent[m.PartitionKey] = append(sent[m.PartitionKey], m.ID)
			}
		}
		// A republication after an uncertain publish: same event id, so the broker must drop it.
		for w := range wallets {
			m := outbox.Message{ID: fmt.Sprintf("evt-%d-0", w), PartitionKey: fmt.Sprintf("wallet-%d", w), Type: "WalletBalanceChanged", Payload: []byte(`{"republished":true}`)}
			if err := pub.Publish(ctx, m); err != nil {
				t.Fatal(err)
			}
		}

		got := q.WaitFor(t, wallets*perWallet, 20e9)

		if len(got) != wallets*perWallet {
			t.Fatalf("received %d messages, want %d (the republished ones must be dropped)", len(got), wallets*perWallet)
		}
		byGroup := map[string][]string{}
		for _, r := range got {
			byGroup[r.GroupID] = append(byGroup[r.GroupID], r.EventID)
			if r.EventType != "WalletBalanceChanged" || r.Body != fmt.Sprintf(`{"eventId":"%s"}`, r.EventID) {
				t.Fatalf("message = %+v", r)
			}
		}
		for group, want := range sent {
			if fmt.Sprint(byGroup[group]) != fmt.Sprint(want) {
				t.Fatalf("%s arrived as %v, want %v", group, byGroup[group], want)
			}
		}
	})

	t.Run("an unknown queue is a publish error", func(t *testing.T) {
		bad := sqs.NewPublisher(q.Client, q.URL+"-missing")
		if err := bad.Publish(ctx, outbox.Message{ID: "x", PartitionKey: "w", Payload: []byte(`{}`)}); err == nil {
			t.Fatal("publishing to a missing queue succeeded")
		}
	})
}
