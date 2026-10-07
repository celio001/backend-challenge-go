package sqs

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/celio001/backend-challenge-go/internal/infra/outbox"
)

type fakeAPI struct {
	sent []*awssqs.SendMessageInput
	err  error
}

func (f *fakeAPI) SendMessage(_ context.Context, in *awssqs.SendMessageInput, _ ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.sent = append(f.sent, in)
	return &awssqs.SendMessageOutput{}, nil
}

func (f *fakeAPI) GetQueueUrl(context.Context, *awssqs.GetQueueUrlInput, ...func(*awssqs.Options)) (*awssqs.GetQueueUrlOutput, error) {
	return &awssqs.GetQueueUrlOutput{QueueUrl: aws.String("http://queue/events.fifo")}, f.err
}

func (f *fakeAPI) GetQueueAttributes(context.Context, *awssqs.GetQueueAttributesInput, ...func(*awssqs.Options)) (*awssqs.GetQueueAttributesOutput, error) {
	return &awssqs.GetQueueAttributesOutput{}, f.err
}

func TestPublisher(t *testing.T) {
	errBroker := errors.New("throttled")
	tests := []struct {
		name    string
		msg     outbox.Message
		apiErr  error
		wantErr error
	}{
		{name: "sends", msg: outbox.Message{ID: "evt-1", PartitionKey: "wallet-1", Type: "WalletBalanceChanged", Payload: []byte(`{"a":1}`)}},
		{name: "broker failure is wrapped", msg: outbox.Message{ID: "evt-1", PartitionKey: "wallet-1", Payload: []byte(`{}`)}, apiErr: errBroker, wantErr: errBroker},
		{name: "event without id", msg: outbox.Message{PartitionKey: "wallet-1", Payload: []byte(`{}`)}, wantErr: ErrMissingRouting},
		{name: "event without partition key", msg: outbox.Message{ID: "evt-1", Payload: []byte(`{}`)}, wantErr: ErrMissingRouting},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := &fakeAPI{err: tt.apiErr}

			err := NewPublisher(api, "http://queue/events.fifo").Publish(context.Background(), tt.msg)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				if len(api.sent) != 0 {
					t.Fatal("a message was sent despite the error")
				}
				return
			}
			in := api.sent[0]
			if aws.ToString(in.QueueUrl) != "http://queue/events.fifo" || aws.ToString(in.MessageBody) != `{"a":1}` ||
				aws.ToString(in.MessageGroupId) != "wallet-1" || aws.ToString(in.MessageDeduplicationId) != "evt-1" {
				t.Fatalf("input = %+v", in)
			}
			if aws.ToString(in.MessageAttributes["eventId"].StringValue) != "evt-1" || aws.ToString(in.MessageAttributes["eventType"].StringValue) != "WalletBalanceChanged" {
				t.Fatalf("attributes = %+v", in.MessageAttributes)
			}
		})
	}
}

func TestQueueURLAndPing(t *testing.T) {
	url, err := QueueURL(context.Background(), &fakeAPI{}, "events.fifo")
	if err != nil || url != "http://queue/events.fifo" {
		t.Fatalf("url = %q, err = %v", url, err)
	}
	broken := &fakeAPI{err: errors.New("no such queue")}
	if _, err := QueueURL(context.Background(), broken, "events.fifo"); err == nil {
		t.Fatal("a missing queue was not reported")
	}
	if err := Ping(context.Background(), broken, "http://queue/events.fifo"); err == nil {
		t.Fatal("an unreachable queue passed the ping")
	}
	if err := Ping(context.Background(), &fakeAPI{}, "http://queue/events.fifo"); err != nil {
		t.Fatal(err)
	}
}
