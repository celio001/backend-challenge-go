package sqs

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/celio001/backend-challenge-go/internal/infra/outbox"
)

var ErrMissingRouting = errors.New("sqs: event has no id or partition key")

// Publisher sends outbox events to a FIFO queue: the wallet is the message group, so one wallet's events stay in order
// while different wallets proceed in parallel, and the event id is the deduplication id, so a repeated send is dropped.
type Publisher struct {
	api      API
	queueURL string
}

func NewPublisher(api API, queueURL string) *Publisher {
	return &Publisher{api: api, queueURL: queueURL}
}

func (p *Publisher) Publish(ctx context.Context, m outbox.Message) error {
	if m.ID == "" || m.PartitionKey == "" {
		return ErrMissingRouting
	}
	_, err := p.api.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:               aws.String(p.queueURL),
		MessageBody:            aws.String(string(m.Payload)),
		MessageGroupId:         aws.String(m.PartitionKey),
		MessageDeduplicationId: aws.String(m.ID),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"eventId":   {DataType: aws.String("String"), StringValue: aws.String(m.ID)},
			"eventType": {DataType: aws.String("String"), StringValue: aws.String(m.Type)},
		},
	})
	if err != nil {
		return fmt.Errorf("send event %s: %w", m.ID, err)
	}
	return nil
}
