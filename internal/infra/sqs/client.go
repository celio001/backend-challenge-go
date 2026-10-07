// Package sqs adapts Amazon SQS (or LocalStack) to what the application needs: sending outbox events and checking the queues.
package sqs

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type Config struct {
	Region string
	// Endpoint overrides the AWS endpoint; it is set for LocalStack and empty for real AWS.
	Endpoint string
}

// API is the part of the SDK client this package uses, so tests can replace it.
type API interface {
	SendMessage(ctx context.Context, in *awssqs.SendMessageInput, opts ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error)
	GetQueueUrl(ctx context.Context, in *awssqs.GetQueueUrlInput, opts ...func(*awssqs.Options)) (*awssqs.GetQueueUrlOutput, error)
	GetQueueAttributes(ctx context.Context, in *awssqs.GetQueueAttributesInput, opts ...func(*awssqs.Options)) (*awssqs.GetQueueAttributesOutput, error)
}

// NewClient builds an SDK client. Credentials come from the standard chain (environment, profile, role).
func NewClient(ctx context.Context, cfg Config) (*awssqs.Client, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	return awssqs.NewFromConfig(awsCfg, func(o *awssqs.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	}), nil
}

// QueueURL resolves a queue name, which also proves the queue exists.
func QueueURL(ctx context.Context, api API, name string) (string, error) {
	out, err := api.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{QueueName: aws.String(name)})
	if err != nil {
		return "", fmt.Errorf("get queue url %q: %w", name, err)
	}
	return aws.ToString(out.QueueUrl), nil
}

// Ping checks that the queue is reachable, for readiness probes.
func Ping(ctx context.Context, api API, queueURL string) error {
	if _, err := api.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	}); err != nil {
		return fmt.Errorf("get queue attributes: %w", err)
	}
	return nil
}
