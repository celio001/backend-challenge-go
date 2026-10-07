package app

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.uber.org/fx"

	"github.com/celio001/backend-challenge-go/internal/infra/outbox"
	infrasqs "github.com/celio001/backend-challenge-go/internal/infra/sqs"
)

var SQSModule = fx.Module("sqs",
	fx.Provide(
		newSQSClient,
		newEventsQueue,
		func(q *eventsQueue) outbox.Publisher { return q },
	),
)

func newSQSClient(cfg Config) (*awssqs.Client, error) {
	return infrasqs.NewClient(context.Background(), infrasqs.Config{Region: cfg.AWSRegion, Endpoint: cfg.SQSEndpoint})
}

// eventsQueue is the destination of the outbox. Its URL is resolved on start, which blocks the workers and the HTTP server
// until the broker answers; before that it refuses to publish.
type eventsQueue struct {
	api infrasqs.API
	url atomic.Pointer[string]
}

func newEventsQueue(lc fx.Lifecycle, cfg Config, client *awssqs.Client, log *slog.Logger) *eventsQueue {
	q := &eventsQueue{api: client}
	lc.Append(fx.Hook{OnStart: func(ctx context.Context) error {
		return retryUntil(ctx, log, "sqs", func(ctx context.Context) error {
			url := cfg.EventsQueueURL
			if url == "" {
				var err error
				if url, err = infrasqs.QueueURL(ctx, client, cfg.EventsQueueName); err != nil {
					return err
				}
			}
			if err := infrasqs.Ping(ctx, client, url); err != nil {
				return err
			}
			q.url.Store(&url)
			return nil
		})
	}})
	return q
}

func (q *eventsQueue) Publish(ctx context.Context, m outbox.Message) error {
	url := q.url.Load()
	if url == nil {
		return fmt.Errorf("events queue is not resolved yet")
	}
	return infrasqs.NewPublisher(q.api, *url).Publish(ctx, m)
}

func (q *eventsQueue) Ping(ctx context.Context) error {
	url := q.url.Load()
	if url == nil {
		return fmt.Errorf("events queue is not resolved yet")
	}
	return infrasqs.Ping(ctx, q.api, *url)
}
