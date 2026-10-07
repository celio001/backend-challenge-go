package app

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.uber.org/fx"

	"github.com/celio001/backend-challenge-go/internal/adapter/sqsmsg"
	"github.com/celio001/backend-challenge-go/internal/infra/observability"
	infrasqs "github.com/celio001/backend-challenge-go/internal/infra/sqs"
)

var ConsumerModule = fx.Module("consumer", fx.Provide(newWagerConsumer), fx.Invoke(func(*wagerConsumer) {}))

// wagerConsumer owns the inbound queue: it resolves the queues on start, runs the consumer, and drains it on stop.
type wagerConsumer struct {
	api      infrasqs.API
	queueURL atomic.Pointer[string]
	consumer atomic.Pointer[infrasqs.Consumer]
}

// newWagerConsumer registers after the pool and the use case, so Fx stops it first: no message is half handled when they close.
func newWagerConsumer(lc fx.Lifecycle, cfg Config, client *awssqs.Client, process sqsmsg.ProcessWager, m *observability.Metrics, log *slog.Logger) *wagerConsumer {
	w := &wagerConsumer{api: client}
	if len(cfg.SenderProviders) == 0 {
		log.Warn("SQS_SENDER_PROVIDER_MAP is empty: every inbound message will be dead-lettered")
	}
	handler := verdictHandler{sqsmsg.NewHandler(process, cfg.SenderProviders)}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	lc.Append(fx.Hook{
		OnStart: func(startCtx context.Context) error {
			var queueURL, dlqURL string
			err := retryUntil(startCtx, log, "sqs-inbound", func(ctx context.Context) error {
				var err error
				if queueURL, err = resolveQueue(ctx, client, cfg.WagerQueueURL, cfg.WagerQueueName); err != nil {
					return err
				}
				if dlqURL, err = resolveQueue(ctx, client, cfg.WagerDLQURL, cfg.WagerDLQName); err != nil {
					return err
				}
				return infrasqs.Ping(ctx, client, queueURL)
			})
			if err != nil {
				return err
			}
			c := infrasqs.NewConsumer(client, handler, infrasqs.ConsumerConfig{QueueURL: queueURL, DLQURL: dlqURL, Observer: m}, log)
			w.queueURL.Store(&queueURL)
			w.consumer.Store(c)
			go func() {
				defer close(done)
				c.Run(ctx)
			}()
			log.Info("wager consumer started", "queue", queueURL)
			return nil
		},
		OnStop: func(stopCtx context.Context) error {
			cancel()
			if w.consumer.Load() == nil {
				return nil
			}
			select {
			case <-done:
				log.Info("wager consumer stopped")
				return nil
			case <-stopCtx.Done():
				return fmt.Errorf("wager consumer did not stop in time: %w", stopCtx.Err())
			}
		},
	})
	return w
}

func resolveQueue(ctx context.Context, api infrasqs.API, url, name string) (string, error) {
	if url != "" {
		return url, nil
	}
	return infrasqs.QueueURL(ctx, api, name)
}

// Ping is the readiness check: the queue answers and the receive loop is not failing.
func (w *wagerConsumer) Ping(ctx context.Context) error {
	url, c := w.queueURL.Load(), w.consumer.Load()
	if url == nil || c == nil {
		return fmt.Errorf("wager consumer is not started yet")
	}
	if !c.Healthy() {
		return fmt.Errorf("wager consumer cannot receive from the queue")
	}
	return infrasqs.Ping(ctx, w.api, *url)
}

// verdictHandler turns the adapter's result into what the queue consumer applies.
type verdictHandler struct{ h *sqsmsg.Handler }

func (v verdictHandler) Handle(ctx context.Context, m infrasqs.Message) infrasqs.Verdict {
	r := v.h.Handle(ctx, sqsmsg.Delivery{Body: m.Body, SenderID: m.SenderID})
	verdict := infrasqs.Verdict{Code: r.Code, BusinessID: r.MessageID, Replay: r.Replay, Err: r.Err}
	switch r.Action {
	case sqsmsg.Delete:
		verdict.Action = infrasqs.Delete
	case sqsmsg.DeadLetter:
		verdict.Action = infrasqs.DeadLetter
	default:
		verdict.Action = infrasqs.Retry
	}
	return verdict
}
