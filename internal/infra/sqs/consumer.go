package sqs

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/celio001/backend-challenge-go/pkg/backoff"
)

type Action int

const (
	// Delete removes the message: its outcome is durable.
	Delete Action = iota
	// DeadLetter copies the message to the DLQ with the failure code, then removes it.
	DeadLetter
	// Retry leaves the message for redelivery after a backoff.
	Retry
)

type Message struct {
	ID           string
	Body         string
	SenderID     string
	ReceiveCount int
}

type Verdict struct {
	Action Action
	// Code is sent to the DLQ with the message.
	Code string
	// BusinessID and Err are only for logs.
	BusinessID string
	Err        error
}

type Handler interface {
	Handle(ctx context.Context, m Message) Verdict
}

// ConsumerAPI is the part of the SDK client the consumer uses.
type ConsumerAPI interface {
	ReceiveMessage(ctx context.Context, in *awssqs.ReceiveMessageInput, opts ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, in *awssqs.DeleteMessageInput, opts ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, in *awssqs.ChangeMessageVisibilityInput, opts ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error)
	SendMessage(ctx context.Context, in *awssqs.SendMessageInput, opts ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error)
}

type ConsumerConfig struct {
	QueueURL string
	DLQURL   string
	// Workers bounds how many message groups are processed at once.
	Workers int
	// Visibility is the visibility timeout of the queue; a heartbeat renews it while a message is being handled.
	Visibility time.Duration
	WaitTime   time.Duration
	// ShutdownGrace is how long in-flight messages may keep running after the context is canceled before their work is canceled too.
	ShutdownGrace   time.Duration
	RetryBackoffMax time.Duration
	// ReceiveBackoffMax bounds the wait after a failed receive.
	ReceiveBackoffMax time.Duration
}

func (c *ConsumerConfig) setDefaults() {
	if c.Workers <= 0 {
		c.Workers = 10
	}
	if c.Visibility <= 0 {
		c.Visibility = 30 * time.Second
	}
	if c.WaitTime <= 0 {
		c.WaitTime = 20 * time.Second
	}
	if c.ShutdownGrace <= 0 {
		c.ShutdownGrace = 20 * time.Second
	}
	if c.RetryBackoffMax <= 0 {
		c.RetryBackoffMax = 60 * time.Second
	}
	if c.ReceiveBackoffMax <= 0 {
		c.ReceiveBackoffMax = 30 * time.Second
	}
}

// Consumer reads a FIFO queue: messages of one group run in order, different groups run in parallel.
type Consumer struct {
	api     ConsumerAPI
	handler Handler
	cfg     ConsumerConfig
	log     *slog.Logger
	healthy atomic.Bool
}

func NewConsumer(api ConsumerAPI, handler Handler, cfg ConsumerConfig, log *slog.Logger) *Consumer {
	cfg.setDefaults()
	c := &Consumer{api: api, handler: handler, cfg: cfg, log: log}
	c.healthy.Store(true)
	return c
}

// Healthy is false while receiving from the queue keeps failing.
func (c *Consumer) Healthy() bool { return c.healthy.Load() }

// Run returns once ctx is canceled and every message taken from the queue was settled or released.
func (c *Consumer) Run(ctx context.Context) {
	// Handlers run on work, which outlives ctx by the grace period so a message in flight can still commit.
	work, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			select {
			case <-time.After(c.cfg.ShutdownGrace):
				cancelWork()
			case <-done:
			}
		case <-done:
		}
	}()

	failures := 0
	for ctx.Err() == nil {
		msgs, err := c.receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			c.healthy.Store(false)
			c.log.Error("receive messages", "error", err)
			select {
			case <-ctx.Done():
			case <-time.After(backoff.Delay(failures, time.Second, c.cfg.ReceiveBackoffMax, 0.2)):
			}
			failures++
			continue
		}
		failures = 0
		c.healthy.Store(true)
		c.dispatch(ctx, work, msgs)
	}
}

func (c *Consumer) receive(ctx context.Context) ([]types.Message, error) {
	out, err := c.api.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            aws.String(c.cfg.QueueURL),
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     int32(c.cfg.WaitTime.Seconds()),
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameSenderId,
			types.MessageSystemAttributeNameApproximateReceiveCount,
			types.MessageSystemAttributeNameMessageGroupId,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("receive message: %w", err)
	}
	return out.Messages, nil
}

func (c *Consumer) dispatch(ctx, work context.Context, msgs []types.Message) {
	var order []string
	groups := map[string][]types.Message{}
	for _, m := range msgs {
		g := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
		if _, ok := groups[g]; !ok {
			order = append(order, g)
		}
		groups[g] = append(groups[g], m)
	}

	sem := make(chan struct{}, c.cfg.Workers)
	var wg sync.WaitGroup
	for _, g := range order {
		sem <- struct{}{}
		wg.Add(1)
		go func(batch []types.Message) {
			defer func() { <-sem; wg.Done() }()
			c.processGroup(ctx, work, batch)
		}(groups[g])
	}
	wg.Wait()
}

// processGroup stops at the first message that must be retried: later ones would otherwise overtake it.
func (c *Consumer) processGroup(ctx, work context.Context, batch []types.Message) {
	for i, m := range batch {
		if ctx.Err() != nil {
			c.release(batch[i:])
			return
		}
		if c.processOne(work, m) == Retry {
			c.release(batch[i+1:])
			return
		}
	}
}

func (c *Consumer) processOne(work context.Context, m types.Message) Action {
	msg := Message{
		ID:       aws.ToString(m.MessageId),
		Body:     aws.ToString(m.Body),
		SenderID: m.Attributes[string(types.MessageSystemAttributeNameSenderId)],
	}
	msg.ReceiveCount, _ = strconv.Atoi(m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])

	stopBeat := c.heartbeat(m)
	v := c.handler.Handle(work, msg)
	stopBeat()

	// Settling must work even when work was canceled: a committed message that stays on the queue is only redelivered noise.
	settle, cancel := context.WithTimeout(context.WithoutCancel(work), 5*time.Second)
	defer cancel()

	log := c.log.With("sqsMessageId", msg.ID, "messageId", v.BusinessID, "action", v.Action, "code", v.Code)
	switch v.Action {
	case Delete:
		if err := c.delete(settle, m); err != nil {
			log.Error("delete message", "error", err)
		}
	case DeadLetter:
		if err := c.deadLetter(settle, m, v.Code); err != nil {
			log.Error("dead-letter message", "error", err)
			return Retry
		}
		log.Warn("message dead-lettered", "error", v.Err)
	default:
		delay := time.Duration(0)
		if work.Err() == nil {
			delay = backoff.Delay(msg.ReceiveCount, time.Second, c.cfg.RetryBackoffMax, 0)
		}
		if err := c.setVisibility(settle, m, delay); err != nil {
			log.Error("schedule retry", "error", err)
		}
		log.Warn("message will be retried", "delay", delay, "error", v.Err)
	}
	return v.Action
}

func (c *Consumer) heartbeat(m types.Message) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		t := time.NewTicker(c.cfg.Visibility / 3)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := c.setVisibility(ctx, m, c.cfg.Visibility); err != nil && ctx.Err() == nil {
					c.log.Warn("extend visibility", "error", err)
				}
			}
		}
	}()
	return func() { cancel(); <-finished }
}

func (c *Consumer) release(batch []types.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, m := range batch {
		if err := c.setVisibility(ctx, m, 0); err != nil {
			c.log.Warn("release message", "error", err)
		}
	}
}

func (c *Consumer) setVisibility(ctx context.Context, m types.Message, d time.Duration) error {
	_, err := c.api.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(c.cfg.QueueURL),
		ReceiptHandle:     m.ReceiptHandle,
		VisibilityTimeout: int32(d.Seconds()),
	})
	if err != nil {
		return fmt.Errorf("change visibility: %w", err)
	}
	return nil
}

func (c *Consumer) delete(ctx context.Context, m types.Message) error {
	if _, err := c.api.DeleteMessage(ctx, &awssqs.DeleteMessageInput{QueueUrl: aws.String(c.cfg.QueueURL), ReceiptHandle: m.ReceiptHandle}); err != nil {
		return fmt.Errorf("delete message: %w", err)
	}
	return nil
}

// deadLetter keeps the group, so the DLQ preserves per-wallet order, and uses the SQS message id to deduplicate a repeated send.
func (c *Consumer) deadLetter(ctx context.Context, m types.Message, code string) error {
	attr := func(v string) types.MessageAttributeValue {
		return types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(v)}
	}
	if _, err := c.api.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:               aws.String(c.cfg.DLQURL),
		MessageBody:            m.Body,
		MessageGroupId:         aws.String(m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]),
		MessageDeduplicationId: m.MessageId,
		MessageAttributes: map[string]types.MessageAttributeValue{
			"failureCode":       attr(code),
			"originalMessageId": attr(aws.ToString(m.MessageId)),
			"senderId":          attr(m.Attributes[string(types.MessageSystemAttributeNameSenderId)]),
		},
	}); err != nil {
		return fmt.Errorf("send to dlq: %w", err)
	}
	return c.delete(ctx, m)
}
