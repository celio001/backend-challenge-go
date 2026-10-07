// Package sqstest creates throwaway FIFO queues on LocalStack for integration tests.
package sqstest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	infrasqs "github.com/celio001/backend-challenge-go/internal/infra/sqs"
)

type Received struct {
	Body      string
	GroupID   string
	EventID   string
	EventType string
}

type Queue struct {
	Client *awssqs.Client
	URL    string
	Name   string
}

// New creates a FIFO queue and deletes it when the test ends. It skips the test when TEST_SQS_ENDPOINT is unset.
func New(t *testing.T) Queue {
	t.Helper()
	endpoint := os.Getenv("TEST_SQS_ENDPOINT")
	if endpoint == "" {
		t.Skip("TEST_SQS_ENDPOINT not set")
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	ctx := context.Background()
	client, err := infrasqs.NewClient(ctx, infrasqs.Config{Region: "us-east-1", Endpoint: endpoint})
	if err != nil {
		t.Fatal(err)
	}
	suffix := make([]byte, 4)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatal(err)
	}
	name := "test-" + hex.EncodeToString(suffix) + ".fifo"
	out, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName:  aws.String(name),
		Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "false", "VisibilityTimeout": "30"},
	})
	if err != nil {
		t.Fatal(err)
	}
	url := aws.ToString(out.QueueUrl)
	t.Cleanup(func() { _, _ = client.DeleteQueue(ctx, &awssqs.DeleteQueueInput{QueueUrl: aws.String(url)}) })
	return Queue{Client: client, URL: url, Name: name}
}

// Drain receives and deletes messages until the queue stays empty for a moment, returning them in arrival order.
// Deleting matters on FIFO queues: the next message of a group is only delivered after the previous one is gone.
func (q Queue) Drain(t *testing.T) []Received {
	t.Helper()
	ctx := context.Background()
	var all []Received
	empty := 0
	for empty < 2 {
		out, err := q.Client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
			QueueUrl:                    aws.String(q.URL),
			MaxNumberOfMessages:         10,
			WaitTimeSeconds:             1,
			MessageAttributeNames:       []string{"All"},
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameMessageGroupId},
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(out.Messages) == 0 {
			empty++
			continue
		}
		empty = 0
		for _, m := range out.Messages {
			all = append(all, Received{
				Body:      aws.ToString(m.Body),
				GroupID:   m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)],
				EventID:   aws.ToString(m.MessageAttributes["eventId"].StringValue),
				EventType: aws.ToString(m.MessageAttributes["eventType"].StringValue),
			})
			if _, err := q.Client.DeleteMessage(ctx, &awssqs.DeleteMessageInput{QueueUrl: aws.String(q.URL), ReceiptHandle: m.ReceiptHandle}); err != nil {
				t.Fatal(err)
			}
		}
	}
	return all
}

// WaitFor drains until at least n messages arrived or the timeout passes.
func (q Queue) WaitFor(t *testing.T, n int, timeout time.Duration) []Received {
	t.Helper()
	var all []Received
	deadline := time.Now().Add(timeout)
	for len(all) < n && time.Now().Before(deadline) {
		all = append(all, q.Drain(t)...)
	}
	return all
}
