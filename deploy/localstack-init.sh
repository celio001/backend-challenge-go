#!/bin/bash
# Runs inside LocalStack once it is ready. create-queue is idempotent, so restarts are safe.
set -euo pipefail

attrs=$(mktemp)
trap 'rm -f "$attrs"' EXIT

awslocal sqs create-queue --queue-name wager-transactions-dlq.fifo \
  --attributes FifoQueue=true,MessageRetentionPeriod=1209600 >/dev/null

dlq_url=$(awslocal sqs get-queue-url --queue-name wager-transactions-dlq.fifo --query QueueUrl --output text)
dlq_arn=$(awslocal sqs get-queue-attributes --queue-url "$dlq_url" --attribute-names QueueArn --query Attributes.QueueArn --output text)

cat >"$attrs" <<JSON
{
  "FifoQueue": "true",
  "VisibilityTimeout": "30",
  "ReceiveMessageWaitTimeSeconds": "20",
  "RedrivePolicy": "{\"deadLetterTargetArn\":\"$dlq_arn\",\"maxReceiveCount\":\"5\"}"
}
JSON
awslocal sqs create-queue --queue-name wager-transactions.fifo --attributes "file://$attrs" >/dev/null

awslocal sqs create-queue --queue-name wallet-events.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=false >/dev/null

echo "SQS queues ready:"
awslocal sqs list-queues
