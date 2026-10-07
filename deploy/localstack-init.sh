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

# Access policies: providers may only send to the inbound queue and the service alone may read, delete and dead-letter.
# LocalStack's free tier stores these policies without enforcing them, so locally the consumer's SenderId check is what protects the
# queue; on AWS the same documents are enforced by IAM. The principals are the identities each party would use there.
account=000000000000
set_policy() { # $1 queue name, $2 JSON statements
  url=$(awslocal sqs get-queue-url --queue-name "$1" --query QueueUrl --output text)
  arn=$(awslocal sqs get-queue-attributes --queue-url "$url" --attribute-names QueueArn --query Attributes.QueueArn --output text)
  policy=$(printf '{"Version":"2012-10-17","Statement":[%s]}' "${2//__ARN__/$arn}")
  python3 -c 'import json,sys; print(json.dumps({"Policy": sys.argv[1]}))' "$policy" >"$attrs"
  awslocal sqs set-queue-attributes --queue-url "$url" --attributes "file://$attrs" >/dev/null
}
principal() { printf '"arn:aws:iam::%s:user/%s"' "$account" "$1"; }
consume='"sqs:ReceiveMessage","sqs:DeleteMessage","sqs:ChangeMessageVisibility","sqs:GetQueueAttributes","sqs:GetQueueUrl"'

set_policy wager-transactions.fifo "$(printf '{"Sid":"ProvidersSend","Effect":"Allow","Principal":{"AWS":[%s,%s]},"Action":"sqs:SendMessage","Resource":"__ARN__"},{"Sid":"ServiceConsumes","Effect":"Allow","Principal":{"AWS":%s},"Action":[%s],"Resource":"__ARN__"}' \
  "$(principal provider-a)" "$(principal provider-b)" "$(principal wallet-service)" "$consume")"
set_policy wager-transactions-dlq.fifo "$(printf '{"Sid":"ServiceOnly","Effect":"Allow","Principal":{"AWS":%s},"Action":["sqs:SendMessage",%s],"Resource":"__ARN__"}' "$(principal wallet-service)" "$consume")"
set_policy wallet-events.fifo "$(printf '{"Sid":"ServiceSends","Effect":"Allow","Principal":{"AWS":%s},"Action":"sqs:SendMessage","Resource":"__ARN__"},{"Sid":"DownstreamReads","Effect":"Allow","Principal":{"AWS":%s},"Action":[%s],"Resource":"__ARN__"}' \
  "$(principal wallet-service)" "$(principal event-consumer)" "$consume")"

echo "SQS queues ready:"
awslocal sqs list-queues
