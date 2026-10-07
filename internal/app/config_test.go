package app

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		want    Config
		wantErr error
	}{
		{
			name: "defaults",
			env:  map[string]string{"DATABASE_URL": "postgres://db", "OIDC_ISSUER": "http://idp/realms/wallet"},
			want: Config{HTTPAddr: ":8081", DatabaseURL: "postgres://db", OIDCIssuer: "http://idp/realms/wallet", OIDCAudience: "wallet-api", ReferenceTTL: 10 * time.Minute, AWSRegion: "us-east-1", EventsQueueName: "wallet-events.fifo",
				WagerQueueName: "wager-transactions.fifo", WagerDLQName: "wager-transactions-dlq.fifo", OutboxLease: 30 * time.Second, ReferenceLease: 30 * time.Second},
		},
		{
			name: "everything set",
			env: map[string]string{
				"HTTP_ADDR": ":9000", "DATABASE_URL": "postgres://db", "OIDC_ISSUER": "http://localhost:8080/realms/wallet",
				"OIDC_DISCOVERY_URL": "http://keycloak:8080/realms/wallet", "OIDC_AUDIENCE": "other", "REFERENCE_TTL": "90s",
				"AWS_REGION": "sa-east-1", "SQS_ENDPOINT": "http://localstack:4566", "EVENTS_QUEUE_NAME": "events.fifo", "EVENTS_QUEUE_URL": "http://localstack:4566/000000000000/events.fifo",
				"WAGER_QUEUE_NAME": "wagers.fifo", "WAGER_QUEUE_URL": "http://localstack:4566/000000000000/wagers.fifo", "WAGER_DLQ_NAME": "wagers-dlq.fifo", "WAGER_DLQ_URL": "http://localstack:4566/000000000000/wagers-dlq.fifo",
				"SQS_SENDER_PROVIDER_MAP": " AIDAA = provider-a ,AIDAB=provider-b", "OUTBOX_LEASE": "3s", "REFERENCE_LEASE": "5s",
			},
			want: Config{HTTPAddr: ":9000", DatabaseURL: "postgres://db", OIDCIssuer: "http://localhost:8080/realms/wallet", OIDCDiscoveryURL: "http://keycloak:8080/realms/wallet", OIDCAudience: "other", ReferenceTTL: 90 * time.Second,
				AWSRegion: "sa-east-1", SQSEndpoint: "http://localstack:4566", EventsQueueName: "events.fifo", EventsQueueURL: "http://localstack:4566/000000000000/events.fifo",
				WagerQueueName: "wagers.fifo", WagerQueueURL: "http://localstack:4566/000000000000/wagers.fifo", WagerDLQName: "wagers-dlq.fifo", WagerDLQURL: "http://localstack:4566/000000000000/wagers-dlq.fifo",
				SenderProviders: map[string]string{"AIDAA": "provider-a", "AIDAB": "provider-b"}, OutboxLease: 3 * time.Second, ReferenceLease: 5 * time.Second},
		},
		{name: "missing database", env: map[string]string{"OIDC_ISSUER": "http://idp"}, wantErr: ErrInvalidConfig},
		{name: "missing issuer", env: map[string]string{"DATABASE_URL": "postgres://db"}, wantErr: ErrInvalidConfig},
		{name: "reference ttl is not a duration", env: map[string]string{"DATABASE_URL": "postgres://db", "OIDC_ISSUER": "http://idp", "REFERENCE_TTL": "ten"}, wantErr: ErrInvalidConfig},
		{name: "reference ttl is not positive", env: map[string]string{"DATABASE_URL": "postgres://db", "OIDC_ISSUER": "http://idp", "REFERENCE_TTL": "0s"}, wantErr: ErrInvalidConfig},
		{name: "sender map without a provider", env: map[string]string{"DATABASE_URL": "postgres://db", "OIDC_ISSUER": "http://idp", "SQS_SENDER_PROVIDER_MAP": "AIDAA="}, wantErr: ErrInvalidConfig},
		{name: "sender map without a separator", env: map[string]string{"DATABASE_URL": "postgres://db", "OIDC_ISSUER": "http://idp", "SQS_SENDER_PROVIDER_MAP": "AIDAA"}, wantErr: ErrInvalidConfig},
		{name: "outbox lease is not a duration", env: map[string]string{"DATABASE_URL": "postgres://db", "OIDC_ISSUER": "http://idp", "OUTBOX_LEASE": "soon"}, wantErr: ErrInvalidConfig},
		{name: "reference lease is not positive", env: map[string]string{"DATABASE_URL": "postgres://db", "OIDC_ISSUER": "http://idp", "REFERENCE_LEASE": "0s"}, wantErr: ErrInvalidConfig},
		{name: "nothing set", env: map[string]string{}, wantErr: ErrInvalidConfig},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := LoadConfig(func(k string) string { return tt.env[k] })
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("config = %+v, want %+v", got, tt.want)
			}
		})
	}
}
