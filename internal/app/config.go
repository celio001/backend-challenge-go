package app

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

var ErrInvalidConfig = errors.New("app: invalid configuration")

type Config struct {
	HTTPAddr         string
	DatabaseURL      string
	OIDCIssuer       string
	OIDCDiscoveryURL string
	OIDCAudience     string
	ReferenceTTL     time.Duration
	AWSRegion        string
	// SQSEndpoint is only set for LocalStack; real AWS leaves it empty.
	SQSEndpoint     string
	EventsQueueName string
	// EventsQueueURL skips the name lookup; useful where the queue URL the broker reports is not reachable from the replica.
	EventsQueueURL string
	// The queue URLs have the same escape hatch as EventsQueueURL.
	WagerQueueName string
	WagerQueueURL  string
	WagerDLQName   string
	WagerDLQURL    string
	// SenderProviders maps the SQS SenderId (the broker's view of who sent) to the provider it may act as.
	SenderProviders map[string]string
}

func LoadConfig(getenv func(string) string) (Config, error) {
	cfg := Config{
		HTTPAddr:         withDefault(getenv("HTTP_ADDR"), ":8081"),
		DatabaseURL:      getenv("DATABASE_URL"),
		OIDCIssuer:       getenv("OIDC_ISSUER"),
		OIDCDiscoveryURL: getenv("OIDC_DISCOVERY_URL"),
		OIDCAudience:     withDefault(getenv("OIDC_AUDIENCE"), "wallet-api"),
		AWSRegion:        withDefault(getenv("AWS_REGION"), "us-east-1"),
		SQSEndpoint:      getenv("SQS_ENDPOINT"),
		EventsQueueName:  withDefault(getenv("EVENTS_QUEUE_NAME"), "wallet-events.fifo"),
		EventsQueueURL:   getenv("EVENTS_QUEUE_URL"),
		WagerQueueName:   withDefault(getenv("WAGER_QUEUE_NAME"), "wager-transactions.fifo"),
		WagerQueueURL:    getenv("WAGER_QUEUE_URL"),
		WagerDLQName:     withDefault(getenv("WAGER_DLQ_NAME"), "wager-transactions-dlq.fifo"),
		WagerDLQURL:      getenv("WAGER_DLQ_URL"),
	}
	senders, err := parseSenderProviders(getenv("SQS_SENDER_PROVIDER_MAP"))
	if err != nil {
		return Config{}, err
	}
	cfg.SenderProviders = senders
	ttl, err := time.ParseDuration(withDefault(getenv("REFERENCE_TTL"), "10m"))
	if err != nil || ttl <= 0 {
		return Config{}, fmt.Errorf("%w: REFERENCE_TTL must be a positive duration such as 10m", ErrInvalidConfig)
	}
	cfg.ReferenceTTL = ttl

	var missing []string
	if cfg.DatabaseURL == "" {
		missing = append(missing, "DATABASE_URL")
	}
	if cfg.OIDCIssuer == "" {
		missing = append(missing, "OIDC_ISSUER")
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("%w: missing %s", ErrInvalidConfig, strings.Join(missing, ", "))
	}
	return cfg, nil
}

// parseSenderProviders reads "senderId=providerId,senderId=providerId". An empty value maps nobody, so every message is dead-lettered.
func parseSenderProviders(raw string) (map[string]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	out := map[string]string{}
	for _, pair := range strings.Split(raw, ",") {
		sender, provider, ok := strings.Cut(strings.TrimSpace(pair), "=")
		sender, provider = strings.TrimSpace(sender), strings.TrimSpace(provider)
		if !ok || sender == "" || provider == "" {
			return nil, fmt.Errorf("%w: SQS_SENDER_PROVIDER_MAP must look like senderId=providerId[,senderId=providerId]", ErrInvalidConfig)
		}
		out[sender] = provider
	}
	return out, nil
}

func withDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func configFromEnv() (Config, error) { return LoadConfig(os.Getenv) }
