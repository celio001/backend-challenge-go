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
	}
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

func withDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func configFromEnv() (Config, error) { return LoadConfig(os.Getenv) }
