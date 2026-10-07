package app

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

var ErrInvalidConfig = errors.New("app: invalid configuration")

type Config struct {
	HTTPAddr         string
	DatabaseURL      string
	OIDCIssuer       string
	OIDCDiscoveryURL string
	OIDCAudience     string
}

func LoadConfig(getenv func(string) string) (Config, error) {
	cfg := Config{
		HTTPAddr:         withDefault(getenv("HTTP_ADDR"), ":8081"),
		DatabaseURL:      getenv("DATABASE_URL"),
		OIDCIssuer:       getenv("OIDC_ISSUER"),
		OIDCDiscoveryURL: getenv("OIDC_DISCOVERY_URL"),
		OIDCAudience:     withDefault(getenv("OIDC_AUDIENCE"), "wallet-api"),
	}
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
