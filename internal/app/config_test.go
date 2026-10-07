package app

import (
	"errors"
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
			want: Config{HTTPAddr: ":8081", DatabaseURL: "postgres://db", OIDCIssuer: "http://idp/realms/wallet", OIDCAudience: "wallet-api", ReferenceTTL: 10 * time.Minute},
		},
		{
			name: "everything set",
			env: map[string]string{
				"HTTP_ADDR": ":9000", "DATABASE_URL": "postgres://db", "OIDC_ISSUER": "http://localhost:8080/realms/wallet",
				"OIDC_DISCOVERY_URL": "http://keycloak:8080/realms/wallet", "OIDC_AUDIENCE": "other", "REFERENCE_TTL": "90s",
			},
			want: Config{HTTPAddr: ":9000", DatabaseURL: "postgres://db", OIDCIssuer: "http://localhost:8080/realms/wallet", OIDCDiscoveryURL: "http://keycloak:8080/realms/wallet", OIDCAudience: "other", ReferenceTTL: 90 * time.Second},
		},
		{name: "missing database", env: map[string]string{"OIDC_ISSUER": "http://idp"}, wantErr: ErrInvalidConfig},
		{name: "missing issuer", env: map[string]string{"DATABASE_URL": "postgres://db"}, wantErr: ErrInvalidConfig},
		{name: "reference ttl is not a duration", env: map[string]string{"DATABASE_URL": "postgres://db", "OIDC_ISSUER": "http://idp", "REFERENCE_TTL": "ten"}, wantErr: ErrInvalidConfig},
		{name: "reference ttl is not positive", env: map[string]string{"DATABASE_URL": "postgres://db", "OIDC_ISSUER": "http://idp", "REFERENCE_TTL": "0s"}, wantErr: ErrInvalidConfig},
		{name: "nothing set", env: map[string]string{}, wantErr: ErrInvalidConfig},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := LoadConfig(func(k string) string { return tt.env[k] })
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("config = %+v, want %+v", got, tt.want)
			}
		})
	}
}
