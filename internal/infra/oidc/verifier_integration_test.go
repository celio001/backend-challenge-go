//go:build integration

package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/celio001/backend-challenge-go/internal/auth"
)

func keycloakURL(t *testing.T) string {
	t.Helper()
	u := os.Getenv("TEST_KEYCLOAK_URL")
	if u == "" {
		t.Skip("TEST_KEYCLOAK_URL not set")
	}
	return strings.TrimRight(u, "/")
}

func clientToken(t *testing.T, base, client string) string {
	t.Helper()
	resp, err := http.PostForm(base+"/realms/wallet/protocol/openid-connect/token", url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {client},
		"client_secret": {client + "-secret"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("token for %s: status %d, err %v", client, resp.StatusCode, err)
	}
	return body.AccessToken
}

func TestVerifier(t *testing.T) {
	base := keycloakURL(t)
	ctx := context.Background()
	issuer := base + "/realms/wallet"
	v, err := NewVerifier(ctx, Config{Issuer: issuer, Audience: "wallet-api"})
	if err != nil {
		t.Fatal(err)
	}

	valid := clientToken(t, base, "provider-a")
	tampered := valid[:len(valid)-4] + "AAAA"
	if tampered == valid {
		tampered = valid[:len(valid)-4] + "BBBB"
	}

	tests := []struct {
		name    string
		token   string
		wantErr error
		want    auth.Principal
	}{
		{name: "provider a", token: valid, want: auth.Principal{ProviderID: "provider-a", Roles: []string{"wager:read", "wager:write"}}},
		{name: "provider b keeps its own identity", token: clientToken(t, base, "provider-b"), want: auth.Principal{ProviderID: "provider-b", Roles: []string{"wager:read", "wager:write"}}},
		{name: "internal service has admin and no provider", token: clientToken(t, base, "wallet-internal"), want: auth.Principal{Roles: []string{"wallet:admin"}}},
		{name: "missing token", token: "", wantErr: auth.ErrUnauthenticated},
		{name: "not a jwt", token: "not-a-token", wantErr: auth.ErrUnauthenticated},
		{name: "tampered signature", token: tampered, wantErr: auth.ErrUnauthenticated},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := v.Verify(ctx, tt.token)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if got.ProviderID != tt.want.ProviderID || got.Subject == "" || len(got.Roles) != len(tt.want.Roles) {
				t.Fatalf("principal = %+v, want %+v", got, tt.want)
			}
			for _, r := range tt.want.Roles {
				if !got.HasRole(r) {
					t.Fatalf("missing role %s in %+v", r, got)
				}
			}
		})
	}

	t.Run("token for another audience is refused", func(t *testing.T) {
		other, err := NewVerifier(ctx, Config{Issuer: issuer, Audience: "another-api"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := other.Verify(ctx, valid); !errors.Is(err, auth.ErrUnauthenticated) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("expired token is refused", func(t *testing.T) {
		short := clientToken(t, base, "provider-a-short")
		if _, err := v.Verify(ctx, short); err != nil {
			t.Fatalf("fresh short token: %v", err)
		}
		time.Sleep(6 * time.Second)
		if _, err := v.Verify(ctx, short); !errors.Is(err, auth.ErrUnauthenticated) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("discovery through another address keeps the expected issuer", func(t *testing.T) {
		alt := strings.Replace(issuer, "localhost", "127.0.0.1", 1)
		viaAlt, err := NewVerifier(ctx, Config{Issuer: issuer, DiscoveryURL: alt, Audience: "wallet-api"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := viaAlt.Verify(ctx, valid); err != nil {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("boot fails when the issuer does not match or the idp is down", func(t *testing.T) {
		if _, err := NewVerifier(ctx, Config{Issuer: "http://localhost:8080/realms/other", Audience: "wallet-api"}); err == nil {
			t.Fatal("unknown realm accepted")
		}
		if _, err := NewVerifier(ctx, Config{Issuer: "http://127.0.0.1:1/realms/wallet", Audience: "wallet-api"}); err == nil {
			t.Fatal("unreachable idp accepted")
		}
	})
}
