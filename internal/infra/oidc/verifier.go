package oidc

import (
	"context"
	"errors"
	"fmt"

	gooidc "github.com/coreos/go-oidc/v3/oidc"

	"github.com/celio001/backend-challenge-go/internal/auth"
)

type Config struct {
	// Issuer is the exact `iss` the tokens carry.
	Issuer string
	// DiscoveryURL is where the provider is reached from this process; empty means Issuer.
	// They differ when Keycloak is addressed by its container name but advertises a public hostname.
	DiscoveryURL string
	Audience     string
}

type Verifier struct {
	idToken *gooidc.IDTokenVerifier
}

// NewVerifier fetches the discovery document and keys, so it fails at boot when the IdP is unreachable.
func NewVerifier(ctx context.Context, cfg Config) (*Verifier, error) {
	discovery := cfg.DiscoveryURL
	if discovery == "" {
		discovery = cfg.Issuer
	}
	if discovery != cfg.Issuer {
		ctx = gooidc.InsecureIssuerURLContext(ctx, cfg.Issuer)
	}
	provider, err := gooidc.NewProvider(ctx, discovery)
	if err != nil {
		return nil, fmt.Errorf("oidc discovery: %w", err)
	}
	return &Verifier{idToken: provider.Verifier(&gooidc.Config{
		ClientID:             cfg.Audience,
		SupportedSigningAlgs: []string{gooidc.RS256},
	})}, nil
}

type claims struct {
	ProviderID string   `json:"provider_id"`
	Roles      []string `json:"roles"`
}

func (v *Verifier) Verify(ctx context.Context, rawToken string) (auth.Principal, error) {
	if rawToken == "" {
		return auth.Principal{}, auth.ErrUnauthenticated
	}
	tok, err := v.idToken.Verify(ctx, rawToken)
	if err != nil {
		return auth.Principal{}, fmt.Errorf("%w: %v", auth.ErrUnauthenticated, err)
	}
	var c claims
	if err := tok.Claims(&c); err != nil {
		return auth.Principal{}, errors.Join(auth.ErrUnauthenticated, err)
	}
	return auth.Principal{Subject: tok.Subject, ProviderID: c.ProviderID, Roles: c.Roles}, nil
}
