package app

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	"go.uber.org/fx"

	"github.com/celio001/backend-challenge-go/internal/auth"
	"github.com/celio001/backend-challenge-go/internal/infra/oidc"
)

var OIDCModule = fx.Module("oidc", fx.Provide(newVerifier))

// startedVerifier lets the process boot before the IdP is reachable: discovery runs in OnStart, which blocks the HTTP server from listening until it succeeds.
type startedVerifier struct {
	v atomic.Pointer[oidc.Verifier]
}

func (s *startedVerifier) Verify(ctx context.Context, token string) (auth.Principal, error) {
	v := s.v.Load()
	if v == nil {
		return auth.Principal{}, auth.ErrUnauthenticated
	}
	return v.Verify(ctx, token)
}

func newVerifier(lc fx.Lifecycle, cfg Config, log *slog.Logger) auth.Verifier {
	sv := &startedVerifier{}
	lc.Append(fx.Hook{OnStart: func(ctx context.Context) error {
		return retryUntil(ctx, log, "oidc", func(ctx context.Context) error {
			v, err := oidc.NewVerifier(ctx, oidc.Config{Issuer: cfg.OIDCIssuer, DiscoveryURL: cfg.OIDCDiscoveryURL, Audience: cfg.OIDCAudience})
			if err != nil {
				return fmt.Errorf("oidc verifier: %w", err)
			}
			sv.v.Store(v)
			return nil
		})
	}})
	return sv
}
