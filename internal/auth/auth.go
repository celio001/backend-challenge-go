package auth

import (
	"context"
	"errors"
	"slices"
)

const (
	RoleWagerWrite  = "wager:write"
	RoleWagerRead   = "wager:read"
	RoleWalletAdmin = "wallet:admin"
)

var (
	ErrUnauthenticated = errors.New("auth: missing or invalid token")
	ErrForbidden       = errors.New("auth: not allowed")
)

type Principal struct {
	Subject    string
	ProviderID string
	Roles      []string
}

func (p Principal) HasRole(role string) bool {
	return slices.Contains(p.Roles, role)
}

type Verifier interface {
	Verify(ctx context.Context, rawToken string) (Principal, error)
}

type ctxKey struct{}

func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, ctxKey{}, p)
}

func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(ctxKey{}).(Principal)
	return p, ok
}
