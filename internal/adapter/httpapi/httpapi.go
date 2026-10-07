package httpapi

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/celio001/backend-challenge-go/internal/auth"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/internal/usecase"
	"github.com/celio001/backend-challenge-go/internal/usecase/openwallet"
	"github.com/celio001/backend-challenge-go/internal/usecase/queries"
)

type OpenWallet interface {
	Execute(ctx context.Context, in openwallet.Input) (*wallet.Wallet, error)
}

type WalletQueries interface {
	Wallet(ctx context.Context, id string) (*wallet.Wallet, error)
	Ledger(ctx context.Context, id, cursor string, limit int) (queries.LedgerPage, error)
}

type Deps struct {
	OpenWallet OpenWallet
	Queries    WalletQueries
	Verifier   auth.Verifier
	IDs        usecase.IDGenerator
	Health     *Health
	Log        *slog.Logger
}

func New(d Deps) http.Handler {
	h := walletHandlers{open: d.OpenWallet, queries: d.Queries, log: d.Log}
	admin := func(next http.HandlerFunc) http.Handler {
		return authenticate(d.Verifier, d.Log, requireRole(auth.RoleWalletAdmin, next))
	}

	mux := http.NewServeMux()
	mux.Handle("POST /wallets", admin(h.post))
	mux.Handle("GET /wallets/{id}", admin(h.get))
	mux.Handle("GET /wallets/{id}/ledger", admin(h.ledger))
	mux.HandleFunc("GET /health/live", d.Health.live)
	mux.HandleFunc("GET /health/ready", d.Health.ready)
	return withCorrelation(d.IDs, mux)
}
