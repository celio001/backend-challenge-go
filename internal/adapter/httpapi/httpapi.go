package httpapi

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/celio001/backend-challenge-go/internal/auth"
	"github.com/celio001/backend-challenge-go/internal/domain/wager"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/internal/usecase"
	"github.com/celio001/backend-challenge-go/internal/usecase/openwallet"
	"github.com/celio001/backend-challenge-go/internal/usecase/processwager"
	"github.com/celio001/backend-challenge-go/internal/usecase/queries"
	"github.com/celio001/backend-challenge-go/internal/usecase/reconcile"
)

type OpenWallet interface {
	Execute(ctx context.Context, in openwallet.Input) (*wallet.Wallet, error)
}

type WalletQueries interface {
	Wallet(ctx context.Context, id string) (*wallet.Wallet, error)
	Ledger(ctx context.Context, id, cursor string, limit int) (queries.LedgerPage, error)
}

type ProcessWager interface {
	Execute(ctx context.Context, in processwager.Input) (processwager.Output, error)
}

type TransactionQueries interface {
	Transaction(ctx context.Context, id string) (*wager.Transaction, error)
	ProviderTransaction(ctx context.Context, providerID, externalID string) (*wager.Transaction, error)
}

type Reconciler interface {
	Execute(ctx context.Context, id string) (reconcile.Report, error)
}

type Deps struct {
	OpenWallet   OpenWallet
	ProcessWager ProcessWager
	Queries      WalletQueries
	TxQueries    TransactionQueries
	Reconcile    Reconciler
	Verifier     auth.Verifier
	IDs          usecase.IDGenerator
	Health       *Health
	// Metrics serves GET /metrics when set.
	Metrics http.Handler
	Log     *slog.Logger
}

func New(d Deps) http.Handler {
	h := walletHandlers{open: d.OpenWallet, queries: d.Queries, reconcile: d.Reconcile, log: d.Log}
	wg := wageringHandlers{process: d.ProcessWager, queries: d.TxQueries, log: d.Log}
	protected := func(role string, next http.HandlerFunc) http.Handler {
		return authenticate(d.Verifier, d.Log, requireRole(role, next))
	}
	admin := func(next http.HandlerFunc) http.Handler { return protected(auth.RoleWalletAdmin, next) }

	mux := http.NewServeMux()
	mux.Handle("POST /wallets", admin(h.post))
	mux.Handle("GET /wallets/{id}", admin(h.get))
	mux.Handle("GET /wallets/{id}/ledger", admin(h.ledger))
	mux.Handle("POST /wallets/{id}/reconciliation", admin(h.reconciliation))
	mux.Handle("POST /wagering/transactions", protected(auth.RoleWagerWrite, wg.post))
	// An admin reads any transaction by id; a provider only its own, and the handler hides the rest as 404.
	mux.Handle("GET /wagering/transactions/{id}", authenticate(d.Verifier, d.Log, requireAnyRole([]string{auth.RoleWagerRead, auth.RoleWalletAdmin}, http.HandlerFunc(wg.get))))
	mux.Handle("GET /providers/{provider}/wagering/transactions/{external}", protected(auth.RoleWagerRead, wg.getByProvider))
	mux.HandleFunc("GET /health/live", d.Health.live)
	mux.HandleFunc("GET /health/ready", d.Health.ready)
	if d.Metrics != nil {
		mux.Handle("GET /metrics", d.Metrics)
	}
	return withCorrelation(d.IDs, mux)
}
