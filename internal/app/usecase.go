package app

import (
	"context"
	"log/slog"

	"go.uber.org/fx"

	"github.com/celio001/backend-challenge-go/internal/adapter/httpapi"
	"github.com/celio001/backend-challenge-go/internal/adapter/sqsmsg"
	"github.com/celio001/backend-challenge-go/internal/infra/observability"
	"github.com/celio001/backend-challenge-go/internal/usecase/openwallet"
	"github.com/celio001/backend-challenge-go/internal/usecase/processwager"
	"github.com/celio001/backend-challenge-go/internal/usecase/queries"
	"github.com/celio001/backend-challenge-go/internal/usecase/reconcile"
	"github.com/celio001/backend-challenge-go/internal/usecase/resolvereference"
)

var UseCaseModule = fx.Module("usecase",
	fx.Provide(
		fx.Annotate(openwallet.New, fx.As(new(httpapi.OpenWallet))),
		queries.New,
		func(q *queries.Queries) httpapi.WalletQueries { return q },
		func(q *queries.Queries) httpapi.TransactionQueries { return q },
		func(cfg Config) processwager.Options { return processwager.Options{ReferenceTTL: cfg.ReferenceTTL} },
		processwager.New,
		resolvereference.New,
		reconcile.New,
		fx.Annotate(newDivergenceObserver, fx.As(new(reconcile.Observer))),
		func(uc *reconcile.UseCase) httpapi.Reconciler { return uc },
		func(uc *processwager.UseCase, m *observability.Metrics, log *slog.Logger) httpapi.ProcessWager {
			return m.Instrument("http", uc, log)
		},
		func(uc *processwager.UseCase, m *observability.Metrics, log *slog.Logger) sqsmsg.ProcessWager {
			return m.Instrument("sqs", uc, log)
		},
	),
)

// divergenceObserver reports a divergence both ways: the log line explains it, the counter alerts on it.
type divergenceObserver struct{ observers []reconcile.Observer }

func newDivergenceObserver(log *slog.Logger, m *observability.Metrics) *divergenceObserver {
	return &divergenceObserver{observers: []reconcile.Observer{newDivergenceLogger(log), m}}
}

func (d *divergenceObserver) Divergence(ctx context.Context, r reconcile.Report) {
	for _, o := range d.observers {
		o.Divergence(ctx, r)
	}
}

// divergenceLogger is the log half of the divergence report; the amounts are logged because that is the whole point of the alert.
type divergenceLogger struct{ log *slog.Logger }

func newDivergenceLogger(log *slog.Logger) *divergenceLogger { return &divergenceLogger{log: log} }

func (d *divergenceLogger) Divergence(_ context.Context, r reconcile.Report) {
	d.log.Error("wallet balance diverges from its ledger",
		"walletId", string(r.WalletID), "stored", r.Stored.String(), "calculated", r.Calculated.String(),
		"difference", r.Difference.String(), "currency", string(r.Stored.Currency()), "checkedEntries", r.CheckedEntries)
}
