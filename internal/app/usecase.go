package app

import (
	"go.uber.org/fx"

	"github.com/celio001/backend-challenge-go/internal/adapter/httpapi"
	"github.com/celio001/backend-challenge-go/internal/adapter/sqsmsg"
	"github.com/celio001/backend-challenge-go/internal/usecase/openwallet"
	"github.com/celio001/backend-challenge-go/internal/usecase/processwager"
	"github.com/celio001/backend-challenge-go/internal/usecase/queries"
)

var UseCaseModule = fx.Module("usecase",
	fx.Provide(
		fx.Annotate(openwallet.New, fx.As(new(httpapi.OpenWallet))),
		queries.New,
		func(q *queries.Queries) httpapi.WalletQueries { return q },
		func(q *queries.Queries) httpapi.TransactionQueries { return q },
		func(cfg Config) processwager.Options { return processwager.Options{ReferenceTTL: cfg.ReferenceTTL} },
		processwager.New,
		func(uc *processwager.UseCase) httpapi.ProcessWager { return uc },
		func(uc *processwager.UseCase) sqsmsg.ProcessWager { return uc },
	),
)
