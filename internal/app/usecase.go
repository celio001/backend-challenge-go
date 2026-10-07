package app

import (
	"go.uber.org/fx"

	"github.com/celio001/backend-challenge-go/internal/adapter/httpapi"
	"github.com/celio001/backend-challenge-go/internal/usecase/openwallet"
	"github.com/celio001/backend-challenge-go/internal/usecase/queries"
)

var UseCaseModule = fx.Module("usecase",
	fx.Provide(
		fx.Annotate(openwallet.New, fx.As(new(httpapi.OpenWallet))),
		fx.Annotate(queries.New, fx.As(new(httpapi.WalletQueries))),
	),
)
