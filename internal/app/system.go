package app

import (
	"go.uber.org/fx"

	"github.com/celio001/backend-challenge-go/internal/infra/system"
	"github.com/celio001/backend-challenge-go/internal/usecase"
)

var SystemModule = fx.Module("system",
	fx.Provide(
		fx.Annotate(func() system.Clock { return system.Clock{} }, fx.As(new(usecase.Clock))),
		fx.Annotate(func() system.IDs { return system.IDs{} }, fx.As(new(usecase.IDGenerator))),
	),
)
