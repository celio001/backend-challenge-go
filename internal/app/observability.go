package app

import (
	"go.uber.org/fx"

	"github.com/celio001/backend-challenge-go/internal/infra/observability"
)

// ObservabilityModule provides the metrics registry that the transports, workers and the database layer report to.
var ObservabilityModule = fx.Module("observability", fx.Provide(observability.New))
