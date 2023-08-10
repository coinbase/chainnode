package workflow

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainnode/internal/workflow/activity"
)

var Module = fx.Options(
	activity.Module,
	fx.Provide(NewManager),
	fx.Provide(NewIngestor),
	fx.Provide(NewCoordinator),
)
