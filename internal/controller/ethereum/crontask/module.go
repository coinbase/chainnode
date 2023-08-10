package crontask

import (
	"go.uber.org/fx"
)

const (
	subScope = "crontask"
)

var Module = fx.Options(
	fx.Provide(fx.Annotated{
		Group:  "ethereum",
		Target: NewPollingCanaryTask,
	}),
	fx.Provide(fx.Annotated{
		Group:  "ethereum",
		Target: NewShadowTask,
	}),
	fx.Provide(fx.Annotated{
		Group:  "ethereum",
		Target: NewSLATask,
	}),
	fx.Provide(fx.Annotated{
		Group:  "ethereum",
		Target: NewFailoverTask,
	}),
	fx.Provide(fx.Annotated{
		Group:  "ethereum",
		Target: NewNodeCanaryTask,
	}),
	fx.Provide(fx.Annotated{
		Group:  "ethereum",
		Target: NewWorkflowStatus,
	}),
)
