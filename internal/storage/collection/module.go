package collection

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(fx.Annotated{
		Name:   "collection",
		Target: NewCollectionStorage,
	}),
	fx.Provide(fx.Annotated{
		Name:   "logs",
		Target: NewLogsStorage,
	}),
)
