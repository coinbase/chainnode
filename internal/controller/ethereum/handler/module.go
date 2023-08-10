package handler

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(fx.Annotated{
		Name:   "ethereum",
		Target: NewHandler,
	}),
	fx.Provide(NewValidator),
)
