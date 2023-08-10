package controller

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainnode/internal/controller/ethereum"
	"github.com/coinbase/chainnode/internal/controller/internal"
)

var Module = fx.Options(
	fx.Provide(NewController),
	internal.Module,
	ethereum.Module,
)
