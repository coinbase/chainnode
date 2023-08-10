package utils

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainnode/internal/utils/fxparams"
	"github.com/coinbase/chainnode/internal/utils/tally"
	"github.com/coinbase/chainnode/internal/utils/taskpool"
	"github.com/coinbase/chainnode/internal/utils/tracer"
)

var Module = fx.Options(
	fxparams.Module,
	tally.Module,
	taskpool.Module,
	tracer.Module,
)
