package ethereum

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainnode/internal/controller/ethereum/crontask"
	"github.com/coinbase/chainnode/internal/controller/ethereum/handler"
	"github.com/coinbase/chainnode/internal/controller/ethereum/indexer"
	"github.com/coinbase/chainnode/internal/controller/ethereum/reverseproxy"
)

var Module = fx.Options(
	fx.Provide(fx.Annotated{
		Name:   "ethereum",
		Target: NewController,
	}),
	indexer.Module,
	handler.Module,
	crontask.Module,
	reverseproxy.Module,
)
