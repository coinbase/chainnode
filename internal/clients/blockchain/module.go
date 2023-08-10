package blockchain

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainnode/internal/clients/blockchain/endpoints"
	"github.com/coinbase/chainnode/internal/clients/blockchain/jsonrpc"
)

var Module = fx.Options(
	endpoints.Module,
	jsonrpc.Module,
)
