package clients

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainnode/internal/clients/blockchain"
	"github.com/coinbase/chainnode/internal/clients/chainstorage"
)

var Module = fx.Options(
	blockchain.Module,
	chainstorage.Module,
)
