package controller

import (
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/controller/internal"
	"github.com/coinbase/chainnode/internal/utils/fxparams"

	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

// NOTE: The interfaces are defined in an internal package to avoid cyclic imports.
type (
	// Controller is a facade to blockchain-agnostic interfaces.
	// The blockchain-specific implementation is injected at runtime.
	Controller = internal.Controller

	// Handler defines the RPC handler.
	Handler = internal.Handler

	// Checkpointer defines the read and write interfaces of checkpoints.
	Checkpointer = internal.Checkpointer

	// Indexer defines the read and write interfaces of a collection.
	Indexer = internal.Indexer

	// CronTask defines the interface of a periodic task.
	CronTask = internal.CronTask

	// ReverseProxy defines the interface of a reverse proxy.
	ReverseProxy = internal.ReverseProxy

	// PreHandler defines the interface to run before the handler.
	PreHandler = internal.PreHandler

	ControllerParams struct {
		fx.In
		fxparams.Params
		Ethereum Controller `name:"ethereum"`
	}
)

func NewController(params ControllerParams) (Controller, error) {
	var controller Controller
	blockchain := params.Config.Blockchain()
	switch blockchain {
	case common.Blockchain_BLOCKCHAIN_ETHEREUM:
		controller = params.Ethereum
	case common.Blockchain_BLOCKCHAIN_POLYGON:
		controller = params.Ethereum
	case common.Blockchain_BLOCKCHAIN_ARBITRUM:
		controller = params.Ethereum
	case common.Blockchain_BLOCKCHAIN_OPTIMISM:
		controller = params.Ethereum
	}

	if controller == nil {
		return nil, xerrors.Errorf("controller is not implemented: %v", blockchain)
	}

	return controller, nil
}
