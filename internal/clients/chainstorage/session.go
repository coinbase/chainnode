package chainstorage

import (
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/sdk"

	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/utils/fxparams"

	"github.com/coinbase/chainstorage/sdk/services"
)

type (
	Client = sdk.Client
	Parser = sdk.Parser

	SessionParams struct {
		fx.In
		fxparams.Params
		Manager services.SystemManager
	}

	SessionResult struct {
		fx.Out
		Client Client
		Parser Parser
	}
)

const (
	InitialPositionLatest   = "LATEST"
	InitialPositionEarliest = "EARLIEST"
)

func NewSession(params SessionParams) (SessionResult, error) {
	session, err := sdk.New(params.Manager, &sdk.Config{
		Blockchain: params.Config.Blockchain(),
		Network:    params.Config.Network(),
		Env:        mapEnv(params.Config.Env()),
	})
	if err != nil {
		return SessionResult{}, xerrors.Errorf("failed to create chainstorage session: %w", err)
	}

	return SessionResult{
		Client: session.Client(),
		Parser: session.Parser(),
	}, nil
}

func mapEnv(env config.Env) sdk.Env {
	switch env {
	case config.EnvLocal, config.EnvDevelopment:
		return sdk.EnvDevelopment
	case config.EnvProduction:
		return sdk.EnvProduction
	default:
		return ""
	}
}
