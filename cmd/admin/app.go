package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/fatih/color"
	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/utils/auxiliary"

	"github.com/coinbase/chainstorage/sdk/services"

	"github.com/coinbase/chainnode/internal/aws"
	"github.com/coinbase/chainnode/internal/cadence"
	"github.com/coinbase/chainnode/internal/clients"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/controller"
	"github.com/coinbase/chainnode/internal/storage"
	"github.com/coinbase/chainnode/internal/utils/fxparams"
	"github.com/coinbase/chainnode/internal/utils/log"
	"github.com/coinbase/chainnode/internal/utils/taskpool"
	"github.com/coinbase/chainnode/internal/utils/tracer"
	"github.com/coinbase/chainnode/internal/workflow"
)

type (
	App struct {
		Manager services.SystemManager
		Config  *config.Config
		Logger  *zap.Logger

		app *fx.App
	}
)

func NewApp(opts ...fx.Option) (*App, error) {
	manager := services.NewManager()
	ctx := manager.Context()

	logger, err := log.NewDevelopment()
	if err != nil {
		return nil, xerrors.Errorf("failed to create logger: %w", err)
	}

	env := config.Env(rootFlags.env)

	blockchain, err := auxiliary.ParseBlockchain(rootFlags.blockchain)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse blockchain: %v", rootFlags.blockchain)
	}

	network, err := auxiliary.ParseNetwork(fmt.Sprintf("%v_%v", rootFlags.blockchain, rootFlags.network))
	if err != nil {
		return nil, xerrors.Errorf("failed to parse network: %v", rootFlags.network)
	}

	cfg, err := config.New(
		config.WithEnvironment(env),
		config.WithBlockchain(blockchain),
		config.WithNetwork(network),
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to create config: %w", err)
	}

	logger.Info(
		"starting app",
		zap.String("env", string(env)),
		zap.String("blockchain", blockchain.GetName()),
		zap.String("network", network.GetName()),
	)

	opts = append(opts,
		aws.Module,
		cadence.Module,
		clients.Module,
		config.Module,
		config.WithCustomConfig(cfg),
		controller.Module,
		fxparams.Module,
		storage.Module,
		taskpool.Module,
		tracer.Module,
		workflow.Module,
		fx.NopLogger,
		fx.Provide(func() *zap.Logger { return logger }),
		fx.Provide(func() tally.Scope { return tally.NoopScope }),
		fx.Provide(func() services.SystemManager { return manager }),
	)
	app := fx.New(opts...)
	if err := app.Start(ctx); err != nil {
		return nil, xerrors.Errorf("failed to start app: %w", err)
	}

	return &App{
		Manager: manager,
		Config:  cfg,
		Logger:  logger,
		app:     app,
	}, nil
}

func (a *App) Context() context.Context {
	return a.Manager.Context()
}

func (a *App) Close() {
	if a == nil {
		return
	}

	if err := a.app.Stop(a.Context()); err != nil {
		a.Logger.Error("failed to stop app", zap.Error(err))
	}

	a.Manager.Shutdown()
}

func (a *App) Confirm(prompt string) bool {
	msg := color.MagentaString(fmt.Sprintf("[%v::%v] ", a.Config.Env(), a.Config.Network().GetName())) +
		color.CyanString(prompt+" (y/N) ")

	fmt.Printf(msg)
	response, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		a.Logger.Error("failed to read from console", zap.Error(err))
		return false
	}

	if strings.ToLower(strings.TrimSpace(response)) != "y" {
		return false
	}

	return true
}
