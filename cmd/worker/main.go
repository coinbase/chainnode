package main

import (
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainnode/internal/aws"
	"github.com/coinbase/chainnode/internal/cadence"
	"github.com/coinbase/chainnode/internal/clients"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/controller"
	"github.com/coinbase/chainnode/internal/storage"
	"github.com/coinbase/chainnode/internal/utils"
	"github.com/coinbase/chainnode/internal/workflow"

	"github.com/coinbase/chainstorage/sdk/services"
)

func main() {
	manager := startManager()

	manager.WaitForInterrupt()
}

func startManager(opts ...fx.Option) services.SystemManager {
	manager := services.NewManager()
	ctx := manager.Context()
	logger := manager.Logger()

	opts = append(
		opts,
		aws.Module,
		cadence.Module,
		clients.Module,
		config.Module,
		controller.Module,
		storage.Module,
		workflow.Module,
		utils.Module,
		fx.NopLogger,
		fx.Provide(func() services.SystemManager { return manager }),
		fx.Provide(func() *zap.Logger { return logger }),
		fx.Invoke(workflow.NewManager),
	)

	app := fx.New(opts...)

	if err := app.Start(ctx); err != nil {
		logger.Fatal("failed to start app", zap.Error(err))
	}
	manager.AddPreShutdownHook(func() {
		logger.Info("shutting down worker")
		if err := app.Stop(ctx); err != nil {
			logger.Error("failed to stop app", zap.Error(err))
		}
	})

	logger.Info("started worker")
	return manager
}
