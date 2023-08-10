package main

import (
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/sdk/services"

	"github.com/coinbase/chainnode/internal/aws"
	"github.com/coinbase/chainnode/internal/cadence"
	"github.com/coinbase/chainnode/internal/clients"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/controller"
	"github.com/coinbase/chainnode/internal/cron"
	"github.com/coinbase/chainnode/internal/storage"
	"github.com/coinbase/chainnode/internal/utils"
)

func main() {
	manager := startManager()
	manager.WaitForInterrupt()
}

func startManager(opts ...fx.Option) services.SystemManager {
	manager := services.NewManager()
	logger := manager.Logger()
	ctx := manager.Context()

	opts = append(
		opts,
		aws.Module,
		config.Module,
		controller.Module,
		clients.Module,
		cadence.Module,
		storage.Module,
		utils.Module,
		fx.Provide(func() services.SystemManager { return manager }),
		fx.Provide(func() *zap.Logger { return logger }),
		fx.Invoke(cron.RegisterRunner),
	)
	app := fx.New(opts...)

	if err := app.Start(ctx); err != nil {
		logger.Fatal("failed to start app", zap.Error(err))
	}
	manager.AddPreShutdownHook(func() {
		logger.Info("shutting down cron")
		if err := app.Stop(ctx); err != nil {
			logger.Error("failed to stop app", zap.Error(err))
		}
	})

	logger.Info("started cron")
	return manager
}
