package workflow

import (
	"context"

	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/cadence"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/utils/fxparams"
	"github.com/coinbase/chainnode/internal/utils/log"
)

type (
	Manager struct {
		config      *config.Config
		logger      *zap.Logger
		runtime     cadence.Runtime
		ingestor    *Ingestor
		coordinator *Coordinator
	}

	ManagerParams struct {
		fx.In
		fxparams.Params
		Lifecycle   fx.Lifecycle
		Runtime     cadence.Runtime
		Ingestor    *Ingestor
		Coordinator *Coordinator
	}
)

func NewManager(params ManagerParams) *Manager {
	mgr := &Manager{
		config:      params.Config,
		logger:      log.WithPackage(params.Logger),
		runtime:     params.Runtime,
		ingestor:    params.Ingestor,
		coordinator: params.Coordinator,
	}

	params.Lifecycle.Append(fx.Hook{
		OnStart: mgr.onStart,
		OnStop:  mgr.onStop,
	})

	return mgr
}

func (m *Manager) onStart(ctx context.Context) error {
	m.logger.Info(
		"starting workflow manager",
		zap.String("env", string(m.config.Env())),
		zap.String("blockchain", m.config.Blockchain().GetName()),
		zap.String("network", m.config.Network().GetName()),
	)

	if err := m.runtime.OnStart(ctx); err != nil {
		return xerrors.Errorf("failed to start runtime: %w", err)
	}

	return nil
}

func (m *Manager) onStop(ctx context.Context) error {
	m.logger.Info("stopping workflow manager")

	if err := m.runtime.OnStop(ctx); err != nil {
		return xerrors.Errorf("failed to stop runtime: %w", err)
	}

	return nil
}
