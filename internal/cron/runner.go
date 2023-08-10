package cron

import (
	"context"
	"time"

	"github.com/robfig/cron/v3"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/controller"
	"github.com/coinbase/chainnode/internal/utils/fxparams"
	"github.com/coinbase/chainnode/internal/utils/log"

	"github.com/coinbase/chainstorage/sdk/services"
)

type (
	RunnerParams struct {
		fx.In
		fxparams.Params
		Lifecycle  fx.Lifecycle
		Manager    services.SystemManager
		Controller controller.Controller
	}
)

const (
	subScope    = "cron"
	stopTimeout = time.Second * 5
)

func RegisterRunner(params RunnerParams) error {
	logger := log.WithPackage(params.Logger)
	cfg := params.Config
	metrics := params.Metrics.SubScope(subScope)

	c := cron.New()
	jobCtx, cancel := context.WithCancel(params.Manager.ServiceContext())

	tasks := params.Controller.CronTasks()
	params.Lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("starting cron", zap.Int("num_jobs", len(tasks)))
			c.Start()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("stopping cron")
			timer := time.After(stopTimeout)
			cancel()
			ctx = c.Stop()
			select {
			case <-ctx.Done():
				logger.Info("stopped cron")
			case <-timer:
				logger.Error("timed out while stopping cron")
			}
			return nil
		},
	})

	for _, task := range tasks {
		taskName := task.Name()
		if !task.Enabled() {
			logger.Warn("task is disabled", zap.String("task", taskName))
			continue
		}

		job, err := NewJob(jobCtx, cfg, logger, metrics, task)
		if err != nil {
			return xerrors.Errorf("failed to create job %v: %w", taskName, err)
		}

		if _, err := c.AddJob(task.Spec(), job); err != nil {
			return xerrors.Errorf("failed to add job %v: %w", taskName, err)
		}
	}

	return nil
}
