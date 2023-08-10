package cron

import (
	"context"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/controller"
	"github.com/coinbase/chainnode/internal/utils/instrument"
)

type (
	Job struct {
		ctx        context.Context
		logger     *zap.Logger
		instrument instrument.Call
		task       controller.CronTask
		semaphore  *semaphore.Weighted
	}
)

const (
	jobScope                = "job"
	taskTag                 = "task"
	loggerMsg               = "cron.job"
	delayStartDurationLocal = 10 * time.Second
)

var _ cron.Job = (*Job)(nil)

func NewJob(ctx context.Context, cfg *config.Config, logger *zap.Logger, scope tally.Scope, task controller.CronTask) (*Job, error) {
	parallelism := task.Parallelism()
	if parallelism <= 0 {
		return nil, xerrors.Errorf("invalid parallelism: %v", parallelism)
	}

	sem := semaphore.NewWeighted(parallelism)
	if err := sem.Acquire(ctx, parallelism); err != nil {
		return nil, xerrors.Errorf("failed to acquire the semaphore: %w", err)
	}

	// In order to make the metric chain agnostic,
	// use static metric name "chainnode.cron.job" and emit the task name as a tag.
	taskName := task.Name()
	scope = scope.Tagged(map[string]string{
		taskTag: taskName,
	})
	call := instrument.NewCall(
		scope,
		jobScope,
		instrument.WithLogger(logger.With(zap.String(taskTag, taskName)), loggerMsg),
	)

	// Delay the job to prevent false alarms during the deployment.
	delayStartDuration := task.DelayStartDuration()
	if cfg.Env() == config.EnvLocal {
		delayStartDuration = delayStartDurationLocal
	}
	timer := time.NewTimer(delayStartDuration)
	go func() {
		logger.Info("delay start", zap.String("duration", delayStartDuration.String()))
		<-timer.C
		sem.Release(parallelism)
	}()

	return &Job{
		ctx:        ctx,
		logger:     logger,
		instrument: call,
		task:       task,
		semaphore:  sem,
	}, nil
}

func (j *Job) Run() {
	ctx := j.ctx
	taskName := j.task.Name()
	if j.semaphore.TryAcquire(1) {
		defer j.semaphore.Release(1)

		_ = j.instrument.Instrument(ctx, func(ctx context.Context) error {
			return j.task.Run(ctx)
		})
	} else {
		j.logger.Info("skipped task", zap.String("task", taskName))
	}
}
