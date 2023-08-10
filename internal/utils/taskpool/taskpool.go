package taskpool

import (
	"context"

	"github.com/panjf2000/ants/v2"
	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/utils/fxparams"

	"github.com/coinbase/chainnode/internal/utils/instrument"
	"github.com/coinbase/chainnode/internal/utils/log"

	"github.com/coinbase/chainstorage/sdk/services"
)

type (
	TaskPool interface {
		Submit(name string, task TaskFn) error
	}

	TaskPoolParams struct {
		fx.In
		fxparams.Params
		Lifecycle fx.Lifecycle
		Manager   services.SystemManager
	}

	TaskFn func(ctx context.Context) error

	taskPool struct {
		logger  *zap.Logger
		scope   tally.Scope
		taskCtx context.Context
		pool    *ants.Pool
		metrics *taskPoolMetrics
	}

	taskPoolMetrics struct {
		submitSuccess tally.Counter
		submitError   tally.Counter
		submitFull    tally.Counter
	}
)

const (
	scopeName     = "taskpool"
	submitCounter = "submit"
	resultTypeTag = "result_type"
	loggerMsg     = "taskpool.request"
)

var (
	ErrFull = ants.ErrPoolOverload
)

func New(params TaskPoolParams) (TaskPool, error) {
	logger := log.WithPackage(params.Logger)
	size := params.Config.TaskPool.Size
	pool, err := ants.NewPool(
		size,
		ants.WithNonblocking(true),
		ants.WithLogger(log.NewStandard(logger)),
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to create ants pool: %w", err)
	}

	scope := params.Metrics.SubScope(scopeName)
	tp := &taskPool{
		logger:  logger,
		scope:   scope,
		taskCtx: params.Manager.ServiceContext(),
		pool:    pool,
		metrics: newMetrics(scope),
	}

	pool.Running()

	params.Lifecycle.Append(fx.Hook{
		OnStart: tp.onStart,
		OnStop:  tp.onStop,
	})

	return tp, nil
}

func newMetrics(scope tally.Scope) *taskPoolMetrics {
	return &taskPoolMetrics{
		submitSuccess: scope.Tagged(map[string]string{resultTypeTag: "success"}).Counter(submitCounter),
		submitError:   scope.Tagged(map[string]string{resultTypeTag: "error"}).Counter(submitCounter),
		submitFull:    scope.Tagged(map[string]string{resultTypeTag: "full"}).Counter(submitCounter),
	}
}

// Submit submits a task to this pool.
// The call is non-blocking and may return ErrFull if the pool runs out of capacity.
func (p *taskPool) Submit(name string, task TaskFn) error {
	err := p.pool.Submit(func() {
		call := instrument.NewCall(p.scope, name, instrument.WithLogger(p.logger, loggerMsg))
		_ = call.Instrument(p.taskCtx, func(ctx context.Context) error {
			return task(ctx)
		})
	})
	if err != nil {
		if xerrors.Is(err, ErrFull) {
			p.metrics.submitFull.Inc(1)
		} else {
			p.metrics.submitError.Inc(1)
		}
		return xerrors.Errorf("failed to submit task: %w", err)
	}

	p.metrics.submitSuccess.Inc(1)
	return nil
}

func (p *taskPool) onStart(ctx context.Context) error {
	p.logger.Info("starting task pool")
	return nil
}

func (p *taskPool) onStop(ctx context.Context) error {
	p.logger.Info("stopping task pool")
	p.pool.Release()
	return nil
}
