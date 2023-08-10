package workflow

import (
	"context"
	"time"

	"github.com/go-playground/validator/v10"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/cadence"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/utils/log"
	"github.com/coinbase/chainnode/internal/workflow/instrument"
)

type (
	baseWorkflow struct {
		name       string
		config     config.BaseWorkflowConfig
		runtime    cadence.Runtime
		validate   *validator.Validate
		instrument *instrument.Instrument
	}
)

const (
	activityRetryInitialInterval    = 1 * time.Second
	activityRetryMaximumInterval    = 1 * time.Minute
	activityRetryBackoffCoefficient = 2.0

	workflowSignalChannel   = "workflow.signal"
	workflowSignalInterrupt = "SIGINT"

	loggerMsg = "workflow.request"
)

var (
	errInterrupted = xerrors.New("workflow interrupted")
)

func newBaseWorkflow(config config.BaseWorkflowConfig, runtime cadence.Runtime) baseWorkflow {
	name := config.Base().WorkflowIdentity
	return baseWorkflow{
		name:       name,
		config:     config,
		runtime:    runtime,
		validate:   validator.New(),
		instrument: instrument.New(runtime, name, loggerMsg),
	}
}

func (w *baseWorkflow) registerWorkflow(workflowFn interface{}) {
	w.runtime.RegisterWorkflow(workflowFn, workflow.RegisterOptions{
		Name: w.name,
	})
}

func (w *baseWorkflow) validateRequest(request interface{}) error {
	return w.validateRequestCtx(context.Background(), request)
}

func (w *baseWorkflow) validateRequestCtx(ctx context.Context, request interface{}) error {
	if err := w.validate.StructCtx(ctx, request); err != nil {
		return xerrors.Errorf("invalid workflow request (name=%v, request=%+v): %w", w.name, request, err)
	}

	return nil
}

func (w *baseWorkflow) startWorkflow(ctx context.Context, workflowID string, request interface{}) (client.WorkflowRun, error) {
	if err := w.validateRequestCtx(ctx, request); err != nil {
		return nil, err
	}

	cfg := w.config.Base()
	workflowOptions := client.StartWorkflowOptions{
		ID:                                       workflowID,
		TaskQueue:                                cfg.TaskList,
		WorkflowRunTimeout:                       cfg.WorkflowExecutionTimeout,
		WorkflowIDReusePolicy:                    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
	}

	execution, err := w.runtime.ExecuteWorkflow(ctx, workflowOptions, w.name, request)
	if err != nil {
		return nil, xerrors.Errorf("failed to execute workflow: %w", err)
	}

	return execution, nil
}

func (w *baseWorkflow) executeWorkflow(ctx workflow.Context, request interface{}, fn instrument.Fn, opts ...instrument.Option) error {
	opts = append(
		opts,
		instrument.WithLoggerField(zap.String("workflow", w.name)),
		instrument.WithLoggerField(zap.Reflect("request", request)),
		instrument.WithFilter(IsContinueAsNewError),
	)
	return w.instrument.Instrument(ctx, func() error {
		if err := w.validateRequest(request); err != nil {
			return err
		}

		if err := fn(); err != nil {
			return xerrors.Errorf("failed to execute workflow (name=%v): %w", w.name, err)
		}

		return nil
	}, opts...)
}

func (w *baseWorkflow) executeChildWorkflow(ctx workflow.Context, workflowID string, request interface{}, response interface{}) error {
	workflowOptions := workflow.ChildWorkflowOptions{
		WorkflowID:            workflowID,
		WorkflowRunTimeout:    w.config.Base().WorkflowExecutionTimeout,
		ParentClosePolicy:     enums.PARENT_CLOSE_POLICY_TERMINATE,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WaitForCancellation:   true,
	}
	ctx = workflow.WithChildOptions(ctx, workflowOptions)
	if err := w.runtime.ExecuteChildWorkflow(ctx, w.name, request, response); err != nil {
		return xerrors.Errorf("failed to execute child workflow: %w", err)
	}

	return nil
}

func (w *baseWorkflow) StopWorkflow(ctx context.Context, workflowID string, reason string) error {
	if err := w.runtime.TerminateWorkflow(ctx, workflowID, "", reason); err != nil {
		return xerrors.Errorf("failed to terminate workflowID=%s: %w", workflowID, err)
	}

	return nil
}

func (w *baseWorkflow) readConfig(ctx workflow.Context, output interface{}) error {
	// Read config as a SideEffect to guarantee deterministic workflow execution.
	// As a result, config changes only take effect after finishing the current checkpoint.
	val := workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
		return w.config
	})

	if err := val.Get(output); err != nil {
		return xerrors.Errorf("failed to retrieve config: %w", err)
	}

	return nil
}

func (w *baseWorkflow) getLogger(ctx workflow.Context) *zap.Logger {
	return log.WithPackage(w.runtime.GetLogger(ctx))
}

func (w *baseWorkflow) getMetricsHandler(ctx workflow.Context) client.MetricsHandler {
	return w.runtime.GetMetricsHandler(ctx)
}

func (w *baseWorkflow) withActivityOptions(ctx workflow.Context) workflow.Context {
	cfg := w.config.Base()
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           cfg.TaskList,
		StartToCloseTimeout: cfg.ActivityStartToCloseTimeout,
		HeartbeatTimeout:    cfg.ActivityHeartbeatTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    activityRetryInitialInterval,
			MaximumInterval:    activityRetryMaximumInterval,
			BackoffCoefficient: activityRetryBackoffCoefficient,
			MaximumAttempts:    cfg.ActivityRetryMaximumAttempts,
		},
	})
}

// AwaitWaitGroup blocks the calling thread until the wait group completes
// or the blocking time exceeds the timeout value.
// Returns true if the wait group completes, false if timed out,
// or err equals to CanceledError if the ctx is canceled.
func AwaitWaitGroup(ctx workflow.Context, wg workflow.WaitGroup, timeout time.Duration) (bool, error) {
	future, done := workflow.NewFuture(ctx)
	workflow.Go(ctx, func(ctx workflow.Context) {
		wg.Wait(ctx)
		done.SetValue(true)
	})
	return workflow.AwaitWithTimeout(ctx, timeout, func() bool {
		return future.IsReady()
	})
}

func IsContinueAsNewError(err error) bool {
	return workflow.IsContinueAsNewError(err)
}

func IsInterruptedError(err error) bool {
	return xerrors.Is(err, errInterrupted)
}
