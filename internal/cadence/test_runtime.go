package cadence

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/utils/timesource"
)

type (
	testRuntime struct {
		env    *TestEnv
		logger *zap.Logger
	}

	testWorkflowRun struct{}
)

func newTestRuntime(env *TestEnv, logger *zap.Logger) (Runtime, error) {
	return &testRuntime{
		env:    env,
		logger: logger,
	}, nil
}

func (r *testRuntime) OnStart(ctx context.Context) error {
	return nil
}

func (r *testRuntime) OnStop(ctx context.Context) error {
	return nil
}

func (r *testRuntime) RegisterWorkflow(w interface{}, options workflow.RegisterOptions) {
	r.env.RegisterWorkflowWithOptions(w, options)
}

func (r *testRuntime) RegisterActivity(a interface{}, options activity.RegisterOptions) {
	if r.env.IsActivityEnv() {
		r.env.testActivityEnvironment.RegisterActivityWithOptions(a, options)
		return
	}

	r.env.RegisterActivityWithOptions(a, options)
}

func (r *testRuntime) ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, request interface{}) (client.WorkflowRun, error) {
	r.env.ExecuteWorkflow(workflow, request)
	if !r.env.IsWorkflowCompleted() {
		return nil, xerrors.New("workflow not completed")
	}

	if err := r.env.GetWorkflowError(); err != nil {
		return nil, xerrors.Errorf("workflow failed: %w", err)
	}

	return testWorkflowRun{}, nil
}

func (r *testRuntime) ExecuteChildWorkflow(ctx workflow.Context, name string, request interface{}, response interface{}) error {
	future := workflow.ExecuteChildWorkflow(ctx, name, request)
	err := future.Get(ctx, response)
	if err != nil {
		if workflow.IsContinueAsNewError(err) {
			// HACK: ExecuteChildWorkflow is not supposed to return when the child workflow continues as new.
			// To work around the bug in TestWorkflowEnvironment, ExecuteChildWorkflow is called recursively to simulate the expected behavior.
			r.logger.Info("child workflow continues as new", zap.String("name", name))
			return r.ExecuteChildWorkflow(ctx, name, request, response)
		}

		return err
	}

	return nil
}

func (r *testRuntime) ExecuteActivity(ctx workflow.Context, activity interface{}, request interface{}, response interface{}) error {
	if r.env.IsActivityEnv() {
		val, err := r.env.testActivityEnvironment.ExecuteActivity(activity, request)
		if err != nil {
			return err
		}

		return val.Get(response)
	}

	future := workflow.ExecuteActivity(ctx, activity, request)
	return future.Get(ctx, response)
}

func (r *testRuntime) GetLogger(ctx workflow.Context) *zap.Logger {
	return r.logger
}

func (r *testRuntime) GetMetricsHandler(ctx workflow.Context) client.MetricsHandler {
	return client.MetricsNopHandler
}

func (r *testRuntime) GetActivityLogger(ctx context.Context) *zap.Logger {
	return r.logger
}

func (r *testRuntime) GetTimeSource(ctx workflow.Context) timesource.TimeSource {
	if r.env.IsActivityEnv() {
		return timesource.NewRealTimeSource()
	}

	return timesource.NewWorkflowTimeSource(ctx)
}

func (r *testRuntime) TerminateWorkflow(ctx context.Context, workflowID string, runID string, reason string) error {
	r.env.CancelWorkflow()
	return nil
}

func (t *testRuntime) ListOpenWorkflows(ctx context.Context, namespace string, maxPageSize int32) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {
	return nil, nil
}

func (t testWorkflowRun) GetID() string {
	return ""
}

func (t testWorkflowRun) GetRunID() string {
	return ""
}

func (t testWorkflowRun) Get(ctx context.Context, valuePtr interface{}) error {
	return nil
}

func (t testWorkflowRun) GetWithOptions(ctx context.Context, valuePtr interface{}, options client.WorkflowRunGetOptions) error {
	return nil
}
