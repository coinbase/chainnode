package main

import (
	"context"
	"fmt"
	"net/url"

	"github.com/fatih/color"
	"go.temporal.io/sdk/client"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/workflow"
)

const (
	workflowFlagName       = "workflow"
	workflowIDFlagName     = "workflowID"
	inputFlagName          = "input"
	defaultTerminateReason = "Terminated by workflow cmd"
)

var (
	cadenceHost = map[config.Env]string{
		config.EnvLocal: "http://localhost:8088",
		// TODO: Update this to the correct dev host once it's available.
		config.EnvDevelopment: "https://temporal-dev.example.com",
		// TODO: Update this to the correct prod host once it's available.
		config.EnvProduction: "https://temporal.example.com",
	}
)

var (
	workflowCmd = NewCommand("workflow", nil)

	workflowStartCmd = NewCommand("start", func() error {
		return startWorkflow()
	})

	workflowStopCmd = NewCommand("stop", func() error {
		return stopWorkflow()
	})

	workflowFlags struct {
		workflow   string
		workflowID string
		input      string
	}
)

func init() {
	rootCommand.AddCommand(workflowCmd)
	workflowCmd.AddCommand(workflowStartCmd)
	workflowCmd.AddCommand(workflowStopCmd)

	workflowCmd.StringVar(&workflowFlags.workflow, workflowFlagName, "", true)
	workflowCmd.StringVar(&workflowFlags.workflowID, workflowIDFlagName, "", false)
	workflowCmd.StringVar(&workflowFlags.input, inputFlagName, "", true)
}

func startWorkflow() error {
	workflowIdentity := workflow.GetWorkflowIdentify(workflowFlags.workflow)
	if workflowIdentity == workflow.UnknownIdentity {
		return xerrors.Errorf("invalid workflow: %v", workflowFlags.workflow)
	}

	var executors struct {
		fx.In
		Coordinator *workflow.Coordinator
		Ingestor    *workflow.Ingestor
	}
	app, err := NewApp(fx.Populate(&executors))
	if err != nil {
		return xerrors.Errorf("failed to init app: %w", err)
	}
	defer app.Close()

	ctx := context.Background()
	workflowIdentityString, err := workflowIdentity.String()
	if err != nil {
		return xerrors.Errorf("error parsing workflowIdentity: %w", err)
	}

	req, err := workflowIdentity.UnmarshalJsonStringToRequest(workflowFlags.input)
	if err != nil {
		return xerrors.Errorf("error converting input flag to request: %w", err)
	}

	if !confirmWorkflowOperation(app, "start", workflowIdentityString, req) {
		return nil
	}

	var run client.WorkflowRun
	switch workflowIdentity {
	case workflow.CoordinatorIdentity:
		request, ok := req.(workflow.CoordinatorRequest)
		if !ok {
			return xerrors.Errorf("error converting to request type")
		}
		run, err = executors.Coordinator.Execute(ctx, &request)
	case workflow.IngestorIdentity:
		request, ok := req.(workflow.IngestorRequest)
		if !ok {
			return xerrors.Errorf("error converting to request type")
		}
		run, err = executors.Ingestor.Execute(ctx, &request)
	default:
		return xerrors.Errorf("unsupported workflow identity: %v", workflowIdentity)
	}

	if err != nil {
		app.Logger.Error("failed to start workflow",
			zap.String("workflowIdentity", workflowIdentityString),
		)
		return xerrors.Errorf("failed to start workflow: %w", err)
	}

	workflowURL := getWorkflowURL(app, run)
	app.Logger.Info("started workflow",
		zap.String("workflowIdentity", workflowIdentityString),
		zap.String("workflowRunID", run.GetRunID()),
		zap.String("workflowID", run.GetID()),
		zap.String("workflowURL", workflowURL),
	)
	return nil
}

func stopWorkflow() error {
	var workflowIdentityString string
	var err error
	workflowIdentity := workflow.GetWorkflowIdentify(workflowFlags.workflow)
	if workflowIdentity == workflow.UnknownIdentity {
		return xerrors.Errorf("invalid workflow: %v", workflowFlags.workflow)
	}

	var executors struct {
		fx.In
		Coordinator *workflow.Coordinator
		Ingestor    *workflow.Ingestor
	}
	app, err := NewApp(fx.Populate(&executors))
	if err != nil {
		return xerrors.Errorf("failed to init app: %w", err)
	}
	defer app.Close()

	// validate ingestor flags. ingestor has to specify workflowID
	if workflowIdentity == workflow.IngestorIdentity && workflowFlags.workflowID == "" {
		return xerrors.Errorf("missing --workflowID for ingestor workflow")
	}

	if workflowFlags.workflowID != "" {
		workflowIdentityString = workflowFlags.workflowID
	} else {
		workflowIdentityString, err = workflowIdentity.String()
		if err != nil {
			return xerrors.Errorf("error parsing workflowIdentity: %w", err)
		}
	}

	if !confirmWorkflowOperation(app, "stop", workflowIdentityString, nil) {
		return nil
	}

	reason := defaultTerminateReason
	if workflowFlags.input != "" {
		reason = workflowFlags.input
	}

	ctx := context.Background()
	switch workflowIdentity {
	case workflow.CoordinatorIdentity:
		err = executors.Coordinator.StopWorkflow(ctx, workflowIdentityString, reason)
	case workflow.IngestorIdentity:
		err = executors.Ingestor.StopWorkflow(ctx, workflowIdentityString, reason)
	default:
		return xerrors.Errorf("unsupported workflow identity: %v", workflowIdentity)
	}

	if err != nil {
		return xerrors.Errorf("failed to stop workflow for workflowID=%s: %w", workflowIdentityString, err)
	}

	app.Logger.Info("stopped workflow",
		zap.String("workflowIdentity", workflowIdentityString),
	)

	return nil
}

func confirmWorkflowOperation(app *App, operation string, workflowIdentity string, input interface{}) bool {
	app.Logger.Info(
		"workflow arguments",
		zap.String("workflow", workflowIdentity),
		zap.String("env", string(app.Config.Env())),
		zap.String("blockchain", app.Config.Blockchain().GetName()),
		zap.String("network", app.Config.Network().GetName()),
		zap.Reflect("input", input),
	)

	prompt := fmt.Sprintf(
		"%v%v%v%v%v",
		color.CyanString(fmt.Sprintf("Are you sure you want to %v ", operation)),
		color.MagentaString(fmt.Sprintf("\"%v\" ", workflowIdentity)),
		color.CyanString("in "),
		color.MagentaString(fmt.Sprintf("%v::%v-%v", rootFlags.env, rootFlags.blockchain, rootFlags.network)),
		color.CyanString("? (y/N) "),
	)
	return app.Confirm(prompt)
}

func getWorkflowURL(app *App, run client.WorkflowRun) string {
	return fmt.Sprintf(
		"%s/namespaces/%s/workflows/%s/%s/summary",
		cadenceHost[app.Config.Env()],
		app.Config.Cadence.Domain,
		url.PathEscape(run.GetID()),
		run.GetRunID(),
	)
}
