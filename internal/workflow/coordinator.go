package workflow

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/cadence"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/utils/fxparams"
	"github.com/coinbase/chainnode/internal/workflow/activity"
)

type (
	Coordinator struct {
		baseWorkflow
		config                 *config.Config
		checkpointSynchronizer *activity.CheckpointSynchronizer
		ingestor               *Ingestor
	}

	CoordinatorParams struct {
		fx.In
		fxparams.Params
		Runtime                cadence.Runtime
		CheckpointSynchronizer *activity.CheckpointSynchronizer
		Ingestor               *Ingestor
	}

	CoordinatorRequest struct {
		Tag       uint32       // The stable tag is used if not specified.
		EventTag  uint32       // ChainStorage event tag
		MaxEvents int64        // Non-stop if not specified.
		Sequence  api.Sequence // When sequence is not set, read the last checkpoint from storage. Overriding sequence is not allowed in production.
	}
)

const (
	workflowAlreadyStartedError = "workflow execution already started"

	coordinatorSequenceGauge           = "workflow.coordinator.sequence"
	coordinatorHeightGauge             = "workflow.coordinator.height"
	coordinatorGapGauge                = "workflow.coordinator.gap"
	coordinatorTimeSinceLastBlockGauge = "workflow.coordinator.time_since_last_block"
	coordinatorInterruptTimeoutCounter = "workflow.coordinator.interrupt_timeout"
)

func NewCoordinator(params CoordinatorParams) *Coordinator {
	w := &Coordinator{
		config:                 params.Config,
		baseWorkflow:           newBaseWorkflow(&params.Config.Workflows.Coordinator, params.Runtime),
		checkpointSynchronizer: params.CheckpointSynchronizer,
		ingestor:               params.Ingestor,
	}
	w.registerWorkflow(w.execute)
	return w
}

func (w *Coordinator) Execute(ctx context.Context, request *CoordinatorRequest) (client.WorkflowRun, error) {
	workflowID := w.GetWorkflowID(request.Tag)
	return w.startWorkflow(ctx, workflowID, request)
}

func (w *Coordinator) GetWorkflowID(tag uint32) string {
	if tag == defaultTag {
		return w.name
	} else {
		return fmt.Sprintf("%s/%d", w.name, tag)
	}
}

func (w *Coordinator) execute(ctx workflow.Context, request *CoordinatorRequest) error {
	return w.executeWorkflow(ctx, request, func() error {
		return w.processCheckpoint(ctx, request)
	})
}

func (w *Coordinator) processCheckpoint(ctx workflow.Context, request *CoordinatorRequest) error {
	var cfg config.CoordinatorWorkflowConfig
	if err := w.readConfig(ctx, &cfg); err != nil {
		return xerrors.Errorf("failed to read config: %w", err)
	}

	tag := cfg.GetEffectiveTag(request.Tag)
	metricsHandler := w.getMetricsHandler(ctx).WithTags(map[string]string{"tag": strconv.Itoa(int(tag))})

	eventTag := request.EventTag
	if request.EventTag == 0 {
		eventTag = cfg.EventTag
	}

	sequence := request.Sequence
	if w.config.Env() == config.EnvProduction && sequence != api.InitialSequence {
		return xerrors.Errorf("cannot override sequence in production")
	}

	ctx = w.withActivityOptions(ctx)
	wg := workflow.NewWaitGroup(ctx)
	wg.Add(len(cfg.Ingestors))
	errorChannel := workflow.NewNamedBufferedChannel(ctx, "coordinator.error", 1)
	defer errorChannel.Close()

	deadline := workflow.Now(ctx).Add(cfg.CheckpointInterval)

	synchronizedCollections := make([]api.Collection, 0)
	for _, ingestor := range cfg.Ingestors {
		if ingestor.Synchronized {
			synchronizedCollections = append(synchronizedCollections, api.Collection(ingestor.Collection))
		}
	}
	if len(synchronizedCollections) == 0 {
		return xerrors.Errorf("at least one collection should be set as synchronized")
	}

	logger := w.getLogger(ctx).With(
		zap.String("tag", strconv.Itoa(int(tag))),
	)
	logger.Info(
		"coordinator started",
		zap.Reflect("request", request),
		zap.Reflect("config", cfg),
		zap.Uint32("tag", tag),
	)

	ingestorIDs := make([]string, len(cfg.Ingestors))
	for i := range cfg.Ingestors {
		ingestor := &cfg.Ingestors[i]
		ingestorID := w.ingestor.GetWorkflowID(tag, ingestor.Collection)
		ingestorIDs[i] = ingestorID
		workflow.Go(ctx, func(ctx workflow.Context) {
			defer wg.Done()
			ingestorRequest := &IngestorRequest{
				Collection:     api.Collection(ingestor.Collection),
				Sequence:       sequence,
				Tag:            tag,
				EventTag:       eventTag,
				CheckpointSize: ingestor.CheckpointSize,
				BatchSize:      ingestor.BatchSize,
				MiniBatchSize:  ingestor.MiniBatchSize,
				Parallelism:    ingestor.Parallelism,
			}

			if err := w.ingestor.ExecuteChildWorkflow(ctx, ingestorID, ingestorRequest); err != nil {
				err = w.retryChildWorkflowError(ctx, err, ingestorID, ingestorRequest)
				if err != nil {
					logger.Error(
						"child workflow failed",
						zap.Error(err),
						zap.String("collection", ingestor.Collection),
					)
					errorChannel.SendAsync(xerrors.Errorf("child workflow failed: %v: %w", ingestor.Collection, err))
					return
				}
			}

			errorChannel.SendAsync(xerrors.Errorf("child workflow exited prematurely: %v", ingestor.Collection))
		})
	}

	maxEventsExceeded := false
	numEvents := int64(0)
	for workflow.Now(ctx).Before(deadline) {
		var childErr error
		if ok := errorChannel.ReceiveAsync(&childErr); ok {
			return xerrors.Errorf("received an error from child workflow: %w", childErr)
		}

		resp, err := w.checkpointSynchronizer.Execute(ctx, &activity.CheckpointSynchronizerRequest{
			Tag:         tag,
			Collections: synchronizedCollections,
			Backoff:     cfg.SynchronizerInterval,
			EventTag:    eventTag,
		})
		if err != nil {
			return xerrors.Errorf("failed to write checkpoint: %w", err)
		}

		if resp.Sequence != int64(api.InitialSequence) {
			metricsHandler.Gauge(coordinatorSequenceGauge).Update(float64(resp.Sequence))
			metricsHandler.Gauge(coordinatorHeightGauge).Update(float64(resp.Height))
			metricsHandler.Gauge(coordinatorGapGauge).Update(float64(resp.Gap))
			if resp.TimeSinceLastBlock > 0 {
				metricsHandler.Gauge(coordinatorTimeSinceLastBlockGauge).Update(resp.TimeSinceLastBlock.Seconds())
			}
		}

		numEvents += resp.Delta
		if maxEvents := request.MaxEvents; maxEvents > 0 && numEvents >= maxEvents {
			logger.Info(
				"max events exceeded",
				zap.Int64("numEvents", numEvents),
				zap.Int64("maxEvents", maxEvents),
			)
			maxEventsExceeded = true
			break
		}
	}

	logger.Info("sending SIGINT to ingestors")
	for _, ingestorID := range ingestorIDs {
		if err := workflow.SignalExternalWorkflow(ctx, ingestorID, "", workflowSignalChannel, workflowSignalInterrupt).Get(ctx, nil); err != nil {
			return xerrors.Errorf("failed to interrupt ingestor %v: %w", ingestorID, err)
		}
	}

	interruptTimeout := cfg.InterruptTimeout

	ok, err := AwaitWaitGroup(ctx, wg, interruptTimeout)
	if err != nil {
		return xerrors.Errorf("failed to await ingestors: %w", err)
	}

	if !ok {
		// This race condition could happen if SIGINT was sent right after
		// the ingestor workflow reads from the signal channel, but before
		// it continued as a new workflow.
		// See "Final check of SIGINT before finishing" in ingestor.go.
		//
		// Because the child workflows have PARENT_CLOSE_POLICY_TERMINATE,
		// they will be forcefully terminated by temporal after the parent
		// workflow is restarted.
		metricsHandler.Counter(coordinatorInterruptTimeoutCounter).Inc(1)
		logger.Error("timed out waiting for ingestors")
	}

	logger.Info("coordinator finished", zap.Int64("numEvents", numEvents))
	if maxEventsExceeded {
		return nil
	}

	newRequest := *request
	newRequest.Sequence = api.InitialSequence
	return workflow.NewContinueAsNewError(ctx, w.name, &newRequest)
}

func (w *Coordinator) retryChildWorkflowError(ctx workflow.Context, workflowErr error, ingestorID string, ingestorRequest *IngestorRequest) error {
	if workflowErr == nil {
		return nil
	}

	if strings.Contains(strings.ToLower(workflowErr.Error()), workflowAlreadyStartedError) {
		err := workflow.Sleep(ctx, time.Second*2)
		if err != nil {
			return xerrors.Errorf("failed to backoff: %w", err)
		}

		// retry one more time
		return w.ingestor.ExecuteChildWorkflow(ctx, ingestorID, ingestorRequest)
	}

	return workflowErr
}
