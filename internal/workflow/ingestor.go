package workflow

import (
	"context"
	"fmt"
	"strconv"
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
	"github.com/coinbase/chainnode/internal/workflow/instrument"
)

type (
	Ingestor struct {
		baseWorkflow
		checkpointReader *activity.CheckpointReader
		checkpointWriter *activity.CheckpointWriter
		streamer         *activity.Streamer
		transformer      *activity.Transformer
	}

	IngestorParams struct {
		fx.In
		fxparams.Params
		Runtime          cadence.Runtime
		CheckpointReader *activity.CheckpointReader
		CheckpointWriter *activity.CheckpointWriter
		Streamer         *activity.Streamer
		Transformer      *activity.Transformer
	}

	IngestorRequest struct {
		Collection           api.Collection `validate:"required"`
		Tag                  uint32
		EventTag             uint32
		Sequence             api.Sequence
		CheckpointSize       uint64
		BatchSize            uint64
		Parallelism          uint64
		MiniBatchSize        uint64
		MiniBatchParallelism uint64
		Backoff              time.Duration
	}

	processBatchResult struct {
		NextCheckpoint     *api.Checkpoint
		NumEventsProcessed uint64
	}
)

const (
	ingestorSequenceGauge           = "workflow.ingestor.sequence"
	ingestorHeightGauge             = "workflow.ingestor.height"
	ingestorGapGauge                = "workflow.ingestor.gap"
	ingestorTimeSinceLastBlockGauge = "workflow.ingestor.time_since_last_block"
	ingestorEventCounter            = "workflow.ingestor.events"
)

func NewIngestor(params IngestorParams) *Ingestor {
	w := &Ingestor{
		baseWorkflow:     newBaseWorkflow(&params.Config.Workflows.Ingestor, params.Runtime),
		checkpointReader: params.CheckpointReader,
		checkpointWriter: params.CheckpointWriter,
		streamer:         params.Streamer,
		transformer:      params.Transformer,
	}
	w.registerWorkflow(w.execute)
	return w
}

func (w *Ingestor) Execute(ctx context.Context, request *IngestorRequest) (client.WorkflowRun, error) {
	ingestorID := w.GetWorkflowID(request.Tag, request.Collection.String())
	return w.startWorkflow(ctx, ingestorID, request)
}

func (w *Ingestor) ExecuteChildWorkflow(ctx workflow.Context, workflowID string, request *IngestorRequest) error {
	return w.executeChildWorkflow(ctx, workflowID, request, nil)
}

func (w *Ingestor) execute(ctx workflow.Context, request *IngestorRequest) error {
	return w.executeWorkflow(
		ctx,
		request,
		func() error {
			err := w.processCheckpoint(ctx, request)
			if IsInterruptedError(err) {
				// This error occurs when the workflow is interrupted by its parent (i.e. Coordinator).
				// Swallow the error and return success.
				return nil
			}
			return err
		},
		instrument.WithScopeTag("collection", request.Collection.String()),
	)
}

func (w *Ingestor) processCheckpoint(ctx workflow.Context, request *IngestorRequest) error {
	if err := w.validateRequest(request); err != nil {
		return err
	}

	var cfg config.IngestorWorkflowConfig
	if err := w.readConfig(ctx, &cfg); err != nil {
		return xerrors.Errorf("failed to read config: %w", err)
	}

	ctx = w.withActivityOptions(ctx)
	tag := cfg.GetEffectiveTag(request.Tag)
	collection := request.Collection
	effectiveRequest := w.getEffectiveRequest(request, &cfg)

	metricsHandler := w.getMetricsHandler(ctx).WithTags(map[string]string{
		"tag":        strconv.Itoa(int(tag)),
		"collection": collection.String(),
	})
	logger := w.getLogger(ctx).With(
		zap.String("collection", collection.String()),
		zap.String("tag", strconv.Itoa(int(tag))),
	)
	logger.Info(
		"ingestor started",
		zap.Reflect("request", effectiveRequest),
		zap.Reflect("config", cfg),
		zap.Uint32("tag", tag),
	)

	signalChannel := workflow.GetSignalChannel(ctx, workflowSignalChannel)
	checkpoint := &api.Checkpoint{
		Collection: collection,
		Tag:        tag,
		Sequence:   effectiveRequest.Sequence,
	}
	if effectiveRequest.Sequence == api.InitialSequence {
		// When sequence is not set, read the last checkpoint from storage.
		checkpointResponse, err := w.checkpointReader.Execute(ctx, &activity.CheckpointReaderRequest{
			Tag:        tag,
			Collection: collection,
		})
		if err != nil {
			return xerrors.Errorf("failed to read checkpoint: %w", err)
		}

		checkpoint = checkpointResponse.Checkpoint
	}

	checkpointSize := int(effectiveRequest.CheckpointSize)
	batchResult, err := w.processBatch(
		ctx,
		effectiveRequest,
		logger,
		metricsHandler,
		signalChannel,
		checkpoint,
	)
	if err != nil {
		return xerrors.Errorf("failed to process first batch: %w", err)
	}
	checkpoint = batchResult.NextCheckpoint

	// if near tip, use a larger checkpoint size
	if uint64(checkpointSize) < cfg.CheckpointSizeAtTip && batchResult.NumEventsProcessed < effectiveRequest.BatchSize {
		checkpointSize = int(cfg.CheckpointSizeAtTip)
		logger.Info("checkpointSize adjusted", zap.Int("checkpoint_size", checkpointSize))
	}

	for i := 1; i < checkpointSize; i++ {
		batchResult, err = w.processBatch(
			ctx,
			effectiveRequest,
			logger,
			metricsHandler,
			signalChannel,
			checkpoint,
		)
		if err != nil {
			return xerrors.Errorf("failed to process batch (i=%v): %w", i, err)
		}

		checkpoint = batchResult.NextCheckpoint
	}

	// Final check of SIGINT before finishing.
	if w.isInterrupted(signalChannel) {
		logger.Info("received SIGINT")
		return errInterrupted
	}

	// Clear the sequence and then restart the workflow.
	// The new workflow should resume from the last checkpoint.
	logger.Info("ingestor finished")
	newRequest := *request
	newRequest.Sequence = api.InitialSequence
	return workflow.NewContinueAsNewError(ctx, w.name, &newRequest)
}

func (w *Ingestor) processBatch(
	ctx workflow.Context,
	request *IngestorRequest,
	logger *zap.Logger,
	metricsHandler client.MetricsHandler,
	signalChannel workflow.ReceiveChannel,
	checkpoint *api.Checkpoint,
) (*processBatchResult, error) {
	if w.isInterrupted(signalChannel) {
		logger.Info("received SIGINT")
		return nil, errInterrupted
	}

	streamerResponse, err := w.streamer.Execute(ctx, &activity.StreamerRequest{
		Checkpoint: checkpoint,
		BatchSize:  request.BatchSize,
		EventTag:   request.EventTag,
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to execute streamer: %w", err)
	}

	nextCheckpoint := streamerResponse.NextCheckpoint
	numEvents := uint64(len(streamerResponse.Events))
	if numEvents == 0 {
		if err := workflow.Sleep(ctx, request.Backoff); err != nil {
			return nil, xerrors.Errorf("failed to back off: %w", err)
		}
		return &processBatchResult{
			NextCheckpoint:     nextCheckpoint,
			NumEventsProcessed: numEvents,
		}, nil
	}

	if err := w.processEvents(
		ctx,
		request,
		logger,
		metricsHandler,
		signalChannel,
		streamerResponse.Events,
	); err != nil {
		return nil, xerrors.Errorf("failed to process events: %w", err)
	}

	chkpWriterResponse, err := w.checkpointWriter.Execute(ctx, &activity.CheckpointWriterRequest{
		Checkpoint: nextCheckpoint,
		EventTag:   request.EventTag,
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to write checkpoint: %w", err)
	}

	metricsHandler.Gauge(ingestorSequenceGauge).Update(float64(chkpWriterResponse.Sequence))
	metricsHandler.Gauge(ingestorHeightGauge).Update(float64(chkpWriterResponse.Height))
	metricsHandler.Gauge(ingestorGapGauge).Update(float64(chkpWriterResponse.Gap))

	timeSinceLastBlock := chkpWriterResponse.TimeSinceLastBlock
	if timeSinceLastBlock > 0 {
		metricsHandler.Gauge(ingestorTimeSinceLastBlockGauge).Update(timeSinceLastBlock.Seconds())
	}

	logger.Info(
		"processed batch",
		zap.Reflect("nextCheckpoint", nextCheckpoint),
		zap.Uint64("numEvents", numEvents),
		zap.String("timeSinceLastBlock", timeSinceLastBlock.String()),
	)

	return &processBatchResult{
		NextCheckpoint:     nextCheckpoint,
		NumEventsProcessed: numEvents,
	}, err
}

func (w *Ingestor) processEvents(
	ctx workflow.Context,
	request *IngestorRequest,
	logger *zap.Logger,
	metricsHandler client.MetricsHandler,
	signalChannel workflow.ReceiveChannel,
	events []*api.Event,
) error {
	if w.isInterrupted(signalChannel) {
		logger.Info("received SIGINT")
		return errInterrupted
	}

	numEvents := uint64(len(events))
	if numEvents == 0 {
		return nil
	}

	miniBatchSize := request.MiniBatchSize
	if numEvents <= miniBatchSize {
		// If the number of events is no larger than the mini batch size,
		// the events can be processed with a single transformer.
		// As a further optimization, the events are processed in parallel within the transformer.
		if err := w.processMiniBatch(ctx, request, metricsHandler, events, numEvents); err != nil {
			return err
		}

		return nil
	}

	numMiniBatches := (numEvents + miniBatchSize - 1) / miniBatchSize
	inputChannel := workflow.NewNamedBufferedChannel(ctx, "ingestor.input", int(numMiniBatches))
	errorChannel := workflow.NewNamedBufferedChannel(ctx, "ingestor.error", 1)
	defer errorChannel.Close()

	wg := workflow.NewWaitGroup(ctx)
	wg.Add(int(request.Parallelism + 1))

	// Split the events into mini batches.
	workflow.Go(ctx, func(ctx workflow.Context) {
		defer wg.Done()

		for miniBatchStart := uint64(0); miniBatchStart < numEvents; miniBatchStart += miniBatchSize {
			miniBatchEnd := miniBatchStart + miniBatchSize
			if miniBatchEnd > numEvents {
				miniBatchEnd = numEvents
			}

			inputChannel.Send(ctx, events[miniBatchStart:miniBatchEnd])
		}

		inputChannel.Close()
	})

	// Process the mini batches in parallel.
	childCtx, cancel := workflow.WithCancel(ctx)
	for i := uint64(0); i < request.Parallelism; i++ {
		workflow.Go(childCtx, func(ctx workflow.Context) {
			defer wg.Done()

			for {
				if w.isInterrupted(signalChannel) {
					logger.Info("received SIGINT")
					errorChannel.SendAsync(errInterrupted)
					cancel()
					break
				}

				var miniBatch []*api.Event
				if ok := inputChannel.Receive(ctx, &miniBatch); !ok {
					break
				}

				if err := w.processMiniBatch(ctx, request, metricsHandler, miniBatch, request.MiniBatchParallelism); err != nil {
					// Abort on the first error.
					errorChannel.SendAsync(err)
					cancel()
					break
				}
			}
		})
	}

	// Return any error received by the transformer.
	wg.Wait(ctx)
	var err error
	if ok := errorChannel.ReceiveAsync(&err); ok {
		return xerrors.Errorf("failed to process events: %w", err)
	}

	return nil
}

func (w *Ingestor) processMiniBatch(
	ctx workflow.Context,
	request *IngestorRequest,
	metricsHandler client.MetricsHandler,
	events []*api.Event,
	parallelism uint64,
) error {
	_, err := w.transformer.Execute(ctx, &activity.TransformerRequest{
		Tag:         request.Tag,
		Collection:  request.Collection,
		Events:      events,
		Parallelism: parallelism,
	})
	if err != nil {
		return xerrors.Errorf("failed to execute transformer: %w", err)
	}

	metricsHandler.Counter(ingestorEventCounter).Inc(int64(len(events)))
	return nil
}

func (w *Ingestor) getEffectiveRequest(request *IngestorRequest, cfg *config.IngestorWorkflowConfig) *IngestorRequest {
	v := *request

	if v.Tag == 0 {
		v.Tag = cfg.GetEffectiveTag(request.Tag)
	}

	if v.CheckpointSize == 0 {
		v.CheckpointSize = cfg.CheckpointSize
	}

	if v.BatchSize == 0 {
		v.BatchSize = cfg.BatchSize
	}

	if v.Parallelism == 0 {
		v.Parallelism = cfg.Parallelism
	}

	if v.MiniBatchSize == 0 {
		v.MiniBatchSize = cfg.MiniBatchSize
	}

	if v.MiniBatchParallelism == 0 {
		v.MiniBatchParallelism = cfg.MiniBatchParallelism
	}

	if v.Backoff == 0 {
		v.Backoff = cfg.Backoff
	}

	return &v
}

func (w *Ingestor) isInterrupted(channel workflow.ReceiveChannel) bool {
	var signal string
	ok := channel.ReceiveAsync(&signal)
	return ok && signal == workflowSignalInterrupt
}

func (w *Ingestor) GetWorkflowID(tag uint32, collection string) string {
	return fmt.Sprintf("%v/%v/%d", w.name, collection, tag)
}
