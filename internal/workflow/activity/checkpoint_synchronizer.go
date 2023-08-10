package activity

import (
	"context"
	"time"

	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	chainstorageapi "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/cadence"
	"github.com/coinbase/chainnode/internal/clients/chainstorage"
	"github.com/coinbase/chainnode/internal/controller"
	"github.com/coinbase/chainnode/internal/utils/fxparams"
)

type (
	CheckpointSynchronizer struct {
		baseActivity
		controller         controller.Controller
		chainStorageClient chainstorage.Client
	}

	CheckpointSynchronizerParams struct {
		fx.In
		fxparams.Params
		Runtime            cadence.Runtime
		Controller         controller.Controller
		ChainStorageClient chainstorage.Client
	}

	CheckpointSynchronizerRequest struct {
		Tag         uint32           `validate:"required"`
		Collections []api.Collection `validate:"required"`
		Backoff     time.Duration
		EventTag    uint32
	}

	CheckpointSynchronizerResponse struct {
		Sequence           int64
		Height             uint64
		Gap                int64
		Delta              int64
		TimeSinceLastBlock time.Duration
	}
)

func NewCheckpointSynchronizer(params CheckpointSynchronizerParams) *CheckpointSynchronizer {
	a := &CheckpointSynchronizer{
		baseActivity:       newBaseActivity(ActivityCheckpointSynchronizer, params.Runtime),
		controller:         params.Controller,
		chainStorageClient: params.ChainStorageClient,
	}
	a.register(a.execute)
	return a
}

func (a *CheckpointSynchronizer) Execute(ctx workflow.Context, request *CheckpointSynchronizerRequest) (*CheckpointSynchronizerResponse, error) {
	var response CheckpointSynchronizerResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *CheckpointSynchronizer) execute(ctx context.Context, request *CheckpointSynchronizerRequest) (*CheckpointSynchronizerResponse, error) {
	timer := time.NewTimer(request.Backoff)
	defer func() {
		<-timer.C
	}()

	logger := a.getLogger(ctx)
	logger.Info("writing earliest checkpoint")
	if err := a.writeEarliestCheckpoint(ctx, request, logger); err != nil {
		return nil, xerrors.Errorf("failed to write earliest checkpoint: %w", err)
	}

	logger.Info("writing latest checkpoint")
	metricsResult, err := a.writeLatestCheckpoint(ctx, request, logger)
	if err != nil {
		return nil, xerrors.Errorf("failed to write latest checkpoint: %w", err)
	}

	return &CheckpointSynchronizerResponse{
		Sequence:           metricsResult.Sequence,
		Height:             metricsResult.Height,
		Gap:                metricsResult.Gap,
		Delta:              metricsResult.Delta,
		TimeSinceLastBlock: metricsResult.TimeSinceLastBlock,
	}, nil
}

func (a *CheckpointSynchronizer) writeEarliestCheckpoint(ctx context.Context, request *CheckpointSynchronizerRequest, logger *zap.Logger) error {
	checkpointer := a.controller.Checkpointer()
	tag := request.Tag
	earliestCheckpoint, err := checkpointer.Get(ctx, api.CollectionEarliestCheckpoint, tag)
	if err != nil {
		return xerrors.Errorf("failed to get earliest checkpoint: %w", err)
	}

	if earliestCheckpoint.Present() {
		return nil
	}

	// If the earliest checkpoint is not set before, query the info from ChainStorage.
	events, err := a.chainStorageClient.GetChainEvents(ctx, &chainstorageapi.GetChainEventsRequest{
		InitialPositionInStream: chainstorage.InitialPositionEarliest,
		MaxNumEvents:            1,
		EventTag:                request.EventTag,
	})
	if err != nil {
		return xerrors.Errorf("failed to query the earliest event: %w", err)
	}

	if len(events) != 1 {
		return xerrors.Errorf("unexpected number of events: %v", len(events))
	}

	firstEvent := events[0]
	firstSequence := api.ParseSequenceNum(firstEvent.SequenceNum)

	earliestCheckpoint = api.NewCheckpoint(
		api.CollectionEarliestCheckpoint,
		tag,
		firstSequence,
		firstEvent.Block.Height,
	).WithLastBlockTimestamp(firstEvent.Block.Timestamp)
	if err := checkpointer.Set(ctx, earliestCheckpoint); err != nil {
		return xerrors.Errorf("failed to set latest checkpoint: %w", err)
	}

	logger.Info("earliest checkpoint", zap.Reflect("checkpoint", earliestCheckpoint))
	return nil
}

func (a *CheckpointSynchronizer) writeLatestCheckpoint(
	ctx context.Context,
	request *CheckpointSynchronizerRequest,
	logger *zap.Logger,
) (*checkpointMetrics, error) {
	// latest = max(latest, min(collection[0], collection[1], ...))
	checkpointer := a.controller.Checkpointer()
	tag := request.Tag

	logger.Info("getting the earliest checkpoints across all collections")
	checkpoint, err := checkpointer.GetEarliest(ctx, request.Collections, tag)
	if err != nil {
		return nil, xerrors.Errorf("failed to get earliest checkpoint from collections %v: %w", request.Collections, err)
	}

	if checkpoint.Empty() {
		// Skip if local checkpoint is not yet available.
		return &checkpointMetrics{}, nil
	}

	latestCheckpoint := api.NewCheckpoint(
		api.CollectionLatestCheckpoint,
		tag,
		checkpoint.Sequence,
		checkpoint.Height,
	).WithLastBlockTime(checkpoint.LastBlockTime)

	logger.Info("getting current latest checkpoint")
	currentCheckpoint, err := checkpointer.Get(ctx, api.CollectionLatestCheckpoint, tag)
	if err != nil {
		return nil, xerrors.Errorf("failed to get the current latest checkpoint: %w", err)
	}

	if currentCheckpoint != nil && currentCheckpoint.Sequence >= latestCheckpoint.Sequence {
		if currentCheckpoint.Sequence > latestCheckpoint.Sequence {
			logger.Warn("new latest checkpoint is behind the current latest checkpoint, skip updating",
				zap.Reflect("current", currentCheckpoint),
				zap.Reflect("new", latestCheckpoint),
			)
		}
		return &checkpointMetrics{}, nil
	}

	logger.Info("setting new latest checkpoint")
	if err := checkpointer.Set(ctx, latestCheckpoint); err != nil {
		return nil, xerrors.Errorf("failed to set latest checkpoint: %w", err)
	}

	metricsResult, err := getCheckpointMetrics(
		ctx,
		latestCheckpoint,
		currentCheckpoint,
		a.chainStorageClient,
		request.EventTag,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to get metrics: %w", err)
	}

	logger.Info(
		"finished checkpoint synchronizer",
		zap.Reflect("checkpoint", latestCheckpoint),
		zap.Reflect("metrics", metricsResult),
	)
	return metricsResult, nil
}
