package activity

import (
	"context"
	"time"

	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/cadence"
	"github.com/coinbase/chainnode/internal/clients/chainstorage"
	"github.com/coinbase/chainnode/internal/controller"
	"github.com/coinbase/chainnode/internal/utils/fxparams"
)

type (
	CheckpointWriter struct {
		baseActivity
		controller         controller.Controller
		chainStorageClient chainstorage.Client
	}

	CheckpointWriterParams struct {
		fx.In
		fxparams.Params
		Runtime            cadence.Runtime
		Controller         controller.Controller
		ChainStorageClient chainstorage.Client
	}

	CheckpointWriterRequest struct {
		Checkpoint *api.Checkpoint `validate:"required"`
		EventTag   uint32
	}

	CheckpointWriterResponse struct {
		Sequence           int64
		Height             uint64
		Gap                int64
		TimeSinceLastBlock time.Duration
	}
)

func NewCheckpointWriter(params CheckpointWriterParams) *CheckpointWriter {
	a := &CheckpointWriter{
		baseActivity:       newBaseActivity(ActivityCheckpointWriter, params.Runtime),
		controller:         params.Controller,
		chainStorageClient: params.ChainStorageClient,
	}
	a.register(a.execute)
	return a
}

func (a *CheckpointWriter) Execute(ctx workflow.Context, request *CheckpointWriterRequest) (*CheckpointWriterResponse, error) {
	var response CheckpointWriterResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *CheckpointWriter) execute(ctx context.Context, request *CheckpointWriterRequest) (*CheckpointWriterResponse, error) {
	logger := a.getLogger(ctx)
	checkpointer := a.controller.Checkpointer()
	newCheckpoint := request.Checkpoint
	oldCheckpoint, err := checkpointer.Get(ctx, newCheckpoint.Collection, newCheckpoint.Tag)
	if err != nil {
		return nil, xerrors.Errorf("failed to get last checkpoint: %w", err)
	}

	if newCheckpoint.Sequence < oldCheckpoint.Sequence {
		return nil, xerrors.Errorf("checkpoint cannot be moved backwards (new=%v, old=%v)", newCheckpoint.Sequence, oldCheckpoint.Sequence)
	}

	if err := checkpointer.Set(ctx, newCheckpoint); err != nil {
		return nil, xerrors.Errorf("failed to set local checkpoint: %w", err)
	}

	metricsResult, err := getCheckpointMetrics(
		ctx,
		newCheckpoint,
		oldCheckpoint,
		a.chainStorageClient,
		request.EventTag,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to get metrics: %w", err)
	}

	logger.Info(
		"finished checkpoint writer",
		zap.Reflect("checkpoint", newCheckpoint),
		zap.Reflect("metrics", metricsResult),
	)
	return &CheckpointWriterResponse{
		Sequence:           metricsResult.Sequence,
		Height:             metricsResult.Height,
		Gap:                metricsResult.Gap,
		TimeSinceLastBlock: metricsResult.TimeSinceLastBlock,
	}, nil
}
