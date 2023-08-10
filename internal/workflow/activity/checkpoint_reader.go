package activity

import (
	"context"

	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/cadence"
	"github.com/coinbase/chainnode/internal/controller"
	"github.com/coinbase/chainnode/internal/utils/fxparams"
)

type (
	CheckpointReader struct {
		baseActivity
		controller controller.Controller
	}

	CheckpointReaderParams struct {
		fx.In
		fxparams.Params
		Runtime    cadence.Runtime
		Controller controller.Controller
	}

	CheckpointReaderRequest struct {
		Tag        uint32         `validate:"required"`
		Collection api.Collection `validate:"required"`
	}

	CheckpointReaderResponse struct {
		Checkpoint *api.Checkpoint
	}
)

func NewCheckpointReader(params CheckpointReaderParams) *CheckpointReader {
	a := &CheckpointReader{
		baseActivity: newBaseActivity(ActivityCheckpointReader, params.Runtime),
		controller:   params.Controller,
	}
	a.register(a.execute)
	return a
}

func (a *CheckpointReader) Execute(ctx workflow.Context, request *CheckpointReaderRequest) (*CheckpointReaderResponse, error) {
	var response CheckpointReaderResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *CheckpointReader) execute(ctx context.Context, request *CheckpointReaderRequest) (*CheckpointReaderResponse, error) {
	logger := a.getLogger(ctx)
	checkpointer := a.controller.Checkpointer()
	checkpoint, err := checkpointer.Get(ctx, request.Collection, request.Tag)
	if err != nil {
		return nil, xerrors.Errorf("failed to get checkpoint: %w", err)
	}

	logger.Info("last checkpoint", zap.Reflect("checkpoint", checkpoint))
	return &CheckpointReaderResponse{
		Checkpoint: checkpoint,
	}, nil
}
