package activity

import (
	"context"

	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	chainstorageapi "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/cadence"
	"github.com/coinbase/chainnode/internal/clients/chainstorage"
	"github.com/coinbase/chainnode/internal/utils/fxparams"
	"github.com/coinbase/chainnode/internal/utils/timeutil"
)

type (
	Streamer struct {
		baseActivity
		chainStorageClient chainstorage.Client
	}

	StreamerParams struct {
		fx.In
		fxparams.Params
		Runtime            cadence.Runtime
		ChainStorageClient chainstorage.Client
	}

	StreamerRequest struct {
		Checkpoint *api.Checkpoint `validate:"required"`
		BatchSize  uint64          `validate:"required"`
		EventTag   uint32
	}

	StreamerResponse struct {
		Events         []*api.Event
		NextCheckpoint *api.Checkpoint
	}
)

func NewStreamer(params StreamerParams) *Streamer {
	a := &Streamer{
		baseActivity:       newBaseActivity(ActivityStreamer, params.Runtime),
		chainStorageClient: params.ChainStorageClient,
	}
	a.register(a.execute)
	return a
}

func (a *Streamer) Execute(ctx workflow.Context, request *StreamerRequest) (*StreamerResponse, error) {
	var response StreamerResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *Streamer) execute(ctx context.Context, request *StreamerRequest) (*StreamerResponse, error) {
	logger := a.getLogger(ctx)
	events, err := a.chainStorageClient.GetChainEvents(ctx, &chainstorageapi.GetChainEventsRequest{
		SequenceNum:  request.Checkpoint.Sequence.AsInt64(),
		MaxNumEvents: request.BatchSize,
		EventTag:     request.EventTag,
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to get chain events: %w", err)
	}

	numEvents := len(events)
	nextCheckpoint := *request.Checkpoint
	if numEvents > 0 {
		lastEvent := events[numEvents-1]
		lastSequence := api.ParseSequenceNum(lastEvent.SequenceNum)

		nextCheckpoint.Sequence = lastSequence
		nextCheckpoint.Height = lastEvent.Block.Height

		// Use the timestamp of the first added event to measure time_since_last_block.
		for _, event := range events {
			if event.Type == api.EventAdded {
				nextCheckpoint.LastBlockTime = timeutil.TimestampToTime(event.Block.Timestamp)
				break
			}
		}
	}

	logger.Info(
		"finished streamer",
		zap.Int("numEvents", numEvents),
		zap.Reflect("nextCheckpoint", nextCheckpoint),
	)
	return &StreamerResponse{
		NextCheckpoint: &nextCheckpoint,
		Events:         events,
	}, nil
}
