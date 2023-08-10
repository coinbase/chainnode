package activity

import (
	"context"
	"time"

	"github.com/go-playground/validator/v10"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	chainstorageapi "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/cadence"
	"github.com/coinbase/chainnode/internal/clients/chainstorage"
	"github.com/coinbase/chainnode/internal/utils/log"
	"github.com/coinbase/chainnode/internal/workflow/instrument"
)

type (
	baseActivity struct {
		name       string
		runtime    cadence.Runtime
		validate   *validator.Validate
		instrument *instrument.Instrument
	}

	checkpointMetrics struct {
		Sequence           int64
		Height             uint64
		Gap                int64
		Delta              int64
		TimeSinceLastBlock time.Duration
	}
)

const (
	ActivityCheckpointReader       = "activity.checkpoint_reader"
	ActivityCheckpointSynchronizer = "activity.checkpoint_synchronizer"
	ActivityCheckpointWriter       = "activity.checkpoint_writer"
	ActivityStreamer               = "activity.streamer"
	ActivityTransformer            = "activity.transformer"

	loggerMsg = "activity.request"
)

func newBaseActivity(name string, runtime cadence.Runtime) baseActivity {
	return baseActivity{
		name:       name,
		runtime:    runtime,
		validate:   validator.New(),
		instrument: instrument.New(runtime, name, loggerMsg),
	}
}

func (a *baseActivity) register(activityFn interface{}) {
	a.runtime.RegisterActivity(activityFn, activity.RegisterOptions{
		Name: a.name,
	})
}

func (a *baseActivity) executeActivity(ctx workflow.Context, request interface{}, response interface{}, opts ...instrument.Option) error {
	opts = append(
		opts,
		instrument.WithLoggerField(zap.String("activity", a.name)),
		instrument.WithLoggerField(zap.Reflect("request", request)),
		instrument.WithFilter(IsCanceledError),
	)
	return a.instrument.Instrument(ctx, func() error {
		if err := a.validateRequest(request); err != nil {
			return err
		}

		if err := a.runtime.ExecuteActivity(ctx, a.name, request, response); err != nil {
			return xerrors.Errorf("failed to execute activity (name=%v): %w", a.name, err)
		}

		return nil
	}, opts...)
}

func (a *baseActivity) validateRequest(request interface{}) error {
	if err := a.validate.Struct(request); err != nil {
		return xerrors.Errorf("invalid activity request (name=%v, request=%+v): %w", a.name, request, err)
	}

	return nil
}

func (a *baseActivity) getLogger(ctx context.Context) *zap.Logger {
	info := activity.GetInfo(ctx)
	logger := a.runtime.GetActivityLogger(ctx)
	logger = log.WithPackage(logger)
	logger = log.WithSpan(ctx, logger)
	logger = logger.With(
		zap.Int32("Attempt", info.Attempt),
		zap.Time("StartedTime", info.StartedTime),
		zap.Time("Deadline", info.Deadline),
	)
	return logger
}

func IsCanceledError(err error) bool {
	return xerrors.Is(err, workflow.ErrCanceled)
}

func getCheckpointMetrics(
	ctx context.Context,
	newCheckpoint *api.Checkpoint,
	oldCheckpoint *api.Checkpoint,
	chainStorageClient chainstorage.Client,
	eventTag uint32,
) (*checkpointMetrics, error) {
	if newCheckpoint == nil {
		return &checkpointMetrics{}, nil
	}

	latestEvents, err := chainStorageClient.GetChainEvents(ctx, &chainstorageapi.GetChainEventsRequest{
		InitialPositionInStream: chainstorage.InitialPositionLatest,
		MaxNumEvents:            1,
		EventTag:                eventTag,
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to get latest chain events: %w", err)
	}
	if len(latestEvents) == 0 {
		return nil, xerrors.Errorf("got empty latest chain events")
	}
	latestSequence := api.ParseSequenceNum(latestEvents[0].SequenceNum)

	var timeSinceLastBlock time.Duration
	if !newCheckpoint.LastBlockTime.IsZero() {
		timeSinceLastBlock = time.Since(newCheckpoint.LastBlockTime)
	}

	return &checkpointMetrics{
		Sequence:           int64(newCheckpoint.Sequence),
		Height:             newCheckpoint.Height,
		Gap:                int64(latestSequence - newCheckpoint.Sequence),
		Delta:              int64(newCheckpoint.Sequence - oldCheckpoint.Sequence),
		TimeSinceLastBlock: timeSinceLastBlock,
	}, nil
}
