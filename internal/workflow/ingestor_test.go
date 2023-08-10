package workflow

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	xapi "github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/cadence"
	"github.com/coinbase/chainnode/internal/clients"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/controller"
	"github.com/coinbase/chainnode/internal/storage"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
	"github.com/coinbase/chainnode/internal/workflow/activity"
)

const (
	ingestorTag            = uint32(2)
	ingestorBatchSize      = uint64(30)
	ingestorMiniBatchSize  = uint64(5)
	ingestorCheckpointSize = 2
	ingestorSequence       = api.Sequence(123)
	ingestorHeight         = uint64(ingestorSequence + testutil.SequenceOffset)
	ingestorCollection     = xapi.CollectionBlocks
	ingestorMetricSequence = int64(ingestorSequence)
	ingestorMetricHeight   = ingestorHeight
	ingestorMetricGap      = int64(2)
	ingestorEventTag       = uint32(1)
)

type IngestorTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env      *cadence.TestEnv
	cfg      *config.Config
	ctrl     *gomock.Controller
	ingestor *Ingestor
	app      testapp.TestApp
}

func TestIngestorTestSuite(t *testing.T) {
	suite.Run(t, new(IngestorTestSuite))
}

func (s *IngestorTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	// Override config to speed up the test.
	cfg, err := config.New()
	require.NoError(err)
	cfg.Workflows.Ingestor.BatchSize = ingestorBatchSize
	cfg.Workflows.Ingestor.MiniBatchSize = ingestorMiniBatchSize
	cfg.Workflows.Ingestor.CheckpointSize = ingestorCheckpointSize
	s.cfg = cfg
	s.env = cadence.NewTestEnv(s)
	s.ctrl = gomock.NewController(s.T())
	s.app = testapp.New(
		s.T(),
		Module,
		clients.Module,
		controller.Module,
		storage.Module,
		storage.WithEmptyStorage(),
		testapp.WithConfig(s.cfg),
		cadence.WithTestEnv(s.env),
		fx.Populate(&s.ingestor),
	)
}

func (s *IngestorTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
	s.env.AssertExpectations(s.T())
}

func (s *IngestorTestSuite) TestDefaultParams() {
	s.testWithBatchSize(ingestorBatchSize)
}

func (s *IngestorTestSuite) TestWithBatchSize31() {
	s.testWithBatchSize(31)
}

func (s *IngestorTestSuite) TestWithBatchSize33() {
	s.testWithBatchSize(33)
}

func (s *IngestorTestSuite) TestWithBatchSize39() {
	s.testWithBatchSize(39)
}

func (s *IngestorTestSuite) testWithBatchSize(batchSize uint64) {
	require := testutil.Require(s.T())
	cfg := &s.app.Config().Workflows.Ingestor

	currentCheckpoint := &api.Checkpoint{
		Collection: ingestorCollection,
		Tag:        ingestorTag,
		Sequence:   ingestorSequence,
		Height:     ingestorHeight,
	}

	s.env.OnActivity(activity.ActivityCheckpointReader, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.CheckpointReaderRequest) (*activity.CheckpointReaderResponse, error) {
			require.Equal(ingestorTag, request.Tag)
			require.Equal(ingestorCollection, request.Collection)
			return &activity.CheckpointReaderResponse{
				Checkpoint: currentCheckpoint,
			}, nil
		})

	s.env.OnActivity(activity.ActivityStreamer, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.StreamerRequest) (*activity.StreamerResponse, error) {
			require.Equal(batchSize, request.BatchSize)
			require.Equal(currentCheckpoint, request.Checkpoint)
			require.Equal(ingestorEventTag, request.EventTag)

			events := testutil.MakeBlockEvents(currentCheckpoint.Sequence, int(batchSize), ingestorTag, ingestorEventTag)
			lastEvent := events[len(events)-1]
			currentCheckpoint.Sequence = api.ParseSequenceNum(lastEvent.SequenceNum)
			currentCheckpoint.Height = lastEvent.Block.Height
			return &activity.StreamerResponse{
				Events:         events,
				NextCheckpoint: currentCheckpoint,
			}, nil
		})

	seen := sync.Map{}
	s.env.OnActivity(activity.ActivityTransformer, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.TransformerRequest) (*activity.TransformerResponse, error) {
			require.Equal(ingestorTag, request.Tag)
			require.Equal(ingestorCollection, request.Collection)
			require.True(len(request.Events) == int(cfg.MiniBatchSize) ||
				len(request.Events) == int(batchSize%cfg.MiniBatchSize))
			for _, event := range request.Events {
				_, ok := seen.LoadOrStore(event.Block.Height, struct{}{})
				require.False(ok)
			}
			return &activity.TransformerResponse{}, nil
		})

	s.env.OnActivity(activity.ActivityCheckpointWriter, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.CheckpointWriterRequest) (*activity.CheckpointWriterResponse, error) {
			require.Equal(currentCheckpoint, request.Checkpoint)
			require.Equal(ingestorEventTag, request.EventTag)
			return &activity.CheckpointWriterResponse{
				Sequence: ingestorMetricSequence,
				Height:   ingestorMetricHeight,
				Gap:      ingestorMetricGap,
			}, nil
		}).Times(ingestorCheckpointSize)

	request := &IngestorRequest{
		Tag:        ingestorTag,
		Collection: ingestorCollection,
		EventTag:   ingestorEventTag,
	}
	if batchSize != ingestorBatchSize {
		// Override batch size if it is different from the default value.
		request.BatchSize = batchSize
	}

	_, err := s.ingestor.Execute(context.Background(), request)
	require.Error(err)
	require.True(IsContinueAsNewError(err))

	for i := uint64(0); i < batchSize*ingestorCheckpointSize; i++ {
		height := ingestorHeight + i + 1
		_, ok := seen.Load(height)
		require.True(ok, "block %v not seen", i)
	}
}

func (s *IngestorTestSuite) TestWithEmptyEvents() {
	require := testutil.Require(s.T())
	batchSize := ingestorBatchSize

	currentCheckpoint := &api.Checkpoint{
		Collection: ingestorCollection,
		Tag:        ingestorTag,
		Sequence:   ingestorSequence,
		Height:     ingestorHeight,
	}

	s.env.OnActivity(activity.ActivityCheckpointReader, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.CheckpointReaderRequest) (*activity.CheckpointReaderResponse, error) {
			require.Equal(ingestorTag, request.Tag)
			require.Equal(ingestorCollection, request.Collection)
			return &activity.CheckpointReaderResponse{
				Checkpoint: currentCheckpoint,
			}, nil
		})

	s.env.OnActivity(activity.ActivityStreamer, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.StreamerRequest) (*activity.StreamerResponse, error) {
			require.Equal(batchSize, request.BatchSize)
			require.Equal(currentCheckpoint, request.Checkpoint)
			require.Equal(ingestorEventTag, request.EventTag)

			return &activity.StreamerResponse{
				Events:         []*api.Event{},
				NextCheckpoint: currentCheckpoint,
			}, nil
		})

	request := &IngestorRequest{
		Collection: ingestorCollection,
		BatchSize:  ingestorBatchSize,
		EventTag:   ingestorEventTag,
		Tag:        ingestorTag,
	}

	_, err := s.ingestor.Execute(context.Background(), request)
	require.Error(err)
	require.True(IsContinueAsNewError(err))
}

func (s *IngestorTestSuite) TestIngestAtTip() {
	require := testutil.Require(s.T())
	batchSize := ingestorBatchSize

	currentCheckpoint := &api.Checkpoint{
		Collection: ingestorCollection,
		Tag:        ingestorTag,
		Sequence:   ingestorSequence,
		Height:     ingestorHeight,
	}

	s.env.OnActivity(activity.ActivityCheckpointReader, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.CheckpointReaderRequest) (*activity.CheckpointReaderResponse, error) {
			require.Equal(ingestorTag, request.Tag)
			require.Equal(ingestorCollection, request.Collection)
			return &activity.CheckpointReaderResponse{
				Checkpoint: currentCheckpoint,
			}, nil
		})

	s.env.OnActivity(activity.ActivityStreamer, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.StreamerRequest) (*activity.StreamerResponse, error) {
			require.Equal(batchSize, request.BatchSize)
			require.Equal(currentCheckpoint, request.Checkpoint)
			require.Equal(ingestorEventTag, request.EventTag)

			events := testutil.MakeBlockEvents(currentCheckpoint.Sequence, int(batchSize-1), ingestorTag, ingestorEventTag)
			lastEvent := events[len(events)-1]
			currentCheckpoint.Sequence = api.ParseSequenceNum(lastEvent.SequenceNum)
			currentCheckpoint.Height = lastEvent.Block.Height
			return &activity.StreamerResponse{
				Events:         events,
				NextCheckpoint: currentCheckpoint,
			}, nil
		}).Times(int(s.cfg.Workflows.Ingestor.CheckpointSizeAtTip))

	s.env.OnActivity(activity.ActivityTransformer, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.TransformerRequest) (*activity.TransformerResponse, error) {
			require.Equal(ingestorTag, request.Tag)
			require.Equal(ingestorCollection, request.Collection)
			return &activity.TransformerResponse{}, nil
		})

	s.env.OnActivity(activity.ActivityCheckpointWriter, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.CheckpointWriterRequest) (*activity.CheckpointWriterResponse, error) {
			require.Equal(currentCheckpoint, request.Checkpoint)
			require.Equal(ingestorEventTag, request.EventTag)
			return &activity.CheckpointWriterResponse{
				Sequence: ingestorMetricSequence,
				Height:   ingestorMetricHeight,
				Gap:      ingestorMetricGap,
			}, nil
		}).Times(int(s.cfg.Workflows.Ingestor.CheckpointSizeAtTip))

	request := &IngestorRequest{
		Collection: ingestorCollection,
		BatchSize:  ingestorBatchSize,
		EventTag:   ingestorEventTag,
		Tag:        ingestorTag,
	}

	_, err := s.ingestor.Execute(context.Background(), request)
	require.Error(err)
	require.True(IsContinueAsNewError(err))
}

func (s *IngestorTestSuite) TestWithSequence() {
	require := testutil.Require(s.T())

	currentCheckpoint := &api.Checkpoint{
		Collection: ingestorCollection,
		Tag:        ingestorTag,
		Sequence:   ingestorSequence,
	}

	s.env.OnActivity(activity.ActivityStreamer, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.StreamerRequest) (*activity.StreamerResponse, error) {
			require.Equal(ingestorBatchSize, request.BatchSize)
			require.Equal(currentCheckpoint, request.Checkpoint)
			require.Equal(ingestorEventTag, request.EventTag)

			events := testutil.MakeBlockEvents(currentCheckpoint.Sequence, int(ingestorBatchSize), ingestorTag, ingestorEventTag)
			lastEvent := events[len(events)-1]
			currentCheckpoint.Sequence = api.ParseSequenceNum(lastEvent.SequenceNum)
			currentCheckpoint.Height = lastEvent.Block.Height
			return &activity.StreamerResponse{
				Events:         events,
				NextCheckpoint: currentCheckpoint,
			}, nil
		})

	seen := sync.Map{}
	s.env.OnActivity(activity.ActivityTransformer, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.TransformerRequest) (*activity.TransformerResponse, error) {
			require.Equal(ingestorTag, request.Tag)
			require.Equal(ingestorCollection, request.Collection)
			for _, event := range request.Events {
				_, ok := seen.LoadOrStore(event.Block.Height, struct{}{})
				require.False(ok)
			}
			return &activity.TransformerResponse{}, nil
		})

	s.env.OnActivity(activity.ActivityCheckpointWriter, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.CheckpointWriterRequest) (*activity.CheckpointWriterResponse, error) {
			require.Equal(currentCheckpoint, request.Checkpoint)
			require.Equal(ingestorEventTag, request.EventTag)
			return &activity.CheckpointWriterResponse{
				Sequence: ingestorMetricSequence,
				Height:   ingestorMetricHeight,
				Gap:      ingestorMetricGap,
			}, nil
		}).Times(ingestorCheckpointSize)

	_, err := s.ingestor.Execute(context.Background(), &IngestorRequest{
		Collection: ingestorCollection,
		Sequence:   ingestorSequence,
		EventTag:   ingestorEventTag,
		Tag:        ingestorTag,
	})
	require.Error(err)
	require.True(IsContinueAsNewError(err))

	for i := uint64(0); i < ingestorBatchSize*ingestorCheckpointSize; i++ {
		height := ingestorHeight + i + 1
		_, ok := seen.Load(height)
		require.True(ok, "block %v not seen", i)
	}
}

func (s *IngestorTestSuite) TestSingleTransformer() {
	const (
		numEvents = 2
	)

	require := testutil.Require(s.T())
	currentCheckpoint := &api.Checkpoint{
		Collection: ingestorCollection,
		Tag:        ingestorTag,
		Sequence:   ingestorSequence,
		Height:     ingestorHeight,
	}

	s.env.OnActivity(activity.ActivityCheckpointReader, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.CheckpointReaderRequest) (*activity.CheckpointReaderResponse, error) {
			require.Equal(ingestorTag, request.Tag)
			require.Equal(ingestorCollection, request.Collection)
			return &activity.CheckpointReaderResponse{
				Checkpoint: currentCheckpoint,
			}, nil
		})

	s.env.OnActivity(activity.ActivityStreamer, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.StreamerRequest) (*activity.StreamerResponse, error) {
			require.Equal(ingestorBatchSize, request.BatchSize)
			require.Equal(currentCheckpoint, request.Checkpoint)
			require.Equal(ingestorEventTag, request.EventTag)

			events := testutil.MakeBlockEvents(currentCheckpoint.Sequence, numEvents, ingestorTag, ingestorEventTag)
			lastEvent := events[len(events)-1]
			currentCheckpoint.Sequence = api.ParseSequenceNum(lastEvent.SequenceNum)
			currentCheckpoint.Height = lastEvent.Block.Height
			return &activity.StreamerResponse{
				Events:         events,
				NextCheckpoint: currentCheckpoint,
			}, nil
		})

	seen := sync.Map{}
	s.env.OnActivity(activity.ActivityTransformer, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.TransformerRequest) (*activity.TransformerResponse, error) {
			require.Equal(ingestorTag, request.Tag)
			require.Equal(ingestorCollection, request.Collection)
			require.Equal(uint64(numEvents), request.Parallelism)
			for _, event := range request.Events {
				_, ok := seen.LoadOrStore(event.Block.Height, struct{}{})
				require.False(ok)
			}
			return &activity.TransformerResponse{}, nil
		}).Times(int(s.cfg.Workflows.Ingestor.CheckpointSizeAtTip))

	s.env.OnActivity(activity.ActivityCheckpointWriter, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.CheckpointWriterRequest) (*activity.CheckpointWriterResponse, error) {
			require.Equal(currentCheckpoint, request.Checkpoint)
			require.Equal(ingestorEventTag, request.EventTag)
			return &activity.CheckpointWriterResponse{
				Sequence: ingestorMetricSequence,
				Height:   ingestorMetricHeight,
				Gap:      ingestorMetricGap,
			}, nil
		}).Times(int(s.cfg.Workflows.Ingestor.CheckpointSizeAtTip))

	_, err := s.ingestor.Execute(context.Background(), &IngestorRequest{
		Collection: ingestorCollection,
		EventTag:   ingestorEventTag,
		Tag:        ingestorTag,
	})
	require.Error(err)
	require.True(IsContinueAsNewError(err))

	for i := uint64(0); i < numEvents*ingestorCheckpointSize; i++ {
		height := ingestorHeight + i + 1
		_, ok := seen.Load(height)
		require.True(ok, "block %v not seen", i)
	}
}

func (s *IngestorTestSuite) TestTransformerError() {
	require := testutil.Require(s.T())
	currentCheckpoint := &api.Checkpoint{
		Collection: ingestorCollection,
		Tag:        ingestorTag,
		Sequence:   ingestorSequence,
		Height:     ingestorHeight,
	}

	s.env.OnActivity(activity.ActivityCheckpointReader, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.CheckpointReaderRequest) (*activity.CheckpointReaderResponse, error) {
			require.Equal(ingestorTag, request.Tag)
			require.Equal(ingestorCollection, request.Collection)
			return &activity.CheckpointReaderResponse{
				Checkpoint: currentCheckpoint,
			}, nil
		})

	s.env.OnActivity(activity.ActivityStreamer, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.StreamerRequest) (*activity.StreamerResponse, error) {
			require.Equal(ingestorBatchSize, request.BatchSize)
			require.Equal(currentCheckpoint, request.Checkpoint)
			require.Equal(ingestorEventTag, request.EventTag)
			events := testutil.MakeBlockEvents(currentCheckpoint.Sequence, int(ingestorBatchSize), ingestorTag, ingestorEventTag)
			lastEvent := events[len(events)-1]
			currentCheckpoint.Sequence = api.ParseSequenceNum(lastEvent.SequenceNum)
			currentCheckpoint.Height = lastEvent.Block.Height
			return &activity.StreamerResponse{
				Events:         events,
				NextCheckpoint: currentCheckpoint,
			}, nil
		})

	counter := int32(0)
	errTransformer := xerrors.New("transformer error")
	s.env.OnActivity(activity.ActivityTransformer, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.TransformerRequest) (*activity.TransformerResponse, error) {
			if atomic.AddInt32(&counter, 1) > 2 {
				// Simulate an error in transformer.
				// Note that the activity is retried a number of times,
				// so we need to return the error repeatedly.
				return nil, errTransformer
			}
			return &activity.TransformerResponse{}, nil
		})

	_, err := s.ingestor.Execute(context.Background(), &IngestorRequest{
		Collection: ingestorCollection,
		EventTag:   ingestorEventTag,
		Tag:        ingestorTag,
	})
	require.Error(err)
	require.Contains(err.Error(), errTransformer.Error())
}

func (s *IngestorTestSuite) TestSignalInterrupt() {
	require := testutil.Require(s.T())

	s.env.OnActivity(activity.ActivityCheckpointReader, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.CheckpointReaderRequest) (*activity.CheckpointReaderResponse, error) {
			require.Equal(ingestorTag, request.Tag)
			require.Equal(ingestorCollection, request.Collection)
			return &activity.CheckpointReaderResponse{
				Checkpoint: &api.Checkpoint{
					Collection: ingestorCollection,
					Tag:        ingestorTag,
					Sequence:   ingestorSequence,
					Height:     ingestorHeight,
				},
			}, nil
		}).
		Maybe()

	s.env.RegisterDelayedCallback(func() {
		s.env.SignalWorkflow(workflowSignalChannel, workflowSignalInterrupt)
	}, 0)

	_, err := s.ingestor.Execute(context.Background(), &IngestorRequest{
		Collection: ingestorCollection,
		Tag:        ingestorTag,
		EventTag:   ingestorEventTag,
	})
	require.NoError(err)
}

func (s *IngestorTestSuite) TestSignalInterruptWhileProcessingEvents() {
	require := testutil.Require(s.T())
	currentCheckpoint := &api.Checkpoint{
		Collection: ingestorCollection,
		Tag:        ingestorTag,
		Sequence:   ingestorSequence,
		Height:     ingestorHeight,
	}

	s.env.OnActivity(activity.ActivityCheckpointReader, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.CheckpointReaderRequest) (*activity.CheckpointReaderResponse, error) {
			require.Equal(ingestorTag, request.Tag)
			require.Equal(ingestorCollection, request.Collection)
			return &activity.CheckpointReaderResponse{
				Checkpoint: currentCheckpoint,
			}, nil
		})

	s.env.OnActivity(activity.ActivityStreamer, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.StreamerRequest) (*activity.StreamerResponse, error) {
			require.Equal(ingestorBatchSize, request.BatchSize)
			require.Equal(currentCheckpoint, request.Checkpoint)
			require.Equal(ingestorEventTag, request.EventTag)
			events := testutil.MakeBlockEvents(currentCheckpoint.Sequence, int(ingestorBatchSize), ingestorTag, ingestorEventTag)
			lastEvent := events[len(events)-1]
			currentCheckpoint.Sequence = api.ParseSequenceNum(lastEvent.SequenceNum)
			currentCheckpoint.Height = lastEvent.Block.Height
			return &activity.StreamerResponse{
				Events:         events,
				NextCheckpoint: currentCheckpoint,
			}, nil
		})

	counter := int32(0)
	s.env.OnActivity(activity.ActivityTransformer, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.TransformerRequest) (*activity.TransformerResponse, error) {
			if atomic.AddInt32(&counter, 1) == 5 {
				s.env.SignalWorkflow(workflowSignalChannel, workflowSignalInterrupt)
			}
			return &activity.TransformerResponse{}, nil
		})

	s.env.OnActivity(activity.ActivityCheckpointWriter, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.CheckpointWriterRequest) (*activity.CheckpointWriterResponse, error) {
			require.Equal(ingestorEventTag, request.EventTag)
			return &activity.CheckpointWriterResponse{
				Sequence: ingestorMetricSequence,
				Height:   ingestorMetricHeight,
				Gap:      ingestorMetricGap,
			}, nil
		}).
		Maybe()

	_, err := s.ingestor.Execute(context.Background(), &IngestorRequest{
		Collection: ingestorCollection,
		EventTag:   ingestorEventTag,
		Tag:        ingestorTag,
	})
	require.NoError(err)
}
