package activity

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"

	chainstorageapi "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"

	"github.com/coinbase/chainnode/internal/api"
	xapi "github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/cadence"
	"github.com/coinbase/chainnode/internal/clients/chainstorage"
	chainstoragemocks "github.com/coinbase/chainnode/internal/clients/chainstorage/mocks"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

type StreamerTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env                *cadence.TestEnv
	ctrl               *gomock.Controller
	chainStorageClient *chainstoragemocks.MockClient
	streamer           *Streamer
	app                testapp.TestApp
}

func TestStreamerTestSuite(t *testing.T) {
	suite.Run(t, new(StreamerTestSuite))
}

func (s *StreamerTestSuite) SetupTest() {
	s.env = cadence.NewTestActivityEnv(s)
	s.ctrl = gomock.NewController(s.T())
	s.chainStorageClient = chainstoragemocks.NewMockClient(s.ctrl)
	s.app = testapp.New(
		s.T(),
		Module,
		cadence.WithTestEnv(s.env),
		fx.Provide(func() chainstorage.Client { return s.chainStorageClient }),
		fx.Populate(&s.streamer),
	)
}

func (s *StreamerTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
	s.env.AssertExpectations(s.T())
}

func (s *StreamerTestSuite) TestStreamer() {
	const (
		collection = xapi.CollectionBlocks
		tag        = 1
		eventTag   = 2
		sequence   = api.Sequence(10100)
		numEvents  = 100
		batchSize  = 1000
	)

	require := testutil.Require(s.T())

	events := testutil.MakeBlockEvents(sequence, numEvents, tag, eventTag)
	startHeight := events[0].Block.Height
	endHeight := events[numEvents-1].Block.Height
	s.chainStorageClient.EXPECT().GetChainEvents(gomock.Any(), &chainstorageapi.GetChainEventsRequest{
		SequenceNum:  sequence.AsInt64(),
		MaxNumEvents: batchSize,
	}).Return(events, nil)

	ctx := s.env.BackgroundContext()
	response, err := s.streamer.Execute(ctx, &StreamerRequest{
		Checkpoint: &api.Checkpoint{
			Collection: collection,
			Tag:        tag,
			Sequence:   sequence,
			Height:     startHeight,
		},
		BatchSize: batchSize,
	})
	require.NoError(err)
	require.Equal(numEvents, len(response.Events))
	nextSequence := api.ParseSequenceNum(events[len(events)-1].SequenceNum)
	require.NoError(err)
	require.Equal(&api.Checkpoint{
		Collection:    collection,
		Tag:           tag,
		Sequence:      nextSequence,
		Height:        endHeight,
		LastBlockTime: testutil.MustTime("2020-11-24T16:07:21Z"),
	}, response.NextCheckpoint)
}

func (s *StreamerTestSuite) TestStreamer_UseFirstEventsTimestamp() {
	const (
		collection = xapi.CollectionBlocks
		tag        = 1
		eventTag   = 2
		sequence   = api.Sequence(10100)
		numEvents  = 2 // only has two events
		batchSize  = 1000
	)

	require := testutil.Require(s.T())

	events := testutil.MakeBlockEvents(sequence, numEvents, tag, eventTag)
	events[numEvents-1].Block.Timestamp = &timestamp.Timestamp{Seconds: time.Now().Unix()}
	startHeight := events[0].Block.Height
	endHeight := events[numEvents-1].Block.Height
	s.chainStorageClient.EXPECT().GetChainEvents(gomock.Any(), &chainstorageapi.GetChainEventsRequest{
		SequenceNum:  sequence.AsInt64(),
		MaxNumEvents: batchSize,
	}).Return(events, nil)

	ctx := s.env.BackgroundContext()
	response, err := s.streamer.Execute(ctx, &StreamerRequest{
		Checkpoint: &api.Checkpoint{
			Collection: collection,
			Tag:        tag,
			Sequence:   sequence,
			Height:     startHeight,
		},
		BatchSize: batchSize,
	})
	require.NoError(err)
	require.Equal(numEvents, len(response.Events))
	nextSequence := api.ParseSequenceNum(events[len(events)-1].SequenceNum)
	require.NoError(err)
	require.Equal(&api.Checkpoint{
		Collection:    collection,
		Tag:           tag,
		Sequence:      nextSequence,
		Height:        endHeight,
		LastBlockTime: testutil.MustTime("2020-11-24T16:07:21Z"), // use the first event's timestamp
	}, response.NextCheckpoint)
}

func (s *StreamerTestSuite) TestStreamer_UseFirstAddedEventTimestamp() {
	const (
		collection = xapi.CollectionBlocks
		tag        = 1
		eventTag   = 2
		sequence   = api.Sequence(10100)
		numEvents  = 4
		batchSize  = 1000
	)

	require := testutil.Require(s.T())

	events := testutil.MakeBlockEvents(sequence, numEvents, tag, eventTag)
	irrelevantTimestamp := &timestamp.Timestamp{Seconds: time.Now().Unix()}
	events[0].Block.Timestamp = irrelevantTimestamp
	events[0].Type = api.EventRemoved
	events[1].Block.Timestamp = irrelevantTimestamp
	events[1].Type = api.EventRemoved
	events[3].Block.Timestamp = irrelevantTimestamp
	events[3].Type = api.EventRemoved
	startHeight := events[0].Block.Height
	endHeight := events[numEvents-1].Block.Height
	s.chainStorageClient.EXPECT().GetChainEvents(gomock.Any(), &chainstorageapi.GetChainEventsRequest{
		SequenceNum:  sequence.AsInt64(),
		MaxNumEvents: batchSize,
	}).Return(events, nil)

	ctx := s.env.BackgroundContext()
	response, err := s.streamer.Execute(ctx, &StreamerRequest{
		Checkpoint: &api.Checkpoint{
			Collection: collection,
			Tag:        tag,
			Sequence:   sequence,
			Height:     startHeight,
		},
		BatchSize: batchSize,
	})
	require.NoError(err)
	require.Equal(numEvents, len(response.Events))
	nextSequence := api.ParseSequenceNum(events[len(events)-1].SequenceNum)
	require.NoError(err)
	require.Equal(&api.Checkpoint{
		Collection:    collection,
		Tag:           tag,
		Sequence:      nextSequence,
		Height:        endHeight,
		LastBlockTime: testutil.MustTime("2020-11-24T16:07:21Z"), // use the first added event's timestamp
	}, response.NextCheckpoint)
}

func (s *StreamerTestSuite) TestStreamer_NilTimestamp() {
	const (
		collection = xapi.CollectionBlocks
		tag        = 1
		eventTag   = 2
		sequence   = api.Sequence(10100)
		numEvents  = 100
		batchSize  = 1000
	)

	require := testutil.Require(s.T())

	events := testutil.MakeBlockEvents(sequence, numEvents, tag, eventTag, testutil.WithoutTimestamp())
	startHeight := events[0].Block.Height
	endHeight := events[numEvents-1].Block.Height
	s.chainStorageClient.EXPECT().GetChainEvents(gomock.Any(), &chainstorageapi.GetChainEventsRequest{
		SequenceNum:  sequence.AsInt64(),
		MaxNumEvents: batchSize,
	}).Return(events, nil)

	ctx := s.env.BackgroundContext()
	response, err := s.streamer.Execute(ctx, &StreamerRequest{
		Checkpoint: &api.Checkpoint{
			Collection: collection,
			Tag:        tag,
			Sequence:   sequence,
			Height:     startHeight,
		},
		BatchSize: batchSize,
	})
	require.NoError(err)
	require.Equal(numEvents, len(response.Events))
	nextSequence := api.ParseSequenceNum(events[len(events)-1].SequenceNum)
	require.NoError(err)
	require.Equal(&api.Checkpoint{
		Collection: collection,
		Tag:        tag,
		Sequence:   nextSequence,
		Height:     endHeight,
	}, response.NextCheckpoint)
}

func (s *StreamerTestSuite) TestStreamer_Empty() {
	const (
		collection = xapi.CollectionBlocks
		tag        = uint32(1)
		sequence   = api.Sequence(10100)
		batchSize  = 1000
	)

	require := testutil.Require(s.T())

	events := make([]*api.Event, 0)
	s.chainStorageClient.EXPECT().GetChainEvents(gomock.Any(), &chainstorageapi.GetChainEventsRequest{
		SequenceNum:  sequence.AsInt64(),
		MaxNumEvents: batchSize,
	}).Return(events, nil)

	ctx := s.env.BackgroundContext()
	checkpoint := &api.Checkpoint{
		Collection: collection,
		Tag:        tag,
		Sequence:   sequence,
	}
	response, err := s.streamer.Execute(ctx, &StreamerRequest{
		BatchSize:  batchSize,
		Checkpoint: checkpoint,
	})
	require.NoError(err)
	require.Equal(0, len(response.Events))
	require.Equal(checkpoint, response.NextCheckpoint)
}
