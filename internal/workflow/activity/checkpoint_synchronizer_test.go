package activity

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	chainstorageapi "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"

	"github.com/coinbase/chainnode/internal/api"
	xapi "github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/cadence"
	"github.com/coinbase/chainnode/internal/clients/chainstorage"
	chainstoragemocks "github.com/coinbase/chainnode/internal/clients/chainstorage/mocks"
	"github.com/coinbase/chainnode/internal/controller"
	controllermocks "github.com/coinbase/chainnode/internal/controller/mocks"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

type CheckpointSynchronizerTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env                    *cadence.TestEnv
	ctrl                   *gomock.Controller
	checkpointer           *controllermocks.MockCheckpointer
	controller             *controllermocks.MockController
	chainStorageClient     *chainstoragemocks.MockClient
	checkpointSynchronizer *CheckpointSynchronizer
	app                    testapp.TestApp
}

func TestCheckpointSynchronizerTestSuite(t *testing.T) {
	suite.Run(t, new(CheckpointSynchronizerTestSuite))
}

func (s *CheckpointSynchronizerTestSuite) SetupTest() {
	s.env = cadence.NewTestActivityEnv(s)
	s.ctrl = gomock.NewController(s.T())
	s.checkpointer = controllermocks.NewMockCheckpointer(s.ctrl)
	s.controller = controllermocks.NewMockController(s.ctrl)
	s.controller.EXPECT().
		Checkpointer().
		Return(s.checkpointer).
		AnyTimes()
	s.chainStorageClient = chainstoragemocks.NewMockClient(s.ctrl)
	s.app = testapp.New(
		s.T(),
		Module,
		cadence.WithTestEnv(s.env),
		fx.Provide(func() controller.Controller { return s.controller }),
		fx.Provide(func() chainstorage.Client { return s.chainStorageClient }),
		fx.Populate(&s.checkpointSynchronizer),
	)
}

func (s *CheckpointSynchronizerTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
	s.env.AssertExpectations(s.T())
}

func (s *CheckpointSynchronizerTestSuite) TestWriteEarliestCheckpoint() {
	const (
		collection       = xapi.CollectionBlocks
		tag              = uint32(1)
		eventTag         = uint32(2)
		sequence         = api.Sequence(10100)
		height           = uint64(10_000)
		earliestSequence = 1
		earliestHeight   = earliestSequence + testutil.SequenceOffset
		backoff          = time.Millisecond * 10
	)

	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")
	oldBlockTime := testutil.MustTime("2020-10-24T16:07:21Z")
	require := testutil.Require(s.T())

	emptyCheckpoint := &api.Checkpoint{
		Collection: api.CollectionEarliestCheckpoint,
		Tag:        tag,
		Sequence:   api.InitialSequence,
		Height:     0,
	}
	earliestCheckpoint := &api.Checkpoint{
		Collection:    api.CollectionEarliestCheckpoint,
		Tag:           tag,
		Sequence:      earliestSequence,
		Height:        earliestHeight,
		LastBlockTime: blockTime,
	}
	s.checkpointer.EXPECT().
		Get(gomock.Any(), api.CollectionEarliestCheckpoint, tag).
		Return(emptyCheckpoint, nil)
	s.checkpointer.EXPECT().
		Set(gomock.Any(), earliestCheckpoint).
		Return(nil)
	s.chainStorageClient.EXPECT().
		GetChainEvents(gomock.Any(), &chainstorageapi.GetChainEventsRequest{
			InitialPositionInStream: chainstorage.InitialPositionEarliest,
			MaxNumEvents:            1,
			EventTag:                eventTag,
		}).
		Return(testutil.MakeBlockEvents(api.InitialSequence, 1, tag, eventTag), nil)

	oldCheckpoint := &api.Checkpoint{
		Collection:    collection,
		Tag:           tag,
		Sequence:      sequence - 1,
		Height:        height,
		LastBlockTime: oldBlockTime,
	}
	s.checkpointer.EXPECT().
		Get(gomock.Any(), api.CollectionLatestCheckpoint, tag).
		Return(oldCheckpoint, nil)

	checkpoint := &api.Checkpoint{
		Collection:    collection,
		Tag:           tag,
		Sequence:      sequence,
		Height:        height,
		LastBlockTime: blockTime,
	}
	s.checkpointer.EXPECT().
		GetEarliest(gomock.Any(), []api.Collection{collection}, tag).
		Return(checkpoint, nil)
	latestCheckpoint := &api.Checkpoint{
		Collection:    api.CollectionLatestCheckpoint,
		Tag:           tag,
		Sequence:      sequence,
		Height:        height,
		LastBlockTime: blockTime,
	}
	s.checkpointer.EXPECT().
		Set(gomock.Any(), latestCheckpoint).
		Return(nil)

	latestEvents := testutil.MakeBlockEvents(sequence, 1, tag, eventTag)
	s.chainStorageClient.EXPECT().GetChainEvents(gomock.Any(), &chainstorageapi.GetChainEventsRequest{
		InitialPositionInStream: chainstorage.InitialPositionLatest,
		MaxNumEvents:            1,
		EventTag:                eventTag,
	}).Return(latestEvents, nil)

	ctx := s.env.BackgroundContext()
	response, err := s.checkpointSynchronizer.Execute(ctx, &CheckpointSynchronizerRequest{
		Tag:         tag,
		Collections: []api.Collection{collection},
		Backoff:     backoff,
		EventTag:    eventTag,
	})
	require.NoError(err)
	require.Equal(int64(sequence), response.Sequence)
	require.Equal(height, response.Height)
	latestSequence := api.ParseSequenceNum(latestEvents[0].SequenceNum)
	require.Equal(int64(latestSequence-sequence), response.Gap)
	require.NotZero(response.TimeSinceLastBlock)
}

func (s *CheckpointSynchronizerTestSuite) TestWriteLatestCheckpoint() {
	const (
		collection = xapi.CollectionBlocks
		tag        = uint32(1)
		eventTag   = uint32(2)
		sequence   = api.Sequence(10100)
		height     = uint64(10_000)
		backoff    = time.Millisecond * 10
	)
	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")
	oldBlockTime := testutil.MustTime("2020-10-24T16:07:21Z")

	require := testutil.Require(s.T())

	earliestCheckpoint := &api.Checkpoint{
		Collection: api.CollectionEarliestCheckpoint,
		Tag:        tag,
		Sequence:   1,
		Height:     0,
	}
	s.checkpointer.EXPECT().
		Get(gomock.Any(), api.CollectionEarliestCheckpoint, tag).
		Return(earliestCheckpoint, nil)

	oldCheckpoint := &api.Checkpoint{
		Collection:    collection,
		Tag:           tag,
		Sequence:      sequence - 1,
		Height:        height,
		LastBlockTime: oldBlockTime,
	}
	s.checkpointer.EXPECT().
		Get(gomock.Any(), api.CollectionLatestCheckpoint, tag).
		Return(oldCheckpoint, nil)

	checkpoint := &api.Checkpoint{
		Collection:    collection,
		Tag:           tag,
		Sequence:      sequence,
		Height:        height,
		LastBlockTime: blockTime,
	}
	s.checkpointer.EXPECT().GetEarliest(gomock.Any(), []api.Collection{collection}, tag).Return(checkpoint, nil)

	latestCheckpoint := &api.Checkpoint{
		Collection:    api.CollectionLatestCheckpoint,
		Tag:           tag,
		Sequence:      sequence,
		Height:        height,
		LastBlockTime: blockTime,
	}
	s.checkpointer.EXPECT().
		Set(gomock.Any(), latestCheckpoint).
		Return(nil)

	latestEvents := testutil.MakeBlockEvents(sequence, 1, tag, eventTag)
	s.chainStorageClient.EXPECT().GetChainEvents(gomock.Any(), &chainstorageapi.GetChainEventsRequest{
		InitialPositionInStream: chainstorage.InitialPositionLatest,
		MaxNumEvents:            1,
		EventTag:                eventTag,
	}).Return(latestEvents, nil)

	ctx := s.env.BackgroundContext()
	response, err := s.checkpointSynchronizer.Execute(ctx, &CheckpointSynchronizerRequest{
		Tag:         tag,
		Collections: []api.Collection{collection},
		Backoff:     backoff,
		EventTag:    eventTag,
	})
	require.NoError(err)
	require.Equal(int64(sequence), response.Sequence)
	require.Equal(height, response.Height)
	latestSequence := api.ParseSequenceNum(latestEvents[0].SequenceNum)
	require.Equal(int64(latestSequence-sequence), response.Gap)
	require.NotZero(response.TimeSinceLastBlock)
}

func (s *CheckpointSynchronizerTestSuite) TestWriteLatestCheckpoint_EarliestIsEmpty() {
	const (
		collection = xapi.CollectionBlocks
		tag        = uint32(1)
		eventTag   = uint32(2)
		backoff    = time.Millisecond * 10
	)

	require := testutil.Require(s.T())

	earliestCheckpoint := &api.Checkpoint{
		Collection: api.CollectionEarliestCheckpoint,
		Tag:        tag,
		Sequence:   1,
		Height:     0,
	}
	s.checkpointer.EXPECT().
		Get(gomock.Any(), api.CollectionEarliestCheckpoint, tag).
		Return(earliestCheckpoint, nil)

	// empty checkpoint
	checkpoint := api.NewCheckpoint(collection, tag, api.InitialSequence, 0)
	s.checkpointer.EXPECT().GetEarliest(gomock.Any(), []api.Collection{collection}, tag).Return(checkpoint, nil)

	ctx := s.env.BackgroundContext()
	response, err := s.checkpointSynchronizer.Execute(ctx, &CheckpointSynchronizerRequest{
		Tag:         tag,
		Collections: []api.Collection{collection},
		Backoff:     backoff,
		EventTag:    eventTag,
	})
	require.NoError(err)
	require.Equal(&CheckpointSynchronizerResponse{}, response)
}

func (s *CheckpointSynchronizerTestSuite) TestWriteLatestCheckpoint_OldCheckpointWithoutBlockTime() {
	const (
		collection = xapi.CollectionBlocks
		tag        = uint32(1)
		eventTag   = uint32(2)
		sequence   = api.Sequence(10100)
		height     = uint64(10_000)
	)
	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")

	require := testutil.Require(s.T())

	earliestCheckpoint := &api.Checkpoint{
		Collection: api.CollectionEarliestCheckpoint,
		Tag:        tag,
		Sequence:   1,
		Height:     0,
	}
	s.checkpointer.EXPECT().
		Get(gomock.Any(), api.CollectionEarliestCheckpoint, tag).
		Return(earliestCheckpoint, nil)

	oldCheckpoint := &api.Checkpoint{
		Collection: collection,
		Tag:        tag,
		Sequence:   sequence - 1,
		Height:     height,
	}
	s.checkpointer.EXPECT().
		Get(gomock.Any(), api.CollectionLatestCheckpoint, tag).
		Return(oldCheckpoint, nil)

	checkpoint := &api.Checkpoint{
		Collection:    collection,
		Tag:           tag,
		Sequence:      sequence,
		Height:        height,
		LastBlockTime: blockTime,
	}
	s.checkpointer.EXPECT().GetEarliest(gomock.Any(), []api.Collection{collection}, tag).Return(checkpoint, nil)

	latestCheckpoint := &api.Checkpoint{
		Collection:    api.CollectionLatestCheckpoint,
		Tag:           tag,
		Sequence:      sequence,
		Height:        height,
		LastBlockTime: blockTime,
	}
	s.checkpointer.EXPECT().
		Set(gomock.Any(), latestCheckpoint).
		Return(nil)

	latestEvents := testutil.MakeBlockEvents(sequence, 1, tag, eventTag)
	s.chainStorageClient.EXPECT().GetChainEvents(gomock.Any(), &chainstorageapi.GetChainEventsRequest{
		InitialPositionInStream: chainstorage.InitialPositionLatest,
		MaxNumEvents:            1,
		EventTag:                eventTag,
	}).Return(latestEvents, nil)

	ctx := s.env.BackgroundContext()
	response, err := s.checkpointSynchronizer.Execute(ctx, &CheckpointSynchronizerRequest{
		Tag:         tag,
		Collections: []api.Collection{collection},
		Backoff:     0,
		EventTag:    eventTag,
	})
	require.NoError(err)
	require.Equal(int64(sequence), response.Sequence)
	require.Equal(height, response.Height)
	latestSequence := api.ParseSequenceNum(latestEvents[0].SequenceNum)
	require.Equal(int64(latestSequence-sequence), response.Gap)
	require.NotZero(response.TimeSinceLastBlock)
}

func (s *CheckpointSynchronizerTestSuite) TestWriteLatestCheckpoint_LatestIsBehind() {
	const (
		collection = xapi.CollectionBlocks
		tag        = uint32(1)
		eventTag   = uint32(2)
		sequence   = api.Sequence(10100)
		height     = uint64(10_000)
	)
	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")
	oldBlockTime := testutil.MustTime("2020-10-24T16:07:21Z")

	require := testutil.Require(s.T())

	earliestCheckpoint := &api.Checkpoint{
		Collection: api.CollectionEarliestCheckpoint,
		Tag:        tag,
		Sequence:   1,
		Height:     0,
	}
	s.checkpointer.EXPECT().
		Get(gomock.Any(), api.CollectionEarliestCheckpoint, tag).
		Return(earliestCheckpoint, nil)

	oldCheckpoint := &api.Checkpoint{
		Collection:    collection,
		Tag:           tag,
		Sequence:      sequence + 1,
		Height:        height,
		LastBlockTime: oldBlockTime,
	}
	s.checkpointer.EXPECT().
		Get(gomock.Any(), api.CollectionLatestCheckpoint, tag).
		Return(oldCheckpoint, nil)

	checkpoint := &api.Checkpoint{
		Collection:    collection,
		Tag:           tag,
		Sequence:      sequence,
		Height:        height,
		LastBlockTime: blockTime,
	}
	s.checkpointer.EXPECT().GetEarliest(gomock.Any(), []api.Collection{collection}, tag).Return(checkpoint, nil)

	ctx := s.env.BackgroundContext()
	response, err := s.checkpointSynchronizer.Execute(ctx, &CheckpointSynchronizerRequest{
		Tag:         tag,
		Collections: []api.Collection{collection},
		Backoff:     0,
		EventTag:    eventTag,
	})
	require.NoError(err)
	require.Equal(&CheckpointSynchronizerResponse{}, response)
}

func (s *CheckpointSynchronizerTestSuite) TestWriteLatestCheckpoint_LatestIsCurrent() {
	const (
		collection = xapi.CollectionBlocks
		tag        = uint32(1)
		eventTag   = uint32(2)
		sequence   = api.Sequence(10100)
		height     = uint64(10_000)
	)
	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")

	require := testutil.Require(s.T())

	earliestCheckpoint := &api.Checkpoint{
		Collection: api.CollectionEarliestCheckpoint,
		Tag:        tag,
		Sequence:   1,
		Height:     0,
	}
	s.checkpointer.EXPECT().
		Get(gomock.Any(), api.CollectionEarliestCheckpoint, tag).
		Return(earliestCheckpoint, nil)

	checkpoint := &api.Checkpoint{
		Collection:    collection,
		Tag:           tag,
		Sequence:      sequence,
		Height:        height,
		LastBlockTime: blockTime,
	}

	s.checkpointer.EXPECT().
		Get(gomock.Any(), api.CollectionLatestCheckpoint, tag).
		Return(checkpoint, nil)

	s.checkpointer.EXPECT().GetEarliest(gomock.Any(), []api.Collection{collection}, tag).Return(checkpoint, nil)

	ctx := s.env.BackgroundContext()
	response, err := s.checkpointSynchronizer.Execute(ctx, &CheckpointSynchronizerRequest{
		Tag:         tag,
		Collections: []api.Collection{collection},
		EventTag:    eventTag,
	})
	require.NoError(err)
	require.Equal(&CheckpointSynchronizerResponse{}, response)
}

func (s *CheckpointSynchronizerTestSuite) TestWriteLatestCheckpoint_FailedToGetCurrent() {
	const (
		collection = xapi.CollectionBlocks
		tag        = uint32(1)
		eventTag   = uint32(2)
		sequence   = api.Sequence(10100)
		height     = uint64(10_000)
	)

	require := testutil.Require(s.T())

	earliestCheckpoint := &api.Checkpoint{
		Collection: api.CollectionEarliestCheckpoint,
		Tag:        tag,
		Sequence:   1,
		Height:     0,
	}
	s.checkpointer.EXPECT().
		Get(gomock.Any(), api.CollectionEarliestCheckpoint, tag).
		Return(earliestCheckpoint, nil)

	s.checkpointer.EXPECT().
		Get(gomock.Any(), api.CollectionLatestCheckpoint, tag).
		Return(nil, xerrors.Errorf("mock error: failed to get the current checkpoint"))

	checkpoint := &api.Checkpoint{
		Collection: collection,
		Tag:        tag,
		Sequence:   sequence,
		Height:     height,
	}
	s.checkpointer.EXPECT().GetEarliest(gomock.Any(), []api.Collection{collection}, tag).Return(checkpoint, nil)

	ctx := s.env.BackgroundContext()
	_, err := s.checkpointSynchronizer.Execute(ctx, &CheckpointSynchronizerRequest{
		Tag:         tag,
		Collections: []api.Collection{collection},
		Backoff:     0,
		EventTag:    eventTag,
	})
	require.Error(err)
}
