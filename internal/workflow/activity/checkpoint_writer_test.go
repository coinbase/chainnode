package activity

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"

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

type CheckpointWriterTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env                *cadence.TestEnv
	ctrl               *gomock.Controller
	checkpointer       *controllermocks.MockCheckpointer
	controller         *controllermocks.MockController
	chainStorageClient *chainstoragemocks.MockClient
	checkpointWriter   *CheckpointWriter
	app                testapp.TestApp
}

func TestCheckpointWriterTestSuite(t *testing.T) {
	suite.Run(t, new(CheckpointWriterTestSuite))
}

func (s *CheckpointWriterTestSuite) SetupTest() {
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
		fx.Populate(&s.checkpointWriter),
	)
}

func (s *CheckpointWriterTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
	s.env.AssertExpectations(s.T())
}

func (s *CheckpointWriterTestSuite) TestHappyCase() {
	const (
		collection  = xapi.CollectionBlocks
		tag         = uint32(1)
		eventTag    = uint32(2)
		oldSequence = api.Sequence(10100)
		newSequence = oldSequence + 100
		oldHeight   = uint64(10_000)
		newHeight   = oldHeight + 100
	)

	require := testutil.Require(s.T())
	oldCheckpoint := &api.Checkpoint{
		Collection:    collection,
		Tag:           tag,
		Sequence:      oldSequence,
		Height:        oldHeight,
		LastBlockTime: testutil.MustTime("2019-11-24T16:07:21Z"),
	}
	s.checkpointer.EXPECT().
		Get(gomock.Any(), collection, tag).
		Return(oldCheckpoint, nil)
	newCheckpoint := &api.Checkpoint{
		Collection:    collection,
		Tag:           tag,
		Sequence:      newSequence,
		Height:        newHeight,
		LastBlockTime: testutil.MustTime("2020-11-24T16:07:21Z"),
	}
	s.checkpointer.EXPECT().
		Set(gomock.Any(), newCheckpoint).
		Return(nil)
	latestEvents := testutil.MakeBlockEvents(newSequence, 1, tag, eventTag)
	s.chainStorageClient.EXPECT().GetChainEvents(gomock.Any(), &chainstorageapi.GetChainEventsRequest{
		InitialPositionInStream: chainstorage.InitialPositionLatest,
		MaxNumEvents:            1,
		EventTag:                eventTag,
	}).Return(latestEvents, nil)

	ctx := s.env.BackgroundContext()
	response, err := s.checkpointWriter.Execute(ctx, &CheckpointWriterRequest{
		Checkpoint: newCheckpoint,
		EventTag:   eventTag,
	})
	require.NoError(err)
	require.Equal(int64(newSequence), response.Sequence)
	require.Equal(newHeight, response.Height)
	latestSequence := api.ParseSequenceNum(latestEvents[0].SequenceNum)
	require.Equal(int64(latestSequence-newSequence), response.Gap)
	require.NotZero(response.TimeSinceLastBlock)
}

func (s *CheckpointWriterTestSuite) TestEmptyLastBlockTime() {
	const (
		collection  = xapi.CollectionBlocks
		tag         = uint32(1)
		eventTag    = uint32(2)
		oldSequence = api.Sequence(10100)
		newSequence = oldSequence + 100
		oldHeight   = uint64(10_000)
		newHeight   = oldHeight + 100
	)

	require := testutil.Require(s.T())
	oldCheckpoint := &api.Checkpoint{
		Collection: collection,
		Tag:        tag,
		Sequence:   oldSequence,
		Height:     oldHeight,
	}
	s.checkpointer.EXPECT().
		Get(gomock.Any(), collection, tag).
		Return(oldCheckpoint, nil)
	newCheckpoint := &api.Checkpoint{
		Collection: collection,
		Tag:        tag,
		Sequence:   newSequence,
		Height:     newHeight,
	}
	s.checkpointer.EXPECT().
		Set(gomock.Any(), newCheckpoint).
		Return(nil)
	latestEvents := testutil.MakeBlockEvents(newSequence, 1, tag, eventTag)
	s.chainStorageClient.EXPECT().GetChainEvents(gomock.Any(), &chainstorageapi.GetChainEventsRequest{
		InitialPositionInStream: chainstorage.InitialPositionLatest,
		MaxNumEvents:            1,
		EventTag:                eventTag,
	}).Return(latestEvents, nil)

	ctx := s.env.BackgroundContext()
	response, err := s.checkpointWriter.Execute(ctx, &CheckpointWriterRequest{
		Checkpoint: newCheckpoint,
		EventTag:   eventTag,
	})
	require.NoError(err)
	require.Equal(int64(newSequence), response.Sequence)
	require.Equal(newHeight, response.Height)
	latestSequence := api.ParseSequenceNum(latestEvents[0].SequenceNum)
	require.Equal(int64(latestSequence-newSequence), response.Gap)
	require.Zero(response.TimeSinceLastBlock)
}

func (s *CheckpointWriterTestSuite) TestMoveBackwards() {
	const (
		collection  = xapi.CollectionBlocks
		tag         = uint32(1)
		eventTag    = uint32(2)
		oldSequence = api.Sequence(10100)
		newSequence = oldSequence - 1
		oldHeight   = uint64(10_000)
		newHeight   = oldHeight - 1
	)

	require := testutil.Require(s.T())
	oldCheckpoint := &api.Checkpoint{
		Collection: collection,
		Tag:        tag,
		Sequence:   oldSequence,
		Height:     oldHeight,
	}
	s.checkpointer.EXPECT().
		Get(gomock.Any(), collection, tag).
		Return(oldCheckpoint, nil)
	newCheckpoint := &api.Checkpoint{
		Collection: collection,
		Tag:        tag,
		Sequence:   newSequence,
		Height:     newHeight,
	}
	ctx := s.env.BackgroundContext()
	_, err := s.checkpointWriter.Execute(ctx, &CheckpointWriterRequest{
		Checkpoint: newCheckpoint,
		EventTag:   eventTag,
	})
	require.Error(err)
	require.Contains(err.Error(), "checkpoint cannot be moved backwards")
}

func (s *CheckpointWriterTestSuite) TestEmptyLatestEvents() {
	const (
		collection  = xapi.CollectionBlocks
		tag         = uint32(1)
		eventTag    = uint32(2)
		oldSequence = api.Sequence(10100)
		newSequence = oldSequence + 100
		oldHeight   = uint64(10_000)
		newHeight   = oldHeight + 100
	)

	require := testutil.Require(s.T())
	oldCheckpoint := &api.Checkpoint{
		Collection:    collection,
		Tag:           tag,
		Sequence:      oldSequence,
		Height:        oldHeight,
		LastBlockTime: testutil.MustTime("2019-11-24T16:07:21Z"),
	}
	s.checkpointer.EXPECT().
		Get(gomock.Any(), collection, tag).
		Return(oldCheckpoint, nil)
	newCheckpoint := &api.Checkpoint{
		Collection:    collection,
		Tag:           tag,
		Sequence:      newSequence,
		Height:        newHeight,
		LastBlockTime: testutil.MustTime("2020-11-24T16:07:21Z"),
	}
	s.checkpointer.EXPECT().
		Set(gomock.Any(), newCheckpoint).
		Return(nil)
	latestEvents := make([]*api.Event, 0)
	s.chainStorageClient.EXPECT().GetChainEvents(gomock.Any(), &chainstorageapi.GetChainEventsRequest{
		InitialPositionInStream: chainstorage.InitialPositionLatest,
		MaxNumEvents:            1,
		EventTag:                eventTag,
	}).Return(latestEvents, nil)

	ctx := s.env.BackgroundContext()
	response, err := s.checkpointWriter.Execute(ctx, &CheckpointWriterRequest{
		Checkpoint: newCheckpoint,
		EventTag:   eventTag,
	})
	require.Error(err)
	require.Contains(err.Error(), "got empty latest chain events")
	require.Equal(&CheckpointWriterResponse{}, response)
}
