package internal

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	xapi "github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/storage"
	storagemocks "github.com/coinbase/chainnode/internal/storage/mocks"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

type CheckpointerTestSuite struct {
	suite.Suite
	ctrl         *gomock.Controller
	app          testapp.TestApp
	storage      *storagemocks.MockCheckpointStorage
	checkpointer Checkpointer
}

func TestCheckpointerTestSuite(t *testing.T) {
	suite.Run(t, new(CheckpointerTestSuite))
}

func (s *CheckpointerTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.storage = storagemocks.NewMockCheckpointStorage(s.ctrl)
	s.app = testapp.New(
		s.T(),
		Module,
		fx.Provide(func() storage.CheckpointStorage { return s.storage }),
		fx.Populate(&s.checkpointer),
	)
}

func (s *CheckpointerTestSuite) TearDownTest() {
	s.app.Close()
}

func (s *CheckpointerTestSuite) TestGet() {
	const (
		collection = xapi.CollectionBlocks
		tag        = uint32(1)
		sequence   = api.Sequence(10_100)
		height     = 10_000
	)

	require := testutil.Require(s.T())
	expected := &api.Checkpoint{
		Collection: collection,
		Tag:        tag,
		Sequence:   sequence,
		Height:     height,
	}
	s.storage.EXPECT().
		GetCheckpoint(gomock.Any(), xapi.CollectionBlocks, tag).
		Return(expected, nil)

	actual, err := s.checkpointer.Get(context.Background(), collection, tag)
	require.NoError(err)
	require.Equal(expected, actual)
}

func (s *CheckpointerTestSuite) TestGetWithoutTag() {
	const (
		collection = xapi.CollectionBlocks
	)

	require := testutil.Require(s.T())
	_, err := s.checkpointer.Get(context.Background(), collection, 0)
	require.Error(err)
}

func (s *CheckpointerTestSuite) TestGetNotFound() {
	const (
		collection = xapi.CollectionBlocks
		tag        = uint32(1)
	)

	require := testutil.Require(s.T())
	expected := &api.Checkpoint{
		Collection: collection,
		Tag:        tag,
		Sequence:   0,
		Height:     0,
	}
	s.storage.EXPECT().
		GetCheckpoint(gomock.Any(), xapi.CollectionBlocks, tag).
		Return(nil, xerrors.Errorf("mock error: %w", storage.ErrItemNotFound))

	actual, err := s.checkpointer.Get(context.Background(), collection, tag)
	require.NoError(err)
	require.Equal(expected, actual)
}

func (s *CheckpointerTestSuite) TestGetInitialCheckpoint() {
	const (
		collection = xapi.CollectionBlocks
		tag        = uint32(1)
	)

	require := testutil.Require(s.T())
	for _, checkpoint := range []*api.Checkpoint{
		{
			Collection: collection,
			Tag:        0,
			Sequence:   123,
			Height:     100,
		},
		{
			Collection: collection,
			Tag:        1,
			Sequence:   0,
			Height:     100,
		},
	} {
		expected := &api.Checkpoint{
			Collection: collection,
			Tag:        tag,
			Sequence:   0,
			Height:     0,
		}
		s.storage.EXPECT().
			GetCheckpoint(gomock.Any(), xapi.CollectionBlocks, tag).
			Return(checkpoint, nil)

		actual, err := s.checkpointer.Get(context.Background(), collection, tag)
		require.NoError(err)
		require.Equal(expected, actual)
	}
}

func (s *CheckpointerTestSuite) TestGetEarliest() {
	const (
		collection = xapi.CollectionBlocks
		tag        = uint32(1)
		sequence   = api.Sequence(10100)
		height     = 10_000
	)

	require := testutil.Require(s.T())
	expected := &api.Checkpoint{
		Collection: collection,
		Tag:        tag,
		Sequence:   sequence,
		Height:     height,
	}

	checkpoints := []*api.Checkpoint{
		{
			Collection: "test-1",
			Tag:        tag,
			Sequence:   sequence + 1,
			Height:     height,
		},
		expected,
		{
			Collection: "test-2",
			Tag:        tag,
			Sequence:   sequence + 2,
			Height:     height,
		},
	}
	s.storage.EXPECT().
		GetCheckpoints(gomock.Any(), []api.Collection{collection, "test-1", "test-2"}, tag).
		Return(checkpoints, nil)

	actual, err := s.checkpointer.GetEarliest(context.Background(), []api.Collection{collection, "test-1", "test-2"}, tag)
	require.NoError(err)
	require.Equal(expected, actual)
}

func (s *CheckpointerTestSuite) TestGetEarliestNotFound() {
	const (
		collection = xapi.CollectionBlocks
		tag        = uint32(1)
	)

	require := testutil.Require(s.T())
	expected := &api.Checkpoint{
		Collection: collection,
		Tag:        tag,
		Sequence:   0,
		Height:     0,
	}

	s.storage.EXPECT().
		GetCheckpoints(gomock.Any(), []api.Collection{collection, "test-1", "test-2"}, tag).
		Return(nil, xerrors.Errorf("mock error: %w", storage.ErrItemNotFound))

	actual, err := s.checkpointer.GetEarliest(context.Background(), []api.Collection{collection, "test-1", "test-2"}, tag)
	require.NoError(err)
	require.Equal(expected, actual)
}

func (s *CheckpointerTestSuite) TestGetEarliestInitialCheckpoint() {
	const (
		collection = xapi.CollectionBlocks
		tag        = uint32(1)
	)

	require := testutil.Require(s.T())
	for _, checkpoint := range []*api.Checkpoint{
		{
			Collection: collection,
			Tag:        0,
			Sequence:   123,
			Height:     100,
		},
		{
			Collection: collection,
			Tag:        1,
			Sequence:   0,
			Height:     100,
		},
	} {
		expected := &api.Checkpoint{
			Collection: collection,
			Tag:        tag,
			Sequence:   0,
			Height:     0,
		}
		s.storage.EXPECT().
			GetCheckpoints(gomock.Any(), []api.Collection{collection, "test-1", "test-2"}, tag).
			Return([]*api.Checkpoint{checkpoint}, nil)

		actual, err := s.checkpointer.GetEarliest(context.Background(), []api.Collection{collection, "test-1", "test-2"}, tag)
		require.NoError(err)
		require.Equal(expected, actual)
	}
}

func (s *CheckpointerTestSuite) TestGetEarliestWithoutTag() {
	require := testutil.Require(s.T())
	_, err := s.checkpointer.GetEarliest(context.Background(), xapi.ParentCollections, 0)
	require.Error(err)
}

func (s *CheckpointerTestSuite) TestSet() {
	const (
		collection = xapi.CollectionBlocks
		tag        = uint32(1)
		sequence   = api.Sequence(10_100)
		height     = 10_000
	)

	require := testutil.Require(s.T())
	checkpoint := &api.Checkpoint{
		Collection: collection,
		Tag:        tag,
		Sequence:   sequence,
		Height:     height,
	}
	s.storage.EXPECT().
		PersistCheckpoint(gomock.Any(), checkpoint).
		Return(nil)

	err := s.checkpointer.Set(context.Background(), checkpoint)
	require.NoError(err)
}
