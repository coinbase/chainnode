package checkpoint

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	xapi "github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/storage/blob"
	"github.com/coinbase/chainnode/internal/storage/collection"
	"github.com/coinbase/chainnode/internal/storage/internal"
	"github.com/coinbase/chainnode/internal/storage/s3"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

type checkpointStorageIntegrationTestSuite struct {
	suite.Suite
	storage CheckpointStorage
	cfg     *config.Config
	app     testapp.TestApp
}

func (s *checkpointStorageIntegrationTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	var storage CheckpointStorage
	cfg, err := config.New()
	require.NoError(err)
	s.cfg = cfg
	s.app = testapp.New(
		s.T(),
		testapp.WithIntegration(),
		Module,
		blob.Module,
		collection.Module,
		s3.Module,
		testapp.WithConfig(s.cfg),
		fx.Populate(&storage),
	)
	s.storage = storage
}

func (s *checkpointStorageIntegrationTestSuite) TearDownTest() {
	if s.app != nil {
		s.app.Close()
	}
}

func TestIntegrationCheckpointStorageTestSuite(t *testing.T) {
	suite.Run(t, new(checkpointStorageIntegrationTestSuite))
}

func (s *checkpointStorageIntegrationTestSuite) TestPersistAndGetCheckpoint() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	collectionName := xapi.CollectionBlocks
	tag := uint32(1)
	sequence := api.Sequence(12312)
	height := uint64(123456)
	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")
	// first update attempt
	err := s.storage.PersistCheckpoint(ctx, &api.Checkpoint{
		Collection:    collectionName,
		Tag:           tag,
		Sequence:      sequence,
		Height:        height,
		LastBlockTime: blockTime,
	})
	require.NoError(err)

	persisted, err := s.storage.GetCheckpoint(ctx, collectionName, tag)
	require.NoError(err)
	require.NotNil(persisted)
	require.Equal(tag, persisted.Tag)
	require.Equal(sequence, persisted.Sequence)
	require.Equal(height, persisted.Height)
	require.Equal(blockTime, persisted.LastBlockTime)

	// second update attempt
	sequence = sequence + 20
	height = height + 10
	blockTime = testutil.MustTime("2020-11-24T17:07:21Z")
	err = s.storage.PersistCheckpoint(ctx, &api.Checkpoint{
		Collection:    collectionName,
		Tag:           tag,
		Sequence:      sequence,
		Height:        height,
		LastBlockTime: blockTime,
	})
	require.NoError(err)

	persisted, err = s.storage.GetCheckpoint(ctx, collectionName, tag)
	require.NoError(err)
	require.NotNil(persisted)
	require.Equal(tag, persisted.Tag)
	require.Equal(sequence, persisted.Sequence)
	require.Equal(height, persisted.Height)
	require.Equal(blockTime, persisted.LastBlockTime)
}

func (s *checkpointStorageIntegrationTestSuite) TestPersistAndGetCheckpoint_NoBlockTime() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	collectionName := xapi.CollectionBlocks
	tag := uint32(1)
	sequence := api.Sequence(12312)
	height := uint64(123456)
	// first update attempt
	err := s.storage.PersistCheckpoint(ctx, &api.Checkpoint{
		Collection: collectionName,
		Tag:        tag,
		Sequence:   sequence,
		Height:     height,
	})
	require.NoError(err)

	persisted, err := s.storage.GetCheckpoint(ctx, collectionName, tag)
	require.NoError(err)
	require.NotNil(persisted)
	require.Equal(tag, persisted.Tag)
	require.Equal(sequence, persisted.Sequence)
	require.Equal(height, persisted.Height)
	require.True(persisted.LastBlockTime.IsZero())
}

func (s *checkpointStorageIntegrationTestSuite) TestPersistAndGetCheckpoint_GlobalCheckpoints() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	collectionName := api.CollectionLatestCheckpoint
	tag := uint32(1)
	sequence := api.Sequence(12312)
	height := uint64(123456)
	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")
	numberOfShards := 10

	s.cfg.Storage.GlobalCheckpoint = config.GlobalCheckpointConfig{
		NumberOfShards: numberOfShards,
	}

	err := s.storage.PersistCheckpoint(ctx, &api.Checkpoint{
		Collection:    collectionName,
		Tag:           tag,
		Sequence:      sequence,
		Height:        height,
		LastBlockTime: blockTime,
	})
	require.NoError(err)

	persisted, err := s.storage.GetCheckpoint(ctx, collectionName, tag)
	require.NoError(err)
	require.NotNil(persisted)
	require.Equal(tag, persisted.Tag)
	require.Equal(sequence, persisted.Sequence)
	require.Equal(height, persisted.Height)
	require.Equal(blockTime, persisted.LastBlockTime)
}

func (s *checkpointStorageIntegrationTestSuite) TestGetCheckpoint_NotExists() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	tag := uint32(2)
	checkpoint, err := s.storage.GetCheckpoint(ctx, xapi.CollectionLogsV2, tag)
	require.Error(err)
	require.True(xerrors.Is(err, internal.ErrItemNotFound))
	require.Nil(checkpoint)
}

func (s *checkpointStorageIntegrationTestSuite) TestPersistAndGetCheckpoints() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	collectionNames := []api.Collection{xapi.CollectionBlocks, xapi.CollectionLogsV2}
	tag := uint32(1)
	sequences := []api.Sequence{12312, 6}
	heights := []uint64{123456, 2345}
	blockTimes := []time.Time{
		testutil.MustTime("2020-11-24T16:07:21Z"),
		testutil.MustTime("2020-12-24T16:07:21Z"),
	}

	expected := make([]*api.Checkpoint, len(collectionNames))
	for i, collectionName := range collectionNames {
		expected[i] = &api.Checkpoint{
			Collection:    collectionName,
			Tag:           tag,
			Sequence:      sequences[i],
			Height:        heights[i],
			LastBlockTime: blockTimes[i],
		}
	}

	for i := range collectionNames {
		err := s.storage.PersistCheckpoint(ctx, expected[i])
		require.NoError(err)
	}

	checkpoints, err := s.storage.GetCheckpoints(context.Background(), collectionNames, tag)
	require.NoError(err)
	require.Equal(len(collectionNames), len(checkpoints))

	for _, checkpoint := range checkpoints {
		for i := range expected {
			if expected[i].Collection == checkpoint.Collection {
				require.False(checkpoint.UpdatedAt.IsZero())
				expected[i].UpdatedAt = checkpoint.UpdatedAt
				require.Equal(expected[i], checkpoint)
				break
			}
		}
	}
}

func (s *checkpointStorageIntegrationTestSuite) TestGetCheckpoints_NotExists() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	tag := uint32(2)
	checkpoint, err := s.storage.GetCheckpoints(ctx, []api.Collection{xapi.CollectionLogsV2}, tag)
	require.Error(err)
	require.True(xerrors.Is(err, internal.ErrItemNotFound))
	require.Nil(checkpoint)
}

func (s *checkpointStorageIntegrationTestSuite) TestPersistAndGetCheckpoints_OneDoesNotExist() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	collectionNames := []api.Collection{xapi.CollectionBlocks, xapi.CollectionLogsV2}
	tag := uint32(1)
	sequences := []api.Sequence{12312, 6}
	heights := []uint64{123456, 2345}
	blockTimes := []string{"2020-11-24T16:07:21Z", "2020-12-24T16:07:21Z"}

	expected := make([]*api.Checkpoint, len(collectionNames))
	for i, collectionName := range collectionNames {
		expected[i] = &api.Checkpoint{
			Collection:    collectionName,
			Tag:           tag,
			Sequence:      sequences[i],
			Height:        heights[i],
			LastBlockTime: testutil.MustTime(blockTimes[i]),
		}
	}

	for i := range collectionNames {
		err := s.storage.PersistCheckpoint(ctx, expected[i])
		require.NoError(err)
	}

	unexpectedNames := []api.Collection{xapi.CollectionBlocks, xapi.CollectionTransactions}
	checkpoints, err := s.storage.GetCheckpoints(context.Background(), unexpectedNames, tag)
	require.Error(err)
	require.Nil(checkpoints)
}
