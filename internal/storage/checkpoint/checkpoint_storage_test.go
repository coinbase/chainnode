package checkpoint

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	xapi "github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/storage/checkpoint/models"
	"github.com/coinbase/chainnode/internal/storage/collection"
	cmocks "github.com/coinbase/chainnode/internal/storage/collection/mocks"
	"github.com/coinbase/chainnode/internal/storage/internal"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

type checkpointStorageTestSuite struct {
	suite.Suite
	ctrl              *gomock.Controller
	collectionStorage *cmocks.MockCollectionStorage
	storage           CheckpointStorage
	app               testapp.TestApp
	blockTimeFixture  time.Time
	cfg               *config.Config
}

const (
	cpTagFixture          = uint32(1)
	cpPartitionKeyFixture = "2#checkpoints"
	cpSortKeyFixture      = "test-collection"
)

func TestCheckpointStorageTestSuite(t *testing.T) {
	suite.Run(t, new(checkpointStorageTestSuite))
}

func (s *checkpointStorageTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	s.ctrl = gomock.NewController(s.T())
	s.collectionStorage = cmocks.NewMockCollectionStorage(s.ctrl)
	s.collectionStorage.EXPECT().
		WithCollection(api.CollectionCheckpoints).
		Return(s.collectionStorage)
	cfg, err := config.New()
	require.NoError(err)
	s.cfg = cfg

	s.app = testapp.New(
		s.T(),
		Module,
		testapp.WithConfig(s.cfg),
		fx.Provide(fx.Annotated{Name: "collection", Target: func() collection.CollectionStorage { return s.collectionStorage }}),
		fx.Populate(&s.storage),
	)
}

func (s *checkpointStorageTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
}

func (s *checkpointStorageTestSuite) TestPersistCheckpoint() {
	require := testutil.Require(s.T())

	tag := uint32(1)
	sequence := api.Sequence(12312)
	height := uint64(123456)
	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")

	s.collectionStorage.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			ctx context.Context,
			request *collection.UpdateItemRequest,
		) (interface{}, error) {
			require.NotNil(request.ExpressionAttributesNames)
			require.NotEmpty(request.ExpressionAttributeValues)
			require.NotEmpty(request.Key)
			require.NotNil(request.UpdateExpression)
			require.Equal(reflect.TypeOf(models.CheckpointDDBEntry{}), request.EntryType)
			return &models.CheckpointDDBEntry{}, nil
		})
	err := s.storage.PersistCheckpoint(context.Background(), &api.Checkpoint{
		Collection:    "test-collection",
		Tag:           tag,
		Sequence:      sequence,
		Height:        height,
		LastBlockTime: blockTime,
	})
	require.NoError(err)
}

func (s *checkpointStorageTestSuite) TestPersistCheckpoint_GlobalCheckpoint() {
	require := testutil.Require(s.T())

	// override config var
	numberOfShards := 10
	s.cfg.Storage.GlobalCheckpoint = config.GlobalCheckpointConfig{
		NumberOfShards: numberOfShards,
	}

	tag := uint32(1)
	sequence := api.Sequence(12312)
	height := uint64(123456)
	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")

	// write to shards
	shardUpdateExpression := ""
	s.collectionStorage.EXPECT().TransactWriteItems(gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			ctx context.Context,
			request *collection.TransactWriteItemsRequest,
		) error {
			require.Equal(numberOfShards, len(request.TransactItems))
			for i, transactItem := range request.TransactItems {
				require.NotNil(transactItem.Update)
				require.Nil(transactItem.Put)
				update := transactItem.Update
				require.NotNil(update.ExpressionAttributesNames)
				require.NotEmpty(update.ExpressionAttributeValues)
				require.NotEmpty(update.Key)
				require.NotNil(update.UpdateExpression)

				if i == 0 {
					shardUpdateExpression = *update.UpdateExpression
				} else {
					require.Equal(shardUpdateExpression, *update.UpdateExpression)
				}

				require.Equal(fmt.Sprintf("1#checkpoints#%v", i+1), *update.Key["pk"].S)
				require.Equal("latest", *update.Key["sk"].S)
			}
			return nil
		})

	err := s.storage.PersistCheckpoint(context.Background(), &api.Checkpoint{
		Collection:    api.CollectionLatestCheckpoint,
		Tag:           tag,
		Sequence:      sequence,
		Height:        height,
		LastBlockTime: blockTime,
	})
	require.NoError(err)
}

func (s *checkpointStorageTestSuite) TestPersistCheckpoint_UpdateError() {
	require := testutil.Require(s.T())

	tag := uint32(1)
	sequence := api.Sequence(12312)
	height := uint64(123456)
	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")

	s.collectionStorage.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).
		Return(nil, xerrors.Errorf("failure"))
	err := s.storage.PersistCheckpoint(context.Background(), &api.Checkpoint{
		Collection:    "test-collection",
		Tag:           tag,
		Sequence:      sequence,
		Height:        height,
		LastBlockTime: blockTime,
	})
	require.Error(err)
}

func (s *checkpointStorageTestSuite) TestGetCheckpoint_NoBlockTime() {
	require := testutil.Require(s.T())

	tag := uint32(1)
	sequence := api.Sequence(12312)
	height := uint64(123456)
	collectionName := api.Collection("test-collection")

	entry := &models.CheckpointDDBEntry{
		BaseItem: collection.NewBaseItem(
			cpPartitionKeyFixture,
			cpSortKeyFixture,
			cpTagFixture,
		),
		Sequence: sequence,
		Height:   height,
	}

	checkpoint := &api.Checkpoint{
		Collection: collectionName,
		Tag:        tag,
		Sequence:   sequence,
		Height:     height,
		UpdatedAt:  testutil.MustTime(entry.UpdatedAt),
	}

	s.collectionStorage.EXPECT().GetItem(gomock.Any(), gomock.Any()).Return(entry, nil)
	actualCheckpoint, err := s.storage.GetCheckpoint(context.Background(), collectionName, tag)
	require.NoError(err)
	require.Equal(checkpoint, actualCheckpoint)
}

func (s *checkpointStorageTestSuite) TestGetCheckpoint() {
	require := testutil.Require(s.T())

	tag := uint32(1)
	sequence := api.Sequence(12312)
	height := uint64(123456)
	collectionName := api.Collection("test-collection")
	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")

	entry := &models.CheckpointDDBEntry{
		BaseItem: collection.NewBaseItem(
			cpPartitionKeyFixture,
			cpSortKeyFixture,
			cpTagFixture,
		),
		Sequence:      sequence,
		Height:        height,
		LastBlockTime: "2020-11-24T16:07:21Z",
	}

	checkpoint := &api.Checkpoint{
		Collection:    collectionName,
		Tag:           tag,
		Sequence:      sequence,
		Height:        height,
		UpdatedAt:     testutil.MustTime(entry.UpdatedAt),
		LastBlockTime: blockTime,
	}

	s.collectionStorage.EXPECT().GetItem(gomock.Any(), gomock.Any()).Return(entry, nil)
	actualCheckpoint, err := s.storage.GetCheckpoint(context.Background(), collectionName, tag)
	require.NoError(err)
	require.Equal(checkpoint, actualCheckpoint)
}

func (s *checkpointStorageTestSuite) TestGetCheckpoint_GlobalCheckpoint() {
	require := testutil.Require(s.T())

	// override config var
	numberOfShards := 10
	s.cfg.Storage.GlobalCheckpoint = config.GlobalCheckpointConfig{
		NumberOfShards: numberOfShards,
	}

	tag := uint32(2)
	sequence := api.Sequence(12312)
	height := uint64(123456)
	collectionName := api.CollectionLatestCheckpoint
	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")

	entry := &models.CheckpointDDBEntry{
		BaseItem: collection.NewBaseItem(
			cpPartitionKeyFixture,
			collectionName.String(),
			tag,
		),
		Sequence:      sequence,
		Height:        height,
		LastBlockTime: "2020-11-24T16:07:21Z",
	}

	checkpoint := &api.Checkpoint{
		Collection:    collectionName,
		Tag:           tag,
		Sequence:      sequence,
		Height:        height,
		UpdatedAt:     testutil.MustTime(entry.UpdatedAt),
		LastBlockTime: blockTime,
	}

	s.collectionStorage.EXPECT().GetItem(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, request *collection.GetItemRequest) (interface{}, error) {
			pk := request.KeyMap["pk"].(string)
			pkStrings := strings.Split(pk, "#")

			require.Equal(3, len(pkStrings))
			index, err := strconv.ParseInt(pkStrings[2], 10, 64)
			require.NoError(err)
			require.LessOrEqual(index, int64(numberOfShards))
			require.Less(int64(0), index)
			entry.PartitionKey = pk
			return entry, nil
		})
	actualCheckpoint, err := s.storage.GetCheckpoint(context.Background(), collectionName, tag)
	require.NoError(err)
	require.Equal(checkpoint, actualCheckpoint)
}

func (s *checkpointStorageTestSuite) TestGetCheckpoint_GetFailure() {
	require := testutil.Require(s.T())

	tag := uint32(1)
	sequence := api.Sequence(12312)
	height := uint64(123456)
	collectionName := api.Collection("test-collection")
	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")

	checkpoint := &api.Checkpoint{
		Collection:    collectionName,
		Tag:           tag,
		Sequence:      sequence,
		Height:        height,
		LastBlockTime: blockTime,
	}

	entry, err := models.MakeCheckpointDDBEntry(checkpoint, models.PartitionKeyIndexWithoutSharding)
	require.NoError(err)
	require.NotNil(entry)

	request := &collection.GetItemRequest{
		KeyMap: collection.StringMap{
			"pk": "1#checkpoints",
			"sk": "test-collection",
		},
		EntryType: reflect.TypeOf(models.CheckpointDDBEntry{}),
	}
	s.collectionStorage.EXPECT().GetItem(gomock.Any(), request).
		Return(nil, xerrors.Errorf("failure"))
	actualCheckpoint, err := s.storage.GetCheckpoint(context.Background(), collectionName, tag)
	require.Error(err)
	require.Nil(actualCheckpoint)
}

func (s *checkpointStorageTestSuite) TestGetCheckpoints_NoBlockTime() {
	require := testutil.Require(s.T())

	tag := uint32(1)

	collectionNames := []api.Collection{xapi.CollectionBlocks, xapi.CollectionLogsV2}
	sequences := []api.Sequence{12312, 6}
	heights := []uint64{123456, 2345}

	entries := make([]interface{}, len(collectionNames))
	for i := range collectionNames {
		entry := &models.CheckpointDDBEntry{
			BaseItem: collection.NewBaseItem(
				models.MakeCheckpointPartitionKey(tag, models.PartitionKeyIndexWithoutSharding),
				models.MakeCheckpointSortKey(collectionNames[i]),
				tag,
			),
			Sequence: sequences[i],
			Height:   heights[i],
		}
		entries[i] = entry
	}

	expected := make([]*api.Checkpoint, len(collectionNames))
	for i := range entries {
		entry, ok := entries[i].(*models.CheckpointDDBEntry)
		require.True(ok)
		expected[i] = &api.Checkpoint{
			Collection: collectionNames[i],
			Tag:        tag,
			Sequence:   entry.Sequence,
			Height:     entry.Height,
			UpdatedAt:  testutil.MustTime(entry.UpdatedAt),
		}
	}

	s.collectionStorage.EXPECT().GetItems(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *collection.GetItemsRequest) ([]interface{}, error) {
			require.Equal(reflect.TypeOf(models.CheckpointDDBEntry{}), request.EntryType)
			require.Equal(len(collectionNames), len(request.KeyMaps))
			return entries, nil
		})
	checkpoints, err := s.storage.GetCheckpoints(context.Background(), collectionNames, tag)
	require.NoError(err)
	for i, checkpoint := range checkpoints {
		require.Equal(expected[i], checkpoint)
	}
}

func (s *checkpointStorageTestSuite) TestGetCheckpoints() {
	require := testutil.Require(s.T())

	tag := uint32(1)

	collectionNames := []api.Collection{xapi.CollectionBlocks, xapi.CollectionLogsV2}
	sequences := []api.Sequence{12312, 6}
	heights := []uint64{123456, 2345}
	blockTimes := []string{"2020-11-24T16:07:21Z", "2020-12-24T16:07:21Z"}

	entries := make([]interface{}, len(collectionNames))
	for i := range collectionNames {
		entry := &models.CheckpointDDBEntry{
			BaseItem: collection.NewBaseItem(
				models.MakeCheckpointPartitionKey(tag, models.PartitionKeyIndexWithoutSharding),
				models.MakeCheckpointSortKey(collectionNames[i]),
				tag,
			),
			Sequence:      sequences[i],
			Height:        heights[i],
			LastBlockTime: blockTimes[i],
		}
		entries[i] = entry
	}

	expected := make([]*api.Checkpoint, len(collectionNames))
	for i := range entries {
		entry, ok := entries[i].(*models.CheckpointDDBEntry)
		require.True(ok)
		expected[i] = &api.Checkpoint{
			Collection:    collectionNames[i],
			Tag:           tag,
			Sequence:      entry.Sequence,
			Height:        entry.Height,
			LastBlockTime: testutil.MustTime(blockTimes[i]),
			UpdatedAt:     testutil.MustTime(entry.UpdatedAt),
		}
	}

	s.collectionStorage.EXPECT().GetItems(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *collection.GetItemsRequest) ([]interface{}, error) {
			require.Equal(reflect.TypeOf(models.CheckpointDDBEntry{}), request.EntryType)
			require.Equal(len(collectionNames), len(request.KeyMaps))
			return entries, nil
		})
	checkpoints, err := s.storage.GetCheckpoints(context.Background(), collectionNames, tag)
	require.NoError(err)
	for i, checkpoint := range checkpoints {
		require.Equal(expected[i], checkpoint)
	}
}

func (s *checkpointStorageTestSuite) TestGetCheckpoints_Mismatch() {
	require := testutil.Require(s.T())

	tag := uint32(1)

	collectionNames := []api.Collection{xapi.CollectionBlocks, xapi.CollectionLogsV2}
	sequences := []api.Sequence{12312, 6}
	heights := []uint64{123456, 2345}
	blockTimes := []string{"2020-11-24T16:07:21Z", "2020-12-24T16:07:21Z"}

	entries := make([]interface{}, len(collectionNames))
	for i := range collectionNames {
		entry := &models.CheckpointDDBEntry{
			BaseItem: collection.NewBaseItem(
				models.MakeCheckpointPartitionKey(tag, models.PartitionKeyIndexWithoutSharding),
				models.MakeCheckpointSortKey(collectionNames[i]),
				tag,
			),
			Sequence:      sequences[i],
			Height:        heights[i],
			LastBlockTime: blockTimes[i],
		}
		entries[i] = entry
	}

	s.collectionStorage.EXPECT().GetItems(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *collection.GetItemsRequest) ([]interface{}, error) {
			require.Equal(reflect.TypeOf(models.CheckpointDDBEntry{}), request.EntryType)
			require.Equal(len(collectionNames), len(request.KeyMaps))
			return []interface{}{entries[0]}, nil
		})
	checkpoints, err := s.storage.GetCheckpoints(context.Background(), collectionNames, tag)
	require.Error(err)
	require.Nil(checkpoints)
}

func (s *checkpointStorageTestSuite) TestGetCheckpoints_ItemNotFound() {
	require := testutil.Require(s.T())

	tag := uint32(1)
	collectionNames := []api.Collection{xapi.CollectionBlocks, xapi.CollectionLogsV2}

	s.collectionStorage.EXPECT().GetItems(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *collection.GetItemsRequest) ([]interface{}, error) {
			require.Equal(reflect.TypeOf(models.CheckpointDDBEntry{}), request.EntryType)
			require.Equal(len(collectionNames), len(request.KeyMaps))
			return nil, internal.ErrItemNotFound
		})
	checkpoints, err := s.storage.GetCheckpoints(context.Background(), collectionNames, tag)
	require.Error(err)
	require.True(xerrors.Is(err, internal.ErrItemNotFound))
	require.Nil(checkpoints)
}
