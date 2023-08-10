package ethereum

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/storage/collection"
	cmocks "github.com/coinbase/chainnode/internal/storage/collection/mocks"
	"github.com/coinbase/chainnode/internal/storage/ethereum/models"
	"github.com/coinbase/chainnode/internal/utils/compression"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

type BlockStorageTestSuite struct {
	suite.Suite
	ctrl              *gomock.Controller
	collectionStorage *cmocks.MockCollectionStorage
	storage           BlockStorage
	app               testapp.TestApp
	blockTimeFixture  time.Time
}

const (
	blockTagFixture          = uint32(1)
	blockSequenceFixture     = api.Sequence(12312)
	blockHeightFixture       = uint64(123456)
	blockHashFixture         = "0xaa7d33f3dac41109eb305936ac4d09697af243bfbbb44a43049b7c1127d388d6"
	blockPartitionKeyFixture = "1#blocks#123456"
	blockSortKeyFixture      = "0000000000003018"
	blockTimeFixture         = "2020-11-24T16:07:21Z"
)

func TestBlockStorageTestSuite(t *testing.T) {
	suite.Run(t, new(BlockStorageTestSuite))
}

func (s *BlockStorageTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.collectionStorage = cmocks.NewMockCollectionStorage(s.ctrl)
	s.collectionStorage.EXPECT().
		WithCollection(ethereum.CollectionBlocks).
		Return(s.collectionStorage)
	logsStorage := cmocks.NewMockCollectionStorage(s.ctrl)

	s.app = testapp.New(
		s.T(),
		fx.Provide(newBlockStorage),
		fx.Provide(fx.Annotated{Name: "collection", Target: func() collection.CollectionStorage { return s.collectionStorage }}),
		fx.Provide(fx.Annotated{Name: "logs", Target: func() collection.CollectionStorage { return logsStorage }}),
		fx.Populate(&s.storage),
	)
	s.blockTimeFixture = testutil.MustTime("2020-11-24T16:07:21Z")
}

func (s *BlockStorageTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
}

func (s *BlockStorageTestSuite) TestPersistBlock() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqewdw")

	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	expectedEntry := &models.BlockDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockPartitionKeyFixture,
			blockSortKeyFixture,
			blockTagFixture,
		).WithData(compressed),
		Height:    blockHeightFixture,
		Hash:      blockHashFixture,
		BlockTime: blockTimeFixture,
	}

	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).Return(nil)
	s.collectionStorage.EXPECT().WriteItem(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx2 context.Context, entry interface{}) error {
			actualEntry, ok := entry.(*models.BlockDDBEntry)
			require.True(ok)
			testutil.MustTime(actualEntry.UpdatedAt)
			expectedEntry.UpdatedAt = actualEntry.UpdatedAt
			require.Equal(expectedEntry, actualEntry)
			return nil
		})
	block := ethereum.NewBlock(
		blockTagFixture,
		blockHeightFixture,
		blockHashFixture,
		blockSequenceFixture,
		s.blockTimeFixture,
		data,
	)
	err = s.storage.PersistBlock(ctx, block)
	require.NoError(err)
}

func (s *BlockStorageTestSuite) TestPersistBlock_NoBlockTime() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqewdw")

	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	expectedEntry := &models.BlockDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockPartitionKeyFixture,
			blockSortKeyFixture,
			blockTagFixture,
		).WithData(compressed),
		Height: blockHeightFixture,
		Hash:   blockHashFixture,
	}

	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).Return(nil)
	s.collectionStorage.EXPECT().WriteItem(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx2 context.Context, entry interface{}) error {
			actualEntry, ok := entry.(*models.BlockDDBEntry)
			require.True(ok)
			testutil.MustTime(actualEntry.UpdatedAt)
			expectedEntry.UpdatedAt = actualEntry.UpdatedAt
			require.Equal(expectedEntry, actualEntry)
			return nil
		})
	block := ethereum.NewBlock(
		blockTagFixture,
		blockHeightFixture,
		blockHashFixture,
		blockSequenceFixture,
		time.Time{},
		data,
	)
	err = s.storage.PersistBlock(ctx, block)
	require.NoError(err)
}

func (s *BlockStorageTestSuite) TestPersistBlock_LargeFile() {
	require := testutil.Require(s.T())

	ctx := context.Background()

	data := testutil.MakeFile(10)
	objectKey := "1/blocks/123456/12312"
	expectedEntry := &models.BlockDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockPartitionKeyFixture,
			blockSortKeyFixture,
			blockTagFixture,
		).WithObjectKey(objectKey),
		Height:    blockHeightFixture,
		Hash:      blockHashFixture,
		BlockTime: blockTimeFixture,
	}

	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).
		DoAndReturn(func(ctx context.Context, entry collection.Item, enforceUpload bool) error {
			err := entry.SetData(nil)
			require.NoError(err)
			err = entry.SetObjectKey(objectKey)
			require.NoError(err)
			return nil
		})
	s.collectionStorage.EXPECT().WriteItem(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, entry interface{}) error {
			actualEntry, ok := entry.(*models.BlockDDBEntry)
			require.True(ok)
			testutil.MustTime(actualEntry.UpdatedAt)
			expectedEntry.UpdatedAt = actualEntry.UpdatedAt
			require.Equal(expectedEntry, actualEntry)
			return nil
		})

	block := ethereum.NewBlock(
		blockTagFixture,
		blockHeightFixture,
		blockHashFixture,
		blockSequenceFixture,
		s.blockTimeFixture,
		data,
	)
	err := s.storage.PersistBlock(ctx, block)
	require.NoError(err)
}

func (s *BlockStorageTestSuite) TestPersistBlock_LargeFileUploadError() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := testutil.MakeFile(10)

	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).
		Return(xerrors.Errorf("failed to upload"))

	block := ethereum.NewBlock(
		blockTagFixture,
		blockHeightFixture,
		blockHashFixture,
		blockSequenceFixture,
		s.blockTimeFixture,
		data,
	)
	err := s.storage.PersistBlock(ctx, block)
	require.Error(err)
}

func (s *BlockStorageTestSuite) TestGetBlock() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqewdw")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)
	blockTime := testutil.MustTime(txBlockTimeFixture)

	expectedBlockEntry := &models.BlockDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockPartitionKeyFixture,
			blockSortKeyFixture,
			blockTagFixture,
		).WithData(compressed),
		Height:    blockHeightFixture,
		Hash:      blockHashFixture,
		BlockTime: blockTimeFixture,
	}
	expectedBlock := &ethereum.Block{
		Tag:       blockTagFixture,
		Height:    blockHeightFixture,
		Hash:      blockHashFixture,
		Sequence:  blockSequenceFixture,
		Data:      data,
		BlockTime: blockTime,
		UpdatedAt: testutil.MustTime(expectedBlockEntry.UpdatedAt),
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedBlockEntry

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		blockPartitionKeyFixture,
		blockSortKeyFixture,
		models.BlockDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	block, err := s.storage.GetBlock(ctx, blockTagFixture, blockHeightFixture, blockSequenceFixture)
	require.NoError(err)
	require.Equal(expectedBlock, block)
}

func (s *BlockStorageTestSuite) TestGetBlock_NoBlockTime() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqewdw")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	expectedBlockEntry := &models.BlockDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockPartitionKeyFixture,
			blockSortKeyFixture,
			blockTagFixture,
		).WithData(compressed),
		Height: blockHeightFixture,
		Hash:   blockHashFixture,
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedBlockEntry

	expectedBlock := &ethereum.Block{
		Tag:       blockTagFixture,
		Height:    blockHeightFixture,
		Hash:      blockHashFixture,
		Sequence:  blockSequenceFixture,
		Data:      data,
		UpdatedAt: testutil.MustTime(expectedBlockEntry.UpdatedAt),
	}

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		blockPartitionKeyFixture,
		blockSortKeyFixture,
		models.BlockDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	block, err := s.storage.GetBlock(ctx, blockTagFixture, blockHeightFixture, blockSequenceFixture)
	require.NoError(err)
	require.Equal(expectedBlock, block)
}

func (s *BlockStorageTestSuite) TestGetBlock_InvalidBlockTime() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqewdw")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	expectedBlockEntry := &models.BlockDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockPartitionKeyFixture,
			blockSortKeyFixture,
			blockTagFixture,
		).WithData(compressed),
		Height:    blockHeightFixture,
		Hash:      blockHashFixture,
		BlockTime: "invalid",
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedBlockEntry

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		blockPartitionKeyFixture,
		blockSortKeyFixture,
		models.BlockDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	block, err := s.storage.GetBlock(ctx, blockTagFixture, blockHeightFixture, blockSequenceFixture)
	require.Error(err)
	require.Nil(block)
}

func (s *BlockStorageTestSuite) TestGetBlock_LargeFile() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	objectKey := "s3-location"
	data := testutil.MakeFile(10)
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)
	blockTime := testutil.MustTime(txBlockTimeFixture)

	expectedBlockEntry := &models.BlockDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockPartitionKeyFixture,
			blockSortKeyFixture,
			blockTagFixture,
		).WithObjectKey(objectKey),
		Height:    blockHeightFixture,
		Hash:      blockHashFixture,
		BlockTime: blockTimeFixture,
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedBlockEntry

	expectedBlock := &ethereum.Block{
		Tag:       blockTagFixture,
		Height:    blockHeightFixture,
		Hash:      blockHashFixture,
		Sequence:  blockSequenceFixture,
		Data:      data,
		BlockTime: blockTime,
		UpdatedAt: testutil.MustTime(expectedBlockEntry.UpdatedAt),
	}

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		blockPartitionKeyFixture,
		blockSortKeyFixture,
		models.BlockDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	s.collectionStorage.EXPECT().
		DownloadFromBlobStorage(gomock.Any(), expectedBlockEntry).
		DoAndReturn(func(ctx context.Context, entry collection.Item) error {
			err := entry.SetData(compressed)
			require.NoError(err)
			return nil
		})

	block, err := s.storage.GetBlock(ctx, blockTagFixture, blockHeightFixture, blockSequenceFixture)
	require.NoError(err)
	require.Equal(expectedBlock, block)
}

func (s *BlockStorageTestSuite) TestGetBlock_LargeFileDownloadFailure() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	objectKey := "s3-location"

	expectedBlockEntry := &models.BlockDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockPartitionKeyFixture,
			blockSortKeyFixture,
			blockTagFixture,
		).WithObjectKey(objectKey),
		Height:    blockHeightFixture,
		Hash:      blockHashFixture,
		BlockTime: blockTimeFixture,
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedBlockEntry
	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		blockPartitionKeyFixture,
		blockSortKeyFixture,
		models.BlockDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	s.collectionStorage.EXPECT().DownloadFromBlobStorage(gomock.Any(), expectedBlockEntry).
		Return(xerrors.Errorf("failed to download"))

	block, err := s.storage.GetBlock(ctx, blockTagFixture, blockHeightFixture, blockSequenceFixture)
	require.Error(err)
	require.Nil(block)
}
