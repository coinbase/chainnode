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

type BlockExtraDataByNumberStorageTestSuite struct {
	suite.Suite
	ctrl              *gomock.Controller
	collectionStorage *cmocks.MockCollectionStorage
	storage           BlockExtraDataByNumberStorage
	app               testapp.TestApp
	blockTimeFixture  time.Time
}

const (
	blockExtraDataByNumberTagFixture          = uint32(1)
	blockExtraDataByNumberSequenceFixture     = api.Sequence(12312)
	blockExtraDataByNumberHeightFixture       = uint64(123456)
	blockExtraDataByNumberHashFixture         = "0xaa7d33f3dac41109eb305936ac4d09697af243bfbbb44a43049b7c1127d388d6"
	blockExtraDataByNumberPartitionKeyFixture = "1#blocks-extra-data-by-number#123456"
	blockExtraDataByNumberSortKeyFixture      = "0000000000003018"
)

func TestBlockExtraDataByNumberStorageTestSuite(t *testing.T) {
	suite.Run(t, new(BlockExtraDataByNumberStorageTestSuite))
}

func (s *BlockExtraDataByNumberStorageTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.collectionStorage = cmocks.NewMockCollectionStorage(s.ctrl)
	s.collectionStorage.EXPECT().
		WithCollection(ethereum.CollectionBlocksExtraDataByNumber).
		Return(s.collectionStorage)
	logsStorage := cmocks.NewMockCollectionStorage(s.ctrl)

	s.app = testapp.New(
		s.T(),
		fx.Provide(newBlockExtraDataByNumberStorage),
		fx.Provide(fx.Annotated{Name: "collection", Target: func() collection.CollectionStorage { return s.collectionStorage }}),
		fx.Provide(fx.Annotated{Name: "logs", Target: func() collection.CollectionStorage { return logsStorage }}),
		fx.Populate(&s.storage),
	)
	s.blockTimeFixture = testutil.MustTime("2020-11-24T16:07:21Z")
}

func (s *BlockExtraDataByNumberStorageTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
}

func (s *BlockExtraDataByNumberStorageTestSuite) TestPersist() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqewdw")

	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	expectedEntry := &models.BlockExtraDataByNumberDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockExtraDataByNumberPartitionKeyFixture,
			blockExtraDataByNumberSortKeyFixture,
			blockExtraDataByNumberTagFixture,
		).WithData(compressed),
		Height:    blockExtraDataByNumberHeightFixture,
		Hash:      blockExtraDataByNumberHashFixture,
		BlockTime: blockTimeFixture,
	}

	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).Return(nil)
	s.collectionStorage.EXPECT().WriteItem(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx2 context.Context, entry interface{}) error {
			actualEntry, ok := entry.(*models.BlockExtraDataByNumberDDBEntry)
			require.True(ok)
			testutil.MustTime(actualEntry.UpdatedAt)
			expectedEntry.UpdatedAt = actualEntry.UpdatedAt
			require.Equal(expectedEntry, actualEntry)
			return nil
		})
	block := ethereum.NewBlockExtraData(
		blockExtraDataByNumberTagFixture,
		blockExtraDataByNumberHeightFixture,
		blockExtraDataByNumberHashFixture,
		blockExtraDataByNumberSequenceFixture,
		s.blockTimeFixture,
		data,
	)
	err = s.storage.PersistBlockExtraDataByNumber(ctx, block)
	require.NoError(err)
}

func (s *BlockExtraDataByNumberStorageTestSuite) TestPersist_NoBlockTime() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqewdw")

	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	expectedEntry := &models.BlockExtraDataByNumberDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockExtraDataByNumberPartitionKeyFixture,
			blockExtraDataByNumberSortKeyFixture,
			blockExtraDataByNumberTagFixture,
		).WithData(compressed),
		Height: blockExtraDataByNumberHeightFixture,
		Hash:   blockExtraDataByNumberHashFixture,
	}

	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).Return(nil)
	s.collectionStorage.EXPECT().WriteItem(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx2 context.Context, entry interface{}) error {
			actualEntry, ok := entry.(*models.BlockExtraDataByNumberDDBEntry)
			require.True(ok)
			testutil.MustTime(actualEntry.UpdatedAt)
			expectedEntry.UpdatedAt = actualEntry.UpdatedAt
			require.Equal(expectedEntry, actualEntry)
			return nil
		})
	block := ethereum.NewBlockExtraData(
		blockExtraDataByNumberTagFixture,
		blockExtraDataByNumberHeightFixture,
		blockExtraDataByNumberHashFixture,
		blockExtraDataByNumberSequenceFixture,
		time.Time{},
		data,
	)
	err = s.storage.PersistBlockExtraDataByNumber(ctx, block)
	require.NoError(err)
}

func (s *BlockExtraDataByNumberStorageTestSuite) TestPersist_LargeFile() {
	require := testutil.Require(s.T())

	ctx := context.Background()

	data := testutil.MakeFile(10)
	objectKey := "1/blocks-extra-data-by-number/123456/12312"
	expectedEntry := &models.BlockExtraDataByNumberDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockExtraDataByNumberPartitionKeyFixture,
			blockExtraDataByNumberSortKeyFixture,
			blockExtraDataByNumberTagFixture,
		).WithObjectKey(objectKey),
		Height:    blockExtraDataByNumberHeightFixture,
		Hash:      blockExtraDataByNumberHashFixture,
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
			actualEntry, ok := entry.(*models.BlockExtraDataByNumberDDBEntry)
			require.True(ok)
			testutil.MustTime(actualEntry.UpdatedAt)
			expectedEntry.UpdatedAt = actualEntry.UpdatedAt
			require.Equal(expectedEntry, actualEntry)
			return nil
		})

	block := ethereum.NewBlockExtraData(
		blockExtraDataByNumberTagFixture,
		blockExtraDataByNumberHeightFixture,
		blockExtraDataByNumberHashFixture,
		blockExtraDataByNumberSequenceFixture,
		s.blockTimeFixture,
		data,
	)
	err := s.storage.PersistBlockExtraDataByNumber(ctx, block)
	require.NoError(err)
}

func (s *BlockExtraDataByNumberStorageTestSuite) TestPersist_LargeFileUploadError() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := testutil.MakeFile(10)

	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).
		Return(xerrors.Errorf("failed to upload"))

	block := ethereum.NewBlockExtraData(
		blockExtraDataByNumberTagFixture,
		blockExtraDataByNumberHeightFixture,
		blockExtraDataByNumberHashFixture,
		blockExtraDataByNumberSequenceFixture,
		s.blockTimeFixture,
		data,
	)
	err := s.storage.PersistBlockExtraDataByNumber(ctx, block)
	require.Error(err)
}

func (s *BlockExtraDataByNumberStorageTestSuite) TestGet() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqewdw")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)
	blockTime := testutil.MustTime(txBlockTimeFixture)

	expectedBlockEntry := &models.BlockExtraDataByNumberDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockExtraDataByNumberPartitionKeyFixture,
			blockExtraDataByNumberSortKeyFixture,
			blockExtraDataByNumberTagFixture,
		).WithData(compressed),
		Height:    blockExtraDataByNumberHeightFixture,
		Hash:      blockExtraDataByNumberHashFixture,
		BlockTime: blockTimeFixture,
	}
	expectedBlock := &ethereum.BlockExtraData{
		Tag:       blockExtraDataByNumberTagFixture,
		Height:    blockExtraDataByNumberHeightFixture,
		Hash:      blockExtraDataByNumberHashFixture,
		Sequence:  blockExtraDataByNumberSequenceFixture,
		Data:      data,
		BlockTime: blockTime,
		UpdatedAt: testutil.MustTime(expectedBlockEntry.UpdatedAt),
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedBlockEntry

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		blockExtraDataByNumberPartitionKeyFixture,
		blockExtraDataByNumberSortKeyFixture,
		models.BlockExtraDataByNumberDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	block, err := s.storage.GetBlockExtraDataByNumber(ctx, blockExtraDataByNumberTagFixture, blockExtraDataByNumberHeightFixture, blockExtraDataByNumberSequenceFixture)
	require.NoError(err)
	require.Equal(expectedBlock, block)
}

func (s *BlockExtraDataByNumberStorageTestSuite) TestGet_NoBlockTime() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqewdw")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	expectedBlockEntry := &models.BlockExtraDataByNumberDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockExtraDataByNumberPartitionKeyFixture,
			blockExtraDataByNumberSortKeyFixture,
			blockExtraDataByNumberTagFixture,
		).WithData(compressed),
		Height: blockExtraDataByNumberHeightFixture,
		Hash:   blockExtraDataByNumberHashFixture,
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedBlockEntry

	expectedBlock := &ethereum.BlockExtraData{
		Tag:       blockExtraDataByNumberTagFixture,
		Height:    blockExtraDataByNumberHeightFixture,
		Hash:      blockExtraDataByNumberHashFixture,
		Sequence:  blockExtraDataByNumberSequenceFixture,
		Data:      data,
		UpdatedAt: testutil.MustTime(expectedBlockEntry.UpdatedAt),
	}

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		blockExtraDataByNumberPartitionKeyFixture,
		blockExtraDataByNumberSortKeyFixture,
		models.BlockExtraDataByNumberDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	block, err := s.storage.GetBlockExtraDataByNumber(ctx, blockExtraDataByNumberTagFixture, blockExtraDataByNumberHeightFixture, blockExtraDataByNumberSequenceFixture)
	require.NoError(err)
	require.Equal(expectedBlock, block)
}

func (s *BlockExtraDataByNumberStorageTestSuite) TestGet_InvalidBlockTime() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqewdw")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	expectedBlockEntry := &models.BlockExtraDataByNumberDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockExtraDataByNumberPartitionKeyFixture,
			blockExtraDataByNumberSortKeyFixture,
			blockExtraDataByNumberTagFixture,
		).WithData(compressed),
		Height:    blockExtraDataByNumberHeightFixture,
		Hash:      blockExtraDataByNumberHashFixture,
		BlockTime: "invalid",
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedBlockEntry

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		blockExtraDataByNumberPartitionKeyFixture,
		blockExtraDataByNumberSortKeyFixture,
		models.BlockExtraDataByNumberDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	block, err := s.storage.GetBlockExtraDataByNumber(ctx, blockExtraDataByNumberTagFixture, blockExtraDataByNumberHeightFixture, blockExtraDataByNumberSequenceFixture)
	require.Error(err)
	require.Nil(block)
}

func (s *BlockExtraDataByNumberStorageTestSuite) TestGet_LargeFile() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	objectKey := "s3-location"
	data := testutil.MakeFile(10)
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)
	blockTime := testutil.MustTime(txBlockTimeFixture)

	expectedBlockEntry := &models.BlockExtraDataByNumberDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockExtraDataByNumberPartitionKeyFixture,
			blockExtraDataByNumberSortKeyFixture,
			blockExtraDataByNumberTagFixture,
		).WithObjectKey(objectKey),
		Height:    blockExtraDataByNumberHeightFixture,
		Hash:      blockExtraDataByNumberHashFixture,
		BlockTime: blockTimeFixture,
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedBlockEntry

	expectedBlock := &ethereum.BlockExtraData{
		Tag:       blockExtraDataByNumberTagFixture,
		Height:    blockExtraDataByNumberHeightFixture,
		Hash:      blockExtraDataByNumberHashFixture,
		Sequence:  blockExtraDataByNumberSequenceFixture,
		Data:      data,
		BlockTime: blockTime,
		UpdatedAt: testutil.MustTime(expectedBlockEntry.UpdatedAt),
	}

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		blockExtraDataByNumberPartitionKeyFixture,
		blockExtraDataByNumberSortKeyFixture,
		models.BlockExtraDataByNumberDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	s.collectionStorage.EXPECT().
		DownloadFromBlobStorage(gomock.Any(), expectedBlockEntry).
		DoAndReturn(func(ctx context.Context, entry collection.Item) error {
			err := entry.SetData(compressed)
			require.NoError(err)
			return nil
		})

	block, err := s.storage.GetBlockExtraDataByNumber(ctx, blockExtraDataByNumberTagFixture, blockExtraDataByNumberHeightFixture, blockExtraDataByNumberSequenceFixture)
	require.NoError(err)
	require.Equal(expectedBlock, block)
}

func (s *BlockExtraDataByNumberStorageTestSuite) TestGet_LargeFileDownloadFailure() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	objectKey := "s3-location"

	expectedBlockEntry := &models.BlockExtraDataByNumberDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockExtraDataByNumberPartitionKeyFixture,
			blockExtraDataByNumberSortKeyFixture,
			blockExtraDataByNumberTagFixture,
		).WithObjectKey(objectKey),
		Height:    blockExtraDataByNumberHeightFixture,
		Hash:      blockExtraDataByNumberHashFixture,
		BlockTime: blockTimeFixture,
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedBlockEntry
	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		blockExtraDataByNumberPartitionKeyFixture,
		blockExtraDataByNumberSortKeyFixture,
		models.BlockExtraDataByNumberDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	s.collectionStorage.EXPECT().DownloadFromBlobStorage(gomock.Any(), expectedBlockEntry).
		Return(xerrors.Errorf("failed to download"))

	block, err := s.storage.GetBlockExtraDataByNumber(ctx, blockExtraDataByNumberTagFixture, blockExtraDataByNumberHeightFixture, blockExtraDataByNumberSequenceFixture)
	require.Error(err)
	require.Nil(block)
}
