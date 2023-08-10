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

type BlockStorageByHashTestSuite struct {
	suite.Suite
	ctrl              *gomock.Controller
	collectionStorage *cmocks.MockCollectionStorage
	storage           BlockByHashStorage
	app               testapp.TestApp
	blockTimeFixture  time.Time
}

const (
	blockByHashTagFixture          = uint32(1)
	blockByHashSequenceFixture     = api.Sequence(12312)
	blockByHashHeightFixture       = uint64(123456)
	blockByHashHashFixture         = "0xaa7d33f"
	blockByHashPartitionKeyFixture = "1#blocks-by-hash#0xaa7d33f"
	blockByHashSortKeyFixture      = "0000000000003018"
	blockByHashBlockTimeFixture    = "2020-11-24T16:07:21Z"
)

func TestBlockByHashStorageTestSuite(t *testing.T) {
	suite.Run(t, new(BlockStorageByHashTestSuite))
}

func (s *BlockStorageByHashTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.collectionStorage = cmocks.NewMockCollectionStorage(s.ctrl)
	s.collectionStorage.EXPECT().
		WithCollection(ethereum.CollectionBlocksByHash).
		Return(s.collectionStorage)
	logsStorage := cmocks.NewMockCollectionStorage(s.ctrl)

	s.app = testapp.New(
		s.T(),
		fx.Provide(newBlockByHashStorage),
		fx.Provide(fx.Annotated{Name: "collection", Target: func() collection.CollectionStorage { return s.collectionStorage }}),
		fx.Provide(fx.Annotated{Name: "logs", Target: func() collection.CollectionStorage { return logsStorage }}),
		fx.Populate(&s.storage),
	)
	s.blockTimeFixture = testutil.MustTime(blockByHashBlockTimeFixture)
}

func (s *BlockStorageByHashTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
}

func (s *BlockStorageByHashTestSuite) TestPersistBlockByHash() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqewdw")

	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	expectedEntry := &models.BlockByHashDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockByHashPartitionKeyFixture,
			blockByHashSortKeyFixture,
			blockByHashTagFixture,
		).WithData(compressed),
		Height:    blockByHashHeightFixture,
		Hash:      blockByHashHashFixture,
		BlockTime: blockByHashBlockTimeFixture,
	}

	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).Return(nil)
	s.collectionStorage.EXPECT().WriteItem(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx2 context.Context, entry interface{}) error {
			actualEntry, ok := entry.(*models.BlockByHashDDBEntry)
			require.True(ok)
			testutil.MustTime(actualEntry.UpdatedAt)
			expectedEntry.UpdatedAt = actualEntry.UpdatedAt
			require.Equal(expectedEntry, actualEntry)
			return nil
		})
	block := ethereum.NewBlock(
		blockByHashTagFixture,
		blockByHashHeightFixture,
		blockByHashHashFixture,
		blockByHashSequenceFixture,
		s.blockTimeFixture,
		data,
	)
	err = s.storage.PersistBlockByHash(ctx, block)
	require.NoError(err)
}

func (s *BlockStorageByHashTestSuite) TestPersistBlockByHash_NoBlockTime() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqewdw")

	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	expectedEntry := &models.BlockByHashDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockByHashPartitionKeyFixture,
			blockByHashSortKeyFixture,
			blockByHashTagFixture,
		).WithData(compressed),
		Height: blockByHashHeightFixture,
		Hash:   blockByHashHashFixture,
	}

	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).Return(nil)
	s.collectionStorage.EXPECT().WriteItem(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx2 context.Context, entry interface{}) error {
			actualEntry, ok := entry.(*models.BlockByHashDDBEntry)
			require.True(ok)
			testutil.MustTime(actualEntry.UpdatedAt)
			expectedEntry.UpdatedAt = actualEntry.UpdatedAt
			require.Equal(expectedEntry, actualEntry)
			return nil
		})
	block := ethereum.NewBlock(
		blockByHashTagFixture,
		blockByHashHeightFixture,
		blockByHashHashFixture,
		blockByHashSequenceFixture,
		time.Time{},
		data,
	)
	err = s.storage.PersistBlockByHash(ctx, block)
	require.NoError(err)
}

func (s *BlockStorageByHashTestSuite) TestPersistBlockByHash_LargeFile() {
	require := testutil.Require(s.T())

	ctx := context.Background()

	data := testutil.MakeFile(10)
	objectKey := "s3-location"
	expectedEntry := &models.BlockByHashDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockByHashPartitionKeyFixture,
			blockByHashSortKeyFixture,
			blockByHashTagFixture,
		).WithObjectKey(objectKey),
		Height:    blockByHashHeightFixture,
		Hash:      blockByHashHashFixture,
		BlockTime: blockByHashBlockTimeFixture,
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
			actualEntry, ok := entry.(*models.BlockByHashDDBEntry)
			require.True(ok)
			testutil.MustTime(actualEntry.UpdatedAt)
			expectedEntry.UpdatedAt = actualEntry.UpdatedAt
			require.Equal(expectedEntry, actualEntry)
			return nil
		})

	block := ethereum.NewBlock(
		blockByHashTagFixture,
		blockByHashHeightFixture,
		blockByHashHashFixture,
		blockByHashSequenceFixture,
		s.blockTimeFixture,
		data,
	)
	err := s.storage.PersistBlockByHash(ctx, block)
	require.NoError(err)
}

func (s *BlockStorageByHashTestSuite) TestPersistBlockByHash_LargeFileUploadError() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := testutil.MakeFile(10)

	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).
		Return(xerrors.Errorf("failed to upload"))

	block := ethereum.NewBlock(
		blockByHashTagFixture,
		blockByHashHeightFixture,
		blockByHashHashFixture,
		blockByHashSequenceFixture,
		s.blockTimeFixture,
		data,
	)
	err := s.storage.PersistBlockByHash(ctx, block)
	require.Error(err)
}

func (s *BlockStorageByHashTestSuite) TestGetBlockByHash() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqewdw")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)
	blockTime := testutil.MustTime(txBlockTimeFixture)

	expectedBlockEntry := &models.BlockByHashDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockByHashPartitionKeyFixture,
			blockByHashSortKeyFixture,
			blockByHashTagFixture,
		).WithData(compressed),
		Height:    blockByHashHeightFixture,
		Hash:      blockByHashHashFixture,
		BlockTime: blockByHashBlockTimeFixture,
	}
	expectedBlock := &ethereum.Block{
		Tag:       blockByHashTagFixture,
		Height:    blockByHashHeightFixture,
		Hash:      blockByHashHashFixture,
		Sequence:  blockByHashSequenceFixture,
		Data:      data,
		BlockTime: blockTime,
		UpdatedAt: testutil.MustTime(expectedBlockEntry.UpdatedAt),
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedBlockEntry

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		blockByHashPartitionKeyFixture,
		blockByHashSortKeyFixture,
		models.BlockByHashDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	block, err := s.storage.GetBlockByHash(ctx, blockByHashTagFixture, blockByHashHashFixture, blockByHashSequenceFixture)
	require.NoError(err)
	require.Equal(expectedBlock, block)
}

func (s *BlockStorageByHashTestSuite) TestGetBlockByHash_NoBlockTime() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqewdw")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	expectedBlockEntry := &models.BlockByHashDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockByHashPartitionKeyFixture,
			blockByHashSortKeyFixture,
			blockByHashTagFixture,
		).WithData(compressed),
		Height: blockByHashHeightFixture,
		Hash:   blockByHashHashFixture,
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedBlockEntry

	expectedBlock := &ethereum.Block{
		Tag:       blockByHashTagFixture,
		Height:    blockByHashHeightFixture,
		Hash:      blockByHashHashFixture,
		Sequence:  blockByHashSequenceFixture,
		Data:      data,
		UpdatedAt: testutil.MustTime(expectedBlockEntry.UpdatedAt),
	}

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		blockByHashPartitionKeyFixture,
		blockByHashSortKeyFixture,
		models.BlockByHashDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	block, err := s.storage.GetBlockByHash(ctx, blockByHashTagFixture, blockByHashHashFixture, blockByHashSequenceFixture)
	require.NoError(err)
	require.Equal(expectedBlock, block)
}

func (s *BlockStorageByHashTestSuite) TestGetBlockByHash_InvalidBlockTime() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqewdw")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	expectedBlockEntry := &models.BlockByHashDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockByHashPartitionKeyFixture,
			blockByHashSortKeyFixture,
			blockByHashTagFixture,
		).WithData(compressed),
		Height:    blockByHashHeightFixture,
		Hash:      blockByHashHashFixture,
		BlockTime: "invalid",
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedBlockEntry

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		blockByHashPartitionKeyFixture,
		blockByHashSortKeyFixture,
		models.BlockByHashDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	block, err := s.storage.GetBlockByHash(ctx, blockByHashTagFixture, blockByHashHashFixture, blockByHashSequenceFixture)
	require.Error(err)
	require.Nil(block)
}

func (s *BlockStorageByHashTestSuite) TestGetBlockByHash_LargeFile() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	objectKey := "s3-location"
	data := testutil.MakeFile(10)
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)
	blockTime := testutil.MustTime(txBlockTimeFixture)

	expectedBlockEntry := &models.BlockByHashDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockByHashPartitionKeyFixture,
			blockByHashSortKeyFixture,
			blockByHashTagFixture,
		).WithObjectKey(objectKey),
		Height:    blockByHashHeightFixture,
		Hash:      blockByHashHashFixture,
		BlockTime: blockByHashBlockTimeFixture,
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedBlockEntry

	expectedBlock := &ethereum.Block{
		Tag:       blockByHashTagFixture,
		Height:    blockByHashHeightFixture,
		Hash:      blockByHashHashFixture,
		Sequence:  blockByHashSequenceFixture,
		Data:      data,
		BlockTime: blockTime,
		UpdatedAt: testutil.MustTime(expectedBlockEntry.UpdatedAt),
	}

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		blockByHashPartitionKeyFixture,
		blockByHashSortKeyFixture,
		models.BlockByHashDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	s.collectionStorage.EXPECT().
		DownloadFromBlobStorage(gomock.Any(), expectedBlockEntry).
		DoAndReturn(func(ctx context.Context, entry collection.Item) error {
			err := entry.SetData(compressed)
			require.NoError(err)
			return nil
		})

	block, err := s.storage.GetBlockByHash(ctx, blockByHashTagFixture, blockByHashHashFixture, blockByHashSequenceFixture)
	require.NoError(err)
	require.Equal(expectedBlock, block)
}

func (s *BlockStorageByHashTestSuite) TestGetBlockByHash_LargeFileDownloadFailure() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	objectKey := "s3-location"

	expectedBlockEntry := &models.BlockByHashDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockByHashPartitionKeyFixture,
			blockByHashSortKeyFixture,
			blockByHashTagFixture,
		).WithObjectKey(objectKey),
		Height:    blockByHashHeightFixture,
		Hash:      blockByHashHashFixture,
		BlockTime: blockByHashBlockTimeFixture,
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedBlockEntry
	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		blockByHashPartitionKeyFixture,
		blockByHashSortKeyFixture,
		models.BlockByHashDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	s.collectionStorage.EXPECT().DownloadFromBlobStorage(gomock.Any(), expectedBlockEntry).
		Return(xerrors.Errorf("failed to download"))

	block, err := s.storage.GetBlockByHash(ctx, blockByHashTagFixture, blockByHashHashFixture, blockByHashSequenceFixture)
	require.Error(err)
	require.Nil(block)
}
